use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::task::LocalSet;

use crate::command::apply;
use crate::config::Config;
use crate::net::listener;
use crate::runtime::router::{ShardClient, ShardMap, ShardRequest};
use crate::storage::shard::Shard;
use crate::telemetry::ServerInfo;

pub struct WorkerContext {
    pub worker_id: usize,
    pub config: Rc<Config>,
    pub shard_map: Arc<ShardMap>,
    pub shard_client: ShardClient,
    pub local_shards: Rc<RefCell<Vec<Shard>>>,
    pub info: Rc<ServerInfo>,
    pub conn_count: Arc<AtomicUsize>,
    pub now_ms: Rc<RefCell<u64>>,
}

pub fn spawn_worker(
    worker_id: usize,
    config: Arc<Config>,
    shard_map: Arc<ShardMap>,
    shard_client: ShardClient,
    request_rx: mpsc::Receiver<ShardRequest>,
    conn_count: Arc<AtomicUsize>,
    hash_seed: u64,
    info: Arc<ServerInfo>,
) -> std::thread::JoinHandle<()> {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    let pin_core = config
        .pin_cores
        .then(|| cores.get(worker_id % cores.len()).copied())
        .flatten();
    let cfg = config.clone();

    std::thread::spawn(move || {
        if let Some(core) = pin_core {
            core_affinity::set_for_current(core);
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("worker runtime");

        rt.block_on(async {
            let local = LocalSet::new();
            local
                .run_until(async {
                    run_worker(
                        worker_id,
                        cfg,
                        shard_map,
                        shard_client,
                        request_rx,
                        conn_count,
                        hash_seed,
                        info,
                    )
                    .await;
                })
                .await;
        });
    })
}

async fn run_worker(
    worker_id: usize,
    config: Arc<Config>,
    shard_map: Arc<ShardMap>,
    shard_client: ShardClient,
    mut request_rx: mpsc::Receiver<ShardRequest>,
    conn_count: Arc<AtomicUsize>,
    hash_seed: u64,
    info: Arc<ServerInfo>,
) {
    let range = shard_map.shards_for_worker(worker_id);
    let mut shards = Vec::new();
    for id in range.clone() {
        shards.push(Shard::new(id, hash_seed.wrapping_add(id as u64)));
    }

    let config_rc = Rc::new((*config).clone());
    let local_shards = Rc::new(RefCell::new(shards));
    let now_ms = Rc::new(RefCell::new(current_ms()));
    let info_rc = Rc::new((*info).clone());

    let ctx = WorkerContext {
        worker_id,
        config: config_rc.clone(),
        shard_map: shard_map.clone(),
        shard_client: shard_client.clone(),
        local_shards: local_shards.clone(),
        info: info_rc.clone(),
        conn_count: conn_count.clone(),
        now_ms: now_ms.clone(),
    };

    tokio::task::spawn_local(executor_loop(
        request_rx,
        local_shards.clone(),
        config_rc.clone(),
        info_rc.clone(),
        now_ms.clone(),
        range,
    ));

    tokio::task::spawn_local(expire_ticker(
        local_shards.clone(),
        config_rc.clone(),
        now_ms.clone(),
    ));

    listener::accept_loop(ctx).await;
}

async fn executor_loop(
    mut rx: mpsc::Receiver<ShardRequest>,
    shards: Rc<RefCell<Vec<Shard>>>,
    config: Rc<Config>,
    info: Rc<ServerInfo>,
    now_ms: Rc<RefCell<u64>>,
    range: std::ops::Range<usize>,
) {
    while let Some(req) = rx.recv().await {
        let mut batch = vec![req];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }
        let now = *now_ms.borrow();
        let mut guard = shards.borrow_mut();
        for req in batch {
            let local_idx = req.shard_id.saturating_sub(range.start);
            let len = guard.len();
            let shard = &mut guard[local_idx.min(len.saturating_sub(1))];
            let reply = apply::apply(shard, req.cmd, now, &config, &info);
            let _ = req.reply.send(reply);
        }
    }
}

fn req_local_shard(cmd: &crate::command::Command, range: &std::ops::Range<usize>) -> usize {
    let _ = cmd;
    range.start.min(range.end.saturating_sub(1))
}

async fn expire_ticker(
    shards: Rc<RefCell<Vec<Shard>>>,
    config: Rc<Config>,
    now_ms: Rc<RefCell<u64>>,
) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_millis(config.expire_interval_ms));
    loop {
        interval.tick().await;
        *now_ms.borrow_mut() = current_ms();
        let now = *now_ms.borrow();
        let mut guard = shards.borrow_mut();
        for shard in guard.iter_mut() {
            shard.active_expire(now, config.expire_budget);
        }
    }
}

pub fn current_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}