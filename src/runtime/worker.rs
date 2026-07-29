use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, mpsc};
use tokio::task::LocalSet;

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
    shutdown_rx: broadcast::Receiver<()>,
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
                        shutdown_rx,
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
    request_rx: mpsc::Receiver<ShardRequest>,
    conn_count: Arc<AtomicUsize>,
    hash_seed: u64,
    info: Arc<ServerInfo>,
    shutdown_rx: broadcast::Receiver<()>,
) {
    let range = shard_map.shards_for_worker(worker_id);
    let mut shards = Vec::new();
    for id in range.clone() {
        shards.push(Shard::new(
            id,
            hash_seed.wrapping_add(id as u64),
            info.shard_stats(id),
        ));
    }

    let config_rc = Rc::new((*config).clone());
    let local_shards = Rc::new(RefCell::new(shards));
    let now_ms = Rc::new(RefCell::new(current_ms()));
    let info_rc = Rc::new((*info).clone());

    let ctx = WorkerContext {
        worker_id,
        config: config_rc,
        shard_map,
        shard_client,
        local_shards,
        info: info_rc,
        conn_count,
        now_ms,
    };

    // Shard apply + expire are folded into the mio reactor; LocalSet remains for
    // multi-gather `spawn_local` in the dispatcher.
    listener::accept_loop(ctx, request_rx, range, shutdown_rx).await;
}

pub fn current_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
