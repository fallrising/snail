use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rand::Rng;
use tokio::sync::{broadcast, mpsc};

use crate::config::Config;
use crate::error::ServerError;
use crate::runtime::router::{ShardClient, ShardMap};
use crate::runtime::shutdown::ShutdownHandle;
use crate::runtime::worker;
use crate::telemetry::ServerInfo;

pub struct BootstrapHandle {
    workers: Vec<std::thread::JoinHandle<()>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl BootstrapHandle {
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.shutdown_tx.send(());
        for w in self.workers {
            let _ = w.join();
        }
        Ok(())
    }
}

pub async fn start(config: Arc<Config>) -> Result<ShutdownHandle, ServerError> {
    let hash_seed: u64 = rand::thread_rng().gen();
    let shard_map = Arc::new(ShardMap::new(config.shards, config.workers, hash_seed));
    let conn_count = Arc::new(AtomicUsize::new(0));
    let info = Arc::new(ServerInfo::new(&config, conn_count.clone()));

    let (shutdown_tx, _) = broadcast::channel(1);

    let mut senders = Vec::with_capacity(config.workers);
    let mut receivers = Vec::with_capacity(config.workers);

    for _ in 0..config.workers {
        let (tx, rx) = mpsc::channel(config.channel_cap);
        senders.push(tx);
        receivers.push(rx);
    }

    let senders_arc = Arc::new(senders.clone());
    let shard_client = ShardClient::new(senders_arc.clone(), shard_map.clone());

    let mut workers = Vec::new();
    for worker_id in 0..config.workers {
        let rx = receivers.remove(0);
        let handle = worker::spawn_worker(
            worker_id,
            config.clone(),
            shard_map.clone(),
            shard_client.clone(),
            rx,
            conn_count.clone(),
            hash_seed,
            info.clone(),
        );
        workers.push(handle);
    }

    Ok(ShutdownHandle {
        workers,
        shutdown_tx,
        conn_count,
    })
}
