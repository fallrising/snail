use std::sync::{Arc, Mutex};

use ahash::RandomState;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::command::Command;
use crate::protocol::frame::Reply;

pub type ShardId = usize;
pub type WorkerId = usize;

#[derive(Debug)]
pub enum CtrlRequest {
    Shutdown,
    Info,
    DbSize,
    Flush,
}

#[derive(Debug)]
pub struct ShardRequest {
    pub shard_id: ShardId,
    pub cmd: Command,
    pub reply: oneshot::Sender<Reply>,
}

#[derive(Clone)]
pub struct ShardMap {
    num_shards: usize,
    num_workers: usize,
    hasher: RandomState,
}

impl ShardMap {
    pub fn new(num_shards: usize, num_workers: usize, seed: u64) -> Self {
        let hasher = RandomState::with_seeds(
            seed,
            seed.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            seed.wrapping_mul(0xBF58_476D_1CE4_E5B9),
            seed.wrapping_mul(0x94D0_49BB_1331_11EB),
        );
        Self {
            num_shards,
            num_workers,
            hasher,
        }
    }

    pub fn shard_of(&self, key: &Bytes) -> ShardId {
        use std::hash::{BuildHasher, Hash, Hasher};
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        (h.finish() as usize) % self.num_shards
    }

    pub fn owner_of(&self, shard_id: ShardId) -> WorkerId {
        let shards_per_worker = self.num_shards / self.num_workers;
        shard_id / shards_per_worker
    }

    pub fn local_shard_index(&self, worker_id: WorkerId) -> ShardId {
        let shards_per_worker = self.num_shards / self.num_workers;
        worker_id * shards_per_worker
    }

    pub fn shards_for_worker(&self, worker_id: WorkerId) -> std::ops::Range<ShardId> {
        let shards_per_worker = self.num_shards / self.num_workers;
        let start = worker_id * shards_per_worker;
        start..start + shards_per_worker
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
}

#[derive(Clone)]
pub struct ShardClient {
    senders: Arc<Vec<mpsc::Sender<ShardRequest>>>,
    shard_map: Arc<ShardMap>,
    /// Per-worker mio wakers — set once each reactor starts.
    wakers: Arc<Mutex<Vec<Option<Arc<mio::Waker>>>>>,
}

impl ShardClient {
    pub fn new(senders: Arc<Vec<mpsc::Sender<ShardRequest>>>, shard_map: Arc<ShardMap>) -> Self {
        let n = senders.len();
        Self {
            senders,
            shard_map,
            wakers: Arc::new(Mutex::new(vec![None; n])),
        }
    }

    pub fn register_waker(&self, worker_id: usize, waker: Arc<mio::Waker>) {
        if let Ok(mut guard) = self.wakers.lock() {
            if worker_id < guard.len() {
                guard[worker_id] = Some(waker);
            }
        }
    }

    pub fn send_to(&self, shard_id: ShardId, cmd: Command) -> oneshot::Receiver<Reply> {
        let worker = self.shard_map.owner_of(shard_id);
        let (tx, rx) = oneshot::channel();
        let req = ShardRequest {
            shard_id,
            cmd,
            reply: tx,
        };
        let sender = &self.senders[worker];
        let _ = sender.try_send(req);
        if let Ok(guard) = self.wakers.lock() {
            if let Some(Some(w)) = guard.get(worker) {
                let _ = w.wake();
            }
        }
        rx
    }
}
