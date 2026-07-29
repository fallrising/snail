use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

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

    pub fn num_workers(&self) -> usize {
        self.num_workers
    }
}

#[derive(Clone)]
pub struct ShardClient {
    senders: Arc<Vec<mpsc::Sender<ShardRequest>>>,
    shard_map: Arc<ShardMap>,
    /// Per-worker mio wakers — set once each reactor starts (lock-free reads).
    wakers: Arc<Vec<OnceLock<Arc<mio::Waker>>>>,
    /// Coalesce wakes: only the first enqueue since last drain calls `wake()`.
    wake_pending: Arc<Vec<AtomicBool>>,
}

impl ShardClient {
    pub fn new(senders: Arc<Vec<mpsc::Sender<ShardRequest>>>, shard_map: Arc<ShardMap>) -> Self {
        let n = senders.len();
        let mut wakers = Vec::with_capacity(n);
        let mut wake_pending = Vec::with_capacity(n);
        for _ in 0..n {
            wakers.push(OnceLock::new());
            wake_pending.push(AtomicBool::new(false));
        }
        Self {
            senders,
            shard_map,
            wakers: Arc::new(wakers),
            wake_pending: Arc::new(wake_pending),
        }
    }

    pub fn register_waker(&self, worker_id: usize, waker: Arc<mio::Waker>) {
        if worker_id < self.wakers.len() {
            let _ = self.wakers[worker_id].set(waker);
        }
    }

    /// Call before draining the worker's shard inbox so subsequent sends wake again.
    #[inline]
    pub fn clear_wake(&self, worker_id: usize) {
        if let Some(flag) = self.wake_pending.get(worker_id) {
            flag.store(false, Ordering::Release);
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
        // First enqueue since last clear_wake → wake the owner reactor once.
        if !self.wake_pending[worker].swap(true, Ordering::AcqRel) {
            if let Some(w) = self.wakers[worker].get() {
                let _ = w.wake();
            }
        }
        rx
    }
}
