use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
struct ShardCounters {
    total_commands: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    expired: AtomicU64,
    keys_flushed: AtomicU64,
    keys: AtomicUsize,
    expires: AtomicUsize,
    memory: AtomicU64,
}

#[derive(Debug, Default, Clone)]
pub struct ShardStats {
    counters: Arc<ShardCounters>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShardStatsSnapshot {
    pub total_commands: u64,
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub keys_flushed: u64,
    pub keys: usize,
    pub expires: usize,
    pub memory: u64,
}

impl ShardStats {
    pub fn record_hit(&self) {
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_command(&self) {
        self.counters
            .total_commands
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_commands(&self, n: u64) {
        if n != 0 {
            self.counters
                .total_commands
                .fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_hits(&self, n: u64) {
        if n != 0 {
            self.counters.hits.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_misses(&self, n: u64) {
        if n != 0 {
            self.counters.misses.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_expired(&self) {
        self.counters.expired.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_expired_n(&self, n: u64) {
        if n != 0 {
            self.counters.expired.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_flushed(&self, count: usize) {
        self.counters
            .keys_flushed
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn update_gauges(&self, keys: usize, expires: usize, memory: u64) {
        self.counters.keys.store(keys, Ordering::Relaxed);
        self.counters.expires.store(expires, Ordering::Relaxed);
        self.counters.memory.store(memory, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ShardStatsSnapshot {
        ShardStatsSnapshot {
            total_commands: self.counters.total_commands.load(Ordering::Relaxed),
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            expired: self.counters.expired.load(Ordering::Relaxed),
            keys_flushed: self.counters.keys_flushed.load(Ordering::Relaxed),
            keys: self.counters.keys.load(Ordering::Relaxed),
            expires: self.counters.expires.load(Ordering::Relaxed),
            memory: self.counters.memory.load(Ordering::Relaxed),
        }
    }
}
