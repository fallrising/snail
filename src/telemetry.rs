use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use crate::config::Config;
use crate::storage::stats::{ShardStats, ShardStatsSnapshot};

#[derive(Clone)]
pub struct ServerInfo {
    pub version: &'static str,
    pub workers: usize,
    pub shards: usize,
    pub start: Instant,
    conn_count: Arc<AtomicUsize>,
    shard_stats: Arc<Vec<ShardStats>>,
}

impl ServerInfo {
    pub fn new(config: &Config, conn_count: Arc<AtomicUsize>) -> Self {
        Self {
            version: "0.1.0",
            workers: config.workers,
            shards: config.shards,
            start: Instant::now(),
            conn_count,
            shard_stats: Arc::new(
                (0..config.shards)
                    .map(|_| ShardStats::default())
                    .collect(),
            ),
        }
    }

    pub fn shard_stats(&self, shard_id: usize) -> ShardStats {
        self.shard_stats[shard_id].clone()
    }

    fn snapshots(&self) -> Vec<ShardStatsSnapshot> {
        self.shard_stats.iter().map(ShardStats::snapshot).collect()
    }

    pub fn format(&self, section: Option<&Bytes>) -> String {
        let sec = section.map(|b| String::from_utf8_lossy(b).to_ascii_lowercase());
        let all = sec.is_none();
        let snapshots = self.snapshots();
        let total_memory: u64 = snapshots.iter().map(|stats| stats.memory).sum();
        let total_keys: usize = snapshots.iter().map(|stats| stats.keys).sum();
        let total_expires: usize = snapshots.iter().map(|stats| stats.expires).sum();
        let mut out = String::new();
        if all || sec.as_deref() == Some("server") {
            out.push_str(&format!(
                "# Server\r\nrudis_version:{}\r\nuptime_in_seconds:{}\r\n\r\n",
                self.version,
                self.start.elapsed().as_secs()
            ));
        }
        if all || sec.as_deref() == Some("clients") {
            out.push_str(&format!(
                "# Clients\r\nconnected_clients:{}\r\n\r\n",
                self.conn_count.load(Ordering::Relaxed)
            ));
        }
        if all || sec.as_deref() == Some("memory") {
            out.push_str(&format!(
                "# Memory\r\nused_memory:{}\r\n\r\n",
                total_memory
            ));
        }
        if all || sec.as_deref() == Some("keyspace") {
            out.push_str(&format!(
                "# Keyspace\r\ndb0:keys={},expires={}\r\n\r\n",
                total_keys, total_expires
            ));
        }
        if all || sec.as_deref() == Some("stats") {
            let total_commands: u64 = snapshots
                .iter()
                .map(|stats| stats.total_commands)
                .sum();
            let hits: u64 = snapshots.iter().map(|stats| stats.hits).sum();
            let misses: u64 = snapshots.iter().map(|stats| stats.misses).sum();
            let expired: u64 = snapshots.iter().map(|stats| stats.expired).sum();
            let keys_flushed: u64 = snapshots
                .iter()
                .map(|stats| stats.keys_flushed)
                .sum();
            out.push_str(&format!(
                "# Stats\r\ntotal_commands_processed:{}\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\nexpired_keys:{}\r\nkeys_flushed:{}\r\n",
                total_commands, hits, misses, expired, keys_flushed
            ));
            for (shard_id, stats) in snapshots.iter().enumerate() {
                out.push_str(&format!(
                    "rudis_shard_{}_commands:{}\r\nrudis_shard_{}_keys:{}\r\nrudis_shard_{}_expires:{}\r\nrudis_shard_{}_used_memory:{}\r\n",
                    shard_id,
                    stats.total_commands,
                    shard_id,
                    stats.keys,
                    shard_id,
                    stats.expires,
                    shard_id,
                    stats.memory
                ));
            }
            out.push_str("\r\n");
        }
        out
    }
}

pub fn init_tracing(config: &Config) {
    let _ = config;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}
