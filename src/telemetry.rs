use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;

use crate::config::Config;
use crate::storage::shard::Shard;

pub struct ServerInfo {
    pub version: &'static str,
    pub workers: usize,
    pub shards: usize,
    pub start: Instant,
    pub total_keys: usize,
    pub total_memory: u64,
    pub conn_count: Option<std::sync::Arc<AtomicUsize>>,
}

impl ServerInfo {
    pub fn new(config: &Config) -> Self {
        Self {
            version: "0.1.0",
            workers: config.workers,
            shards: config.shards,
            start: Instant::now(),
            total_keys: 0,
            total_memory: 0,
            conn_count: None,
        }
    }

    pub fn aggregate_shards(&mut self, shards: &[Shard]) {
        self.total_keys = shards.iter().map(|s| s.len()).sum();
        self.total_memory = shards.iter().map(|s| s.mem_used).sum();
    }

    pub fn format(&self, section: Option<&Bytes>) -> String {
        let sec = section.map(|b| String::from_utf8_lossy(b).to_ascii_lowercase());
        let all = sec.is_none();
        let mut out = String::new();
        if all || sec.as_deref() == Some("server") {
            out.push_str(&format!(
                "# Server\r\nrudis_version:{}\r\nuptime_in_seconds:{}\r\n\r\n",
                self.version,
                self.start.elapsed().as_secs()
            ));
        }
        if all || sec.as_deref() == Some("memory") {
            out.push_str(&format!(
                "# Memory\r\nused_memory:{}\r\n\r\n",
                self.total_memory
            ));
        }
        if all || sec.as_deref() == Some("keyspace") {
            out.push_str(&format!(
                "# Keyspace\r\ndb0:keys={},expires=0\r\n\r\n",
                self.total_keys
            ));
        }
        if all || sec.as_deref() == Some("stats") {
            out.push_str("# Stats\r\n\r\n");
        }
        out
    }
}

impl Clone for ServerInfo {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            workers: self.workers,
            shards: self.shards,
            start: self.start,
            total_keys: self.total_keys,
            total_memory: self.total_memory,
            conn_count: self.conn_count.clone(),
        }
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