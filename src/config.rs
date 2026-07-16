use std::env;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use serde::Deserialize;

use crate::error::ServerError;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub workers: usize,
    #[serde(default)]
    pub shards: usize,
    #[serde(default = "default_maxclients")]
    pub maxclients: usize,
    #[serde(default = "default_tcp_backlog")]
    pub tcp_backlog: u32,
    #[serde(default = "default_max_bulk_len")]
    pub max_bulk_len: usize,
    #[serde(default = "default_max_multibulk")]
    pub max_multibulk: usize,
    #[serde(default = "default_max_inline_len")]
    pub max_inline_len: usize,
    #[serde(default = "default_read_buf_init")]
    pub read_buf_init: usize,
    #[serde(default = "default_out_buf_soft")]
    pub out_buf_soft: usize,
    #[serde(default = "default_out_buf_hard")]
    pub out_buf_hard: usize,
    #[serde(default = "default_pipeline_cap")]
    pub pipeline_cap: usize,
    #[serde(default)]
    pub conn_idle_timeout: u64,
    #[serde(default = "default_expire_interval")]
    pub expire_interval_ms: u64,
    #[serde(default = "default_expire_budget")]
    pub expire_budget: usize,
    #[serde(default)]
    pub maxmemory: u64,
    #[serde(default = "default_pin_cores")]
    pub pin_cores: bool,
    #[serde(default = "default_channel_cap")]
    pub channel_cap: usize,
    #[serde(default = "default_shutdown_deadline")]
    pub shutdown_deadline_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            workers: 0,
            shards: 0,
            maxclients: default_maxclients(),
            tcp_backlog: default_tcp_backlog(),
            max_bulk_len: default_max_bulk_len(),
            max_multibulk: default_max_multibulk(),
            max_inline_len: default_max_inline_len(),
            read_buf_init: default_read_buf_init(),
            out_buf_soft: default_out_buf_soft(),
            out_buf_hard: default_out_buf_hard(),
            pipeline_cap: default_pipeline_cap(),
            conn_idle_timeout: 0,
            expire_interval_ms: default_expire_interval(),
            expire_budget: default_expire_budget(),
            maxmemory: 0,
            pin_cores: default_pin_cores(),
            channel_cap: default_channel_cap(),
            shutdown_deadline_secs: default_shutdown_deadline(),
        }
    }
}

fn default_bind() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    6379
}
fn default_maxclients() -> usize {
    65536
}
fn default_tcp_backlog() -> u32 {
    4096
}
fn default_max_bulk_len() -> usize {
    32 * 1024 * 1024
}
fn default_max_multibulk() -> usize {
    1_048_576
}
fn default_max_inline_len() -> usize {
    65536
}
fn default_read_buf_init() -> usize {
    4096
}
fn default_out_buf_soft() -> usize {
    1_048_576
}
fn default_out_buf_hard() -> usize {
    32 * 1024 * 1024
}
fn default_pipeline_cap() -> usize {
    1024
}
fn default_expire_interval() -> u64 {
    100
}
fn default_expire_budget() -> usize {
    1000
}
fn default_pin_cores() -> bool {
    true
}
fn default_channel_cap() -> usize {
    8192
}
fn default_shutdown_deadline() -> u64 {
    10
}

impl Config {
    pub fn load() -> Result<Self, ServerError> {
        let mut config = Self::default();
        if let Ok(path) = env::var("RUDIS_CONFIG") {
            let file: ConfigFile = toml::from_str(&std::fs::read_to_string(path)?)?;
            config.merge_file(file);
        }
        config.apply_env();
        config.apply_args();
        config.resolve_defaults();
        config.validate()?;
        Ok(config)
    }

    fn apply_env(&mut self) {
        if let Ok(v) = env::var("RUDIS_BIND") {
            self.bind = v;
        }
        if let Ok(v) = env::var("RUDIS_PORT") {
            if let Ok(p) = v.parse() {
                self.port = p;
            }
        }
        if let Ok(v) = env::var("RUDIS_WORKERS") {
            if let Ok(w) = v.parse() {
                self.workers = w;
            }
        }
        if let Ok(v) = env::var("RUDIS_SHARDS") {
            if let Ok(s) = v.parse() {
                self.shards = s;
            }
        }
        if let Ok(v) = env::var("RUDIS_MAXCLIENTS") {
            if let Ok(m) = v.parse() {
                self.maxclients = m;
            }
        }
        if let Ok(v) = env::var("RUDIS_MAXMEMORY") {
            if let Ok(m) = v.parse() {
                self.maxmemory = m;
            }
        }
    }

    fn apply_args(&mut self) {
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bind" => {
                    if let Some(v) = args.next() {
                        self.bind = v;
                    }
                }
                "--port" => {
                    if let Some(v) = args.next() {
                        if let Ok(p) = v.parse() {
                            self.port = p;
                        }
                    }
                }
                "--workers" => {
                    if let Some(v) = args.next() {
                        if let Ok(w) = v.parse() {
                            self.workers = w;
                        }
                    }
                }
                "--shards" => {
                    if let Some(v) = args.next() {
                        if let Ok(s) = v.parse() {
                            self.shards = s;
                        }
                    }
                }
                "--maxclients" => {
                    if let Some(v) = args.next() {
                        if let Ok(m) = v.parse() {
                            self.maxclients = m;
                        }
                    }
                }
                "--maxmemory" => {
                    if let Some(v) = args.next() {
                        if let Ok(m) = v.parse() {
                            self.maxmemory = m;
                        }
                    }
                }
                "--config" => {
                    if let Some(path) = args.next() {
                        if let Ok(content) = std::fs::read_to_string(&path) {
                            if let Ok(file) = toml::from_str::<ConfigFile>(&content) {
                                self.merge_file(file);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn merge_file(&mut self, file: ConfigFile) {
        if let Some(v) = file.bind {
            self.bind = v;
        }
        if let Some(v) = file.port {
            self.port = v;
        }
        if let Some(v) = file.workers {
            self.workers = v;
        }
        if let Some(v) = file.shards {
            self.shards = v;
        }
        if let Some(v) = file.maxclients {
            self.maxclients = v;
        }
        if let Some(v) = file.maxmemory {
            self.maxmemory = v;
        }
    }

    fn resolve_defaults(&mut self) {
        let cpus = std::thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(1);
        if self.workers == 0 {
            self.workers = cpus;
        }
        if self.shards == 0 {
            self.shards = self.workers;
        }
    }

    pub fn validate(&self) -> Result<(), ServerError> {
        if self.workers == 0 {
            return Err(ServerError::Config("workers must be > 0".into()));
        }
        if self.shards == 0 || self.shards % self.workers != 0 {
            return Err(ServerError::Config(
                "shards must be a positive multiple of workers".into(),
            ));
        }
        if self.read_buf_init > self.max_bulk_len {
            return Err(ServerError::Config(
                "read_buf_init must be <= max_bulk_len".into(),
            ));
        }
        if self.out_buf_soft > self.out_buf_hard {
            return Err(ServerError::Config("out_buf_soft must be <= out_buf_hard".into()));
        }
        Ok(())
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(format!("{}:{}", self.bind, self.port).parse()?)
    }
}

#[derive(Debug, Deserialize, Default)]
struct ConfigFile {
    bind: Option<String>,
    port: Option<u16>,
    workers: Option<usize>,
    shards: Option<usize>,
    maxclients: Option<usize>,
    maxmemory: Option<u64>,
}
