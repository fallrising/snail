#[derive(Debug, Default, Clone)]
pub struct ShardStats {
    pub total_commands: u64,
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub keys_flushed: u64,
}

impl ShardStats {
    pub fn record_hit(&mut self) {
        self.hits += 1;
    }
    pub fn record_miss(&mut self) {
        self.misses += 1;
    }
    pub fn record_command(&mut self) {
        self.total_commands += 1;
    }
}