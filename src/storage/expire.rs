use std::collections::BTreeMap;

use bytes::Bytes;

#[derive(Debug, Default)]
pub struct ExpireIndex {
    index: BTreeMap<(u64, u64), Bytes>,
}

impl ExpireIndex {
    pub fn insert(&mut self, deadline_ms: u64, seq: u64, key: Bytes) {
        self.index.insert((deadline_ms, seq), key);
    }

    pub fn remove(&mut self, deadline_ms: u64, seq: u64) {
        self.index.remove(&(deadline_ms, seq));
    }

    pub fn peek_front(&self) -> Option<((u64, u64), &Bytes)> {
        self.index.iter().next().map(|(k, v)| (*k, v))
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }
}