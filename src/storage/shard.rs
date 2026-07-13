use std::collections::HashMap;

use ahash::RandomState;
use bytes::Bytes;

use crate::storage::expire::ExpireIndex;
use crate::storage::stats::ShardStats;
use crate::storage::value::Value;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Value,
    pub expire: Option<(u64, u64)>,
}

#[derive(Debug)]
pub struct Shard {
    pub id: usize,
    dict: HashMap<Bytes, Entry, RandomState>,
    expires: ExpireIndex,
    pub mem_used: u64,
    pub stats: ShardStats,
    seq_counter: u64,
    scan_cursor: usize,
}

impl Shard {
    pub fn new(id: usize, hash_seed: u64) -> Self {
        let state = RandomState::with_seeds(
            hash_seed,
            hash_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            hash_seed.wrapping_mul(0xBF58_476D_1CE4_E5B9),
            hash_seed.wrapping_mul(0x94D0_49BB_1331_11EB),
        );
        Self {
            id,
            dict: HashMap::with_hasher(state),
            expires: ExpireIndex::default(),
            mem_used: 0,
            stats: ShardStats::default(),
            seq_counter: 0,
            scan_cursor: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn expires_len(&self) -> usize {
        self.expires.len()
    }

    pub fn now_key_size(key: &Bytes, value: &Value) -> u64 {
        key.len() as u64 + value.estimate_size()
    }

    fn estimate_entry(key: &Bytes, entry: &Entry) -> u64 {
        Self::now_key_size(key, &entry.value) + 32
    }

    pub fn lookup_live(&mut self, key: &Bytes, now_ms: u64) -> Option<&Entry> {
        let expired = self
            .dict
            .get(key)
            .and_then(|e| e.expire)
            .map(|(deadline, _)| deadline <= now_ms)
            .unwrap_or(false);
        if expired {
            self.remove_key(key);
            self.stats.expired += 1;
            return None;
        }
        self.dict.get(key)
    }

    pub fn lookup_live_mut(&mut self, key: &Bytes, now_ms: u64) -> Option<&mut Entry> {
        let expired = self
            .dict
            .get(key)
            .and_then(|e| e.expire)
            .map(|(deadline, _)| deadline <= now_ms)
            .unwrap_or(false);
        if expired {
            self.remove_key(key);
            self.stats.expired += 1;
            return None;
        }
        self.dict.get_mut(key)
    }

    pub fn contains(&mut self, key: &Bytes, now_ms: u64) -> bool {
        self.lookup_live(key, now_ms).is_some()
    }

    pub fn dict_contains(&self, key: &Bytes) -> bool {
        self.dict.contains_key(key)
    }

    pub fn write_entry(&mut self, key: Bytes, entry: Entry) {
        let new_size = Self::estimate_entry(&key, &entry);
        if let Some(old) = self.dict.get(&key) {
            let old_size = Self::estimate_entry(&key, old);
            self.mem_used = self.mem_used.saturating_sub(old_size);
            if let Some((deadline, seq)) = old.expire {
                self.expires.remove(deadline, seq);
            }
        }
        if let Some((deadline, seq)) = entry.expire {
            self.expires.insert(deadline, seq, key.clone());
        }
        self.mem_used += new_size;
        self.dict.insert(key, entry);
    }

    pub fn remove_key(&mut self, key: &Bytes) -> Option<Entry> {
        if let Some(old) = self.dict.remove(key) {
            let old_size = Self::estimate_entry(key, &old);
            self.mem_used = self.mem_used.saturating_sub(old_size);
            if let Some((deadline, seq)) = old.expire {
                self.expires.remove(deadline, seq);
            }
            Some(old)
        } else {
            None
        }
    }

    pub fn set_expire(&mut self, key: &Bytes, deadline_ms: u64) -> bool {
        let Some(entry) = self.dict.get_mut(key) else {
            return false;
        };
        if let Some((old_deadline, seq)) = entry.expire {
            self.expires.remove(old_deadline, seq);
        }
        self.seq_counter += 1;
        let seq = self.seq_counter;
        entry.expire = Some((deadline_ms, seq));
        self.expires.insert(deadline_ms, seq, key.clone());
        true
    }

    pub fn persist(&mut self, key: &Bytes) -> bool {
        let Some(entry) = self.dict.get_mut(key) else {
            return false;
        };
        if let Some((deadline, seq)) = entry.expire.take() {
            self.expires.remove(deadline, seq);
            true
        } else {
            false
        }
    }

    pub fn ttl_ms(&self, key: &Bytes, now_ms: u64) -> Option<i64> {
        let entry = self.dict.get(key)?;
        match entry.expire {
            Some((deadline, _)) if deadline > now_ms => Some((deadline - now_ms) as i64),
            Some(_) => None,
            None => Some(-1),
        }
    }

    pub fn active_expire(&mut self, now_ms: u64, budget: usize) -> usize {
        let mut removed = 0;
        while removed < budget {
            let Some(((deadline, seq), key)) = self.expires.peek_front() else {
                break;
            };
            if deadline > now_ms {
                break;
            }
            let key = key.clone();
            self.expires.remove(deadline, seq);
            if self.dict.remove(&key).is_some() {
                self.stats.expired += 1;
                removed += 1;
            }
        }
        removed
    }

    pub fn scan_step(
        &mut self,
        cursor: usize,
        count: usize,
        pattern: Option<&str>,
    ) -> (usize, Vec<Bytes>) {
        let keys: Vec<Bytes> = self.dict.keys().cloned().collect();
        let total = keys.len();
        let mut pos = cursor % total.max(1);
        let mut out = Vec::new();
        if total == 0 {
            return (0, out);
        }
        while out.len() < count && pos < total {
            let k = &keys[pos];
            if pattern_matches(pattern, k) {
                out.push(k.clone());
            }
            pos += 1;
        }
        let next = if pos >= total { 0 } else { pos };
        (next, out)
    }

    pub fn flush(&mut self) -> usize {
        let n = self.dict.len();
        self.dict.clear();
        self.expires = ExpireIndex::default();
        self.mem_used = 0;
        self.stats.keys_flushed += n as u64;
        n
    }

    pub fn random_key(&self) -> Option<Bytes> {
        self.dict.keys().next().cloned()
    }

    pub fn keys_matching(&self, pattern: Option<&str>) -> Vec<Bytes> {
        self.dict
            .keys()
            .filter(|k| pattern_matches(pattern, k))
            .cloned()
            .collect()
    }

    pub fn check_memory(&self, maxmemory: u64, delta: u64) -> bool {
        if maxmemory == 0 {
            return true;
        }
        self.mem_used.saturating_add(delta) <= maxmemory
    }
}

fn pattern_matches(pattern: Option<&str>, key: &Bytes) -> bool {
    let Some(pat) = pattern else {
        return true;
    };
    let key_str = String::from_utf8_lossy(key);
    if pat == "*" {
        return true;
    }
    if let Some(prefix) = pat.strip_suffix('*') {
        return key_str.starts_with(prefix);
    }
    if let Some(suffix) = pat.strip_prefix('*') {
        return key_str.ends_with(suffix);
    }
    key_str == pat
}

pub fn encode_scan_cursor(shard_id: usize, local: usize, shards: usize) -> u64 {
    ((shard_id as u64) << 48) | (local as u64 & 0x0000_FFFF_FFFF)
}

pub fn decode_scan_cursor(cursor: u64, shards: usize) -> (usize, usize) {
    if cursor == 0 {
        (0, 0)
    } else {
        let shard = ((cursor >> 48) as usize) % shards;
        let local = (cursor & 0x0000_FFFF_FFFF) as usize;
        (shard, local)
    }
}