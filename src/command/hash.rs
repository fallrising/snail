use std::collections::HashMap;

use ahash::RandomState;
use bytes::Bytes;

use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::{Entry, Shard};
use crate::storage::value::{parse_f64_bytes, parse_i64_bytes, Value};

pub fn apply_hset(
    shard: &mut Shard,
    key: Bytes,
    fields: Vec<(Bytes, Bytes)>,
    nx: bool,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let mut map = match shard.lookup_live(&key, now_ms) {
        None => HashMap::with_hasher(RandomState::new()),
        Some(entry) => match &entry.value {
            Value::Hash(m) => m.clone(),
            _ => return wrongtype(),
        },
    };
    let mut added = 0i64;
    for (f, v) in fields {
        if nx && map.contains_key(&f) {
            continue;
        }
        let is_new = !map.contains_key(&f);
        map.insert(f, v);
        if is_new {
            added += 1;
        }
    }
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::Hash(map.clone())),
    ) {
        return oom();
    }
    if map.is_empty() {
        shard.remove_key(&key);
        return Reply::Int(0);
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Hash(map),
            expire,
        },
    );
    Reply::Int(added)
}

pub fn apply_hget(shard: &mut Shard, key: &Bytes, field: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::NullBulk;
    };
    let Value::Hash(m) = &entry.value else {
        return wrongtype();
    };
    match m.get(field) {
        Some(v) => Reply::Bulk(v.clone()),
        None => Reply::NullBulk,
    }
}

pub fn apply_hmget(shard: &mut Shard, key: &Bytes, fields: &[Bytes], now_ms: u64) -> Reply {
    let items: Vec<Reply> = fields
        .iter()
        .map(|f| match apply_hget(shard, key, f, now_ms) {
            Reply::Bulk(b) => Reply::Bulk(b),
            Reply::NullBulk => Reply::NullBulk,
            other => other,
        })
        .collect();
    Reply::Array(items)
}

pub fn apply_hdel(shard: &mut Shard, key: Bytes, fields: Vec<Bytes>, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::Int(0);
    };
    let Value::Hash(m) = &mut entry.value else {
        return wrongtype();
    };
    let mut removed = 0i64;
    for f in fields {
        if m.remove(&f).is_some() {
            removed += 1;
        }
    }
    if m.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Int(removed)
}

pub fn apply_hlen(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Int(0);
    };
    match &entry.value {
        Value::Hash(m) => Reply::Int(m.len() as i64),
        _ => wrongtype(),
    }
}

pub fn apply_hgetall(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Array(vec![]);
    };
    let Value::Hash(m) = &entry.value else {
        return wrongtype();
    };
    let mut out = Vec::with_capacity(m.len() * 2);
    for (k, v) in m {
        out.push(Reply::Bulk(k.clone()));
        out.push(Reply::Bulk(v.clone()));
    }
    Reply::Array(out)
}

pub fn apply_hincrby(
    shard: &mut Shard,
    key: Bytes,
    field: Bytes,
    delta: i64,
    float: bool,
    delta_f: f64,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let mut map = match shard.lookup_live(&key, now_ms) {
        None => HashMap::with_hasher(RandomState::new()),
        Some(entry) => match &entry.value {
            Value::Hash(m) => m.clone(),
            _ => return wrongtype(),
        },
    };
    let new_bytes = if float {
        let cur = match map.get(&field) {
            Some(v) => match parse_f64_bytes(v) {
                Some(n) => n,
                None => return float_err(),
            },
            None => 0.0,
        };
        let new_val = cur + delta_f;
        if new_val.is_nan() {
            return float_err();
        }
        Bytes::from(new_val.to_string())
    } else {
        let cur = match map.get(&field) {
            Some(v) => match parse_i64_bytes(v) {
                Some(n) => n,
                None => return int_err(),
            },
            None => 0,
        };
        let new_val = match cur.checked_add(delta) {
            Some(n) => n,
            None => return int_err(),
        };
        Bytes::from(new_val.to_string())
    };
    map.insert(field, new_bytes.clone());
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::Hash(map.clone())),
    ) {
        return oom();
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Hash(map),
            expire,
        },
    );
    Reply::Bulk(new_bytes)
}

pub fn apply_hscan(
    shard: &mut Shard,
    key: &Bytes,
    cursor: u64,
    pattern: Option<&Bytes>,
    count: usize,
    now_ms: u64,
) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return scan_reply(0, vec![]);
    };
    let Value::Hash(m) = &entry.value else {
        return wrongtype();
    };
    let mut fields: Vec<_> = m.keys().cloned().collect();
    fields.sort();
    let total = fields.len();
    if total == 0 {
        return scan_reply(0, vec![]);
    }
    let start = (cursor as usize).min(total);
    let mut pos = start;
    let mut scanned = 0usize;
    let mut out = Vec::new();
    while scanned < count && pos < total {
        let f = &fields[pos];
        if pattern_matches(pattern, f) {
            out.push(Reply::Bulk(f.clone()));
            out.push(Reply::Bulk(m[f].clone()));
        }
        pos += 1;
        scanned += 1;
    }
    let next = if pos >= total { 0 } else { pos as u64 };
    scan_reply(next, out)
}

pub fn pattern_matches(pattern: Option<&Bytes>, key: &Bytes) -> bool {
    let Some(pat) = pattern else {
        return true;
    };
    let pat = String::from_utf8_lossy(pat);
    let key = String::from_utf8_lossy(key);
    if pat == "*" {
        return true;
    }
    if let Some(prefix) = pat.strip_suffix('*') {
        return key.starts_with(prefix);
    }
    key == pat
}

fn scan_reply(cursor: u64, items: Vec<Reply>) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(Bytes::from(cursor.to_string())),
        Reply::Array(items),
    ])
}

fn wrongtype() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::WrongType,
        "Operation against a key holding the wrong kind of value".into(),
    )
}

fn oom() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::Generic,
        "command not allowed when used memory > 'maxmemory'".into(),
    )
}

fn float_err() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::Generic,
        "value is not a valid float".into(),
    )
}

fn int_err() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::Generic,
        "value is not an integer or out of range".into(),
    )
}