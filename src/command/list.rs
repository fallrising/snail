use std::collections::VecDeque;

use bytes::Bytes;

use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::{Entry, Shard};
use crate::storage::value::Value;

use super::string::{normalize_index, slice_str};

pub fn apply_lpush(
    shard: &mut Shard,
    key: Bytes,
    values: Vec<Bytes>,
    only_if_exists: bool,
    front: bool,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let exists = shard.lookup_live(&key, now_ms).is_some();
    if only_if_exists && !exists {
        return Reply::Int(0);
    }
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let mut list = match shard.lookup_live(&key, now_ms) {
        None => VecDeque::new(),
        Some(entry) => match &entry.value {
            Value::List(v) => v.clone(),
            _ => return wrongtype(),
        },
    };
    let added = values.len() as i64;
    for v in values {
        if front {
            list.push_front(v);
        } else {
            list.push_back(v);
        }
    }
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::List(list.clone())),
    ) {
        return oom();
    }
    let len = list.len() as i64;
    shard.write_entry(
        key,
        Entry {
            value: Value::List(list),
            expire,
        },
    );
    Reply::Int(len)
}

fn list_len_after(_added: i64, _front: bool, _shard: &Shard, _key: &Bytes, _now: u64) -> Option<i64> {
    None
}

pub fn apply_lpop(
    shard: &mut Shard,
    key: Bytes,
    count: Option<usize>,
    from_front: bool,
    now_ms: u64,
) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::NullBulk;
    };
    let Value::List(list) = &mut entry.value else {
        return wrongtype();
    };
    if list.is_empty() {
        return Reply::NullBulk;
    }
    let n = count.unwrap_or(1);
    if n == 1 {
        let v = if from_front {
            list.pop_front()
        } else {
            list.pop_back()
        };
        if list.is_empty() {
            shard.remove_key(&key);
        }
        return Reply::Bulk(v.unwrap());
    }
    let mut out = Vec::new();
    for _ in 0..n {
        if list.is_empty() {
            break;
        }
        let v = if from_front {
            list.pop_front()
        } else {
            list.pop_back()
        };
        out.push(Reply::Bulk(v.unwrap()));
    }
    if list.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Array(out)
}

pub fn apply_llen(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Int(0);
    };
    match &entry.value {
        Value::List(v) => Reply::Int(v.len() as i64),
        _ => wrongtype(),
    }
}

pub fn apply_lindex(shard: &mut Shard, key: &Bytes, index: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::NullBulk;
    };
    let Value::List(v) = &entry.value else {
        return wrongtype();
    };
    let len = v.len() as i64;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 || idx >= len {
        return Reply::NullBulk;
    }
    Reply::Bulk(v[idx as usize].clone())
}

pub fn apply_lrange(shard: &mut Shard, key: &Bytes, start: i64, stop: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Array(vec![]);
    };
    let Value::List(v) = &entry.value else {
        return wrongtype();
    };
    let len = v.len() as i64;
    let mut s = start;
    let mut e = stop;
    if s < 0 {
        s = len + s;
    }
    if e < 0 {
        e = len + e;
    }
    s = s.clamp(0, len);
    e = e.clamp(0, len - 1);
    if s > e || v.is_empty() {
        return Reply::Array(vec![]);
    }
    let items: Vec<Reply> = v
        .iter()
        .skip(s as usize)
        .take((e - s + 1) as usize)
        .map(|b| Reply::Bulk(b.clone()))
        .collect();
    Reply::Array(items)
}

pub fn apply_lset(shard: &mut Shard, key: Bytes, index: i64, value: Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::Err(
            crate::protocol::frame::CommandErrKind::Generic,
            "no such key".into(),
        );
    };
    let Value::List(list) = &mut entry.value else {
        return wrongtype();
    };
    let len = list.len() as i64;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 || idx >= len {
        return Reply::Err(
            crate::protocol::frame::CommandErrKind::Generic,
            "index out of range".into(),
        );
    }
    list[idx as usize] = value;
    Reply::Ok
}

pub fn apply_lrem(shard: &mut Shard, key: Bytes, count: i64, value: Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::Int(0);
    };
    let Value::List(list) = &mut entry.value else {
        return wrongtype();
    };
    let removed = if count == 0 {
        let before = list.len();
        list.retain(|x| x != &value);
        (before - list.len()) as i64
    } else if count > 0 {
        let mut n = count as usize;
        let mut removed = 0i64;
        let mut i = 0;
        while i < list.len() && n > 0 {
            if list[i] == value {
                list.remove(i);
                removed += 1;
                n -= 1;
            } else {
                i += 1;
            }
        }
        removed
    } else {
        let mut n = (-count) as usize;
        let mut removed = 0i64;
        let mut i = list.len();
        while i > 0 && n > 0 {
            i -= 1;
            if list[i] == value {
                list.remove(i);
                removed += 1;
                n -= 1;
            }
        }
        removed
    };
    if list.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Int(removed)
}

pub fn apply_ltrim(shard: &mut Shard, key: Bytes, start: i64, stop: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::Ok;
    };
    let Value::List(list) = &mut entry.value else {
        return wrongtype();
    };
    let len = list.len() as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    s = s.clamp(0, len);
    e = e.clamp(0, len - 1);
    if s > e || list.is_empty() {
        list.clear();
    } else {
        let trimmed: VecDeque<Bytes> = list.drain(s as usize..=e as usize).collect();
        *list = trimmed;
    }
    if list.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Ok
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