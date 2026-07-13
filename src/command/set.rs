use std::collections::HashSet;

use ahash::RandomState;
use bytes::Bytes;

use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::{Entry, Shard};
use crate::storage::value::Value;

pub fn apply_sadd(
    shard: &mut Shard,
    key: Bytes,
    members: Vec<Bytes>,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let mut set = match shard.lookup_live(&key, now_ms) {
        None => HashSet::with_hasher(RandomState::new()),
        Some(entry) => match &entry.value {
            Value::Set(s) => s.clone(),
            _ => return wrongtype(),
        },
    };
    let before = set.len();
    for m in members {
        set.insert(m);
    }
    let added = (set.len() - before) as i64;
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::Set(set.clone())),
    ) {
        return oom();
    }
    if set.is_empty() {
        shard.remove_key(&key);
        return Reply::Int(0);
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Set(set),
            expire,
        },
    );
    Reply::Int(added)
}

pub fn apply_srem(shard: &mut Shard, key: Bytes, members: Vec<Bytes>, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::Int(0);
    };
    let Value::Set(set) = &mut entry.value else {
        return wrongtype();
    };
    let mut removed = 0i64;
    for m in members {
        if set.remove(&m) {
            removed += 1;
        }
    }
    if set.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Int(removed)
}

pub fn apply_scard(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Int(0);
    };
    match &entry.value {
        Value::Set(s) => Reply::Int(s.len() as i64),
        _ => wrongtype(),
    }
}

pub fn apply_sismember(shard: &mut Shard, key: &Bytes, member: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Int(0);
    };
    let Value::Set(s) = &entry.value else {
        return wrongtype();
    };
    Reply::Int(if s.contains(member) { 1 } else { 0 })
}

pub fn apply_smembers(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Array(vec![]);
    };
    let Value::Set(s) = &entry.value else {
        return wrongtype();
    };
    Reply::Array(s.iter().map(|m| Reply::Bulk(m.clone())).collect())
}

pub fn apply_spop(shard: &mut Shard, key: Bytes, count: Option<usize>, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::NullBulk;
    };
    let Value::Set(set) = &mut entry.value else {
        return wrongtype();
    };
    if set.is_empty() {
        return Reply::NullBulk;
    }
    let n = count.unwrap_or(1);
    if n == 1 {
        let m = set.iter().next().cloned();
        if let Some(m) = m {
            set.remove(&m);
            if set.is_empty() {
                shard.remove_key(&key);
            }
            return Reply::Bulk(m);
        }
        return Reply::NullBulk;
    }
    let mut out = Vec::new();
    for m in set.iter().take(n).cloned().collect::<Vec<_>>() {
        set.remove(&m);
        out.push(Reply::Bulk(m));
    }
    if set.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Array(out)
}

pub fn set_op(
    sets: Vec<HashSet<Bytes, RandomState>>,
    op: SetOp,
) -> HashSet<Bytes, RandomState> {
    if sets.is_empty() {
        return HashSet::with_hasher(RandomState::new());
    }
    match op {
        SetOp::Inter => {
            let mut it = sets.into_iter();
            let first = it.next().unwrap();
            it.fold(first, |acc, s| acc.intersection(&s).cloned().collect())
        }
        SetOp::Union => {
            let mut out = HashSet::with_hasher(RandomState::new());
            for s in sets {
                out.extend(s);
            }
            out
        }
        SetOp::Diff => {
            let mut it = sets.into_iter();
            let first = it.next().unwrap();
            it.fold(first, |acc, s| acc.difference(&s).cloned().collect())
        }
    }
}

pub enum SetOp {
    Inter,
    Union,
    Diff,
}

pub fn get_set_members(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Result<HashSet<Bytes, RandomState>, Reply> {
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Ok(HashSet::with_hasher(RandomState::new()));
    };
    match &entry.value {
        Value::Set(s) => Ok(s.clone()),
        _ => Err(wrongtype()),
    }
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