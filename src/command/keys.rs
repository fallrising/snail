use bytes::Bytes;

use crate::protocol::frame::Reply;
use crate::storage::shard::Shard;

pub fn apply_del(shard: &mut Shard, keys: &[Bytes], now_ms: u64) -> Reply {
    shard.stats.record_command();
    let mut n = 0i64;
    for k in keys {
        if shard.remove_key(k).is_some() {
            n += 1;
        }
    }
    Reply::Int(n)
}

pub fn apply_exists(shard: &mut Shard, keys: &[Bytes], now_ms: u64) -> Reply {
    shard.stats.record_command();
    let mut n = 0i64;
    for k in keys {
        if shard.contains(k, now_ms) {
            n += 1;
        }
    }
    Reply::Int(n)
}

pub fn apply_type(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    match shard.lookup_live(key, now_ms) {
        Some(entry) => Reply::Simple(entry.value.kind_name().into()),
        None => Reply::Simple("none".into()),
    }
}

pub fn apply_expire(shard: &mut Shard, key: &Bytes, ttl_secs: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    if !shard.contains(key, now_ms) {
        return Reply::Int(0);
    }
    if ttl_secs < 0 {
        return Reply::Int(0);
    }
    let deadline = now_ms + ttl_secs as u64 * 1000;
    if shard.set_expire(key, deadline) {
        Reply::Int(1)
    } else {
        Reply::Int(0)
    }
}

pub fn apply_pexpire(shard: &mut Shard, key: &Bytes, ttl_ms: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    if !shard.contains(key, now_ms) {
        return Reply::Int(0);
    }
    if ttl_ms < 0 {
        return Reply::Int(0);
    }
    let deadline = now_ms + ttl_ms as u64;
    if shard.set_expire(key, deadline) {
        Reply::Int(1)
    } else {
        Reply::Int(0)
    }
}

pub fn apply_expireat(shard: &mut Shard, key: &Bytes, at_secs: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    if !shard.contains(key, now_ms) {
        return Reply::Int(0);
    }
    let deadline = (at_secs as u64) * 1000;
    if deadline <= now_ms {
        shard.remove_key(key);
        return Reply::Int(1);
    }
    if shard.set_expire(key, deadline) {
        Reply::Int(1)
    } else {
        Reply::Int(0)
    }
}

pub fn apply_pexpireat(shard: &mut Shard, key: &Bytes, at_ms: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    if !shard.contains(key, now_ms) {
        return Reply::Int(0);
    }
    let deadline = at_ms as u64;
    if deadline <= now_ms {
        shard.remove_key(key);
        return Reply::Int(1);
    }
    if shard.set_expire(key, deadline) {
        Reply::Int(1)
    } else {
        Reply::Int(0)
    }
}

pub fn apply_ttl(shard: &mut Shard, key: &Bytes, now_ms: u64, ms: bool) -> Reply {
    shard.stats.record_command();
    if !shard.dict_contains(key) {
        return Reply::Int(-2);
    }
    match shard.ttl_ms(key, now_ms) {
        None => Reply::Int(-2),
        Some(-1) => Reply::Int(-1),
        Some(v) => {
            if ms {
                Reply::Int(v)
            } else {
                Reply::Int((v + 999) / 1000)
            }
        }
    }
}

pub fn apply_persist(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    if !shard.contains(key, now_ms) {
        return Reply::Int(0);
    }
    Reply::Int(if shard.persist(key) { 1 } else { 0 })
}

pub fn apply_rename(
    shard: &mut Shard,
    src: Bytes,
    dst: Bytes,
    nx: bool,
    now_ms: u64,
    config: &crate::config::Config,
) -> Reply {
    shard.stats.record_command();
    if src == dst {
        return Reply::Ok;
    }
    let Some(entry) = shard.remove_key(&src) else {
        return Reply::Err(
            crate::protocol::frame::CommandErrKind::Generic,
            "no such key".into(),
        );
    };
    if nx && shard.contains(&dst, now_ms) {
        shard.write_entry(src, entry);
        return Reply::Int(0);
    }
    if !shard.check_memory(config.maxmemory, 0) {
        shard.write_entry(src, entry);
        return Reply::Err(
            crate::protocol::frame::CommandErrKind::Generic,
            "command not allowed when used memory > 'maxmemory'".into(),
        );
    }
    shard.write_entry(dst, entry);
    if nx {
        Reply::Int(1)
    } else {
        Reply::Ok
    }
}