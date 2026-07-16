use bytes::Bytes;

use crate::command::hash;
use crate::command::keys;
use crate::command::list;
use crate::command::server;
use crate::command::set;
use crate::command::string;
use crate::command::zset;
use crate::command::Command;
use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::Shard;
use crate::storage::value::Value;
use crate::telemetry::ServerInfo;

pub fn apply(shard: &mut Shard, cmd: Command, now_ms: u64, config: &Config, info: &ServerInfo) -> Reply {
    match cmd {
        Command::Ping(msg) => server::apply_ping(msg),
        Command::Echo(msg) => server::apply_echo(msg),
        Command::Hello(_) => server::apply_hello(),
        Command::Select(_) => Reply::Ok,
        Command::Command(opt) => match opt {
            Some(name) => server::apply_command_info(&name),
            None => server::apply_command_list(),
        },
        Command::CommandCount => server::apply_command_count(),
        Command::ConfigGet(pat) => server::apply_config_get(&pat, config),
        Command::DbSize => server::apply_db_size(shard),
        Command::FlushDb | Command::FlushAll => server::apply_flush(shard),
        Command::Info(section) => server::build_info(info, section.as_ref()),

        Command::Del(ks) => keys::apply_del(shard, &ks, now_ms),
        Command::Exists(ks) => keys::apply_exists(shard, &ks, now_ms),
        Command::Type(k) => keys::apply_type(shard, &k, now_ms),
        Command::Expire(k, s) => keys::apply_expire(shard, &k, s, now_ms),
        Command::PExpire(k, ms) => keys::apply_pexpire(shard, &k, ms, now_ms),
        Command::ExpireAt(k, at) => keys::apply_expireat(shard, &k, at, now_ms),
        Command::PExpireAt(k, at) => keys::apply_pexpireat(shard, &k, at, now_ms),
        Command::Ttl(k) => keys::apply_ttl(shard, &k, now_ms, false),
        Command::PTtl(k) => keys::apply_ttl(shard, &k, now_ms, true),
        Command::Persist(k) => keys::apply_persist(shard, &k, now_ms),
        Command::Rename(src, dst) => keys::apply_rename(shard, src, dst, false, now_ms, config),
        Command::RenameNx(src, dst) => keys::apply_rename(shard, src, dst, true, now_ms, config),
        Command::Keys(pat) => {
            let pattern = String::from_utf8_lossy(&pat).into_owned();
            let keys: Vec<Reply> = shard
                .keys_matching(Some(&pattern))
                .into_iter()
                .map(|k| Reply::Bulk(k))
                .collect();
            Reply::Array(keys)
        }

        Command::Get(k) => string::apply_get(shard, &k, now_ms),
        Command::Set(k, v, opts) => string::apply_set(shard, k, v, opts, now_ms, config),
        Command::SetNx(k, v) => {
            string::apply_set(
                shard,
                k,
                v,
                crate::command::SetOptions {
                    nx: true,
                    ..Default::default()
                },
                now_ms,
                config,
            )
        }
        Command::SetEx(k, sec, v) => {
            string::apply_set(
                shard,
                k,
                v,
                crate::command::SetOptions {
                    ex: Some(sec),
                    ..Default::default()
                },
                now_ms,
                config,
            )
        }
        Command::PSetEx(k, ms, v) => {
            string::apply_set(
                shard,
                k,
                v,
                crate::command::SetOptions {
                    px: Some(ms),
                    ..Default::default()
                },
                now_ms,
                config,
            )
        }
        Command::GetSet(k, v) => {
            let old = string::apply_get(shard, &k, now_ms);
            string::apply_set(
                shard,
                k,
                v,
                crate::command::SetOptions::default(),
                now_ms,
                config,
            );
            old
        }
        Command::GetDel(k) => {
            let old = string::apply_get(shard, &k, now_ms);
            shard.remove_key(&k);
            old
        }
        Command::GetEx(k, opts) => {
            let old = string::apply_get(shard, &k, now_ms);
            if let Reply::Bulk(_) | Reply::NullBulk = &old {
                if opts.persist {
                    shard.persist(&k);
                } else if let Some(expire) = string::deadline_from_opts(
                    opts.ex,
                    opts.px,
                    opts.exat,
                    opts.pxat,
                    now_ms,
                ) {
                    if shard.contains(&k, now_ms) {
                        shard.set_expire(&k, expire.0);
                    }
                }
            }
            old
        }
        Command::MGet(_) | Command::MSet(_) | Command::MSetNx(_) => {
            Reply::Err(crate::protocol::frame::CommandErrKind::Generic, "use dispatcher".into())
        }
        Command::Incr(k) => string::apply_incrby(shard, k, 1, now_ms, config),
        Command::Decr(k) => string::apply_incrby(shard, k, -1, now_ms, config),
        Command::IncrBy(k, n) => string::apply_incrby(shard, k, n, now_ms, config),
        Command::DecrBy(k, n) => string::apply_incrby(shard, k, -n, now_ms, config),
        Command::IncrByFloat(k, f) => string::apply_incrbyfloat(shard, k, f, now_ms, config),
        Command::Append(k, v) => string::apply_append(shard, k, v, now_ms, config),
        Command::StrLen(k) => {
            shard.stats.record_command();
            match shard.lookup_live(&k, now_ms) {
                None => Reply::Int(0),
                Some(e) => match &e.value {
                    Value::Str(v) => Reply::Int(v.len() as i64),
                    _ => wrongtype(),
                },
            }
        }
        Command::GetRange(k, s, e) => string::apply_getrange(shard, &k, s, e, now_ms),
        Command::SetRange(k, off, v) => string::apply_setrange(shard, k, off, v, now_ms, config),

        Command::LPush(k, vs) => list::apply_lpush(shard, k, vs, false, true, now_ms, config),
        Command::RPush(k, vs) => list::apply_lpush(shard, k, vs, false, false, now_ms, config),
        Command::LPushX(k, vs) => list::apply_lpush(shard, k, vs, true, true, now_ms, config),
        Command::RPushX(k, vs) => list::apply_lpush(shard, k, vs, true, false, now_ms, config),
        Command::LPop(k, c) => list::apply_lpop(shard, k, c, true, now_ms),
        Command::RPop(k, c) => list::apply_lpop(shard, k, c, false, now_ms),
        Command::LLen(k) => list::apply_llen(shard, &k, now_ms),
        Command::LIndex(k, i) => list::apply_lindex(shard, &k, i, now_ms),
        Command::LSet(k, i, v) => list::apply_lset(shard, k, i, v, now_ms),
        Command::LRange(k, s, e) => list::apply_lrange(shard, &k, s, e, now_ms),
        Command::LRem(k, c, v) => list::apply_lrem(shard, k, c, v, now_ms),
        Command::LTrim(k, s, e) => list::apply_ltrim(shard, k, s, e, now_ms),

        Command::HSet(k, pairs) => hash::apply_hset(shard, k, pairs, false, now_ms, config),
        Command::HSetNx(k, f, v) => {
            hash::apply_hset(shard, k, vec![(f, v)], true, now_ms, config)
        }
        Command::HGet(k, f) => hash::apply_hget(shard, &k, &f, now_ms),
        Command::HMGet(k, fs) => hash::apply_hmget(shard, &k, &fs, now_ms),
        Command::HDel(k, fs) => hash::apply_hdel(shard, k, fs, now_ms),
        Command::HExists(k, f) => match hash::apply_hget(shard, &k, &f, now_ms) {
            Reply::Bulk(_) => Reply::Int(1),
            Reply::NullBulk => Reply::Int(0),
            other => other,
        },
        Command::HLen(k) => hash::apply_hlen(shard, &k, now_ms),
        Command::HStrLen(k, f) => match hash::apply_hget(shard, &k, &f, now_ms) {
            Reply::Bulk(b) => Reply::Int(b.len() as i64),
            Reply::NullBulk => Reply::Int(0),
            other => other,
        },
        Command::HGetAll(k) => hash::apply_hgetall(shard, &k, now_ms),
        Command::HKeys(k) => match hash::apply_hgetall(shard, &k, now_ms) {
            Reply::Array(mut pairs) => {
                let mut keys = Vec::new();
                let mut i = 0;
                while i < pairs.len() {
                    keys.push(pairs.remove(i));
                    if i < pairs.len() {
                        pairs.remove(i);
                    }
                }
                Reply::Array(keys)
            }
            other => other,
        },
        Command::HVals(k) => match hash::apply_hgetall(shard, &k, now_ms) {
            Reply::Array(mut pairs) => {
                let mut vals = Vec::new();
                let mut i = 0;
                while i < pairs.len() {
                    pairs.remove(i);
                    if i < pairs.len() {
                        vals.push(pairs.remove(i));
                    }
                }
                Reply::Array(vals)
            }
            other => other,
        },
        Command::HIncrBy(k, f, n) => {
            hash::apply_hincrby(shard, k, f, n, false, 0.0, now_ms, config)
        }
        Command::HIncrByFloat(k, f, n) => {
            hash::apply_hincrby(shard, k, f, 0, true, n, now_ms, config)
        }
        Command::HScan {
            key,
            cursor,
            pattern,
            count,
        } => hash::apply_hscan(shard, &key, cursor, pattern.as_ref(), count, now_ms),

        Command::SAdd(k, ms) => set::apply_sadd(shard, k, ms, now_ms, config),
        Command::SRem(k, ms) => set::apply_srem(shard, k, ms, now_ms),
        Command::SCard(k) => set::apply_scard(shard, &k, now_ms),
        Command::SIsMember(k, m) => set::apply_sismember(shard, &k, &m, now_ms),
        Command::SMIsMember(k, ms) => {
            let items: Vec<Reply> = ms
                .iter()
                .map(|m| set::apply_sismember(shard, &k, m, now_ms))
                .collect();
            Reply::Array(items)
        }
        Command::SMembers(k) => set::apply_smembers(shard, &k, now_ms),
        Command::SPop(k, c) => set::apply_spop(shard, k, c, now_ms),
        Command::SRandMember(k, c) => match c {
            None => set::apply_spop(shard, k, None, now_ms),
            Some(n) if n < 0 => {
                let members = set::apply_smembers(shard, &k, now_ms);
                members
            }
            Some(n) => set::apply_spop(shard, k, Some(n as usize), now_ms),
        },
        Command::SScan {
            key,
            cursor,
            pattern,
            count,
        } => set::apply_sscan(shard, &key, cursor, pattern.as_ref(), count, now_ms),

        Command::ZAdd(k, opts, pairs) => zset::apply_zadd(shard, k, opts, pairs, now_ms, config),
        Command::ZRem(k, ms) => {
            shard.stats.record_command();
            let Some(entry) = shard.lookup_live_mut(&k, now_ms) else {
                return Reply::Int(0);
            };
            match &mut entry.value {
                Value::ZSet(z) => {
                    let mut removed = 0i64;
                    for m in ms {
                        if let Some(score) = z.scores.remove(&m) {
                            zset::zset_remove_member(z, &m, score);
                            removed += 1;
                        }
                    }
                    if z.scores.is_empty() {
                        shard.remove_key(&k);
                    }
                    Reply::Int(removed)
                }
                _ => wrongtype(),
            }
        }
        Command::ZScore(k, m) => {
            shard.stats.record_command();
            match shard.lookup_live(&k, now_ms) {
                None => Reply::NullBulk,
                Some(e) => match &e.value {
                    Value::ZSet(z) => match z.scores.get(&m) {
                        Some(s) => Reply::Bulk(Bytes::from(s.to_string())),
                        None => Reply::NullBulk,
                    },
                    _ => wrongtype(),
                },
            }
        }
        Command::ZMScore(k, ms) => {
            let items: Vec<Reply> = ms
                .iter()
                .map(|m| match Command::ZScore(k.clone(), m.clone()) {
                    Command::ZScore(ref key, ref member) => {
                        match shard.lookup_live(key, now_ms) {
                            None => Reply::NullBulk,
                            Some(e) => match &e.value {
                                Value::ZSet(z) => match z.scores.get(member) {
                                    Some(s) => Reply::Bulk(Bytes::from(s.to_string())),
                                    None => Reply::NullBulk,
                                },
                                _ => wrongtype(),
                            },
                        }
                    }
                    _ => Reply::NullBulk,
                })
                .collect();
            Reply::Array(items)
        }
        Command::ZCard(k) => {
            shard.stats.record_command();
            match shard.lookup_live(&k, now_ms) {
                None => Reply::Int(0),
                Some(e) => match &e.value {
                    Value::ZSet(z) => Reply::Int(z.scores.len() as i64),
                    _ => wrongtype(),
                },
            }
        }
        Command::ZIncrBy(k, delta, m) => {
            zset::apply_zadd(
                shard,
                k,
                crate::command::ZAddOptions::default(),
                vec![(delta, m)],
                now_ms,
                config,
            )
        }
        Command::ZRange(k, s, e, ws) => zset::apply_zrange(shard, &k, s, e, ws, false, now_ms),
        Command::ZRevRange(k, s, e, ws) => zset::apply_zrange(shard, &k, s, e, ws, true, now_ms),
        Command::ZRangeByScore(k, min, max, ws, lim) => {
            zset::apply_zrangebyscore(shard, &k, min, max, ws, lim, false, now_ms)
        }
        Command::ZRevRangeByScore(k, min, max, ws, lim) => {
            zset::apply_zrangebyscore(shard, &k, min, max, ws, lim, true, now_ms)
        }
        Command::ZRank(k, m) => zset::apply_zrank(shard, &k, &m, false, now_ms),
        Command::ZRevRank(k, m) => zset::apply_zrank(shard, &k, &m, true, now_ms),
        Command::ZCount(k, min, max) => {
            shard.stats.record_command();
            match shard.lookup_live(&k, now_ms) {
                None => Reply::Int(0),
                Some(e) => match &e.value {
                    Value::ZSet(z) => {
                        let n = z
                            .scores
                            .values()
                            .filter(|s| **s >= min && **s <= max)
                            .count();
                        Reply::Int(n as i64)
                    }
                    _ => wrongtype(),
                },
            }
        }
        Command::ZPopMin(k, c) => zset::apply_zpop(shard, k, c, false, now_ms),
        Command::ZPopMax(k, c) => zset::apply_zpop(shard, k, c, true, now_ms),

        Command::ShardGet(k) => string::apply_get(shard, &k, now_ms),
        Command::ShardGetSet(_) | Command::ShardGetMembers(_) | Command::ShardGetHash(_)
        | Command::ShardGetZSet(_) => Reply::Err(
            crate::protocol::frame::CommandErrKind::Generic,
            "internal".into(),
        ),

        Command::Scan { .. } | Command::RandomKey | Command::Quit | Command::SInter(_)
        | Command::SUnion(_) | Command::SDiff(_) | Command::SInterStore(_, _)
        | Command::SUnionStore(_, _) | Command::SDiffStore(_, _) => Reply::Err(
            crate::protocol::frame::CommandErrKind::Generic,
            "use dispatcher".into(),
        ),
    }
}

fn wrongtype() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::WrongType,
        "Operation against a key holding the wrong kind of value".into(),
    )
}