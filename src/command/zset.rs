use std::collections::{BTreeMap, HashMap};

use ahash::RandomState;
use bytes::Bytes;

use crate::command::{parse_f64_arg, parse_i64_arg, Command, CommandError, ScoreBound, ZAddOptions};
use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::{Entry, Shard};
use crate::storage::value::{OrdF64, Value, ZSetValue};

pub fn parse_zadd(args: &[Bytes]) -> Result<Command, CommandError> {
    let key = args[0].clone();
    let mut opts = ZAddOptions::default();
    let mut i = 1;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "NX" => opts.nx = true,
            "XX" => opts.xx = true,
            "GT" => opts.gt = true,
            "LT" => opts.lt = true,
            "CH" => opts.ch = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &args[i..];
    if rest.len() % 2 != 0 {
        return Err(CommandError::Syntax);
    }
    let mut pairs = Vec::new();
    let mut j = 0;
    while j < rest.len() {
        let score = parse_f64_arg(&rest[j])?;
        pairs.push((score, rest[j + 1].clone()));
        j += 2;
    }
    Ok(Command::ZAdd(key, opts, pairs))
}

pub fn parse_zrange(args: &[Bytes], rev: bool) -> Result<Command, CommandError> {
    let key = args[0].clone();
    let start = parse_i64_arg(&args[1])?;
    let stop = parse_i64_arg(&args[2])?;
    let withscores = args.len() > 3 && eq_ignore_case(&args[3], b"WITHSCORES");
    if rev {
        Ok(Command::ZRevRange(key, start, stop, withscores))
    } else {
        Ok(Command::ZRange(key, start, stop, withscores))
    }
}

pub fn parse_score_bound(b: &Bytes) -> Result<ScoreBound, CommandError> {
    let s = std::str::from_utf8(b).map_err(|_| CommandError::NotFloat)?;
    let inclusive = !s.starts_with('(');
    let num = if inclusive { s } else { &s[1..] };
    let val = match num.to_ascii_lowercase().as_str() {
        "-inf" => f64::NEG_INFINITY,
        "+inf" | "inf" => f64::INFINITY,
        _ => num.parse().map_err(|_| CommandError::NotFloat)?,
    };
    if val.is_nan() {
        return Err(CommandError::NotFloat);
    }
    Ok(ScoreBound { val, inclusive })
}

pub fn parse_zrangebyscore(args: &[Bytes], rev: bool) -> Result<Command, CommandError> {
    let key = args[0].clone();
    // ZRANGEBYSCORE: min max; ZREVRANGEBYSCORE: max min (Redis convention).
    let (min, max) = if rev {
        (parse_score_bound(&args[2])?, parse_score_bound(&args[1])?)
    } else {
        (parse_score_bound(&args[1])?, parse_score_bound(&args[2])?)
    };
    let mut withscores = false;
    let mut limit = None;
    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                let offset = parse_i64_arg(&args[i + 1])? as usize;
                let count = parse_i64_arg(&args[i + 2])? as usize;
                limit = Some((offset, count));
                i += 2;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    if rev {
        Ok(Command::ZRevRangeByScore(key, min, max, withscores, limit))
    } else {
        Ok(Command::ZRangeByScore(key, min, max, withscores, limit))
    }
}

fn eq_ignore_case(a: &Bytes, b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

pub fn apply_zadd(
    shard: &mut Shard,
    key: Bytes,
    opts: ZAddOptions,
    pairs: Vec<(f64, Bytes)>,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let mut zset = match shard.lookup_live(&key, now_ms) {
        None => ZSetValue::default(),
        Some(entry) => match &entry.value {
            Value::ZSet(z) => z.clone(),
            _ => return wrongtype(),
        },
    };
    let mut changed = 0i64;
    for (score, member) in pairs {
        if score.is_nan() {
            return float_err();
        }
        let exists = zset.scores.contains_key(&member);
        if opts.nx && exists {
            continue;
        }
        if opts.xx && !exists {
            continue;
        }
        if exists {
            let old = zset.scores[&member];
            if opts.gt && score <= old {
                continue;
            }
            if opts.lt && score >= old {
                continue;
            }
            zset_remove_member(&mut zset, &member, old);
        }
        zset_insert(&mut zset, member, score);
        if !exists || opts.ch {
            changed += 1;
        } else if opts.ch {
            changed += 1;
        }
    }
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::ZSet(zset.clone())),
    ) {
        return oom();
    }
    if zset.scores.is_empty() {
        shard.remove_key(&key);
        return Reply::Int(changed);
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::ZSet(zset),
            expire,
        },
    );
    Reply::Int(changed)
}

pub fn zset_insert(z: &mut ZSetValue, member: Bytes, score: f64) {
    z.scores.insert(member.clone(), score);
    z.order.insert((OrdF64(score), member), ());
}

pub fn zset_remove_member(z: &mut ZSetValue, member: &Bytes, score: f64) {
    z.scores.remove(member);
    z.order.remove(&(OrdF64(score), member.clone()));
}

fn score_in_range(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    let above = if min.inclusive {
        score >= min.val
    } else {
        score > min.val
    };
    let below = if max.inclusive {
        score <= max.val
    } else {
        score < max.val
    };
    above && below
}

pub fn apply_zrangebyscore(
    shard: &mut Shard,
    key: &Bytes,
    min: ScoreBound,
    max: ScoreBound,
    withscores: bool,
    limit: Option<(usize, usize)>,
    rev: bool,
    now_ms: u64,
) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Array(vec![]);
    };
    let Value::ZSet(z) = &entry.value else {
        return wrongtype();
    };
    let mut matched: Vec<(Bytes, f64)> = z
        .order
        .iter()
        .map(|((s, m), _)| (m.clone(), s.0))
        .filter(|(_, score)| score_in_range(*score, min, max))
        .collect();
    if rev {
        matched.reverse();
    }
    if let Some((offset, count)) = limit {
        if offset >= matched.len() {
            matched.clear();
        } else {
            let end = (offset + count).min(matched.len());
            matched = matched[offset..end].to_vec();
        }
    }
    let mut out = Vec::new();
    for (m, score) in matched {
        out.push(Reply::Bulk(m));
        if withscores {
            out.push(Reply::Bulk(Bytes::from(score.to_string())));
        }
    }
    Reply::Array(out)
}

pub fn apply_zrange(
    shard: &mut Shard,
    key: &Bytes,
    start: i64,
    stop: i64,
    withscores: bool,
    rev: bool,
    now_ms: u64,
) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Array(vec![]);
    };
    let Value::ZSet(z) = &entry.value else {
        return wrongtype();
    };
    let members: Vec<(Bytes, f64)> = z
        .order
        .iter()
        .map(|((s, m), _)| (m.clone(), s.0))
        .collect();
    let len = members.len() as i64;
    let (s, e) = normalize_range(start, stop, len);
    let slice: Vec<_> = if rev {
        members.into_iter().rev().skip(s).take(e - s + 1).collect()
    } else {
        members.into_iter().skip(s).take(e - s + 1).collect()
    };
    let mut out = Vec::new();
    for (m, score) in slice {
        out.push(Reply::Bulk(m));
        if withscores {
            out.push(Reply::Bulk(Bytes::from(score.to_string())));
        }
    }
    Reply::Array(out)
}

pub fn apply_zrank(shard: &mut Shard, key: &Bytes, member: &Bytes, rev: bool, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::NullBulk;
    };
    let Value::ZSet(z) = &entry.value else {
        return wrongtype();
    };
    let members: Vec<Bytes> = z.order.iter().map(|((_, m), _)| m.clone()).collect();
    let iter: Vec<_> = if rev {
        members.into_iter().rev().collect()
    } else {
        members
    };
    for (i, m) in iter.iter().enumerate() {
        if m == member {
            return Reply::Int(i as i64);
        }
    }
    Reply::NullBulk
}

pub fn apply_zpop(
    shard: &mut Shard,
    key: Bytes,
    count: usize,
    max: bool,
    now_ms: u64,
) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live_mut(&key, now_ms) else {
        return Reply::Array(vec![]);
    };
    let Value::ZSet(z) = &mut entry.value else {
        return wrongtype();
    };
    let mut out = Vec::new();
    for _ in 0..count {
        let item = if max {
            z.order.iter().next_back().map(|((s, m), _)| (m.clone(), s.0))
        } else {
            z.order.iter().next().map(|((s, m), _)| (m.clone(), s.0))
        };
        let Some((member, score)) = item else {
            break;
        };
        zset_remove_member(z, &member, score);
        out.push(Reply::Bulk(member));
        out.push(Reply::Bulk(Bytes::from(score.to_string())));
    }
    if z.scores.is_empty() {
        shard.remove_key(&key);
    }
    Reply::Array(out)
}

pub fn get_zset_data(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Result<ZSetValue, Reply> {
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Ok(ZSetValue::default());
    };
    match &entry.value {
        Value::ZSet(z) => Ok(z.clone()),
        _ => Err(wrongtype()),
    }
}

fn normalize_range(start: i64, stop: i64, len: i64) -> (usize, usize) {
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    s = s.clamp(0, len.saturating_sub(1));
    e = e.clamp(0, len.saturating_sub(1));
    if s > e {
        return (0, 0);
    }
    (s as usize, e as usize)
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