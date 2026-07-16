use bytes::Bytes;

use crate::command::{parse_i64_arg, Command, CommandError, GetExOptions, SetOptions};
use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::{Entry, Shard};
use crate::storage::value::{parse_f64_bytes, parse_i64_bytes, Value};

pub fn parse_set(args: &[Bytes]) -> Result<Command, CommandError> {
    let key = args[0].clone();
    let value = args[1].clone();
    let mut opts = SetOptions::default();
    let mut i = 2;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "EX" => {
                i += 1;
                opts.ex = Some(parse_i64_arg(&args[i])?);
            }
            "PX" => {
                i += 1;
                opts.px = Some(parse_i64_arg(&args[i])?);
            }
            "EXAT" => {
                i += 1;
                opts.exat = Some(parse_i64_arg(&args[i])?);
            }
            "PXAT" => {
                i += 1;
                opts.pxat = Some(parse_i64_arg(&args[i])?);
            }
            "NX" => opts.nx = true,
            "XX" => opts.xx = true,
            "GET" => opts.get = true,
            "KEEPTTL" => opts.keepttl = true,
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    if opts.nx && opts.xx {
        return Err(CommandError::Syntax);
    }
    Ok(Command::Set(key, value, opts))
}

pub fn parse_getex(args: &[Bytes]) -> Result<Command, CommandError> {
    let key = args[0].clone();
    let mut opts = GetExOptions::default();
    let mut i = 1;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "EX" => {
                i += 1;
                opts.ex = Some(parse_i64_arg(&args[i])?);
            }
            "PX" => {
                i += 1;
                opts.px = Some(parse_i64_arg(&args[i])?);
            }
            "EXAT" => {
                i += 1;
                opts.exat = Some(parse_i64_arg(&args[i])?);
            }
            "PXAT" => {
                i += 1;
                opts.pxat = Some(parse_i64_arg(&args[i])?);
            }
            "PERSIST" => opts.persist = true,
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    Ok(Command::GetEx(key, opts))
}

pub fn apply_get(shard: &mut Shard, key: &Bytes, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let hit = shard.lookup_live(key, now_ms).map(|entry| match &entry.value {
        Value::Str(v) => Ok(v.clone()),
        _ => Err(()),
    });
    match hit {
        Some(Ok(v)) => {
            shard.stats.record_hit();
            Reply::Bulk(v)
        }
        Some(Err(())) => err_wrongtype(),
        None => {
            shard.stats.record_miss();
            Reply::NullBulk
        }
    }
}

pub fn apply_set(
    shard: &mut Shard,
    key: Bytes,
    value: Bytes,
    opts: SetOptions,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let exists = shard.lookup_live(&key, now_ms).is_some();
    if opts.nx && exists {
        return if opts.get {
            Reply::NullBulk
        } else {
            Reply::NullBulk
        };
    }
    if opts.xx && !exists {
        return if opts.get {
            Reply::NullBulk
        } else {
            Reply::NullBulk
        };
    }

    let old_reply = if opts.get {
        shard.lookup_live(&key, now_ms).and_then(|e| match &e.value {
            Value::Str(v) => Some(Reply::Bulk(v.clone())),
            _ => Some(err_wrongtype()),
        })
    } else {
        None
    };

    if let Some(Reply::Err(_, _)) = old_reply {
        return old_reply.unwrap();
    }

    let old_expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let delta = Shard::now_key_size(&key, &Value::Str(value.clone()));
    if !shard.check_memory(config.maxmemory, delta) {
        return err_oom();
    }

    let expire = if opts.keepttl {
        old_expire
    } else {
        deadline_from_opts(
            opts.ex,
            opts.px,
            opts.exat,
            opts.pxat,
            now_ms,
        )
    };

    let entry = Entry {
        value: Value::Str(value),
        expire,
    };
    shard.write_entry(key, entry);

    if opts.get {
        old_reply.unwrap_or(Reply::NullBulk)
    } else {
        Reply::Ok
    }
}

pub fn apply_incrby(shard: &mut Shard, key: Bytes, delta: i64, now_ms: u64, config: &Config) -> Reply {
    shard.stats.record_command();
    let cur = shard.lookup_live(&key, now_ms);
    let (new_val, expire) = match cur {
        None => (delta, None),
        Some(entry) => match &entry.value {
            Value::Str(v) => {
                let n = match parse_i64_bytes(v) {
                    Some(n) => n,
                    None => return int_err(),
                };
                let new_val = match n.checked_add(delta) {
                    Some(v) => v,
                    None => return int_err(),
                };
                (new_val, entry.expire)
            }
            _ => return err_wrongtype(),
        },
    };
    let new_bytes = Bytes::from(new_val.to_string());
    let delta_mem = Shard::now_key_size(&key, &Value::Str(new_bytes.clone()));
    if !shard.check_memory(config.maxmemory, delta_mem) {
        return err_oom();
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Str(new_bytes),
            expire,
        },
    );
    Reply::Int(new_val)
}

pub fn apply_incrbyfloat(
    shard: &mut Shard,
    key: Bytes,
    delta: f64,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let cur = shard.lookup_live(&key, now_ms);
    let (new_val, expire) = match cur {
        None => (delta, None),
        Some(entry) => match &entry.value {
            Value::Str(v) => {
                let n = match parse_f64_bytes(v) {
                    Some(n) => n,
                    None => return err_float(),
                };
                let new_val = n + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    return err_float();
                }
                (new_val, entry.expire)
            }
            _ => return err_wrongtype(),
        },
    };
    let new_bytes = Bytes::from(new_val.to_string());
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::Str(new_bytes.clone())),
    ) {
        return err_oom();
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Str(new_bytes),
            expire,
        },
    );
    Reply::Bulk(Bytes::from(new_val.to_string()))
}

pub fn apply_append(
    shard: &mut Shard,
    key: Bytes,
    suffix: Bytes,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let new_val = match shard.lookup_live(&key, now_ms) {
        None => suffix,
        Some(entry) => match &entry.value {
            Value::Str(v) => {
                let mut b = v.to_vec();
                b.extend_from_slice(&suffix);
                Bytes::from(b)
            }
            _ => return err_wrongtype(),
        },
    };
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::Str(new_val.clone())),
    ) {
        return err_oom();
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Str(new_val.clone()),
            expire,
        },
    );
    Reply::Int(new_val.len() as i64)
}

pub fn apply_getrange(shard: &mut Shard, key: &Bytes, start: i64, end: i64, now_ms: u64) -> Reply {
    shard.stats.record_command();
    let Some(entry) = shard.lookup_live(key, now_ms) else {
        return Reply::Bulk(Bytes::new());
    };
    let Value::Str(v) = &entry.value else {
        return err_wrongtype();
    };
    let s = slice_str(v, start, end);
    Reply::Bulk(Bytes::from(s))
}

pub fn apply_setrange(
    shard: &mut Shard,
    key: Bytes,
    offset: i64,
    value: Bytes,
    now_ms: u64,
    config: &Config,
) -> Reply {
    shard.stats.record_command();
    if offset < 0 {
        return Reply::Err(crate::protocol::frame::CommandErrKind::Generic, "offset out of range".into());
    }
    let expire = shard.lookup_live(&key, now_ms).and_then(|e| e.expire);
    let mut buf = match shard.lookup_live(&key, now_ms) {
        None => Vec::new(),
        Some(entry) => match &entry.value {
            Value::Str(v) => v.to_vec(),
            _ => return err_wrongtype(),
        },
    };
    let off = offset as usize;
    if off > buf.len() {
        buf.resize(off, 0);
    }
    for (i, b) in value.iter().enumerate() {
        let pos = off + i;
        if pos >= buf.len() {
            buf.push(*b);
        } else {
            buf[pos] = *b;
        }
    }
    let new_val = Bytes::from(buf);
    if !shard.check_memory(
        config.maxmemory,
        Shard::now_key_size(&key, &Value::Str(new_val.clone())),
    ) {
        return err_oom();
    }
    shard.write_entry(
        key,
        Entry {
            value: Value::Str(new_val.clone()),
            expire,
        },
    );
    Reply::Int(new_val.len() as i64)
}

pub fn deadline_from_opts(
    ex: Option<i64>,
    px: Option<i64>,
    exat: Option<i64>,
    pxat: Option<i64>,
    now_ms: u64,
) -> Option<(u64, u64)> {
    if let Some(sec) = ex {
        return Some((now_ms + (sec as u64) * 1000, 0));
    }
    if let Some(ms) = px {
        return Some((now_ms + ms as u64, 0));
    }
    if let Some(at) = exat {
        return Some((at as u64 * 1000, 0));
    }
    if let Some(at) = pxat {
        return Some((at as u64, 0));
    }
    None
}

pub fn slice_str(v: &Bytes, start: i64, end: i64) -> Vec<u8> {
    let len = v.len() as i64;
    let s = normalize_index(start, len);
    let e = normalize_index(end, len);
    if s > e || s >= len as usize {
        return vec![];
    }
    v[s..=e.min(len as usize - 1)].to_vec()
}

pub fn normalize_index(idx: i64, len: i64) -> usize {
    let mut i = idx;
    if i < 0 {
        i = len + i;
    }
    if i < 0 {
        0
    } else {
        i.min(len.saturating_sub(1)) as usize
    }
}

fn err_wrongtype() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::WrongType,
        "Operation against a key holding the wrong kind of value".into(),
    )
}

fn err_oom() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::Generic,
        "command not allowed when used memory > 'maxmemory'".into(),
    )
}

fn err_float() -> Reply {
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
