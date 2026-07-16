pub mod apply;
pub mod dispatcher;
pub mod hash;
pub mod keys;
pub mod list;
pub mod server;
pub mod set;
pub mod string;
pub mod zset;

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::frame::Frame;

#[derive(Debug, Clone)]
pub enum Command {
    // Server
    Ping(Option<Bytes>),
    Echo(Bytes),
    Hello(Option<u64>),
    Select(i64),
    Quit,
    Command(Option<Bytes>),
    CommandCount,
    ConfigGet(Bytes),
    DbSize,
    FlushDb,
    FlushAll,
    Info(Option<Bytes>),

    // Keys
    Del(Vec<Bytes>),
    Exists(Vec<Bytes>),
    Type(Bytes),
    Expire(Bytes, i64),
    PExpire(Bytes, i64),
    ExpireAt(Bytes, i64),
    PExpireAt(Bytes, i64),
    Ttl(Bytes),
    PTtl(Bytes),
    Persist(Bytes),
    Rename(Bytes, Bytes),
    RenameNx(Bytes, Bytes),
    Keys(Bytes),
    Scan { cursor: u64, pattern: Option<Bytes>, count: usize },
    RandomKey,

    // String
    Get(Bytes),
    Set(Bytes, Bytes, SetOptions),
    SetNx(Bytes, Bytes),
    SetEx(Bytes, i64, Bytes),
    PSetEx(Bytes, i64, Bytes),
    GetSet(Bytes, Bytes),
    GetDel(Bytes),
    GetEx(Bytes, GetExOptions),
    MGet(Vec<Bytes>),
    MSet(Vec<(Bytes, Bytes)>),
    MSetNx(Vec<(Bytes, Bytes)>),
    Incr(Bytes),
    Decr(Bytes),
    IncrBy(Bytes, i64),
    DecrBy(Bytes, i64),
    IncrByFloat(Bytes, f64),
    Append(Bytes, Bytes),
    StrLen(Bytes),
    GetRange(Bytes, i64, i64),
    SetRange(Bytes, i64, Bytes),

    // List
    LPush(Bytes, Vec<Bytes>),
    RPush(Bytes, Vec<Bytes>),
    LPushX(Bytes, Vec<Bytes>),
    RPushX(Bytes, Vec<Bytes>),
    LPop(Bytes, Option<usize>),
    RPop(Bytes, Option<usize>),
    LLen(Bytes),
    LIndex(Bytes, i64),
    LSet(Bytes, i64, Bytes),
    LRange(Bytes, i64, i64),
    LRem(Bytes, i64, Bytes),
    LTrim(Bytes, i64, i64),

    // Hash
    HSet(Bytes, Vec<(Bytes, Bytes)>),
    HSetNx(Bytes, Bytes, Bytes),
    HGet(Bytes, Bytes),
    HMGet(Bytes, Vec<Bytes>),
    HDel(Bytes, Vec<Bytes>),
    HExists(Bytes, Bytes),
    HLen(Bytes),
    HStrLen(Bytes, Bytes),
    HGetAll(Bytes),
    HKeys(Bytes),
    HVals(Bytes),
    HIncrBy(Bytes, Bytes, i64),
    HIncrByFloat(Bytes, Bytes, f64),
    HScan { key: Bytes, cursor: u64, pattern: Option<Bytes>, count: usize },

    // Set
    SAdd(Bytes, Vec<Bytes>),
    SRem(Bytes, Vec<Bytes>),
    SCard(Bytes),
    SIsMember(Bytes, Bytes),
    SMIsMember(Bytes, Vec<Bytes>),
    SMembers(Bytes),
    SPop(Bytes, Option<usize>),
    SRandMember(Bytes, Option<i64>),
    SInter(Vec<Bytes>),
    SUnion(Vec<Bytes>),
    SDiff(Vec<Bytes>),
    SInterStore(Bytes, Vec<Bytes>),
    SUnionStore(Bytes, Vec<Bytes>),
    SDiffStore(Bytes, Vec<Bytes>),
    SScan { key: Bytes, cursor: u64, pattern: Option<Bytes>, count: usize },

    // ZSet
    ZAdd(Bytes, ZAddOptions, Vec<(f64, Bytes)>),
    ZRem(Bytes, Vec<Bytes>),
    ZScore(Bytes, Bytes),
    ZMScore(Bytes, Vec<Bytes>),
    ZCard(Bytes),
    ZIncrBy(Bytes, f64, Bytes),
    ZRange(Bytes, i64, i64, bool),
    ZRevRange(Bytes, i64, i64, bool),
    ZRangeByScore(Bytes, ScoreBound, ScoreBound, bool, Option<(usize, usize)>),
    ZRevRangeByScore(Bytes, ScoreBound, ScoreBound, bool, Option<(usize, usize)>),
    ZRank(Bytes, Bytes),
    ZRevRank(Bytes, Bytes),
    ZCount(Bytes, f64, f64),
    ZPopMin(Bytes, usize),
    ZPopMax(Bytes, usize),

    // Internal shard-local
    ShardGet(Bytes),
    ShardGetSet(Bytes),
    ShardGetMembers(Bytes),
    ShardGetHash(Bytes),
    ShardGetZSet(Bytes),
}

#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    pub ex: Option<i64>,
    pub px: Option<i64>,
    pub exat: Option<i64>,
    pub pxat: Option<i64>,
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
    pub keepttl: bool,
}

#[derive(Debug, Clone, Default)]
pub struct GetExOptions {
    pub ex: Option<i64>,
    pub px: Option<i64>,
    pub exat: Option<i64>,
    pub pxat: Option<i64>,
    pub persist: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ScoreBound {
    pub val: f64,
    pub inclusive: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ZAddOptions {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    Local,
    Key,
    MultiDecompose,
    MultiGather,
    Broadcast,
    CursorTargeted,
}

pub struct CommandSpec {
    pub name: &'static str,
    pub min_arity: isize,
    pub max_arity: isize,
    pub route: RouteClass,
    pub write: bool,
}

pub fn lookup_spec(cmd: &str) -> Option<&'static CommandSpec> {
    COMMAND_TABLE.iter().find(|s| s.name == cmd)
}

pub fn parse(frame: &Frame) -> Result<Command, CommandError> {
    if frame.args.is_empty() {
        return Err(CommandError::WrongArity("empty"));
    }
    let name = ascii_upper(&frame.args[0]);
    let spec = lookup_spec(&name).ok_or_else(|| CommandError::UnknownCommand(name.clone()))?;
    let argc = frame.args.len();
    let min = spec.min_arity.max(0) as usize;
    let max = if spec.max_arity < 0 {
        usize::MAX
    } else {
        spec.max_arity as usize
    };
    if argc < min || argc > max {
        return Err(CommandError::WrongArity(spec.name));
    }
    parse_body(&name, &frame.args[1..])
}

fn parse_body(name: &str, args: &[Bytes]) -> Result<Command, CommandError> {
    match name {
        "PING" => Ok(Command::Ping(args.first().cloned())),
        "ECHO" => Ok(Command::Echo(args[0].clone())),
        "HELLO" => {
            if let Some(v) = args.first() {
                let ver = parse_i64_arg(v)?;
                if ver == 3 {
                    return Err(CommandError::Resp3NotSupported);
                }
                Ok(Command::Hello(Some(ver as u64)))
            } else {
                Ok(Command::Hello(None))
            }
        }
        "SELECT" => {
            let db = parse_i64_arg(&args[0])?;
            if db != 0 {
                return Err(CommandError::InvalidDb);
            }
            Ok(Command::Select(db))
        }
        "QUIT" => Ok(Command::Quit),
        "COMMAND" => {
            if args.is_empty() {
                Ok(Command::Command(None))
            } else {
                let sub = ascii_upper(&args[0]);
                match sub.as_str() {
                    "COUNT" => Ok(Command::CommandCount),
                    "INFO" => {
                        if args.len() < 2 {
                            return Err(CommandError::WrongArity("COMMAND"));
                        }
                        Ok(Command::Command(Some(args[1].clone())))
                    }
                    _ => Ok(Command::Command(Some(args[0].clone()))),
                }
            }
        }
        "CONFIG" => {
            if !eq_ignore_case(&args[0], b"GET") {
                return Err(CommandError::UnknownSubcommand("CONFIG".into()));
            }
            Ok(Command::ConfigGet(args[1].clone()))
        }
        "DBSIZE" => Ok(Command::DbSize),
        "FLUSHDB" => Ok(Command::FlushDb),
        "FLUSHALL" => Ok(Command::FlushAll),
        "INFO" => Ok(Command::Info(args.first().cloned())),

        "DEL" => Ok(Command::Del(args.to_vec())),
        "EXISTS" => Ok(Command::Exists(args.to_vec())),
        "TYPE" => Ok(Command::Type(args[0].clone())),
        "EXPIRE" => Ok(Command::Expire(args[0].clone(), parse_i64_arg(&args[1])?)),
        "PEXPIRE" => Ok(Command::PExpire(args[0].clone(), parse_i64_arg(&args[1])?)),
        "EXPIREAT" => Ok(Command::ExpireAt(args[0].clone(), parse_i64_arg(&args[1])?)),
        "PEXPIREAT" => Ok(Command::PExpireAt(args[0].clone(), parse_i64_arg(&args[1])?)),
        "TTL" => Ok(Command::Ttl(args[0].clone())),
        "PTTL" => Ok(Command::PTtl(args[0].clone())),
        "PERSIST" => Ok(Command::Persist(args[0].clone())),
        "RENAME" => Ok(Command::Rename(args[0].clone(), args[1].clone())),
        "RENAMENX" => Ok(Command::RenameNx(args[0].clone(), args[1].clone())),
        "KEYS" => Ok(Command::Keys(args[0].clone())),
        "SCAN" => parse_scan(args),
        "RANDOMKEY" => Ok(Command::RandomKey),

        "GET" => Ok(Command::Get(args[0].clone())),
        "SET" => string::parse_set(args),
        "SETNX" => Ok(Command::SetNx(args[0].clone(), args[1].clone())),
        "SETEX" => Ok(Command::SetEx(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            args[2].clone(),
        )),
        "PSETEX" => Ok(Command::PSetEx(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            args[2].clone(),
        )),
        "GETSET" => Ok(Command::GetSet(args[0].clone(), args[1].clone())),
        "GETDEL" => Ok(Command::GetDel(args[0].clone())),
        "GETEX" => string::parse_getex(args),
        "MGET" => Ok(Command::MGet(args.to_vec())),
        "MSET" => Ok(Command::MSet(parse_pairs(args)?)),
        "MSETNX" => Ok(Command::MSetNx(parse_pairs(args)?)),
        "INCR" => Ok(Command::Incr(args[0].clone())),
        "DECR" => Ok(Command::Decr(args[0].clone())),
        "INCRBY" => Ok(Command::IncrBy(args[0].clone(), parse_i64_arg(&args[1])?)),
        "DECRBY" => Ok(Command::DecrBy(args[0].clone(), parse_i64_arg(&args[1])?)),
        "INCRBYFLOAT" => Ok(Command::IncrByFloat(
            args[0].clone(),
            parse_f64_arg(&args[1])?,
        )),
        "APPEND" => Ok(Command::Append(args[0].clone(), args[1].clone())),
        "STRLEN" => Ok(Command::StrLen(args[0].clone())),
        "GETRANGE" => Ok(Command::GetRange(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            parse_i64_arg(&args[2])?,
        )),
        "SETRANGE" => Ok(Command::SetRange(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            args[2].clone(),
        )),

        "LPUSH" => Ok(Command::LPush(args[0].clone(), args[1..].to_vec())),
        "RPUSH" => Ok(Command::RPush(args[0].clone(), args[1..].to_vec())),
        "LPUSHX" => Ok(Command::LPushX(args[0].clone(), args[1..].to_vec())),
        "RPUSHX" => Ok(Command::RPushX(args[0].clone(), args[1..].to_vec())),
        "LPOP" => Ok(Command::LPop(
            args[0].clone(),
            args.get(1).map(|b| parse_usize_arg(b)).transpose()?,
        )),
        "RPOP" => Ok(Command::RPop(
            args[0].clone(),
            args.get(1).map(|b| parse_usize_arg(b)).transpose()?,
        )),
        "LLEN" => Ok(Command::LLen(args[0].clone())),
        "LINDEX" => Ok(Command::LIndex(args[0].clone(), parse_i64_arg(&args[1])?)),
        "LSET" => Ok(Command::LSet(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            args[2].clone(),
        )),
        "LRANGE" => Ok(Command::LRange(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            parse_i64_arg(&args[2])?,
        )),
        "LREM" => Ok(Command::LRem(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            args[2].clone(),
        )),
        "LTRIM" => Ok(Command::LTrim(
            args[0].clone(),
            parse_i64_arg(&args[1])?,
            parse_i64_arg(&args[2])?,
        )),

        "HSET" => Ok(Command::HSet(args[0].clone(), parse_pairs(&args[1..])?)),
        "HSETNX" => Ok(Command::HSetNx(args[0].clone(), args[1].clone(), args[2].clone())),
        "HGET" => Ok(Command::HGet(args[0].clone(), args[1].clone())),
        "HMGET" => Ok(Command::HMGet(args[0].clone(), args[1..].to_vec())),
        "HDEL" => Ok(Command::HDel(args[0].clone(), args[1..].to_vec())),
        "HEXISTS" => Ok(Command::HExists(args[0].clone(), args[1].clone())),
        "HLEN" => Ok(Command::HLen(args[0].clone())),
        "HSTRLEN" => Ok(Command::HStrLen(args[0].clone(), args[1].clone())),
        "HGETALL" => Ok(Command::HGetAll(args[0].clone())),
        "HKEYS" => Ok(Command::HKeys(args[0].clone())),
        "HVALS" => Ok(Command::HVals(args[0].clone())),
        "HINCRBY" => Ok(Command::HIncrBy(
            args[0].clone(),
            args[1].clone(),
            parse_i64_arg(&args[2])?,
        )),
        "HINCRBYFLOAT" => Ok(Command::HIncrByFloat(
            args[0].clone(),
            args[1].clone(),
            parse_f64_arg(&args[2])?,
        )),
        "HSCAN" => parse_hscan(args),

        "SADD" => Ok(Command::SAdd(args[0].clone(), args[1..].to_vec())),
        "SREM" => Ok(Command::SRem(args[0].clone(), args[1..].to_vec())),
        "SCARD" => Ok(Command::SCard(args[0].clone())),
        "SISMEMBER" => Ok(Command::SIsMember(args[0].clone(), args[1].clone())),
        "SMISMEMBER" => Ok(Command::SMIsMember(args[0].clone(), args[1..].to_vec())),
        "SMEMBERS" => Ok(Command::SMembers(args[0].clone())),
        "SPOP" => Ok(Command::SPop(
            args[0].clone(),
            args.get(1).map(|b| parse_usize_arg(b)).transpose()?,
        )),
        "SRANDMEMBER" => Ok(Command::SRandMember(
            args[0].clone(),
            args.get(1).map(|b| parse_i64_arg(b)).transpose()?,
        )),
        "SINTER" => Ok(Command::SInter(args.to_vec())),
        "SUNION" => Ok(Command::SUnion(args.to_vec())),
        "SDIFF" => Ok(Command::SDiff(args.to_vec())),
        "SINTERSTORE" => Ok(Command::SInterStore(args[0].clone(), args[1..].to_vec())),
        "SUNIONSTORE" => Ok(Command::SUnionStore(args[0].clone(), args[1..].to_vec())),
        "SDIFFSTORE" => Ok(Command::SDiffStore(args[0].clone(), args[1..].to_vec())),
        "SSCAN" => parse_sscan(args),

        "ZADD" => zset::parse_zadd(args),
        "ZREM" => Ok(Command::ZRem(args[0].clone(), args[1..].to_vec())),
        "ZSCORE" => Ok(Command::ZScore(args[0].clone(), args[1].clone())),
        "ZMSCORE" => Ok(Command::ZMScore(args[0].clone(), args[1..].to_vec())),
        "ZCARD" => Ok(Command::ZCard(args[0].clone())),
        "ZINCRBY" => Ok(Command::ZIncrBy(
            args[0].clone(),
            parse_f64_arg(&args[1])?,
            args[2].clone(),
        )),
        "ZRANGE" => zset::parse_zrange(args, false),
        "ZREVRANGE" => zset::parse_zrange(args, true),
        "ZRANGEBYSCORE" => zset::parse_zrangebyscore(args, false),
        "ZREVRANGEBYSCORE" => zset::parse_zrangebyscore(args, true),
        "ZRANK" => Ok(Command::ZRank(args[0].clone(), args[1].clone())),
        "ZREVRANK" => Ok(Command::ZRevRank(args[0].clone(), args[1].clone())),
        "ZCOUNT" => Ok(Command::ZCount(
            args[0].clone(),
            parse_f64_arg(&args[1])?,
            parse_f64_arg(&args[2])?,
        )),
        "ZPOPMIN" => Ok(Command::ZPopMin(
            args[0].clone(),
            match args.get(1) {
                Some(b) => parse_usize_arg(b)?,
                None => 1,
            },
        )),
        "ZPOPMAX" => Ok(Command::ZPopMax(
            args[0].clone(),
            match args.get(1) {
                Some(b) => parse_usize_arg(b)?,
                None => 1,
            },
        )),

        _ => Err(CommandError::UnknownCommand(name.into())),
    }
}

fn parse_scan(args: &[Bytes]) -> Result<Command, CommandError> {
    let cursor = parse_u64_arg(&args[0])?;
    let mut pattern = None;
    let mut count = 10;
    let mut i = 1;
    while i < args.len() {
        let opt = ascii_upper(&args[i]);
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = Some(args[i].clone());
            }
            "COUNT" => {
                i += 1;
                count = parse_usize_arg(&args[i])?;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    Ok(Command::Scan {
        cursor,
        pattern,
        count,
    })
}

fn parse_hscan(args: &[Bytes]) -> Result<Command, CommandError> {
    let key = args[0].clone();
    let cursor = parse_u64_arg(&args[1])?;
    let mut pattern = None;
    let mut count = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = ascii_upper(&args[i]);
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = Some(args[i].clone());
            }
            "COUNT" => {
                i += 1;
                count = parse_usize_arg(&args[i])?;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    Ok(Command::HScan {
        key,
        cursor,
        pattern,
        count,
    })
}

fn parse_sscan(args: &[Bytes]) -> Result<Command, CommandError> {
    let key = args[0].clone();
    let cursor = parse_u64_arg(&args[1])?;
    let mut pattern = None;
    let mut count = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = ascii_upper(&args[i]);
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                pattern = Some(args[i].clone());
            }
            "COUNT" => {
                i += 1;
                count = parse_usize_arg(&args[i])?;
            }
            _ => return Err(CommandError::Syntax),
        }
        i += 1;
    }
    Ok(Command::SScan {
        key,
        cursor,
        pattern,
        count,
    })
}

pub fn command_keys(cmd: &Command) -> Vec<Bytes> {
    match cmd {
        Command::Del(ks) | Command::Exists(ks) | Command::MGet(ks) => ks.clone(),
        Command::MSet(pairs) | Command::MSetNx(pairs) => pairs.iter().map(|(k, _)| k.clone()).collect(),
        Command::LPush(k, _)
        | Command::RPush(k, _)
        | Command::HGetAll(k)
        | Command::HSet(k, _)
        | Command::HGet(k, _)
        | Command::HScan { key: k, .. }
        | Command::SScan { key: k, .. }
        | Command::SAdd(k, _) => {
            vec![k.clone()]
        }
        Command::Rename(a, b) | Command::RenameNx(a, b) => vec![a.clone(), b.clone()],
        Command::SInter(ks) | Command::SUnion(ks) | Command::SDiff(ks) => ks.clone(),
        Command::SInterStore(dst, ks) => {
            let mut v = vec![dst.clone()];
            v.extend(ks.iter().cloned());
            v
        }
        _ => extract_first_key(cmd),
    }
}

fn extract_first_key(cmd: &Command) -> Vec<Bytes> {
    macro_rules! one {
        ($k:expr) => {
            return vec![$k.clone()]
        };
    }
    match cmd {
        Command::Get(k)
        | Command::Set(k, _, _)
        | Command::SetNx(k, _)
        | Command::SetEx(k, _, _)
        | Command::PSetEx(k, _, _)
        | Command::GetSet(k, _)
        | Command::GetDel(k)
        | Command::GetEx(k, _)
        | Command::Type(k)
        | Command::Expire(k, _)
        | Command::PExpire(k, _)
        | Command::ExpireAt(k, _)
        | Command::PExpireAt(k, _)
        | Command::Ttl(k)
        | Command::PTtl(k)
        | Command::Persist(k)
        | Command::Incr(k)
        | Command::Decr(k)
        | Command::IncrBy(k, _)
        | Command::DecrBy(k, _)
        | Command::IncrByFloat(k, _)
        | Command::Append(k, _)
        | Command::StrLen(k)
        | Command::GetRange(k, _, _)
        | Command::SetRange(k, _, _)
        | Command::LPush(k, _)
        | Command::RPush(k, _)
        | Command::LPushX(k, _)
        | Command::RPushX(k, _)
        | Command::LPop(k, _)
        | Command::RPop(k, _)
        | Command::LLen(k)
        | Command::LIndex(k, _)
        | Command::LSet(k, _, _)
        | Command::LRange(k, _, _)
        | Command::LRem(k, _, _)
        | Command::LTrim(k, _, _)
        | Command::HSet(k, _)
        | Command::HSetNx(k, _, _)
        | Command::HGet(k, _)
        | Command::HMGet(k, _)
        | Command::HDel(k, _)
        | Command::HExists(k, _)
        | Command::HLen(k)
        | Command::HStrLen(k, _)
        | Command::HGetAll(k)
        | Command::HKeys(k)
        | Command::HVals(k)
        | Command::HIncrBy(k, _, _)
        | Command::HIncrByFloat(k, _, _)
        | Command::SAdd(k, _)
        | Command::SRem(k, _)
        | Command::SCard(k)
        | Command::SIsMember(k, _)
        | Command::SMIsMember(k, _)
        | Command::SMembers(k)
        | Command::SPop(k, _)
        | Command::SRandMember(k, _)
        | Command::ZAdd(k, _, _)
        | Command::ZRem(k, _)
        | Command::ZScore(k, _)
        | Command::ZMScore(k, _)
        | Command::ZCard(k)
        | Command::ZIncrBy(k, _, _)
        | Command::ZRange(k, _, _, _)
        | Command::ZRevRange(k, _, _, _)
        | Command::ZRank(k, _)
        | Command::ZRevRank(k, _)
        | Command::ZCount(k, _, _)
        | Command::ZPopMin(k, _)
        | Command::ZPopMax(k, _) => one!(k),
        Command::ZRangeByScore(k, _, _, _, _)
        | Command::ZRevRangeByScore(k, _, _, _, _) => one!(k),
        Command::HScan { key: k, .. } | Command::SScan { key: k, .. } => one!(k),
        Command::Rename(src, _) | Command::RenameNx(src, _) => one!(src),
        _ => vec![],
    }
}

pub fn route_class(cmd: &Command) -> RouteClass {
    match cmd {
        Command::Ping(_) | Command::Echo(_) | Command::Hello(_) | Command::Select(_)
        | Command::Quit | Command::Command(_) | Command::CommandCount | Command::ConfigGet(_) => {
            RouteClass::Local
        }
        Command::DbSize | Command::FlushDb | Command::FlushAll | Command::Info(_)
        | Command::Keys(_) | Command::RandomKey => RouteClass::Broadcast,
        Command::Scan { .. } => RouteClass::CursorTargeted,
        Command::Del(_) | Command::Exists(_) | Command::MGet(_) | Command::MSet(_)
        | Command::MSetNx(_) => RouteClass::MultiDecompose,
        Command::SInter(_) | Command::SUnion(_) | Command::SDiff(_)
        | Command::SInterStore(_, _) | Command::SUnionStore(_, _) | Command::SDiffStore(_, _) => {
            RouteClass::MultiGather
        }
        Command::Rename(_, _) | Command::RenameNx(_, _) => RouteClass::Key,
        _ => RouteClass::Key,
    }
}

pub fn is_write(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Set(_, _, _)
            | Command::Del(_)
            | Command::Incr(_)
            | Command::LPush(_, _)
            | Command::HSet(_, _)
            | Command::SAdd(_, _)
            | Command::ZAdd(_, _, _)
            | Command::FlushDb
            | Command::FlushAll
            | Command::Expire(_, _)
            | Command::MSet(_)
    )
}

pub fn parse_i64_arg(b: &Bytes) -> Result<i64, CommandError> {
    let s = std::str::from_utf8(b).map_err(|_| CommandError::NotInteger)?;
    s.parse().map_err(|_| CommandError::NotInteger)
}

pub fn parse_u64_arg(b: &Bytes) -> Result<u64, CommandError> {
    let s = std::str::from_utf8(b).map_err(|_| CommandError::NotInteger)?;
    s.parse().map_err(|_| CommandError::NotInteger)
}

pub fn parse_usize_arg(b: &Bytes) -> Result<usize, CommandError> {
    let v = parse_i64_arg(b)?;
    if v < 0 {
        return Err(CommandError::NotInteger);
    }
    Ok(v as usize)
}

pub fn parse_f64_arg(b: &Bytes) -> Result<f64, CommandError> {
    let s = std::str::from_utf8(b).map_err(|_| CommandError::NotFloat)?;
    let v: f64 = s.parse().map_err(|_| CommandError::NotFloat)?;
    if v.is_nan() || v.is_infinite() {
        Err(CommandError::NotFloat)
    } else {
        Ok(v)
    }
}

pub fn parse_pairs(args: &[Bytes]) -> Result<Vec<(Bytes, Bytes)>, CommandError> {
    if args.len() % 2 != 0 {
        return Err(CommandError::Syntax);
    }
    let mut out = Vec::with_capacity(args.len() / 2);
    let mut i = 0;
    while i < args.len() {
        out.push((args[i].clone(), args[i + 1].clone()));
        i += 2;
    }
    Ok(out)
}

pub fn ascii_upper(b: &Bytes) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

pub fn eq_ignore_case(a: &Bytes, b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

pub static COMMAND_TABLE: &[CommandSpec] = &[
    CommandSpec { name: "PING", min_arity: 1, max_arity: 2, route: RouteClass::Local, write: false },
    CommandSpec { name: "ECHO", min_arity: 2, max_arity: 2, route: RouteClass::Local, write: false },
    CommandSpec { name: "HELLO", min_arity: 1, max_arity: 2, route: RouteClass::Local, write: false },
    CommandSpec { name: "SELECT", min_arity: 2, max_arity: 2, route: RouteClass::Local, write: false },
    CommandSpec { name: "QUIT", min_arity: 1, max_arity: 1, route: RouteClass::Local, write: false },
    CommandSpec { name: "COMMAND", min_arity: 1, max_arity: -1, route: RouteClass::Local, write: false },
    CommandSpec { name: "CONFIG", min_arity: 3, max_arity: 3, route: RouteClass::Local, write: false },
    CommandSpec { name: "DBSIZE", min_arity: 1, max_arity: 1, route: RouteClass::Broadcast, write: false },
    CommandSpec { name: "FLUSHDB", min_arity: 1, max_arity: 1, route: RouteClass::Broadcast, write: true },
    CommandSpec { name: "FLUSHALL", min_arity: 1, max_arity: 1, route: RouteClass::Broadcast, write: true },
    CommandSpec { name: "INFO", min_arity: 1, max_arity: 2, route: RouteClass::Broadcast, write: false },
    CommandSpec { name: "GET", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "SET", min_arity: 3, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "DEL", min_arity: 2, max_arity: -1, route: RouteClass::MultiDecompose, write: true },
    CommandSpec { name: "MGET", min_arity: 2, max_arity: -1, route: RouteClass::MultiDecompose, write: false },
    CommandSpec { name: "MSET", min_arity: 3, max_arity: -1, route: RouteClass::MultiDecompose, write: true },
    CommandSpec { name: "INCR", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: true },
    CommandSpec { name: "LPUSH", min_arity: 3, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "HSET", min_arity: 4, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "SADD", min_arity: 3, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "ZADD", min_arity: 4, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "SCAN", min_arity: 2, max_arity: -1, route: RouteClass::CursorTargeted, write: false },
    CommandSpec { name: "EXPIRE", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: true },
    CommandSpec { name: "TTL", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "TYPE", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "EXISTS", min_arity: 2, max_arity: -1, route: RouteClass::MultiDecompose, write: false },
    CommandSpec { name: "KEYS", min_arity: 2, max_arity: 2, route: RouteClass::Broadcast, write: false },
    CommandSpec { name: "RENAME", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: true },
    CommandSpec { name: "RENAMENX", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: true },
    CommandSpec { name: "MSETNX", min_arity: 3, max_arity: -1, route: RouteClass::MultiDecompose, write: true },
    CommandSpec { name: "SMEMBERS", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "SINTER", min_arity: 2, max_arity: -1, route: RouteClass::MultiGather, write: false },
    CommandSpec { name: "SUNION", min_arity: 2, max_arity: -1, route: RouteClass::MultiGather, write: false },
    CommandSpec { name: "SDIFF", min_arity: 2, max_arity: -1, route: RouteClass::MultiGather, write: false },
    CommandSpec { name: "SINTERSTORE", min_arity: 3, max_arity: -1, route: RouteClass::MultiGather, write: true },
    CommandSpec { name: "SUNIONSTORE", min_arity: 3, max_arity: -1, route: RouteClass::MultiGather, write: true },
    CommandSpec { name: "SDIFFSTORE", min_arity: 3, max_arity: -1, route: RouteClass::MultiGather, write: true },
    CommandSpec { name: "SSCAN", min_arity: 3, max_arity: -1, route: RouteClass::Key, write: false },
    CommandSpec { name: "HSET", min_arity: 4, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "HGET", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: false },
    CommandSpec { name: "HGETALL", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "HSCAN", min_arity: 3, max_arity: -1, route: RouteClass::Key, write: false },
    CommandSpec { name: "HLEN", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZRANGE", min_arity: 4, max_arity: 4, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZRANGEBYSCORE", min_arity: 4, max_arity: -1, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZREVRANGEBYSCORE", min_arity: 4, max_arity: -1, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZCARD", min_arity: 2, max_arity: 2, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZRANK", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZREVRANK", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZSCORE", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: false },
    CommandSpec { name: "ZCOUNT", min_arity: 4, max_arity: 4, route: RouteClass::Key, write: false },
    CommandSpec { name: "APPEND", min_arity: 3, max_arity: 3, route: RouteClass::Key, write: true },
    CommandSpec { name: "GETRANGE", min_arity: 4, max_arity: 4, route: RouteClass::Key, write: false },
    CommandSpec { name: "RPUSH", min_arity: 3, max_arity: -1, route: RouteClass::Key, write: true },
    CommandSpec { name: "LRANGE", min_arity: 4, max_arity: 4, route: RouteClass::Key, write: false },
];