use bytes::Bytes;

use crate::command::{lookup_spec, CommandSpec, RouteClass, COMMAND_TABLE};
use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::storage::shard::Shard;
use crate::telemetry::ServerInfo;

pub fn apply_ping(msg: Option<Bytes>) -> Reply {
    match msg {
        Some(m) => Reply::Bulk(m),
        None => Reply::Simple("PONG".into()),
    }
}

pub fn apply_echo(msg: Bytes) -> Reply {
    Reply::Bulk(msg)
}

pub fn apply_hello() -> Reply {
    Reply::Array(vec![
        Reply::Bulk(Bytes::from("server")),
        Reply::Bulk(Bytes::from("rudis")),
        Reply::Bulk(Bytes::from("version")),
        Reply::Bulk(Bytes::from("0.1.0")),
        Reply::Bulk(Bytes::from("proto")),
        Reply::Int(2),
    ])
}

pub fn apply_command_count() -> Reply {
    Reply::Int(COMMAND_TABLE.len() as i64)
}

pub fn apply_command_list() -> Reply {
    Reply::Array(
        COMMAND_TABLE
            .iter()
            .map(command_info)
            .collect(),
    )
}

pub fn apply_command_info(name: &Bytes) -> Reply {
    let upper = String::from_utf8_lossy(name).to_ascii_uppercase();
    match lookup_spec(&upper) {
        Some(spec) => Reply::Array(vec![command_info(spec)]),
        None => Reply::Array(vec![]),
    }
}

fn command_info(spec: &CommandSpec) -> Reply {
    let arity = if spec.max_arity < 0 {
        -(spec.min_arity as i64)
    } else {
        spec.max_arity as i64
    };
    let flags = if spec.write {
        vec![Reply::Bulk(Bytes::from("write"))]
    } else {
        vec![Reply::Bulk(Bytes::from("readonly"))]
    };
    let (firstkey, lastkey, step) = key_positions(spec);
    Reply::Array(vec![
        Reply::Bulk(Bytes::from(spec.name)),
        Reply::Int(arity),
        Reply::Array(flags),
        Reply::Int(firstkey),
        Reply::Int(lastkey),
        Reply::Int(step),
    ])
}

fn key_positions(spec: &CommandSpec) -> (i64, i64, i64) {
    match spec.route {
        RouteClass::Key => (1, 1, 1),
        RouteClass::MultiDecompose => {
            if spec.name == "MSET" || spec.name == "MSETNX" {
                (1, -1, 2)
            } else {
                (1, -1, 1)
            }
        }
        RouteClass::MultiGather => {
            if spec.name.ends_with("STORE") {
                (1, -1, 1)
            } else {
                (1, -1, 1)
            }
        }
        _ => (0, 0, 0),
    }
}

pub fn apply_config_get(pattern: &Bytes, config: &Config) -> Reply {
    let pat = String::from_utf8_lossy(pattern);
    let mut pairs = Vec::new();
    let entries = [
        ("bind", config.bind.clone()),
        ("port", config.port.to_string()),
        ("workers", config.workers.to_string()),
        ("shards", config.shards.to_string()),
        ("maxclients", config.maxclients.to_string()),
        ("maxmemory", config.maxmemory.to_string()),
    ];
    for (k, v) in entries {
        if pat == "*" || pat == k {
            pairs.push(Reply::Bulk(Bytes::from(k)));
            pairs.push(Reply::Bulk(Bytes::from(v)));
        }
    }
    Reply::Array(pairs)
}

pub fn apply_db_size(shard: &Shard) -> Reply {
    Reply::Int(shard.len() as i64)
}

pub fn apply_flush(shard: &mut Shard) -> Reply {
    shard.flush();
    Reply::Ok
}

pub fn build_info(info: &ServerInfo, section: Option<&Bytes>) -> Reply {
    let text = info.format(section);
    Reply::Bulk(Bytes::from(text))
}