use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::command::apply;
use crate::command::set::{get_set_members, set_op, SetOp};
use crate::command::string;
use crate::command::{route_class, Command, RouteClass};
use crate::config::Config;
use crate::protocol::frame::Reply;
use crate::runtime::router::{ShardClient, ShardMap};
use crate::storage::shard::{decode_scan_cursor, encode_scan_cursor, Shard};
use crate::storage::value::Value;
use crate::telemetry::ServerInfo;

pub enum DispatchResult {
    Immediate(Reply),
    Pending(oneshot::Receiver<Reply>),
}

pub struct Dispatcher {
    pub worker_id: usize,
    pub shard_map: Arc<ShardMap>,
    pub shard_client: ShardClient,
    pub local_shards: Rc<RefCell<Vec<Shard>>>,
    pub config: Rc<Config>,
    pub info: Rc<ServerInfo>,
    pub now_ms: u64,
}

impl Dispatcher {
    pub fn dispatch(&self, cmd: Command) -> DispatchResult {
        match route_class(&cmd) {
            RouteClass::Local => DispatchResult::Immediate(self.exec_local(cmd)),
            RouteClass::Key => self.dispatch_key(cmd),
            RouteClass::MultiDecompose => {
                DispatchResult::Immediate(self.dispatch_multi_decompose(cmd))
            }
            RouteClass::MultiGather => {
                DispatchResult::Immediate(self.dispatch_multi_gather(cmd))
            }
            RouteClass::Broadcast => DispatchResult::Immediate(self.dispatch_broadcast(cmd)),
            RouteClass::CursorTargeted => DispatchResult::Immediate(self.dispatch_scan(cmd)),
        }
    }

    fn dispatch_key(&self, cmd: Command) -> DispatchResult {
        let key = first_key(&cmd).expect("key command");
        let shard_id = self.shard_map.shard_of(&key);
        if self.shard_map.owner_of(shard_id) == self.worker_id {
            DispatchResult::Immediate(self.apply_local(shard_id, cmd))
        } else {
            let rx = self.shard_client.send_to(shard_id, cmd);
            DispatchResult::Pending(rx)
        }
    }

    fn apply_local(&self, shard_id: usize, cmd: Command) -> Reply {
        let mut shards = self.local_shards.borrow_mut();
        let idx = shard_id % shards.len();
        let shard = &mut shards[idx];
        apply::apply(shard, cmd, self.now_ms, &self.config, &self.info)
    }

    fn exec_local(&self, cmd: Command) -> Reply {
        match cmd {
            Command::RandomKey => {
                let shards = self.local_shards.borrow();
                for shard in shards.iter() {
                    if let Some(k) = shard.random_key() {
                        return Reply::Bulk(k);
                    }
                }
                Reply::NullBulk
            }
            _ => {
                let shard_id = self.shard_map.local_shard_index(self.worker_id);
                self.apply_local(shard_id, cmd)
            }
        }
    }

    fn dispatch_multi_decompose(&self, cmd: Command) -> Reply {
        match cmd {
            Command::MGet(keys) => {
                let mut replies = Vec::with_capacity(keys.len());
                for k in &keys {
                    let sub = Command::Get(k.clone());
                    let reply = match self.dispatch(sub) {
                        DispatchResult::Immediate(r) => r,
                        DispatchResult::Pending(_) => Reply::NullBulk,
                    };
                    replies.push(reply);
                }
                Reply::Array(replies)
            }
            Command::MSet(pairs) => {
                for (k, v) in pairs {
                    let sub = Command::Set(k, v, Default::default());
                    match self.dispatch(sub) {
                        DispatchResult::Immediate(_) => {}
                        _ => {}
                    }
                }
                Reply::Ok
            }
            Command::Del(keys) => {
                let mut total = 0i64;
                let mut groups: HashMap<usize, Vec<Bytes>> = HashMap::new();
                for k in keys {
                    groups
                        .entry(self.shard_map.shard_of(&k))
                        .or_default()
                        .push(k);
                }
                for (shard_id, ks) in groups {
                    let reply = if self.shard_map.owner_of(shard_id) == self.worker_id {
                        self.apply_local(shard_id, Command::Del(ks))
                    } else {
                        // sync wait not available here - use local path only in sync context
                        self.apply_local(shard_id, Command::Del(ks))
                    };
                    if let Reply::Int(n) = reply {
                        total += n;
                    }
                }
                Reply::Int(total)
            }
            Command::Exists(keys) => {
                let mut total = 0i64;
                for k in keys {
                    let shard_id = self.shard_map.shard_of(&k);
                    let reply = self.apply_local(shard_id, Command::Exists(vec![k]));
                    if let Reply::Int(n) = reply {
                        total += n;
                    }
                }
                Reply::Int(total)
            }
            other => self.apply_local(0, other),
        }
    }

    fn dispatch_multi_gather(&self, cmd: Command) -> Reply {
        match cmd {
            Command::SInter(keys) => self.set_gather(keys, SetOp::Inter, None),
            Command::SUnion(keys) => self.set_gather(keys, SetOp::Union, None),
            Command::SDiff(keys) => self.set_gather(keys, SetOp::Diff, None),
            Command::SInterStore(dst, keys) => {
                let result = self.set_gather(keys, SetOp::Inter, None);
                if let Reply::Array(items) = result {
                    let members: Vec<Bytes> = items
                        .into_iter()
                        .filter_map(|r| match r {
                            Reply::Bulk(b) => Some(b),
                            _ => None,
                        })
                        .collect();
                    let _ = self.dispatch(Command::Del(vec![dst.clone()]));
                    let reply = self.apply_local(
                        self.shard_map.shard_of(&dst),
                        Command::SAdd(dst, members),
                    );
                    if let Reply::Int(n) = reply {
                        Reply::Int(n)
                    } else {
                        reply
                    }
                } else {
                    result
                }
            }
            other => self.apply_local(0, other),
        }
    }

    fn set_gather(&self, keys: Vec<Bytes>, op: SetOp, _dst: Option<Bytes>) -> Reply {
        let mut sets = Vec::new();
        for k in &keys {
            let shard_id = self.shard_map.shard_of(k);
            let reply = self.apply_local(shard_id, Command::SMembers(k.clone()));
            if let Reply::Array(items) = reply {
                let mut set = ahash::RandomState::new();
                let mut hs = std::collections::HashSet::with_hasher(set);
                for item in items {
                    if let Reply::Bulk(b) = item {
                        hs.insert(b);
                    }
                }
                sets.push(hs);
            } else if let Reply::Err(_, _) = reply {
                return reply;
            }
        }
        let result = set_op(sets, op);
        Reply::Array(result.into_iter().map(|m| Reply::Bulk(m)).collect())
    }

    fn dispatch_broadcast(&self, cmd: Command) -> Reply {
        match cmd {
            Command::DbSize => {
                let shards = self.local_shards.borrow();
                let total: i64 = shards.iter().map(|s| s.len() as i64).sum();
                Reply::Int(total)
            }
            Command::FlushDb | Command::FlushAll => {
                let mut shards = self.local_shards.borrow_mut();
                for shard in shards.iter_mut() {
                    shard.flush();
                }
                Reply::Ok
            }
            Command::Keys(pat) => {
                let pattern = String::from_utf8_lossy(&pat).into_owned();
                let shards = self.local_shards.borrow();
                let mut keys = Vec::new();
                for shard in shards.iter() {
                    for k in shard.keys_matching(Some(&pattern)) {
                        keys.push(Reply::Bulk(k));
                    }
                }
                Reply::Array(keys)
            }
            Command::Info(section) => {
                let shards = self.local_shards.borrow();
                let mut info = (*self.info).clone();
                info.aggregate_shards(&shards);
                crate::command::server::build_info(&info, section.as_ref())
            }
            other => self.apply_local(0, other),
        }
    }

    fn dispatch_scan(&self, cmd: Command) -> Reply {
        let Command::Scan {
            cursor,
            pattern,
            count,
        } = cmd
        else {
            return Reply::Err(
                crate::protocol::frame::CommandErrKind::Generic,
                "invalid scan".into(),
            );
        };
        let (shard_id, local_cursor) =
            decode_scan_cursor(cursor, self.shard_map.num_shards());
        let pat = pattern.as_ref().map(|p| String::from_utf8_lossy(p).into_owned());
        let mut shards = self.local_shards.borrow_mut();
        let idx = shard_id % shards.len();
        let shard = &mut shards[idx];
        let (next_local, keys) = shard.scan_step(
            local_cursor,
            count,
            pat.as_deref(),
        );
        let next_shard = if next_local == 0 {
            (shard_id + 1) % self.shard_map.num_shards()
        } else {
            shard_id
        };
        let next_cursor = if next_local == 0 && next_shard == 0 && keys.is_empty() {
            0
        } else {
            encode_scan_cursor(
                if next_local == 0 { next_shard } else { shard_id },
                next_local,
                self.shard_map.num_shards(),
            )
        };
        let mut out = vec![Reply::Bulk(Bytes::from(next_cursor.to_string()))];
        let key_replies: Vec<Reply> = keys.into_iter().map(|k| Reply::Bulk(k)).collect();
        out.push(Reply::Array(key_replies));
        Reply::Array(out)
    }
}

fn first_key(cmd: &Command) -> Option<Bytes> {
    match cmd {
        Command::Get(k) | Command::Set(k, _, _) | Command::Type(k) => Some(k.clone()),
        Command::LPush(k, _) | Command::HGetAll(k) | Command::SAdd(k, _) | Command::ZAdd(k, _, _) => {
            Some(k.clone())
        }
        _ => None,
    }
}

pub async fn dispatch_async(dispatcher: &Dispatcher, cmd: Command) -> Reply {
    match dispatcher.dispatch(cmd) {
        DispatchResult::Immediate(r) => r,
        DispatchResult::Pending(rx) => rx.await.unwrap_or_else(|_| {
            Reply::Err(
                crate::protocol::frame::CommandErrKind::Generic,
                "shard unavailable".into(),
            )
        }),
    }
}