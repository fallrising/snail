use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use ahash::RandomState;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::command::apply;
use crate::command::set::{set_op, SetOp};
use crate::command::{command_keys, route_class, Command, RouteClass};
use crate::config::Config;
use crate::error::CommandError;
use crate::protocol::frame::Reply;
use crate::runtime::router::{ShardClient, ShardMap};
use crate::storage::shard::{decode_scan_cursor, encode_scan_cursor, Shard};
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
            RouteClass::MultiDecompose | RouteClass::MultiGather => {
                let (tx, rx) = oneshot::channel();
                let d = self.clone_for_async();
                let is_gather = matches!(route_class(&cmd), RouteClass::MultiGather);
                tokio::task::spawn_local(async move {
                    let result = if is_gather {
                        multi_gather_async(&d, cmd).await
                    } else {
                        multi_decompose_async(&d, cmd).await
                    };
                    let _ = tx.send(result);
                });
                DispatchResult::Pending(rx)
            }
            RouteClass::Broadcast => DispatchResult::Immediate(self.dispatch_broadcast(cmd)),
            RouteClass::CursorTargeted => DispatchResult::Immediate(self.dispatch_scan(cmd)),
        }
    }

    fn clone_for_async(&self) -> Dispatcher {
        Dispatcher {
            worker_id: self.worker_id,
            shard_map: self.shard_map.clone(),
            shard_client: self.shard_client.clone(),
            local_shards: self.local_shards.clone(),
            config: self.config.clone(),
            info: self.info.clone(),
            now_ms: self.now_ms,
        }
    }

    fn dispatch_key(&self, cmd: Command) -> DispatchResult {
        if let Command::Rename(src, dst) | Command::RenameNx(src, dst) = &cmd {
            let src_shard = self.shard_map.shard_of(src);
            let dst_shard = self.shard_map.shard_of(dst);
            if src_shard != dst_shard {
                return DispatchResult::Immediate(crossslot_reply());
            }
        }

        let key = command_keys(&cmd)
            .into_iter()
            .next()
            .expect("key command");
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
        let (next_local, keys) = shard.scan_step(local_cursor, count, pat.as_deref());
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

async fn send_shard(d: &Dispatcher, shard_id: usize, cmd: Command) -> Reply {
    if d.shard_map.owner_of(shard_id) == d.worker_id {
        d.apply_local(shard_id, cmd)
    } else {
        let rx = d.shard_client.send_to(shard_id, cmd);
        rx.await.unwrap_or_else(|_| shard_unavailable())
    }
}

async fn multi_decompose_async(d: &Dispatcher, cmd: Command) -> Reply {
    match cmd {
        Command::MGet(keys) => {
            let mut replies = Vec::with_capacity(keys.len());
            for k in keys {
                replies.push(send_shard(d, d.shard_map.shard_of(&k), Command::Get(k)).await);
            }
            Reply::Array(replies)
        }
        Command::MSet(pairs) => {
            for (k, v) in pairs {
                let shard_id = d.shard_map.shard_of(&k);
                send_shard(d, shard_id, Command::Set(k, v, Default::default())).await;
            }
            Reply::Ok
        }
        Command::MSetNx(pairs) => {
            for (k, _) in &pairs {
                let reply = send_shard(
                    d,
                    d.shard_map.shard_of(k),
                    Command::Exists(vec![k.clone()]),
                )
                .await;
                if let Reply::Int(n) = reply {
                    if n > 0 {
                        return Reply::Int(0);
                    }
                }
            }
            for (k, v) in pairs {
                let shard_id = d.shard_map.shard_of(&k);
                send_shard(d, shard_id, Command::Set(k, v, Default::default())).await;
            }
            Reply::Int(1)
        }
        Command::Del(keys) => {
            let mut groups: HashMap<usize, Vec<Bytes>> = HashMap::new();
            for k in keys {
                groups
                    .entry(d.shard_map.shard_of(&k))
                    .or_default()
                    .push(k);
            }
            let mut total = 0i64;
            for (shard_id, ks) in groups {
                let reply = send_shard(d, shard_id, Command::Del(ks)).await;
                if let Reply::Int(n) = reply {
                    total += n;
                }
            }
            Reply::Int(total)
        }
        Command::Exists(keys) => {
            let mut groups: HashMap<usize, Vec<Bytes>> = HashMap::new();
            for k in keys {
                groups
                    .entry(d.shard_map.shard_of(&k))
                    .or_default()
                    .push(k);
            }
            let mut total = 0i64;
            for (shard_id, ks) in groups {
                let reply = send_shard(d, shard_id, Command::Exists(ks)).await;
                if let Reply::Int(n) = reply {
                    total += n;
                }
            }
            Reply::Int(total)
        }
        other => d.apply_local(0, other),
    }
}

async fn multi_gather_async(d: &Dispatcher, cmd: Command) -> Reply {
    match cmd {
        Command::SInter(keys) => set_gather_async(d, keys, SetOp::Inter).await,
        Command::SUnion(keys) => set_gather_async(d, keys, SetOp::Union).await,
        Command::SDiff(keys) => set_gather_async(d, keys, SetOp::Diff).await,
        Command::SInterStore(dst, keys) => {
            set_store_async(d, dst, keys, SetOp::Inter).await
        }
        Command::SUnionStore(dst, keys) => {
            set_store_async(d, dst, keys, SetOp::Union).await
        }
        Command::SDiffStore(dst, keys) => set_store_async(d, dst, keys, SetOp::Diff).await,
        other => d.apply_local(0, other),
    }
}

async fn set_gather_async(d: &Dispatcher, keys: Vec<Bytes>, op: SetOp) -> Reply {
    let sets = match fetch_sets(d, &keys).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let result = set_op(sets, op);
    Reply::Array(result.into_iter().map(|m| Reply::Bulk(m)).collect())
}

async fn set_store_async(d: &Dispatcher, dst: Bytes, keys: Vec<Bytes>, op: SetOp) -> Reply {
    let sets = match fetch_sets(d, &keys).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let result = set_op(sets, op);
    let count = result.len() as i64;
    let members: Vec<Bytes> = result.into_iter().collect();
    let dst_shard = d.shard_map.shard_of(&dst);
    send_shard(d, dst_shard, Command::Del(vec![dst.clone()])).await;
    let reply = send_shard(d, dst_shard, Command::SAdd(dst, members)).await;
    match reply {
        Reply::Err(_, _) => reply,
        _ => Reply::Int(count),
    }
}

async fn fetch_sets(
    d: &Dispatcher,
    keys: &[Bytes],
) -> Result<Vec<HashSet<Bytes, RandomState>>, Reply> {
    let mut sets = Vec::with_capacity(keys.len());
    for k in keys {
        let shard_id = d.shard_map.shard_of(k);
        let reply = send_shard(d, shard_id, Command::SMembers(k.clone())).await;
        match reply {
            Reply::Array(items) => {
                let mut hs = HashSet::with_hasher(RandomState::new());
                for item in items {
                    if let Reply::Bulk(b) = item {
                        hs.insert(b);
                    }
                }
                sets.push(hs);
            }
            Reply::Err(_, _) => return Err(reply),
            _ => sets.push(HashSet::with_hasher(RandomState::new())),
        }
    }
    Ok(sets)
}

fn crossslot_reply() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::Generic,
        CommandError::CrossSlot.to_resp(),
    )
}

fn shard_unavailable() -> Reply {
    Reply::Err(
        crate::protocol::frame::CommandErrKind::Generic,
        "shard unavailable".into(),
    )
}

pub async fn dispatch_async(dispatcher: &Dispatcher, cmd: Command) -> Reply {
    match dispatcher.dispatch(cmd) {
        DispatchResult::Immediate(r) => r,
        DispatchResult::Pending(rx) => rx.await.unwrap_or_else(|_| shard_unavailable()),
    }
}