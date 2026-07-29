use std::cell::RefCell;
use std::io::ErrorKind;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use crate::command::apply;
use crate::net::buffer::BufferPool;
use crate::net::connection::{ConnContext, Connection, DriveResult};
use crate::runtime::router::ShardRequest;
use crate::runtime::worker::{current_ms, WorkerContext};
use crate::storage::shard::Shard;

const TOKEN_LISTENER: Token = Token(0);
const TOKEN_WAKE: Token = Token(1);
const CONN_TOKEN_BASE: usize = 2;

/// Per-worker mio/epoll reactor: only ready FDs are driven (O(ready), not O(conns)).
pub async fn run(
    ctx: WorkerContext,
    mut request_rx: mpsc::Receiver<ShardRequest>,
    shard_range: std::ops::Range<usize>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let addr = ctx.config.socket_addr().expect("valid addr");
    let mut listener = crate::net::listener::bind_reuseport_mio(addr, ctx.config.tcp_backlog)
        .expect("bind");
    info!(worker = ctx.worker_id, %addr, "listening");

    let mut poll = Poll::new().expect("mio poll");
    let mut events = Events::with_capacity(1024);
    poll.registry()
        .register(&mut listener, TOKEN_LISTENER, Interest::READABLE)
        .expect("register listener");
    let waker = mio::Waker::new(poll.registry(), TOKEN_WAKE).expect("mio waker");
    // Expose waker to ShardClient so remote workers can unblock us.
    ctx.shard_client.register_waker(ctx.worker_id, std::sync::Arc::new(waker));

    let pool = Rc::new(BufferPool::default());
    let conn_ctx = ConnContext {
        worker_id: ctx.worker_id,
        config: ctx.config.clone(),
        shard_map: ctx.shard_map.clone(),
        shard_client: ctx.shard_client.clone(),
        local_shards: ctx.local_shards.clone(),
        local_shard_base: shard_range.start,
        info: ctx.info.clone(),
        conn_count: ctx.conn_count.clone(),
        now_ms: ctx.now_ms.clone(),
    };

    let mut conns: Vec<Option<Connection>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    // Indices of connections waiting on cross-shard oneshots (avoids O(n) scan).
    let mut async_waiters: Vec<usize> = Vec::new();
    let mut accepting = true;
    let mut drain_deadline: Option<Instant> = None;
    let mut last_expire = Instant::now();
    let mut last_time_refresh = Instant::now();
    let expire_every = Duration::from_millis(ctx.config.expire_interval_ms.max(1));
    let shutdown_grace = Duration::from_secs(ctx.config.shutdown_deadline_secs.max(1));

    loop {
        // Refresh coarse time at most once per ms (avoid SystemTime per turn).
        if last_time_refresh.elapsed() >= Duration::from_millis(1) {
            *ctx.now_ms.borrow_mut() = current_ms();
            last_time_refresh = Instant::now();
        }

        // 1) Apply inbound shard requests (folded executor).
        drain_shard_requests(
            &mut request_rx,
            &ctx.local_shards,
            &ctx.config,
            &ctx.info,
            &ctx.now_ms,
            &shard_range,
        );

        // 2) Active expire ticker.
        if last_expire.elapsed() >= expire_every {
            *ctx.now_ms.borrow_mut() = current_ms();
            let now = *ctx.now_ms.borrow();
            let mut guard = ctx.local_shards.borrow_mut();
            for shard in guard.iter_mut() {
                shard.active_expire(now, ctx.config.expire_budget);
            }
            last_expire = Instant::now();
        }

        // 3) Shutdown signal.
        match shutdown_rx.try_recv() {
            Ok(()) | Err(broadcast::error::TryRecvError::Lagged(_)) => {
                if accepting {
                    accepting = false;
                    drain_deadline = Some(Instant::now() + shutdown_grace);
                    tracing::info!(worker = ctx.worker_id, "shutdown: draining connections");
                    let _ = poll.registry().deregister(&mut listener);
                    // Immediately close all connections (flush-best-effort already done
                    // on prior turns; under SIGTERM we prefer fast exit).
                    let len = conns.len();
                    for idx in 0..len {
                        if conns[idx].is_some() {
                            remove_conn(&mut poll, &mut conns, &mut free, idx);
                        }
                    }
                }
            }
            Err(broadcast::error::TryRecvError::Empty) => {}
            Err(broadcast::error::TryRecvError::Closed) => {
                if accepting {
                    accepting = false;
                    drain_deadline = Some(Instant::now() + shutdown_grace);
                }
            }
        }

        // 4) Poll ready sockets (non-blocking).
        if let Err(e) = poll.poll(&mut events, Some(Duration::ZERO)) {
            if e.kind() != ErrorKind::Interrupted {
                tracing::warn!("mio poll error: {e}");
            }
        }

        let mut did_work = false;
        for event in events.iter() {
            did_work = true;
            match event.token() {
                TOKEN_LISTENER if accepting => {
                    accept_ready(
                        &mut listener,
                        &mut poll,
                        &mut conns,
                        &mut free,
                        &ctx,
                        &conn_ctx,
                        &pool,
                    );
                }
                TOKEN_LISTENER | TOKEN_WAKE => {}
                token => {
                    let idx = token.0.saturating_sub(CONN_TOKEN_BASE);
                    if idx >= conns.len() || conns[idx].is_none() {
                        continue;
                    }
                    let readable = event.is_readable();
                    let writable = event.is_writable();
                    let closed = {
                        let conn = conns[idx].as_mut().unwrap();
                        matches!(conn.drive(readable, writable), DriveResult::Closed)
                    };
                    if closed {
                        remove_conn(&mut poll, &mut conns, &mut free, idx);
                    } else {
                        track_async_waiter(&mut conns, &mut async_waiters, idx);
                        if let Some(conn) = conns[idx].as_mut() {
                            reregister(&mut poll, conn);
                        }
                    }
                }
            }
        }

        // 5) Harvest async replies only for known waiters (not O(conns)).
        let mut wi = 0;
        while wi < async_waiters.len() {
            let idx = async_waiters[wi];
            let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                async_waiters.swap_remove(wi);
                continue;
            };
            if !conn.has_pending_async() {
                conn.in_async_list = false;
                async_waiters.swap_remove(wi);
                continue;
            }
            if conn.poll_async() {
                did_work = true;
                let closed = matches!(conn.drive(false, true), DriveResult::Closed);
                if closed {
                    remove_conn(&mut poll, &mut conns, &mut free, idx);
                    async_waiters.swap_remove(wi);
                    continue;
                }
                if let Some(conn) = conns[idx].as_mut() {
                    reregister(&mut poll, conn);
                    if !conn.has_pending_async() {
                        conn.in_async_list = false;
                        async_waiters.swap_remove(wi);
                        continue;
                    }
                }
            }
            wi += 1;
        }

        // 6) Drain complete?
        if !accepting {
            let live = conns.iter().filter(|c| c.is_some()).count();
            let timed_out = drain_deadline
                .map(|d| Instant::now() >= d)
                .unwrap_or(false);
            if live == 0 || timed_out {
                tracing::info!(
                    worker = ctx.worker_id,
                    live,
                    timed_out,
                    "shutdown: worker exit"
                );
                return;
            }
            if timed_out {
                conns.iter_mut().for_each(|c| *c = None);
                return;
            }
        }

        // 7) Yield only when multi-gather / oneshot tasks need the LocalSet.
        if !async_waiters.is_empty() {
            tokio::task::yield_now().await;
            continue;
        }

        // 8) Idle wait: block in mio (no tokio sleep) so we wake on the next FD event
        // without a 50µs polling floor. Safe for the C10K local GET/SET path.
        if !did_work {
            if let Err(e) = poll.poll(&mut events, Some(Duration::from_millis(1))) {
                if e.kind() != ErrorKind::Interrupted {
                    tracing::warn!("mio poll error: {e}");
                }
            }
            // Process any events that arrived during the blocking wait before looping
            // back through shard/expire bookkeeping.
            for event in events.iter() {
                match event.token() {
                    TOKEN_LISTENER if accepting => {
                        accept_ready(
                            &mut listener,
                            &mut poll,
                            &mut conns,
                            &mut free,
                            &ctx,
                            &conn_ctx,
                            &pool,
                        );
                    }
                    TOKEN_LISTENER | TOKEN_WAKE => {}
                    token => {
                        let idx = token.0.saturating_sub(CONN_TOKEN_BASE);
                        if idx >= conns.len() || conns[idx].is_none() {
                            continue;
                        }
                        let readable = event.is_readable();
                        let writable = event.is_writable();
                        let closed = {
                            let conn = conns[idx].as_mut().unwrap();
                            matches!(conn.drive(readable, writable), DriveResult::Closed)
                        };
                        if closed {
                            remove_conn(&mut poll, &mut conns, &mut free, idx);
                        } else {
                            track_async_waiter(&mut conns, &mut async_waiters, idx);
                            if let Some(conn) = conns[idx].as_mut() {
                                reregister(&mut poll, conn);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn drain_shard_requests(
    rx: &mut mpsc::Receiver<ShardRequest>,
    shards: &Rc<RefCell<Vec<Shard>>>,
    config: &Rc<crate::config::Config>,
    info: &Rc<crate::telemetry::ServerInfo>,
    now_ms: &Rc<RefCell<u64>>,
    range: &std::ops::Range<usize>,
) {
    while let Ok(req) = rx.try_recv() {
        let mut batch = vec![req];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }
        let now = *now_ms.borrow();
        let mut guard = shards.borrow_mut();
        for req in batch {
            let local_idx = req.shard_id.saturating_sub(range.start);
            let len = guard.len();
            let shard = &mut guard[local_idx.min(len.saturating_sub(1))];
            let reply = apply::apply(shard, req.cmd, now, config, info);
            let _ = req.reply.send(reply);
        }
    }
}

fn accept_ready(
    listener: &mut TcpListener,
    poll: &mut Poll,
    conns: &mut Vec<Option<Connection>>,
    free: &mut Vec<usize>,
    ctx: &WorkerContext,
    conn_ctx: &ConnContext,
    pool: &Rc<BufferPool>,
) {
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let current = ctx.conn_count.fetch_add(1, Ordering::Relaxed) + 1;
                let idx = if let Some(i) = free.pop() {
                    i
                } else {
                    let i = conns.len();
                    conns.push(None);
                    i
                };
                let token = Token(idx + CONN_TOKEN_BASE);
                let interest = if current > ctx.config.maxclients {
                    ctx.conn_count.fetch_sub(1, Ordering::Relaxed);
                    Interest::WRITABLE
                } else {
                    Interest::READABLE
                };
                if let Err(e) = poll.registry().register(&mut stream, token, interest) {
                    tracing::debug!("register conn failed: {e}");
                    if current <= ctx.config.maxclients {
                        ctx.conn_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    free.push(idx);
                    continue;
                }
                let conn = if current > ctx.config.maxclients {
                    Connection::rejected(stream, conn_ctx.clone(), pool.clone(), token)
                } else {
                    Connection::new(stream, conn_ctx.clone(), pool.clone(), token)
                };
                conns[idx] = Some(conn);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                break;
            }
        }
    }
}

fn track_async_waiter(
    conns: &mut [Option<Connection>],
    waiters: &mut Vec<usize>,
    idx: usize,
) {
    let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
        return;
    };
    if conn.has_pending_async() && !conn.in_async_list {
        conn.in_async_list = true;
        waiters.push(idx);
    }
}

fn remove_conn(
    poll: &mut Poll,
    conns: &mut Vec<Option<Connection>>,
    free: &mut Vec<usize>,
    idx: usize,
) {
    if let Some(mut conn) = conns[idx].take() {
        let _ = poll.registry().deregister(&mut conn.stream);
        drop(conn);
        free.push(idx);
    }
}

fn reregister(poll: &mut Poll, conn: &mut Connection) {
    let want = conn.desired_interest();
    if want != conn.registered_interest() {
        if let Err(e) = poll
            .registry()
            .reregister(&mut conn.stream, conn.token, want)
        {
            tracing::debug!("reregister failed: {e}");
        } else {
            conn.set_registered(want);
        }
    }
}
