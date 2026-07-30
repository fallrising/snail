//! Completion-style io_uring reactor: always-in-flight Recv/Send on data sockets.
//!
//! Listener + cross-shard wake stay on mio/epoll. Accepted connections are NOT
//! registered with epoll; each live connection keeps a Recv outstanding (or a
//! Send while flushing). Enable with `RUDIS_IO_URING=1`.

use std::io::{self, ErrorKind};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use io_uring::{opcode, types, IoUring};
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

const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;
const MAX_IOV: usize = 16;
const RING_ENTRIES: u32 = 8192;

#[inline]
fn pack(op: u64, idx: u32) -> u64 {
    (op << 32) | idx as u64
}

#[inline]
fn unpack(ud: u64) -> (u64, u32) {
    (ud >> 32, ud as u32)
}

struct CompletionRing {
    ring: IoUring,
    /// Per-connection iovec scratch (stable while that conn's send is in flight).
    iov_by_conn: Vec<[libc::iovec; MAX_IOV]>,
    /// Conns that need a Recv SQE when ring space frees up.
    pending_recv: Vec<usize>,
    /// Conns that need a Send SQE.
    pending_send: Vec<usize>,
    inflight: usize,
}

impl CompletionRing {
    fn try_new() -> io::Result<Self> {
        let ring = IoUring::builder().build(RING_ENTRIES)?;
        Ok(Self {
            ring,
            iov_by_conn: Vec::new(),
            pending_recv: Vec::new(),
            pending_send: Vec::new(),
            inflight: 0,
        })
    }

    fn ensure_iov_slot(&mut self, idx: usize) {
        if self.iov_by_conn.len() <= idx {
            self.iov_by_conn.resize(
                idx + 1,
                [libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_IOV],
            );
        }
    }

    fn queue_recv(&mut self, idx: usize) {
        if !self.pending_recv.contains(&idx) {
            self.pending_recv.push(idx);
        }
    }

    fn queue_send(&mut self, idx: usize) {
        if !self.pending_send.contains(&idx) {
            self.pending_send.push(idx);
        }
    }

    fn flush_submissions(&mut self, conns: &mut [Option<Connection>]) {
        // Grow iov table first.
        let mut grow_to = self.iov_by_conn.len().saturating_sub(1);
        for &idx in self.pending_send.iter().chain(self.pending_recv.iter()) {
            grow_to = grow_to.max(idx);
        }
        if !self.pending_send.is_empty() || !self.pending_recv.is_empty() {
            self.ensure_iov_slot(grow_to);
        }

        let send_q = std::mem::take(&mut self.pending_send);
        let recv_q = std::mem::take(&mut self.pending_recv);
        let mut send_pos = 0usize;
        let mut recv_pos = 0usize;

        {
            let mut sq = self.ring.submission();
            while send_pos < send_q.len() {
                let idx = send_q[send_pos];
                send_pos += 1;
                let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                    continue;
                };
                if !conn.wants_uring_send() {
                    continue;
                }
                let iov = &mut self.iov_by_conn[idx];
                let n_iov = conn.fill_send_iovecs(iov);
                if n_iov == 0 {
                    continue;
                }
                let fd = types::Fd(conn.as_raw_fd());
                let entry = opcode::Writev::new(fd, iov.as_ptr(), n_iov as u32)
                    .build()
                    .user_data(pack(OP_SEND, idx as u32));
                if unsafe { sq.push(&entry) }.is_err() {
                    send_pos -= 1;
                    break;
                }
                conn.send_inflight = true;
                self.inflight += 1;
            }
            while recv_pos < recv_q.len() {
                let idx = recv_q[recv_pos];
                recv_pos += 1;
                let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                    continue;
                };
                if !conn.wants_uring_recv() {
                    continue;
                }
                let Some((ptr, len)) = conn.prepare_recv_buf() else {
                    continue;
                };
                if len == 0 {
                    continue;
                }
                let fd = types::Fd(conn.as_raw_fd());
                let entry = opcode::Recv::new(fd, ptr, len)
                    .build()
                    .user_data(pack(OP_RECV, idx as u32));
                if unsafe { sq.push(&entry) }.is_err() {
                    recv_pos -= 1;
                    break;
                }
                conn.recv_inflight = true;
                self.inflight += 1;
            }
        }

        if send_pos < send_q.len() {
            self.pending_send.extend_from_slice(&send_q[send_pos..]);
        }
        if recv_pos < recv_q.len() {
            self.pending_recv.extend_from_slice(&recv_q[recv_pos..]);
        }

        if self.inflight > 0 || !self.pending_send.is_empty() || !self.pending_recv.is_empty() {
            let _ = self.ring.submit();
        }
    }

    fn wait_cqe(&mut self, timeout_ns: u32) -> io::Result<()> {
        if self.inflight == 0 {
            return Ok(());
        }
        // Bounded wait so mio accept/wake still run (idle Recvs would otherwise park forever).
        let ts = types::Timespec::new().nsec(timeout_ns);
        let args = types::SubmitArgs::new().timespec(&ts);
        match self.ring.submitter().submit_with_args(1, &args) {
            Ok(_) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) if e.kind() == ErrorKind::Interrupted => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn drain_cqes(
        &mut self,
        conns: &mut [Option<Connection>],
        shards: &mut [Shard],
        to_close: &mut Vec<usize>,
        async_waiters: &mut Vec<usize>,
    ) {
        let mut cq = self.ring.completion();
        cq.sync();
        let mut requeue_send: Vec<usize> = Vec::new();
        let mut requeue_recv: Vec<usize> = Vec::new();
        for cqe in cq {
            if self.inflight > 0 {
                self.inflight -= 1;
            }
            let (op, idx) = unpack(cqe.user_data());
            let idx = idx as usize;
            let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                continue;
            };
            match op {
                OP_RECV => {
                    conn.recv_inflight = false;
                    match conn.complete_recv(cqe.result(), shards) {
                        DriveResult::Closed => {
                            to_close.push(idx);
                            continue;
                        }
                        DriveResult::Pending => {}
                    }
                    track_async(conn, async_waiters, idx);
                    if conn.wants_uring_send() {
                        requeue_send.push(idx);
                    } else if conn.wants_uring_recv() {
                        requeue_recv.push(idx);
                    } else if conn.should_close_now() {
                        to_close.push(idx);
                    }
                }
                OP_SEND => {
                    conn.send_inflight = false;
                    match conn.complete_send(cqe.result()) {
                        DriveResult::Closed => {
                            to_close.push(idx);
                            continue;
                        }
                        DriveResult::Pending => {}
                    }
                    if conn.wants_uring_send() {
                        requeue_send.push(idx);
                    } else if conn.wants_uring_recv() {
                        requeue_recv.push(idx);
                    } else if conn.should_close_now() {
                        to_close.push(idx);
                    }
                }
                _ => {}
            }
        }
        for idx in requeue_send {
            self.queue_send(idx);
        }
        for idx in requeue_recv {
            self.queue_recv(idx);
        }
    }
}

/// Probe whether an io_uring instance can be created (for listener fallback).
pub fn probe() -> io::Result<()> {
    let _ring: IoUring = IoUring::builder().build(32)?;
    Ok(())
}

fn track_async(conn: &mut Connection, waiters: &mut Vec<usize>, idx: usize) {
    if conn.has_pending_async() && !conn.in_async_list {
        conn.in_async_list = true;
        waiters.push(idx);
    }
}

/// Run the completion reactor until shutdown. Returns Err only if ring setup fails.
pub async fn run(
    ctx: WorkerContext,
    mut request_rx: mpsc::Receiver<ShardRequest>,
    shard_range: std::ops::Range<usize>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> io::Result<()> {
    let mut ring = CompletionRing::try_new()?;
    let addr = ctx.config.socket_addr().expect("valid addr");
    let mut listener = crate::net::listener::bind_reuseport_mio(addr, ctx.config.tcp_backlog)?;
    info!(worker = ctx.worker_id, %addr, "listening (io_uring completion)");

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);
    poll.registry()
        .register(&mut listener, TOKEN_LISTENER, Interest::READABLE)?;
    let waker = mio::Waker::new(poll.registry(), TOKEN_WAKE)?;
    ctx.shard_client
        .register_waker(ctx.worker_id, std::sync::Arc::new(waker));

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
    let mut async_waiters: Vec<usize> = Vec::new();
    let mut accepting = true;
    let mut drain_deadline: Option<Instant> = None;
    let mut last_expire = Instant::now();
    let mut last_time_refresh = Instant::now();
    let expire_every = Duration::from_millis(ctx.config.expire_interval_ms.max(1));
    let shutdown_grace = Duration::from_secs(ctx.config.shutdown_deadline_secs.max(1));

    tracing::info!(
        worker = ctx.worker_id,
        entries = RING_ENTRIES,
        "io_uring completion reactor ready"
    );

    loop {
        if last_time_refresh.elapsed() >= Duration::from_millis(1) {
            *ctx.now_ms.borrow_mut() = current_ms();
            last_time_refresh = Instant::now();
        }

        ctx.shard_client.clear_wake(ctx.worker_id);
        drain_shards(
            &mut request_rx,
            &ctx,
            &shard_range,
            &mut conns,
            &mut async_waiters,
            &mut ring,
        );

        if last_expire.elapsed() >= expire_every {
            *ctx.now_ms.borrow_mut() = current_ms();
            let now = *ctx.now_ms.borrow();
            let mut guard = ctx.local_shards.borrow_mut();
            for shard in guard.iter_mut() {
                shard.active_expire(now, ctx.config.expire_budget);
            }
            last_expire = Instant::now();
        }

        match shutdown_rx.try_recv() {
            Ok(()) | Err(broadcast::error::TryRecvError::Lagged(_)) => {
                if accepting {
                    accepting = false;
                    drain_deadline = Some(Instant::now() + shutdown_grace);
                    let _ = poll.registry().deregister(&mut listener);
                    for idx in 0..conns.len() {
                        if conns[idx].is_some() {
                            remove_conn(&mut conns, &mut free, idx);
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

        // Accept / wake via mio (non-blocking).
        let _ = poll.poll(&mut events, Some(Duration::ZERO));
        let mut woke = false;
        for event in events.iter() {
            match event.token() {
                TOKEN_LISTENER if accepting => {
                    accept_uring(
                        &mut listener,
                        &mut conns,
                        &mut free,
                        &ctx,
                        &conn_ctx,
                        &pool,
                        &mut ring,
                    );
                }
                TOKEN_WAKE => woke = true,
                _ => {}
            }
        }
        if woke {
            ctx.shard_client.clear_wake(ctx.worker_id);
            drain_shards(
                &mut request_rx,
                &ctx,
                &shard_range,
                &mut conns,
                &mut async_waiters,
                &mut ring,
            );
        }

        // Harvest cross-shard replies → queue sends.
        harvest_and_queue(&mut conns, &mut async_waiters, &mut ring);

        ring.flush_submissions(&mut conns);

        // Wait for I/O (or briefly park if nothing in flight).
        if ring.inflight > 0 {
            let _ = ring.wait_cqe(50_000); // 50µs — keep accept/wake responsive
        } else if async_waiters.is_empty() {
            let _ = poll.poll(&mut events, Some(Duration::from_millis(1)));
            for event in events.iter() {
                match event.token() {
                    TOKEN_LISTENER if accepting => {
                        accept_uring(
                            &mut listener,
                            &mut conns,
                            &mut free,
                            &ctx,
                            &conn_ctx,
                            &pool,
                            &mut ring,
                        );
                    }
                    TOKEN_WAKE => {
                        ctx.shard_client.clear_wake(ctx.worker_id);
                        drain_shards(
                            &mut request_rx,
                            &ctx,
                            &shard_range,
                            &mut conns,
                            &mut async_waiters,
                            &mut ring,
                        );
                    }
                    _ => {}
                }
            }
        } else {
            // Spin briefly for cross-shard like the mio reactor.
            for _ in 0..16 {
                drain_shards(
                    &mut request_rx,
                    &ctx,
                    &shard_range,
                    &mut conns,
                    &mut async_waiters,
                    &mut ring,
                );
                harvest_and_queue(&mut conns, &mut async_waiters, &mut ring);
                if async_waiters.is_empty() {
                    break;
                }
                std::hint::spin_loop();
            }
            if !async_waiters.is_empty() {
                tokio::task::yield_now().await;
            }
        }

        let mut to_close = Vec::new();
        {
            let mut guard = ctx.local_shards.borrow_mut();
            ring.drain_cqes(
                &mut conns,
                guard.as_mut_slice(),
                &mut to_close,
                &mut async_waiters,
            );
        }
        for idx in to_close {
            remove_conn(&mut conns, &mut free, idx);
        }
        ring.flush_submissions(&mut conns);

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
                    "shutdown: uring worker exit"
                );
                return Ok(());
            }
        }
    }
}

fn harvest_and_queue(
    conns: &mut [Option<Connection>],
    async_waiters: &mut Vec<usize>,
    ring: &mut CompletionRing,
) {
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
            if conn.wants_uring_send() {
                ring.queue_send(idx);
            }
            if !conn.has_pending_async() {
                conn.in_async_list = false;
                async_waiters.swap_remove(wi);
                continue;
            }
        }
        wi += 1;
    }
}

fn drain_shards(
    rx: &mut mpsc::Receiver<ShardRequest>,
    ctx: &WorkerContext,
    range: &std::ops::Range<usize>,
    conns: &mut [Option<Connection>],
    async_waiters: &mut Vec<usize>,
    ring: &mut CompletionRing,
) {
    let Ok(req0) = rx.try_recv() else {
        return;
    };
    let now = *ctx.now_ms.borrow();
    let mut guard = ctx.local_shards.borrow_mut();
    let len = guard.len();
    let mut wake_origins = [false; 64];
    let mut wake_overflow: Vec<usize> = Vec::new();
    let mut note_wake = |origin: usize| {
        if origin == ctx.worker_id {
            return;
        }
        if origin < wake_origins.len() {
            wake_origins[origin] = true;
        } else if !wake_overflow.contains(&origin) {
            wake_overflow.push(origin);
        }
    };
    let apply_one = |guard: &mut Vec<Shard>, req: ShardRequest, note_wake: &mut dyn FnMut(usize)| {
        let origin = req.origin_worker;
        let local_idx = req.shard_id.saturating_sub(range.start);
        let shard = &mut guard[local_idx.min(len.saturating_sub(1))];
        let reply = apply::apply(shard, req.cmd, now, &ctx.config, &ctx.info);
        let _ = req.reply.send(reply);
        note_wake(origin);
    };
    apply_one(&mut guard, req0, &mut note_wake);
    while let Ok(req) = rx.try_recv() {
        apply_one(&mut guard, req, &mut note_wake);
    }
    drop(guard);
    for (origin, flagged) in wake_origins.iter().enumerate() {
        if *flagged {
            ctx.shard_client.wake(origin);
        }
    }
    for origin in wake_overflow {
        ctx.shard_client.wake(origin);
    }
    // Origin is us: harvest local waiters promptly.
    harvest_and_queue(conns, async_waiters, ring);
}

fn accept_uring(
    listener: &mut TcpListener,
    conns: &mut Vec<Option<Connection>>,
    free: &mut Vec<usize>,
    ctx: &WorkerContext,
    conn_ctx: &ConnContext,
    pool: &Rc<BufferPool>,
    ring: &mut CompletionRing,
) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let current = ctx.conn_count.fetch_add(1, Ordering::Relaxed) + 1;
                let idx = if let Some(i) = free.pop() {
                    i
                } else {
                    let i = conns.len();
                    conns.push(None);
                    i
                };
                let token = Token(idx + CONN_TOKEN_BASE);
                // Do NOT register with mio — completion path owns the fd.
                // Blocking sockets: io_uring waits for readiness (avoids EAGAIN spin).
                {
                    use std::os::fd::AsRawFd;
                    let fd = stream.as_raw_fd();
                    unsafe {
                        let flags = libc::fcntl(fd, libc::F_GETFL);
                        if flags >= 0 {
                            let _ = libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK);
                        }
                    }
                }
                let conn = if current > ctx.config.maxclients {
                    ctx.conn_count.fetch_sub(1, Ordering::Relaxed);
                    Connection::rejected(stream, conn_ctx.clone(), pool.clone(), token)
                } else {
                    Connection::new(stream, conn_ctx.clone(), pool.clone(), token)
                };
                let wants_send = conn.is_reject_only() || conn.has_pending_out();
                conns[idx] = Some(conn);
                if wants_send {
                    ring.queue_send(idx);
                } else {
                    ring.queue_recv(idx);
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                break;
            }
        }
    }
}

fn remove_conn(conns: &mut Vec<Option<Connection>>, free: &mut Vec<usize>, idx: usize) {
    if let Some(conn) = conns[idx].take() {
        drop(conn);
        free.push(idx);
    }
}
