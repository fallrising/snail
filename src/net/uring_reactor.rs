//! Completion-style io_uring reactor: always-in-flight Recv/Send on data sockets.
//!
//! Listener AcceptMulti + eventfd wake live on the same ring (no mio/epoll, no
//! 50µs poll tax). Enable with `RUDIS_IO_URING=1`.

use std::io::{self, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use io_uring::{cqueue, opcode, types, IoUring};
use mio::net::TcpStream;
use mio::Token;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use crate::command::apply;
use crate::net::buffer::BufferPool;
use crate::net::connection::{ConnContext, Connection, DriveResult};
use crate::runtime::router::ShardRequest;
use crate::runtime::worker::{current_ms, WorkerContext};
use crate::storage::shard::Shard;

const CONN_TOKEN_BASE: usize = 2;

const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;
const OP_ACCEPT: u64 = 3;
const OP_WAKE: u64 = 4;
const MAX_IOV: usize = 16;
/// Must cover maxclients Recvs (+ AcceptMulti + wake + Sends). 32K fits C10K
/// with headroom; `setup_clamp` lowers if the kernel rejects the size.
const RING_ENTRIES: u32 = 32_768;

#[inline]
fn pack(op: u64, idx: u32) -> u64 {
    (op << 32) | idx as u64
}

#[inline]
fn unpack(ud: u64) -> (u64, u32) {
    (ud >> 32, ud as u32)
}

struct EventFd {
    fd: OwnedFd,
}

impl EventFd {
    fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    fn write_wake(&self) {
        let one = 1u64;
        let _ = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                &one as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
    }
}

struct CompletionRing {
    ring: IoUring,
    /// Per-connection iovec scratch (stable while that conn's send is in flight).
    iov_by_conn: Vec<[libc::iovec; MAX_IOV]>,
    /// Conns that need a Recv SQE when ring space frees up.
    pending_recv: Vec<usize>,
    /// Conns that need a Send SQE.
    pending_send: Vec<usize>,
    /// Data Recv/Send outstanding (AcceptMulti/Wake tracked separately).
    inflight: usize,
    accept_armed: bool,
    need_accept: bool,
    wake_armed: bool,
    need_wake: bool,
    /// Stable buffer for eventfd Read completions.
    wake_buf: u64,
}

impl CompletionRing {
    fn try_new() -> io::Result<Self> {
        // Prefer single-issuer + coop taskrun; fall back if the kernel rejects flags.
        // SQPOLL is opt-in via RUDIS_IO_URING_SQPOLL=1 (needs privileges on some hosts).
        let want_sqpoll = matches!(
            std::env::var("RUDIS_IO_URING_SQPOLL").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
        );
        let ring = build_ring(RING_ENTRIES, want_sqpoll)?;
        Ok(Self {
            ring,
            iov_by_conn: Vec::new(),
            pending_recv: Vec::new(),
            pending_send: Vec::new(),
            inflight: 0,
            accept_armed: false,
            need_accept: true,
            wake_armed: false,
            need_wake: true,
            wake_buf: 0,
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
        self.pending_recv.push(idx);
    }

    fn queue_send(&mut self, idx: usize) {
        self.pending_send.push(idx);
    }

    fn has_waitable(&self) -> bool {
        self.accept_armed
            || self.wake_armed
            || self.inflight > 0
            || self.need_accept
            || self.need_wake
            || !self.pending_recv.is_empty()
            || !self.pending_send.is_empty()
    }

    fn flush_submissions(
        &mut self,
        conns: &mut [Option<Connection>],
        listener_fd: RawFd,
        wake_fd: RawFd,
        accepting: bool,
    ) {
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
        let mut pushed = false;
        let wake_ptr = &mut self.wake_buf as *mut u64 as *mut u8;

        {
            let mut sq = self.ring.submission();

            if accepting && self.need_accept && !self.accept_armed {
                let entry = opcode::AcceptMulti::new(types::Fd(listener_fd))
                    .flags(libc::SOCK_CLOEXEC)
                    .build()
                    .user_data(pack(OP_ACCEPT, 0));
                if unsafe { sq.push(&entry) }.is_ok() {
                    self.accept_armed = true;
                    self.need_accept = false;
                    pushed = true;
                }
            }

            if self.need_wake && !self.wake_armed {
                let entry = opcode::Read::new(types::Fd(wake_fd), wake_ptr, 8)
                    .build()
                    .user_data(pack(OP_WAKE, 0));
                if unsafe { sq.push(&entry) }.is_ok() {
                    self.wake_armed = true;
                    self.need_wake = false;
                    pushed = true;
                }
            }

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
                pushed = true;
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
                pushed = true;
            }
        }

        if send_pos < send_q.len() {
            self.pending_send.extend_from_slice(&send_q[send_pos..]);
        }
        if recv_pos < recv_q.len() {
            self.pending_recv.extend_from_slice(&recv_q[recv_pos..]);
        }

        if pushed || self.inflight > 0 || self.accept_armed || self.wake_armed {
            let _ = self.ring.submit();
        }
    }

    fn wait_cqe(&mut self) -> io::Result<()> {
        if !self.has_waitable() {
            return Ok(());
        }
        match self.ring.submit_and_wait(1) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::Interrupted => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// True if the completion queue already has entries (no wait).
    fn cq_ready(&mut self) -> bool {
        let cq = self.ring.completion();
        !cq.is_empty()
    }

    fn drain_cqes(
        &mut self,
        conns: &mut [Option<Connection>],
        shards: &mut [Shard],
        to_close: &mut Vec<usize>,
        async_waiters: &mut Vec<usize>,
        new_fds: &mut Vec<RawFd>,
        woke: &mut bool,
        accepting: bool,
    ) {
        let mut cq = self.ring.completion();
        cq.sync();
        let mut requeue_send: Vec<usize> = Vec::new();
        let mut requeue_recv: Vec<usize> = Vec::new();
        for cqe in cq {
            let (op, idx) = unpack(cqe.user_data());
            let idx = idx as usize;
            match op {
                OP_ACCEPT => {
                    let res = cqe.result();
                    if res >= 0 {
                        new_fds.push(res);
                    } else if res != -libc::ECANCELED {
                        tracing::warn!("accept multi error: {}", io::Error::from_raw_os_error(-res));
                    }
                    if !cqueue::more(cqe.flags()) {
                        self.accept_armed = false;
                        if accepting {
                            self.need_accept = true;
                        }
                    }
                }
                OP_WAKE => {
                    self.wake_armed = false;
                    self.need_wake = true;
                    if cqe.result() > 0 {
                        *woke = true;
                    }
                }
                OP_RECV => {
                    if self.inflight > 0 {
                        self.inflight -= 1;
                    }
                    let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                        continue;
                    };
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
                    }
                    if conn.wants_uring_recv() {
                        requeue_recv.push(idx);
                    } else if conn.should_close_now() {
                        to_close.push(idx);
                    }
                }
                OP_SEND => {
                    if self.inflight > 0 {
                        self.inflight -= 1;
                    }
                    let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                        continue;
                    };
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
                    }
                    if conn.wants_uring_recv() {
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

fn build_ring(entries: u32, want_sqpoll: bool) -> io::Result<IoUring> {
    if want_sqpoll {
        if let Ok(r) = IoUring::builder()
            .setup_cqsize(entries * 2)
            .setup_clamp()
            .setup_sqpoll(50)
            .build(entries)
        {
            return Ok(r);
        }
    }
    match IoUring::builder()
        .setup_cqsize(entries * 2)
        .setup_clamp()
        .setup_single_issuer()
        .setup_coop_taskrun()
        .build(entries)
    {
        Ok(r) => Ok(r),
        Err(_) => IoUring::builder()
            .setup_cqsize(entries * 2)
            .setup_clamp()
            .build(entries),
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

fn stream_from_fd(fd: RawFd) -> io::Result<TcpStream> {
    let std = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    // Blocking sockets: io_uring waits for readiness (avoids EAGAIN spin).
    std.set_nonblocking(false)?;
    Ok(TcpStream::from_std(std))
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
    let listener = crate::net::listener::bind_reuseport_std(addr, ctx.config.tcp_backlog)?;
    let listener_fd = listener.as_raw_fd();
    info!(worker = ctx.worker_id, %addr, "listening (io_uring completion)");

    let eventfd = EventFd::new()?;
    let wake_fd = eventfd.as_raw_fd();
    let wake_dup = unsafe { libc::dup(wake_fd) };
    if wake_dup < 0 {
        return Err(io::Error::last_os_error());
    }
    let efd_wake = std::sync::Arc::new(EventFd {
        fd: unsafe { OwnedFd::from_raw_fd(wake_dup) },
    });
    ctx.shard_client.register_waker(
        ctx.worker_id,
        std::sync::Arc::new(move || {
            efd_wake.write_wake();
        }),
    );

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
    let mut new_fds: Vec<RawFd> = Vec::new();

    tracing::info!(
        worker = ctx.worker_id,
        entries = RING_ENTRIES,
        "io_uring completion reactor ready (AcceptMulti + eventfd)"
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
                    ring.need_accept = false;
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
                    ring.need_accept = false;
                }
            }
        }

        harvest_and_queue(&mut conns, &mut async_waiters, &mut ring);
        ring.flush_submissions(&mut conns, listener_fd, wake_fd, accepting);

        // Brief spin when waiting on cross-shard replies (wake will unblock park).
        if !async_waiters.is_empty() && ring.inflight == 0 {
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
            ring.flush_submissions(&mut conns, listener_fd, wake_fd, accepting);
        }

        // Burst: wait once, then keep draining/submitting while the CQ stays hot.
        if ring.has_waitable() && !ring.cq_ready() {
            let _ = ring.wait_cqe();
        } else if !ring.has_waitable() {
            if !async_waiters.is_empty() {
                tokio::task::yield_now().await;
            } else {
                tokio::task::yield_now().await;
            }
        }

        for _burst in 0..32 {
            let mut woke = false;
            new_fds.clear();
            let mut to_close = Vec::new();
            {
                let mut guard = ctx.local_shards.borrow_mut();
                ring.drain_cqes(
                    &mut conns,
                    guard.as_mut_slice(),
                    &mut to_close,
                    &mut async_waiters,
                    &mut new_fds,
                    &mut woke,
                    accepting,
                );
            }

            for fd in new_fds.drain(..) {
                install_conn(
                    fd,
                    &mut conns,
                    &mut free,
                    &ctx,
                    &conn_ctx,
                    &pool,
                    &mut ring,
                );
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

            for idx in to_close {
                remove_conn(&mut conns, &mut free, idx);
            }
            harvest_and_queue(&mut conns, &mut async_waiters, &mut ring);
            ring.flush_submissions(&mut conns, listener_fd, wake_fd, accepting);

            if !ring.cq_ready()
                && ring.pending_recv.is_empty()
                && ring.pending_send.is_empty()
            {
                break;
            }
            if ring.has_waitable() && !ring.cq_ready() {
                // More I/O in flight but CQ empty — leave burst; next turn waits.
                break;
            }
        }

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

fn install_conn(
    fd: RawFd,
    conns: &mut Vec<Option<Connection>>,
    free: &mut Vec<usize>,
    ctx: &WorkerContext,
    conn_ctx: &ConnContext,
    pool: &Rc<BufferPool>,
    ring: &mut CompletionRing,
) {
    let stream = match stream_from_fd(fd) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("accept fd wrap error: {e}");
            unsafe {
                let _ = libc::close(fd);
            }
            return;
        }
    };
    let current = ctx.conn_count.fetch_add(1, Ordering::Relaxed) + 1;
    let idx = if let Some(i) = free.pop() {
        i
    } else {
        let i = conns.len();
        conns.push(None);
        i
    };
    let token = Token(idx + CONN_TOKEN_BASE);
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
    harvest_and_queue(conns, async_waiters, ring);
}

fn remove_conn(conns: &mut Vec<Option<Connection>>, free: &mut Vec<usize>, idx: usize) {
    if let Some(conn) = conns[idx].take() {
        drop(conn);
        free.push(idx);
    }
}
