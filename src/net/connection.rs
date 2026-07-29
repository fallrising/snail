use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, ErrorKind, Read, Write};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use mio::net::TcpStream;
use mio::{Interest, Token};
use tokio::sync::oneshot;

use crate::command::dispatcher::Dispatcher;
use crate::command::parse;
use crate::command::string;
use crate::command::{Command, SetOptions};
use crate::config::Config;
use crate::error::{protocol_err_reply, CommandError, ProtocolError};
use crate::net::buffer::BufferPool;
use crate::protocol::encoder;
use crate::protocol::frame::Reply;
use crate::protocol::parser::Parser;
use crate::runtime::router::{ShardClient, ShardMap};
use crate::storage::shard::Shard;
use crate::telemetry::ServerInfo;

#[derive(Clone)]
pub struct ConnContext {
    pub worker_id: usize,
    pub config: Rc<Config>,
    pub shard_map: Arc<ShardMap>,
    pub shard_client: ShardClient,
    pub local_shards: Rc<RefCell<Vec<Shard>>>,
    pub local_shard_base: usize,
    pub info: Rc<ServerInfo>,
    pub conn_count: Arc<AtomicUsize>,
    pub now_ms: Rc<RefCell<u64>>,
}

struct ReplySlot {
    ready: Option<Reply>,
    pending: Option<oneshot::Receiver<Reply>>,
}

enum ConnState {
    Normal,
    CloseAfterFlush,
}

pub enum DriveResult {
    /// Still open.
    Pending,
    /// Closed; drop the connection.
    Closed,
}

pub struct Connection {
    pub stream: TcpStream,
    read_buf: BytesMut,
    out_buf: BytesMut,
    parser: Parser,
    pending: VecDeque<ReplySlot>,
    bytes_pending: usize,
    state: ConnState,
    pool: Rc<BufferPool>,
    ctx: ConnContext,
    dispatcher: Dispatcher,
    reject_only: bool,
    counted: bool,
    /// Last registered mio interest (for reregister elision).
    registered: Interest,
    pub token: Token,
    /// Cached: any ReplySlot waiting on a oneshot (cross-shard).
    async_wait: bool,
    /// In reactor's async_waiters list (avoid duplicate entries).
    pub in_async_list: bool,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        ctx: ConnContext,
        pool: Rc<BufferPool>,
        token: Token,
    ) -> Self {
        let _ = stream.set_nodelay(true);
        let dispatcher = Dispatcher {
            worker_id: ctx.worker_id,
            shard_map: ctx.shard_map.clone(),
            shard_client: ctx.shard_client.clone(),
            local_shards: ctx.local_shards.clone(),
            config: ctx.config.clone(),
            info: ctx.info.clone(),
            now_ms: 0,
        };
        Self {
            stream,
            read_buf: pool.get(ctx.config.read_buf_init),
            out_buf: BytesMut::with_capacity(4096),
            parser: Parser::new(),
            pending: VecDeque::new(),
            bytes_pending: 0,
            state: ConnState::Normal,
            pool,
            ctx,
            dispatcher,
            reject_only: false,
            counted: true,
            registered: Interest::READABLE,
            token,
            async_wait: false,
            in_async_list: false,
        }
    }

    pub fn rejected(
        stream: TcpStream,
        ctx: ConnContext,
        pool: Rc<BufferPool>,
        token: Token,
    ) -> Self {
        let _ = stream.set_nodelay(true);
        let msg = b"-ERR max number of clients reached\r\n";
        let mut out_buf = BytesMut::with_capacity(64);
        out_buf.extend_from_slice(msg);
        let dispatcher = Dispatcher {
            worker_id: ctx.worker_id,
            shard_map: ctx.shard_map.clone(),
            shard_client: ctx.shard_client.clone(),
            local_shards: ctx.local_shards.clone(),
            config: ctx.config.clone(),
            info: ctx.info.clone(),
            now_ms: 0,
        };
        Self {
            stream,
            read_buf: BytesMut::new(),
            out_buf,
            parser: Parser::new(),
            pending: VecDeque::new(),
            bytes_pending: msg.len(),
            state: ConnState::CloseAfterFlush,
            pool,
            ctx,
            dispatcher,
            reject_only: true,
            counted: false,
            registered: Interest::WRITABLE,
            token,
            async_wait: false,
            in_async_list: false,
        }
    }

    pub fn desired_interest(&self) -> Interest {
        if self.reject_only || !self.out_buf.is_empty() {
            if self.can_read() {
                Interest::READABLE | Interest::WRITABLE
            } else {
                Interest::WRITABLE
            }
        } else if self.can_read() {
            Interest::READABLE
        } else {
            // Waiting on async replies or backpressure — still watch readability for EOF.
            Interest::READABLE
        }
    }

    pub fn registered_interest(&self) -> Interest {
        self.registered
    }

    pub fn set_registered(&mut self, interest: Interest) {
        self.registered = interest;
    }

    /// Drive this connection from a mio readiness event (or opportunistic pass).
    pub fn drive(&mut self, readable: bool, writable: bool) -> DriveResult {
        let _ = self.try_harvest();

        if writable || !self.out_buf.is_empty() {
            if let Err(e) = self.try_flush() {
                if e.kind() != ErrorKind::WouldBlock {
                    self.release_buffers();
                    return DriveResult::Closed;
                }
            }
        }

        if self.should_close() {
            self.release_buffers();
            return DriveResult::Closed;
        }

        if self.bytes_pending > self.ctx.config.out_buf_hard {
            self.release_buffers();
            return DriveResult::Closed;
        }

        if self.reject_only {
            return if self.out_buf.is_empty() {
                self.release_buffers();
                DriveResult::Closed
            } else {
                DriveResult::Pending
            };
        }

        if readable && self.can_read() {
            match self.try_read() {
                Ok(false) => {
                    self.release_buffers();
                    return DriveResult::Closed;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(_) => {
                    self.release_buffers();
                    return DriveResult::Closed;
                }
                Ok(true) => {
                    let _ = self.try_harvest();
                    if let Err(e) = self.try_flush() {
                        if e.kind() != ErrorKind::WouldBlock {
                            self.release_buffers();
                            return DriveResult::Closed;
                        }
                    }
                }
            }
        }

        if self.should_close() {
            self.release_buffers();
            return DriveResult::Closed;
        }

        DriveResult::Pending
    }

    /// Opportunistically poll pending oneshots (cross-shard replies).
    pub fn poll_async(&mut self) -> bool {
        self.try_harvest()
    }

    fn try_harvest(&mut self) -> bool {
        let mut progress = false;
        while let Some(front) = self.pending.front_mut() {
            if front.ready.is_none() {
                if let Some(rx) = front.pending.as_mut() {
                    match rx.try_recv() {
                        Ok(r) => {
                            front.ready = Some(r);
                            front.pending = None;
                        }
                        Err(oneshot::error::TryRecvError::Empty) => break,
                        Err(oneshot::error::TryRecvError::Closed) => {
                            front.ready = Some(Reply::Err(
                                crate::protocol::frame::CommandErrKind::Generic,
                                "shard unavailable".into(),
                            ));
                            front.pending = None;
                        }
                    }
                } else {
                    break;
                }
            }
            if front.ready.is_some() {
                let slot = self.pending.pop_front().unwrap();
                if let Some(reply) = slot.ready {
                    let before = self.out_buf.len();
                    encoder::encode(&reply, &mut self.out_buf);
                    self.bytes_pending += self.out_buf.len() - before;
                    progress = true;
                }
            } else {
                break;
            }
        }
        if progress {
            self.refresh_async_wait();
        }
        progress
    }

    fn try_flush(&mut self) -> io::Result<()> {
        while !self.out_buf.is_empty() {
            match self.stream.write(&self.out_buf) {
                Ok(0) => return Err(io::Error::new(ErrorKind::WriteZero, "write zero")),
                Ok(n) => {
                    let _ = self.out_buf.split_to(n);
                    self.bytes_pending = self.bytes_pending.saturating_sub(n);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Err(e),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Ok(true) = read data, Ok(false) = EOF.
    fn try_read(&mut self) -> io::Result<bool> {
        if self.read_buf.capacity() == 0 {
            self.read_buf = self.pool.get(self.ctx.config.read_buf_init);
        }

        let mut tmp = [0u8; 16 * 1024];
        let mut read_any = false;
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => {
                    return if read_any { Ok(true) } else { Ok(false) };
                }
                Ok(n) => {
                    self.read_buf.extend_from_slice(&tmp[..n]);
                    read_any = true;
                    if let Err(e) = self.process_input() {
                        self.enqueue_err(protocol_err_reply(&e));
                        self.state = ConnState::CloseAfterFlush;
                        break;
                    }
                    if !self.can_read() {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        self.maybe_return_read_buf();
        if read_any {
            self.set_quickack();
        }
        Ok(read_any)
    }

    #[inline]
    fn set_quickack(&self) {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let on: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    self.stream.as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &on as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&on) as libc::socklen_t,
                );
            }
        }
    }

    fn maybe_return_read_buf(&mut self) {
        if self.read_buf.is_empty() && self.parser.is_idle() && self.read_buf.capacity() > 0 {
            self.pool.put(std::mem::take(&mut self.read_buf));
        }
    }

    fn release_buffers(&mut self) {
        if self.read_buf.capacity() > 0 {
            self.pool.put(std::mem::take(&mut self.read_buf));
        }
        self.out_buf.clear();
    }

    fn can_read(&self) -> bool {
        !matches!(self.state, ConnState::CloseAfterFlush)
            && self.bytes_pending < self.ctx.config.out_buf_soft
            && self.pending.len() < self.ctx.config.pipeline_cap
    }

    fn should_close(&self) -> bool {
        match self.state {
            ConnState::CloseAfterFlush if self.out_buf.is_empty() && self.pending.is_empty() => {
                true
            }
            _ => false,
        }
    }

    pub fn force_close_after_flush(&mut self) {
        self.state = ConnState::CloseAfterFlush;
    }

    pub fn has_pending_async(&self) -> bool {
        self.async_wait
    }

    fn refresh_async_wait(&mut self) {
        self.async_wait = self
            .pending
            .iter()
            .any(|s| s.ready.is_none() && s.pending.is_some());
    }

    #[inline]
    fn push_async(&mut self, rx: oneshot::Receiver<Reply>) {
        self.pending.push_back(ReplySlot {
            ready: None,
            pending: Some(rx),
        });
        self.async_wait = true;
    }

    fn process_input(&mut self) -> Result<(), ProtocolError> {
        loop {
            match self.parser.next_frame(&mut self.read_buf, &self.ctx.config)? {
                Some(frame) => {
                    let cmd = match parse(&frame) {
                        Ok(c) => c,
                        Err(e) => {
                            self.enqueue_cmd_err(e);
                            continue;
                        }
                    };

                    if matches!(cmd, Command::Quit) {
                        self.enqueue_immediate(Reply::Ok);
                        self.state = ConnState::CloseAfterFlush;
                        return Ok(());
                    }

                    // Hot path / normal dispatch. None ⇒ already written or async queued.
                    if let Some(reply) = self.try_local_fast(cmd) {
                        self.enqueue_immediate(reply);
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    fn try_local_fast(&mut self, cmd: Command) -> Option<Reply> {
        let now = *self.ctx.now_ms.borrow();
        match cmd {
            Command::Ping(None) => {
                if self.pending.is_empty() {
                    self.write_static(encoder::PONG);
                    None
                } else {
                    Some(Reply::Simple("PONG".into()))
                }
            }
            Command::Ping(Some(msg)) => Some(Reply::Bulk(msg)),
            Command::Get(k) => {
                let shard_id = self.ctx.shard_map.shard_of(&k);
                if self.ctx.shard_map.owner_of(shard_id) != self.ctx.worker_id {
                    self.dispatcher.now_ms = now;
                    return match self.dispatcher.dispatch(Command::Get(k)) {
                        crate::command::dispatcher::DispatchResult::Immediate(r) => Some(r),
                        crate::command::dispatcher::DispatchResult::Pending(rx) => {
                            self.push_async(rx);
                            None
                        }
                    };
                }
                let reply = {
                    let mut shards = self.ctx.local_shards.borrow_mut();
                    let idx = shard_id.saturating_sub(self.ctx.local_shard_base);
                    let idx = idx.min(shards.len().saturating_sub(1));
                    string::apply_get(&mut shards[idx], &k, now)
                };
                if self.pending.is_empty() {
                    self.encode_hot(&reply);
                    None
                } else {
                    Some(reply)
                }
            }
            Command::Set(k, v, opts) if opts == SetOptions::default() => {
                let shard_id = self.ctx.shard_map.shard_of(&k);
                if self.ctx.shard_map.owner_of(shard_id) != self.ctx.worker_id {
                    self.dispatcher.now_ms = now;
                    return match self.dispatcher.dispatch(Command::Set(k, v, opts)) {
                        crate::command::dispatcher::DispatchResult::Immediate(r) => Some(r),
                        crate::command::dispatcher::DispatchResult::Pending(rx) => {
                            self.push_async(rx);
                            None
                        }
                    };
                }
                let reply = {
                    let mut shards = self.ctx.local_shards.borrow_mut();
                    let idx = shard_id.saturating_sub(self.ctx.local_shard_base);
                    let idx = idx.min(shards.len().saturating_sub(1));
                    string::apply_set(
                        &mut shards[idx],
                        k,
                        v,
                        opts,
                        now,
                        &self.ctx.config,
                    )
                };
                if self.pending.is_empty() {
                    self.encode_hot(&reply);
                    None
                } else {
                    Some(reply)
                }
            }
            other => {
                self.dispatcher.now_ms = now;
                match self.dispatcher.dispatch(other) {
                    crate::command::dispatcher::DispatchResult::Immediate(r) => Some(r),
                    crate::command::dispatcher::DispatchResult::Pending(rx) => {
                        self.push_async(rx);
                        None
                    }
                }
            }
        }
    }

    #[inline]
    fn write_static(&mut self, bytes: &'static [u8]) {
        self.out_buf.extend_from_slice(bytes);
        self.bytes_pending += bytes.len();
    }

    /// Encode common hot-path replies without going through `Reply::Simple` formatting.
    #[inline]
    fn encode_hot(&mut self, reply: &Reply) {
        let before = self.out_buf.len();
        match reply {
            Reply::Ok => self.out_buf.extend_from_slice(encoder::OK),
            Reply::NullBulk => self.out_buf.extend_from_slice(encoder::NULL_BULK),
            Reply::Simple(s) if s == "PONG" => self.out_buf.extend_from_slice(encoder::PONG),
            Reply::Bulk(b) => encoder::encode_bulk(b, &mut self.out_buf),
            other => encoder::encode(other, &mut self.out_buf),
        }
        self.bytes_pending += self.out_buf.len() - before;
    }

    fn enqueue_immediate(&mut self, reply: Reply) {
        if self.pending.is_empty() {
            self.encode_hot(&reply);
        } else {
            self.pending.push_back(ReplySlot {
                ready: Some(reply),
                pending: None,
            });
        }
    }

    fn enqueue_err(&mut self, msg: String) {
        let before = self.out_buf.len();
        self.out_buf.extend_from_slice(msg.as_bytes());
        self.bytes_pending += self.out_buf.len() - before;
    }

    fn enqueue_cmd_err(&mut self, err: CommandError) {
        self.pending.push_back(ReplySlot {
            ready: Some(Reply::Err(
                crate::protocol::frame::CommandErrKind::Generic,
                err.to_resp(),
            )),
            pending: None,
        });
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.counted {
            self.ctx.conn_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
