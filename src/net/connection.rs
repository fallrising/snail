use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, ErrorKind, Read};
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
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
use crate::net::outbuf::OutBuf;
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
    out_buf: OutBuf,
    parser: Parser,
    pending: VecDeque<ReplySlot>,
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
            out_buf: OutBuf::with_capacity(4096),
            parser: Parser::new(),
            pending: VecDeque::new(),
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
        let mut out_buf = OutBuf::with_capacity(64);
        out_buf.push_slice(msg);
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
    /// `shards` is the worker's local shard slice (already borrowed by the reactor).
    /// When `defer_flush` is set, outbound bytes stay in `out_buf` for io_uring batching.
    pub fn drive(
        &mut self,
        readable: bool,
        writable: bool,
        shards: &mut [Shard],
        defer_flush: bool,
    ) -> DriveResult {
        let _ = self.try_harvest();

        if !defer_flush && (writable || !self.out_buf.is_empty()) {
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

        if self.out_buf.pending() > self.ctx.config.out_buf_hard {
            self.release_buffers();
            return DriveResult::Closed;
        }

        if self.reject_only {
            if defer_flush && !self.out_buf.is_empty() {
                return DriveResult::Pending;
            }
            return if self.out_buf.is_empty() {
                self.release_buffers();
                DriveResult::Closed
            } else {
                DriveResult::Pending
            };
        }

        if readable && self.can_read() {
            match self.try_read(shards) {
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
                    if !defer_flush {
                        if let Err(e) = self.try_flush() {
                            if e.kind() != ErrorKind::WouldBlock {
                                self.release_buffers();
                                return DriveResult::Closed;
                            }
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

    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    #[inline]
    pub fn has_pending_out(&self) -> bool {
        !self.out_buf.is_empty()
    }

    #[inline]
    pub fn should_close_now(&self) -> bool {
        self.should_close()
    }

    pub fn flush_sync(&mut self) -> io::Result<()> {
        self.try_flush()
    }

    pub fn fill_send_iovecs(&self, iov: &mut [libc::iovec]) -> u32 {
        self.out_buf.fill_iovecs(iov) as u32
    }

    /// Prepare spare capacity in the read buffer for an io_uring Recv.
    pub fn prepare_recv_buf(&mut self) -> Option<(*mut u8, u32)> {
        if !self.can_read() {
            return None;
        }
        if self.read_buf.capacity() == 0 {
            self.read_buf = self.pool.get(self.ctx.config.read_buf_init);
        }
        if self.read_buf.capacity() - self.read_buf.len() < 2048 {
            self.read_buf.reserve(16 * 1024);
        }
        let len = self.read_buf.len();
        let spare = self.read_buf.capacity() - len;
        if spare == 0 {
            return None;
        }
        let ptr = unsafe { self.read_buf.as_mut_ptr().add(len) };
        Some((ptr, spare as u32))
    }

    pub fn complete_recv(&mut self, result: i32, shards: &mut [Shard]) -> DriveResult {
        if result == 0 {
            self.release_buffers();
            return DriveResult::Closed;
        }
        if result < 0 {
            let err = io::Error::from_raw_os_error(-result);
            if err.kind() == ErrorKind::WouldBlock {
                return DriveResult::Pending;
            }
            self.release_buffers();
            return DriveResult::Closed;
        }
        let n = result as usize;
        let len = self.read_buf.len();
        unsafe {
            self.read_buf.set_len(len + n);
        }
        if let Err(e) = self.process_input(shards) {
            self.enqueue_err(&protocol_err_reply(&e));
            self.state = ConnState::CloseAfterFlush;
        }
        let _ = self.try_harvest();
        self.maybe_return_read_buf();
        if self.out_buf.pending() > self.ctx.config.out_buf_hard {
            self.release_buffers();
            return DriveResult::Closed;
        }
        if self.should_close() && self.out_buf.is_empty() {
            self.release_buffers();
            return DriveResult::Closed;
        }
        DriveResult::Pending
    }

    pub fn complete_send(&mut self, result: i32) -> DriveResult {
        if result == 0 {
            self.release_buffers();
            return DriveResult::Closed;
        }
        if result < 0 {
            let err = io::Error::from_raw_os_error(-result);
            if err.kind() == ErrorKind::WouldBlock {
                return DriveResult::Pending;
            }
            self.release_buffers();
            return DriveResult::Closed;
        }
        self.out_buf.advance_bytes(result as usize);
        if self.should_close() && self.out_buf.is_empty() && self.pending.is_empty() {
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
                    self.encode_hot(&reply);
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
        self.out_buf.flush_to(&mut self.stream)
    }

    /// Ok(true) = read data, Ok(false) = EOF.
    fn try_read(&mut self, shards: &mut [Shard]) -> io::Result<bool> {
        if self.read_buf.capacity() == 0 {
            self.read_buf = self.pool.get(self.ctx.config.read_buf_init);
        }

        let mut read_any = false;
        loop {
            if self.read_buf.capacity() - self.read_buf.len() < 2048 {
                self.read_buf.reserve(16 * 1024);
            }
            let len = self.read_buf.len();
            let spare = self.read_buf.capacity() - len;
            let n = unsafe {
                let ptr = self.read_buf.as_mut_ptr().add(len);
                let dst = std::slice::from_raw_parts_mut(ptr, spare);
                match self.stream.read(dst) {
                    Ok(n) => n,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            };
            if n == 0 {
                return if read_any { Ok(true) } else { Ok(false) };
            }
            unsafe {
                self.read_buf.set_len(len + n);
            }
            read_any = true;
            if let Err(e) = self.process_input(shards) {
                self.enqueue_err(&protocol_err_reply(&e));
                self.state = ConnState::CloseAfterFlush;
                break;
            }
            if !self.can_read() {
                break;
            }
        }

        self.maybe_return_read_buf();
        Ok(read_any)
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
            && self.out_buf.pending() < self.ctx.config.out_buf_soft
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

    fn process_input(&mut self, shards: &mut [Shard]) -> Result<(), ProtocolError> {
        loop {
            // Fast path: plain GET/SET without building Frame/Command.
            if self.pending.is_empty() && self.parser.is_idle() {
                match try_parse_hot_get_set(&mut self.read_buf) {
                    HotParse::Get(key) => {
                        self.apply_hot_get(key, shards);
                        continue;
                    }
                    HotParse::Set(key, val) => {
                        self.apply_hot_set(key, val, shards);
                        continue;
                    }
                    HotParse::None => {}
                    HotParse::NeedMore => break,
                }
            }

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

                    if let Some(reply) = self.try_local_fast(cmd, shards) {
                        self.enqueue_immediate(reply);
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    #[inline]
    fn apply_hot_get(&mut self, key: Bytes, shards: &mut [Shard]) {
        let now = *self.ctx.now_ms.borrow();
        let shard_id = self.ctx.shard_map.shard_of(&key);
        if self.ctx.shard_map.owner_of(shard_id) != self.ctx.worker_id {
            self.dispatcher.now_ms = now;
            match self.dispatcher.dispatch_on(
                Command::Get(key),
                shards,
                self.ctx.local_shard_base,
            ) {
                crate::command::dispatcher::DispatchResult::Immediate(r) => {
                    self.encode_hot(&r);
                }
                crate::command::dispatcher::DispatchResult::Pending(rx) => {
                    self.push_async(rx);
                }
            }
            return;
        }
        let idx = shard_id.saturating_sub(self.ctx.local_shard_base);
        let idx = idx.min(shards.len().saturating_sub(1));
        let reply = string::apply_get(&mut shards[idx], &key, now);
        self.encode_hot(&reply);
    }

    #[inline]
    fn apply_hot_set(&mut self, key: Bytes, val: Bytes, shards: &mut [Shard]) {
        let now = *self.ctx.now_ms.borrow();
        let opts = SetOptions::default();
        let shard_id = self.ctx.shard_map.shard_of(&key);
        if self.ctx.shard_map.owner_of(shard_id) != self.ctx.worker_id {
            self.dispatcher.now_ms = now;
            match self.dispatcher.dispatch_on(
                Command::Set(key, val, opts),
                shards,
                self.ctx.local_shard_base,
            ) {
                crate::command::dispatcher::DispatchResult::Immediate(r) => {
                    self.encode_hot(&r);
                }
                crate::command::dispatcher::DispatchResult::Pending(rx) => {
                    self.push_async(rx);
                }
            }
            return;
        }
        let idx = shard_id.saturating_sub(self.ctx.local_shard_base);
        let idx = idx.min(shards.len().saturating_sub(1));
        let reply = string::apply_set(
            &mut shards[idx],
            key,
            val,
            opts,
            now,
            &self.ctx.config,
        );
        self.encode_hot(&reply);
    }

    fn try_local_fast(&mut self, cmd: Command, shards: &mut [Shard]) -> Option<Reply> {
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
                    return match self.dispatcher.dispatch_on(
                        Command::Get(k),
                        shards,
                        self.ctx.local_shard_base,
                    ) {
                        crate::command::dispatcher::DispatchResult::Immediate(r) => Some(r),
                        crate::command::dispatcher::DispatchResult::Pending(rx) => {
                            self.push_async(rx);
                            None
                        }
                    };
                }
                let idx = shard_id.saturating_sub(self.ctx.local_shard_base);
                let idx = idx.min(shards.len().saturating_sub(1));
                let reply = string::apply_get(&mut shards[idx], &k, now);
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
                    return match self.dispatcher.dispatch_on(
                        Command::Set(k, v, opts),
                        shards,
                        self.ctx.local_shard_base,
                    ) {
                        crate::command::dispatcher::DispatchResult::Immediate(r) => Some(r),
                        crate::command::dispatcher::DispatchResult::Pending(rx) => {
                            self.push_async(rx);
                            None
                        }
                    };
                }
                let idx = shard_id.saturating_sub(self.ctx.local_shard_base);
                let idx = idx.min(shards.len().saturating_sub(1));
                let reply = string::apply_set(
                    &mut shards[idx],
                    k,
                    v,
                    opts,
                    now,
                    &self.ctx.config,
                );
                if self.pending.is_empty() {
                    self.encode_hot(&reply);
                    None
                } else {
                    Some(reply)
                }
            }
            other => {
                self.dispatcher.now_ms = now;
                match self
                    .dispatcher
                    .dispatch_on(other, shards, self.ctx.local_shard_base)
                {
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
        self.out_buf.push_static(bytes);
    }

    /// Encode common hot-path replies without going through `Reply::Simple` formatting.
    #[inline]
    fn encode_hot(&mut self, reply: &Reply) {
        match reply {
            Reply::Ok => self.out_buf.push_static(encoder::OK),
            Reply::NullBulk => self.out_buf.push_static(encoder::NULL_BULK),
            Reply::Simple(s) if s == "PONG" => self.out_buf.push_static(encoder::PONG),
            Reply::Bulk(b) => self.out_buf.push_bulk(b),
            other => {
                let mut tmp = BytesMut::with_capacity(64);
                encoder::encode(other, &mut tmp);
                self.out_buf.push_slice(&tmp);
            }
        }
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

    fn enqueue_err(&mut self, msg: &str) {
        self.out_buf.push_slice(msg.as_bytes());
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

enum HotParse {
    Get(Bytes),
    Set(Bytes, Bytes),
    /// Not a plain GET/SET — fall through to general parser.
    None,
    /// Looks like GET/SET but incomplete.
    NeedMore,
}

/// Specialized parser for `*2\r\n$3\r\nGET\r\n$N\r\nkey\r\n` and plain `SET key val`.
fn try_parse_hot_get_set(buf: &mut BytesMut) -> HotParse {
    // Minimum GET: *2\r\n$3\r\nGET\r\n$1\r\nk\r\n = 20 bytes
    if buf.len() < 13 {
        return if buf.is_empty() || buf.first() == Some(&b'*') {
            HotParse::NeedMore
        } else {
            HotParse::None
        };
    }
    if !buf.starts_with(b"*2\r\n$3\r\n") {
        return HotParse::None;
    }
    let is_get = buf[8..11] == *b"GET";
    let is_set = buf[8..11] == *b"SET";
    if !is_get && !is_set {
        return HotParse::None;
    }
    if buf[11] != b'\r' || buf[12] != b'\n' {
        return HotParse::None;
    }
    if buf.len() < 14 {
        return HotParse::NeedMore;
    }
    if buf[13] != b'$' {
        return HotParse::None;
    }

    let Some((key_len, key_hdr)) = parse_len_line(&buf[14..]) else {
        return if buf.len() < 48 {
            HotParse::NeedMore
        } else {
            HotParse::None
        };
    };
    let key_start = 14 + key_hdr;
    let key_end = key_start + key_len;
    let after_key = key_end + 2;
    if buf.len() < after_key {
        return HotParse::NeedMore;
    }
    if buf[key_end] != b'\r' || buf[key_end + 1] != b'\n' {
        return HotParse::None;
    }

    if is_get {
        let _ = buf.split_to(key_start);
        let key = buf.split_to(key_len).freeze();
        buf.advance(2);
        return HotParse::Get(key);
    }

    // SET: parse value bulk
    if buf.len() <= after_key {
        return HotParse::NeedMore;
    }
    if buf[after_key] != b'$' {
        return HotParse::None;
    }
    let Some((val_len, val_hdr)) = parse_len_line(&buf[after_key + 1..]) else {
        return if buf.len() - after_key < 48 {
            HotParse::NeedMore
        } else {
            HotParse::None
        };
    };
    let val_start = after_key + 1 + val_hdr;
    let val_end = val_start + val_len;
    let after_val = val_end + 2;
    if buf.len() < after_val {
        return HotParse::NeedMore;
    }
    if buf[val_end] != b'\r' || buf[val_end + 1] != b'\n' {
        return HotParse::None;
    }

    let _ = buf.split_to(key_start);
    let key = buf.split_to(key_len).freeze();
    buf.advance(2); // key CRLF
    // Now buf starts at '$' of value
    let val_hdr_total = 1 + val_hdr; // '$' + len line
    let _ = buf.split_to(val_hdr_total);
    let val = buf.split_to(val_len).freeze();
    buf.advance(2);
    HotParse::Set(key, val)
}

fn parse_len_line(buf: &[u8]) -> Option<(usize, usize)> {
    // returns (value, bytes consumed including CRLF)
    if buf.is_empty() {
        return None;
    }
    let mut n: usize = 0;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if b == b'\r' {
            if i + 1 >= buf.len() || buf[i + 1] != b'\n' || i == 0 {
                return None;
            }
            return Some((n, i + 2));
        }
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
        i += 1;
        if i > 18 {
            return None;
        }
    }
    None
}
