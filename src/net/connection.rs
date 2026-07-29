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
    reject_only: bool,
    counted: bool,
    /// Last registered mio interest (for reregister elision).
    registered: Interest,
    pub token: Token,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        ctx: ConnContext,
        pool: Rc<BufferPool>,
        token: Token,
    ) -> Self {
        let _ = stream.set_nodelay(true);
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
            reject_only: false,
            counted: true,
            registered: Interest::READABLE,
            token,
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
            reject_only: true,
            counted: false,
            registered: Interest::WRITABLE,
            token,
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
        self.pending
            .iter()
            .any(|s| s.ready.is_none() && s.pending.is_some())
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

                    if matches!(cmd, crate::command::Command::Quit) {
                        self.pending.push_back(ReplySlot {
                            ready: Some(Reply::Ok),
                            pending: None,
                        });
                        self.state = ConnState::CloseAfterFlush;
                        return Ok(());
                    }

                    let dispatcher = Dispatcher {
                        worker_id: self.ctx.worker_id,
                        shard_map: self.ctx.shard_map.clone(),
                        shard_client: self.ctx.shard_client.clone(),
                        local_shards: self.ctx.local_shards.clone(),
                        config: self.ctx.config.clone(),
                        info: self.ctx.info.clone(),
                        now_ms: *self.ctx.now_ms.borrow(),
                    };

                    match dispatcher.dispatch(cmd) {
                        crate::command::dispatcher::DispatchResult::Immediate(reply) => {
                            // Fast path: no in-flight async replies → encode directly.
                            if self.pending.is_empty() {
                                let before = self.out_buf.len();
                                encoder::encode(&reply, &mut self.out_buf);
                                self.bytes_pending += self.out_buf.len() - before;
                            } else {
                                self.pending.push_back(ReplySlot {
                                    ready: Some(reply),
                                    pending: None,
                                });
                            }
                        }
                        crate::command::dispatcher::DispatchResult::Pending(rx) => {
                            self.pending.push_back(ReplySlot {
                                ready: None,
                                pending: Some(rx),
                            });
                        }
                    }
                }
                None => break,
            }
        }
        Ok(())
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
