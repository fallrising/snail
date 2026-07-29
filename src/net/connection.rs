use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use tokio::net::TcpStream;
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
use crate::runtime::worker::current_ms;
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
    /// Still open; register interest and wait.
    Pending,
    /// Closed; drop the connection.
    Closed,
}

pub struct Connection {
    stream: TcpStream,
    read_buf: BytesMut,
    out_buf: BytesMut,
    parser: Parser,
    pending: VecDeque<ReplySlot>,
    bytes_pending: usize,
    state: ConnState,
    pool: Rc<BufferPool>,
    ctx: ConnContext,
    /// Rejected at accept (maxclients); flush error then close.
    reject_only: bool,
    /// Whether this connection was counted in `conn_count`.
    counted: bool,
}

impl Connection {
    pub fn new(stream: TcpStream, ctx: ConnContext, pool: Rc<BufferPool>) -> Self {
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
        }
    }

    /// Connection that only writes an error and closes (maxclients).
    /// Not counted toward `maxclients` / `conn_count`.
    pub fn rejected(stream: TcpStream, ctx: ConnContext, pool: Rc<BufferPool>) -> Self {
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
        }
    }

    pub fn poll_drive(&mut self, cx: &mut Context<'_>) -> DriveResult {
        // Encode any completed async replies, then write-first.
        let _ = self.poll_harvest(cx);

        match self.poll_flush(cx) {
            Poll::Ready(Err(_)) => {
                self.release_buffers();
                return DriveResult::Closed;
            }
            Poll::Ready(Ok(_)) | Poll::Pending => {}
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
            if self.out_buf.is_empty() {
                self.release_buffers();
                return DriveResult::Closed;
            }
            self.register_interest(cx);
            return DriveResult::Pending;
        }

        match self.poll_read(cx) {
            Poll::Ready(Ok(false)) | Poll::Ready(Err(_)) => {
                self.release_buffers();
                return DriveResult::Closed;
            }
            Poll::Ready(Ok(true)) => {
                // Local commands produce immediate replies — encode and flush now.
                let _ = self.poll_harvest(cx);
                match self.poll_flush(cx) {
                    Poll::Ready(Err(_)) => {
                        self.release_buffers();
                        return DriveResult::Closed;
                    }
                    Poll::Ready(Ok(_)) | Poll::Pending => {}
                }
            }
            Poll::Pending => {}
        }

        if self.should_close() {
            self.release_buffers();
            return DriveResult::Closed;
        }

        self.register_interest(cx);
        DriveResult::Pending
    }

    fn register_interest(&mut self, cx: &mut Context<'_>) {
        if !self.out_buf.is_empty() {
            let _ = self.stream.poll_write_ready(cx);
        }
        if self.can_read() {
            let _ = self.stream.poll_read_ready(cx);
        }
        // Pending oneshots already registered their waker via poll_harvest.
    }

    fn poll_harvest(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progress = false;
        while let Some(front) = self.pending.front_mut() {
            if front.ready.is_none() {
                if let Some(rx) = front.pending.as_mut() {
                    match Pin::new(rx).poll(cx) {
                        Poll::Ready(Ok(r)) => {
                            front.ready = Some(r);
                            front.pending = None;
                        }
                        Poll::Ready(Err(_)) => {
                            front.ready = Some(Reply::Err(
                                crate::protocol::frame::CommandErrKind::Generic,
                                "shard unavailable".into(),
                            ));
                            front.pending = None;
                        }
                        Poll::Pending => break,
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

    /// Returns Ready(Ok(true)) if bytes were written, Ready(Ok(false)) if nothing to write,
    /// Pending if waiting for writability, Err on fatal write error.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        if self.out_buf.is_empty() {
            return Poll::Ready(Ok(false));
        }
        match self.stream.poll_write_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        let mut wrote_any = false;
        while !self.out_buf.is_empty() {
            match self.stream.try_write(&self.out_buf) {
                Ok(0) => break,
                Ok(n) => {
                    let _ = self.out_buf.split_to(n);
                    self.bytes_pending = self.bytes_pending.saturating_sub(n);
                    wrote_any = true;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    let _ = self.stream.poll_write_ready(cx);
                    break;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        Poll::Ready(Ok(wrote_any))
    }

    /// Ready(Ok(true)) = read+processed, Ready(Ok(false)) = EOF, Pending = wait.
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        if !self.can_read() {
            return Poll::Pending;
        }

        // Ensure we have a buffer (may have been returned to the pool while idle).
        if self.read_buf.capacity() == 0 {
            self.read_buf = self.pool.get(self.ctx.config.read_buf_init);
        }

        match self.stream.poll_read_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        let mut read_any = false;
        loop {
            match self.stream.try_read_buf(&mut self.read_buf) {
                Ok(0) => {
                    if !read_any {
                        return Poll::Ready(Ok(false));
                    }
                    break;
                }
                Ok(_) => {
                    read_any = true;
                    *self.ctx.now_ms.borrow_mut() = current_ms();
                    if let Err(e) = self.process_input() {
                        self.enqueue_err(protocol_err_reply(&e));
                        self.state = ConnState::CloseAfterFlush;
                        break;
                    }
                    if !self.can_read() {
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    let _ = self.stream.poll_read_ready(cx);
                    break;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        self.maybe_return_read_buf();

        if read_any {
            Poll::Ready(Ok(true))
        } else {
            Poll::Pending
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
                            self.pending.push_back(ReplySlot {
                                ready: Some(reply),
                                pending: None,
                            });
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
