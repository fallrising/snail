use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::command::dispatcher::Dispatcher;
use crate::command::parse;
use crate::error::{protocol_err_reply, CommandError, ProtocolError};
use crate::net::buffer::BufferPool;
use crate::protocol::encoder;
use crate::protocol::frame::Reply;
use crate::protocol::parser::Parser;
use crate::runtime::router::{ShardClient, ShardMap};
use crate::runtime::worker::current_ms;
use crate::storage::shard::Shard;
use crate::telemetry::ServerInfo;

pub struct ConnContext {
    pub worker_id: usize,
    pub config: Rc<crate::config::Config>,
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

pub async fn handle_connection(stream: TcpStream, ctx: ConnContext) {
    let _ = stream.set_nodelay(true);
    let pool = Rc::new(BufferPool::default());
    let mut conn = Connection {
        stream,
        read_buf: pool.get(ctx.config.read_buf_init),
        out_buf: BytesMut::with_capacity(4096),
        parser: Parser::new(),
        pending: VecDeque::new(),
        bytes_pending: 0,
        state: ConnState::Normal,
        pool,
        ctx,
    };
    conn.run().await;
    conn.ctx
        .conn_count
        .fetch_sub(1, Ordering::Relaxed);
}

struct Connection {
    stream: TcpStream,
    read_buf: BytesMut,
    out_buf: BytesMut,
    parser: Parser,
    pending: VecDeque<ReplySlot>,
    bytes_pending: usize,
    state: ConnState,
    pool: Rc<BufferPool>,
    ctx: ConnContext,
}

impl Connection {
    async fn run(&mut self) {
        loop {
            self.harvest_replies().await;

            if !self.out_buf.is_empty() {
                match self.stream.write_all(&self.out_buf).await {
                    Ok(()) => {
                        self.bytes_pending =
                            self.bytes_pending.saturating_sub(self.out_buf.len());
                        self.out_buf.clear();
                    }
                    Err(e) => {
                        tracing::debug!("write error: {e}");
                        break;
                    }
                }
            }

            if self.can_read() && !self.read_buf.is_empty() {
                *self.ctx.now_ms.borrow_mut() = current_ms();
                if let Err(e) = self.process_input().await {
                    self.enqueue_err(protocol_err_reply(&e));
                    self.state = ConnState::CloseAfterFlush;
                }
            }

            if self.should_close() {
                break;
            }

            if self.bytes_pending > self.ctx.config.out_buf_hard {
                break;
            }

            if self.can_read() {
                match self.stream.read_buf(&mut self.read_buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        *self.ctx.now_ms.borrow_mut() = current_ms();
                        if let Err(e) = self.process_input().await {
                            self.enqueue_err(protocol_err_reply(&e));
                            self.state = ConnState::CloseAfterFlush;
                        }
                    }
                    Err(e) => {
                        tracing::debug!("read error: {e}");
                        break;
                    }
                }
            } else if !self.out_buf.is_empty() {
                let _ = self.stream.writable().await;
            } else if self.waiting_on_async() {
                tokio::task::yield_now().await;
            } else {
                let _ = self.stream.readable().await;
            }
        }

        if self.read_buf.is_empty() {
            self.pool.put(std::mem::take(&mut self.read_buf));
        }
    }

    fn waiting_on_async(&self) -> bool {
        self.pending
            .iter()
            .any(|s| s.ready.is_none() && s.pending.is_some())
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

    async fn process_input(&mut self) -> Result<(), ProtocolError> {
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

    async fn harvest_replies(&mut self) {
        while let Some(front) = self.pending.front_mut() {
            if front.ready.is_none() {
                if let Some(rx) = front.pending.take() {
                    match rx.await {
                        Ok(r) => front.ready = Some(r),
                        Err(_) => front.ready = Some(Reply::Err(
                            crate::protocol::frame::CommandErrKind::Generic,
                            "shard unavailable".into(),
                        )),
                    }
                }
            }
            if front.ready.is_some() {
                let slot = self.pending.pop_front().unwrap();
                if let Some(reply) = slot.ready {
                    let before = self.out_buf.len();
                    encoder::encode(&reply, &mut self.out_buf);
                    self.bytes_pending += self.out_buf.len() - before;
                }
            } else {
                break;
            }
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
