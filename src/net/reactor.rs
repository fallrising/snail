use std::future::poll_fn;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::Poll;

use tracing::info;

use crate::net::buffer::BufferPool;
use crate::net::connection::{ConnContext, Connection, DriveResult};
use crate::runtime::worker::WorkerContext;

/// Per-worker connection reactor: one task drives accept + all connections
/// via readiness polling (no per-connection `spawn_local`).
pub async fn run(ctx: WorkerContext) {
    let addr = ctx.config.socket_addr().expect("valid addr");
    let listener = crate::net::listener::bind_reuseport(addr, ctx.config.tcp_backlog)
        .expect("bind");
    info!(worker = ctx.worker_id, %addr, "listening");

    let pool = Rc::new(BufferPool::default());
    let conn_ctx = ConnContext {
        worker_id: ctx.worker_id,
        config: ctx.config.clone(),
        shard_map: ctx.shard_map.clone(),
        shard_client: ctx.shard_client.clone(),
        local_shards: ctx.local_shards.clone(),
        info: ctx.info.clone(),
        conn_count: ctx.conn_count.clone(),
        now_ms: ctx.now_ms.clone(),
    };

    let mut conns: Vec<Option<Connection>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();

    // Never-resolving future: the reactor is re-polled whenever any socket or
    // oneshot waker fires (or we self-wake to keep draining).
    poll_fn(|cx| {
        let mut progress = false;

        loop {
            match listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, _))) => {
                    let current = ctx.conn_count.fetch_add(1, Ordering::Relaxed) + 1;
                    let conn = if current > ctx.config.maxclients {
                        ctx.conn_count.fetch_sub(1, Ordering::Relaxed);
                        Connection::rejected(stream, conn_ctx.clone(), pool.clone())
                    } else {
                        Connection::new(stream, conn_ctx.clone(), pool.clone())
                    };
                    insert_conn(&mut conns, &mut free, conn);
                    progress = true;
                }
                Poll::Ready(Err(e)) => {
                    tracing::warn!("accept error: {e}");
                    break;
                }
                Poll::Pending => break,
            }
        }

        let len = conns.len();
        for idx in 0..len {
            let closed = match conns[idx].as_mut() {
                Some(conn) => matches!(conn.poll_drive(cx), DriveResult::Closed),
                None => false,
            };
            if closed {
                conns[idx] = None;
                free.push(idx);
                progress = true;
            }
        }

        if progress {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    })
    .await;
}

fn insert_conn(conns: &mut Vec<Option<Connection>>, free: &mut Vec<usize>, conn: Connection) {
    if let Some(idx) = free.pop() {
        conns[idx] = Some(conn);
    } else {
        conns.push(Some(conn));
    }
}
