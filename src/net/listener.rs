use std::sync::atomic::Ordering;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;
use tracing::info;

use crate::net::connection;
use crate::runtime::worker::WorkerContext;

pub async fn accept_loop(ctx: WorkerContext) {
    let addr = ctx.config.socket_addr().expect("valid addr");
    let listener = bind_reuseport(addr, ctx.config.tcp_backlog).expect("bind");
    info!(worker = ctx.worker_id, %addr, "listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let current = ctx.conn_count.fetch_add(1, Ordering::Relaxed) + 1;
                if current > ctx.config.maxclients {
                    ctx.conn_count.fetch_sub(1, Ordering::Relaxed);
                    drop(stream);
                    continue;
                }
                let conn_ctx = connection::ConnContext {
                    worker_id: ctx.worker_id,
                    config: ctx.config.clone(),
                    shard_map: ctx.shard_map.clone(),
                    shard_client: ctx.shard_client.clone(),
                    local_shards: ctx.local_shards.clone(),
                    info: ctx.info.clone(),
                    conn_count: ctx.conn_count.clone(),
                    now_ms: ctx.now_ms.clone(),
                };
                tokio::task::spawn_local(connection::handle_connection(stream, conn_ctx));
            }
            Err(e) => {
                tracing::warn!("accept error: {e}");
            }
        }
    }
}

pub fn bind_reuseport(addr: std::net::SocketAddr, backlog: u32) -> std::io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog as i32)?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}