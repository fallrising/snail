use socket2::{Domain, Protocol, Socket, Type};
use mio::net::TcpListener;

use crate::net::reactor;
use crate::net::uring_reactor;
use crate::runtime::router::ShardRequest;
use crate::runtime::worker::WorkerContext;
use tokio::sync::{broadcast, mpsc};

pub async fn accept_loop(
    ctx: WorkerContext,
    request_rx: mpsc::Receiver<ShardRequest>,
    shard_range: std::ops::Range<usize>,
    shutdown_rx: broadcast::Receiver<()>,
) {
    if reactor::io_uring_enabled() {
        match uring_reactor::probe() {
            Ok(()) => {
                if let Err(e) =
                    uring_reactor::run(ctx, request_rx, shard_range, shutdown_rx).await
                {
                    tracing::error!(error = %e, "io_uring completion reactor error");
                }
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "io_uring unavailable; falling back to mio");
            }
        }
    }
    reactor::run(ctx, request_rx, shard_range, shutdown_rx).await;
}

pub fn bind_reuseport_mio(
    addr: std::net::SocketAddr,
    backlog: u32,
) -> std::io::Result<TcpListener> {
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
    Ok(TcpListener::from_std(std_listener))
}
