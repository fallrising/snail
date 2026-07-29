use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

use crate::net::reactor;
use crate::runtime::worker::WorkerContext;

pub async fn accept_loop(ctx: WorkerContext) {
    reactor::run(ctx).await;
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
