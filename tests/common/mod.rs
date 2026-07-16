use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(16400);

pub struct TestServer {
    pub port: u16,
    child: Child,
}

impl TestServer {
    pub fn start() -> Self {
        let port = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let child = Command::new("./target/release/rudis")
            .args([
                "--port",
                &port.to_string(),
                "--workers",
                "1",
                "--shards",
                "1",
            ])
            .spawn()
            .expect("start rudis");
        thread::sleep(Duration::from_millis(400));
        Self { port, child }
    }

    pub fn connect(&self) -> TcpStream {
        let mut stream =
            TcpStream::connect(format!("127.0.0.1:{}", self.port)).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
        stream
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

pub fn resp_cmd(args: &[&str]) -> String {
    let mut s = format!("*{}\r\n", args.len());
    for a in args {
        s.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    s
}

pub fn send_cmd(stream: &mut TcpStream, args: &[&str]) -> String {
    stream
        .write_all(resp_cmd(args).as_bytes())
        .expect("write");
    read_resp(stream)
}

pub fn read_resp(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 16384];
    let n = stream.read(&mut buf).expect("read");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}