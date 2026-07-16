use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

fn start_server(port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_rudis"))
        .args(["--port", &port.to_string(), "--workers", "1", "--shards", "1"])
        .spawn()
        .expect("start rudis")
}

fn resp_cmd(args: &[&str]) -> String {
    let mut s = format!("*{}\r\n", args.len());
    for a in args {
        s.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    s
}

fn read_line(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).expect("read");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[test]
fn basic_get_set_ping() {
    let port = 16380u16;
    let mut child = start_server(port);
    thread::sleep(Duration::from_millis(500));

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    stream
        .write_all(resp_cmd(&["PING"]).as_bytes())
        .expect("write ping");
    let pong = read_line(&mut stream);
    assert!(pong.contains("PONG"), "got: {pong}");

    stream
        .write_all(resp_cmd(&["SET", "foo", "bar"]).as_bytes())
        .expect("write set");
    let ok = read_line(&mut stream);
    assert!(ok.contains("OK"), "got: {ok}");

    stream
        .write_all(resp_cmd(&["GET", "foo"]).as_bytes())
        .expect("write get");
    let val = read_line(&mut stream);
    assert!(val.contains("bar"), "got: {val}");

    let _ = child.kill();
}
