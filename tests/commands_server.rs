mod common;

use common::{read_resp, send_cmd, TestServer};
use std::process::Command;
use std::thread;
use std::time::Duration;

#[test]
fn server_ping_echo_dbsize() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["PING"]).contains("PONG"));
    assert!(send_cmd(&mut s, &["ECHO", "hi"]).contains("hi"));
    send_cmd(&mut s, &["SET", "x", "1"]);
    assert!(send_cmd(&mut s, &["DBSIZE"]).contains(":1"));
    assert!(send_cmd(&mut s, &["FLUSHDB"]).contains("OK"));
    assert!(send_cmd(&mut s, &["DBSIZE"]).contains(":0"));
}

#[test]
fn maxclients_returns_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let mut child = Command::new(env!("CARGO_BIN_EXE_rudis"))
        .args([
            "--port",
            &port.to_string(),
            "--workers",
            "1",
            "--shards",
            "1",
            "--maxclients",
            "1",
        ])
        .spawn()
        .expect("start rudis");
    thread::sleep(Duration::from_millis(400));

    let mut first = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).expect("c1");
    first
        .set_read_timeout(Some(Duration::from_secs(2)))
        .ok();
    assert!(send_cmd(&mut first, &["PING"]).contains("PONG"));

    let mut second = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).expect("c2");
    second
        .set_read_timeout(Some(Duration::from_secs(2)))
        .ok();
    // Server writes the rejection error as soon as the connection is accepted.
    let got = read_resp(&mut second);
    let _ = child.kill();
    assert!(
        got.contains("max number of clients"),
        "expected maxclients error, got: {got:?}"
    );
}
