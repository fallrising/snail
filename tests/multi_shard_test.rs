use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

fn start_server(port: u16, workers: u16, shards: u16) -> Child {
    Command::new("./target/release/rudis")
        .args([
            "--port",
            &port.to_string(),
            "--workers",
            &workers.to_string(),
            "--shards",
            &shards.to_string(),
        ])
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

fn read_resp(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).expect("read");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[test]
fn mget_cross_shard() {
    let port = 16381u16;
    let mut child = start_server(port, 2, 4);
    thread::sleep(Duration::from_millis(500));

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    // Keys on different shards (with 4 shards, these should spread)
    for (k, v) in [("key_a", "val_a"), ("key_b", "val_b"), ("key_c", "val_c")] {
        stream
            .write_all(resp_cmd(&["SET", k, v]).as_bytes())
            .expect("write set");
        let ok = read_resp(&mut stream);
        assert!(ok.contains("OK"), "set {k}: {ok}");
    }

    stream
        .write_all(resp_cmd(&["MGET", "key_a", "key_b", "key_c"]).as_bytes())
        .expect("write mget");
    let resp = read_resp(&mut stream);
    assert!(resp.contains("val_a"), "mget missing val_a: {resp}");
    assert!(resp.contains("val_b"), "mget missing val_b: {resp}");
    assert!(resp.contains("val_c"), "mget missing val_c: {resp}");

    let _ = child.kill();
}

#[test]
fn del_cross_shard() {
    let port = 16382u16;
    let mut child = start_server(port, 2, 4);
    thread::sleep(Duration::from_millis(500));

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    for k in ["del_x", "del_y", "del_z"] {
        stream
            .write_all(resp_cmd(&["SET", k, "1"]).as_bytes())
            .expect("write set");
        read_resp(&mut stream);
    }

    stream
        .write_all(resp_cmd(&["DEL", "del_x", "del_y", "del_z"]).as_bytes())
        .expect("write del");
    let resp = read_resp(&mut stream);
    assert!(resp.contains(":3"), "del should remove 3 keys: {resp}");

    stream
        .write_all(resp_cmd(&["EXISTS", "del_x", "del_y"]).as_bytes())
        .expect("write exists");
    let resp = read_resp(&mut stream);
    assert!(resp.contains(":0"), "keys should not exist: {resp}");

    let _ = child.kill();
}

#[test]
fn sinter_cross_shard() {
    let port = 16383u16;
    let mut child = start_server(port, 2, 4);
    thread::sleep(Duration::from_millis(500));

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    stream
        .write_all(resp_cmd(&["SADD", "set1", "a", "b", "c"]).as_bytes())
        .expect("write sadd set1");
    read_resp(&mut stream);

    stream
        .write_all(resp_cmd(&["SADD", "set2", "b", "c", "d"]).as_bytes())
        .expect("write sadd set2");
    read_resp(&mut stream);

    stream
        .write_all(resp_cmd(&["SINTER", "set1", "set2"]).as_bytes())
        .expect("write sinter");
    let resp = read_resp(&mut stream);
    assert!(resp.contains("b"), "sinter missing b: {resp}");
    assert!(resp.contains("c"), "sinter missing c: {resp}");

    let _ = child.kill();
}

#[test]
fn rename_cross_shard_error() {
    let port = 16384u16;
    let mut child = start_server(port, 2, 4);
    thread::sleep(Duration::from_millis(500));

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    // Brute-force a cross-shard key pair (4 shards → typically found quickly).
    let mut cross_found = false;
    for i in 0..32u32 {
        for j in (i + 1)..32u32 {
            let src = format!("cx{i}");
            let dst = format!("cx{j}");
            stream
                .write_all(resp_cmd(&["SET", &src, "hello"]).as_bytes())
                .expect("write set");
            read_resp(&mut stream);
            stream
                .write_all(resp_cmd(&["SET", &dst, "world"]).as_bytes())
                .expect("write set");
            read_resp(&mut stream);

            stream
                .write_all(resp_cmd(&["RENAME", &src, &dst]).as_bytes())
                .expect("write rename");
            let resp = read_resp(&mut stream);
            if resp.contains("same slot") {
                cross_found = true;
                break;
            }
        }
        if cross_found {
            break;
        }
    }
    assert!(cross_found, "should find a cross-shard key pair among 32 keys");

    let _ = child.kill();
}