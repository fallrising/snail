mod common;

use common::{send_cmd, TestServer};

#[test]
fn keys_expire_ttl_type() {
    let server = TestServer::start();
    let mut s = server.connect();

    send_cmd(&mut s, &["SET", "k1", "val"]);
    assert!(send_cmd(&mut s, &["TYPE", "k1"]).contains("string"));
    assert!(send_cmd(&mut s, &["EXISTS", "k1"]).contains(":1"));
    assert!(send_cmd(&mut s, &["EXPIRE", "k1", "3600"]).contains(":1"));
    let ttl = send_cmd(&mut s, &["TTL", "k1"]);
    assert!(ttl.contains(':'), "ttl: {ttl}");
    assert!(send_cmd(&mut s, &["DEL", "k1"]).contains(":1"));
    assert!(send_cmd(&mut s, &["EXISTS", "k1"]).contains(":0"));
}

#[test]
fn server_command_table() {
    let server = TestServer::start();
    let mut s = server.connect();

    let list = send_cmd(&mut s, &["COMMAND"]);
    assert!(list.contains("GET"));
    assert!(list.contains("SET"));
    assert!(list.contains("ZRANGEBYSCORE"));
    assert!(list.contains("HSCAN"));

    let info = send_cmd(&mut s, &["COMMAND", "INFO", "GET"]);
    assert!(info.contains("GET"));
    assert!(info.contains("readonly"));

    let count = send_cmd(&mut s, &["COMMAND", "COUNT"]);
    let n: i64 = count.trim_start_matches(':').trim().parse().unwrap();
    assert!(n >= 30, "command count: {n}");
}