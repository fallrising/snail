mod common;

use common::{send_cmd, TestServer};

#[test]
fn string_get_set_incr_append() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["SET", "s1", "hello"]).contains("OK"));
    assert!(send_cmd(&mut s, &["GET", "s1"]).contains("hello"));
    assert!(send_cmd(&mut s, &["APPEND", "s1", " world"]).contains(":11"));
    assert!(send_cmd(&mut s, &["GET", "s1"]).contains("hello world"));
    assert!(send_cmd(&mut s, &["INCR", "counter"]).contains(":1"));
    assert!(send_cmd(&mut s, &["INCR", "counter"]).contains(":2"));
    assert!(send_cmd(&mut s, &["GETRANGE", "s1", "0", "4"]).contains("hello"));
}

#[test]
fn string_mget_mset() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["MSET", "a", "1", "b", "2"]).contains("OK"));
    let resp = send_cmd(&mut s, &["MGET", "a", "b", "missing"]);
    assert!(resp.contains("1"));
    assert!(resp.contains("2"));
    assert!(resp.contains("$-1"));
}