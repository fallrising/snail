mod common;

use common::{send_cmd, TestServer};

#[test]
fn list_lpush_rpop_lrange() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["LPUSH", "lst", "c", "b", "a"]).contains(":3"));
    assert!(send_cmd(&mut s, &["LLEN", "lst"]).contains(":3"));
    let range = send_cmd(&mut s, &["LRANGE", "lst", "0", "-1"]);
    assert!(range.contains("a"));
    assert!(range.contains("b"));
    assert!(range.contains("c"));
    assert!(send_cmd(&mut s, &["RPOP", "lst"]).contains("c"));
    assert!(send_cmd(&mut s, &["LINDEX", "lst", "0"]).contains("a"));
    assert!(send_cmd(&mut s, &["LTRIM", "lst", "0", "0"]).contains("OK"));
    assert!(send_cmd(&mut s, &["LLEN", "lst"]).contains(":1"));
}

#[test]
fn list_lpushx_missing_key() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["LPUSHX", "missing", "x"]).contains(":0"));
    assert!(send_cmd(&mut s, &["EXISTS", "missing"]).contains(":0"));
}
