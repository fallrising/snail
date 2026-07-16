mod common;

use common::{send_cmd, TestServer};

#[test]
fn zset_zadd_zrange_zrank() {
    let server = TestServer::start();
    let mut s = server.connect();

    send_cmd(&mut s, &["ZADD", "z1", "1", "a", "2", "b", "3", "c"]);
    let range = send_cmd(&mut s, &["ZRANGE", "z1", "0", "-1"]);
    assert!(range.contains("a"));
    assert!(range.contains("b"));
    assert!(range.contains("c"));
    assert!(send_cmd(&mut s, &["ZCARD", "z1"]).contains(":3"));
    assert!(send_cmd(&mut s, &["ZRANK", "z1", "b"]).contains(":1"));
    assert!(send_cmd(&mut s, &["ZSCORE", "z1", "b"]).contains("2"));
}

#[test]
fn zset_zrangebyscore() {
    let server = TestServer::start();
    let mut s = server.connect();

    send_cmd(&mut s, &["ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d"]);

    let mid = send_cmd(&mut s, &["ZRANGEBYSCORE", "zs", "2", "3"]);
    assert!(mid.contains("b"));
    assert!(mid.contains("c"));
    assert!(!mid.contains("a"));
    assert!(!mid.contains("d"));

    let excl = send_cmd(&mut s, &["ZRANGEBYSCORE", "zs", "(2", "3"]);
    assert!(!excl.contains("b"));
    assert!(excl.contains("c"));

    let withscores = send_cmd(&mut s, &["ZRANGEBYSCORE", "zs", "1", "2", "WITHSCORES"]);
    assert!(withscores.contains("a"));
    assert!(withscores.contains("1"));
    assert!(withscores.contains("b"));

    let limited = send_cmd(&mut s, &["ZRANGEBYSCORE", "zs", "-inf", "+inf", "LIMIT", "1", "2"]);
    assert!(limited.contains("b"));
    assert!(limited.contains("c"));
    assert!(!limited.contains("a"));

    let rev = send_cmd(&mut s, &["ZREVRANGEBYSCORE", "zs", "4", "2"]);
    assert!(rev.contains("d"));
    assert!(rev.contains("c"));
    assert!(!rev.contains("a"));
}

#[test]
fn zset_sscan_cursor() {
    let server = TestServer::start();
    let mut s = server.connect();

    send_cmd(&mut s, &["SADD", "ss", "x", "y", "z"]);

    let full = send_cmd(&mut s, &["SSCAN", "ss", "0", "COUNT", "100"]);
    assert!(full.contains("x"));
    assert!(full.contains("y"));
    assert!(full.contains("z"));

    let step1 = send_cmd(&mut s, &["SSCAN", "ss", "0", "COUNT", "1"]);
    let step2 = send_cmd(&mut s, &["SSCAN", "ss", "1", "COUNT", "1"]);
    let step3 = send_cmd(&mut s, &["SSCAN", "ss", "2", "COUNT", "1"]);
    let combined = format!("{step1}{step2}{step3}");
    assert!(combined.contains("x"));
    assert!(combined.contains("y"));
    assert!(combined.contains("z"));
}