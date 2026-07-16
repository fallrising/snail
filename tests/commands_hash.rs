mod common;

use common::{send_cmd, TestServer};

#[test]
fn hash_hset_hget_hgetall() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["HSET", "h1", "f1", "v1", "f2", "v2"]).contains(":2"));
    assert!(send_cmd(&mut s, &["HGET", "h1", "f1"]).contains("v1"));
    let all = send_cmd(&mut s, &["HGETALL", "h1"]);
    assert!(all.contains("f1"));
    assert!(all.contains("v1"));
    assert!(all.contains("f2"));
    assert!(all.contains("v2"));
    assert!(send_cmd(&mut s, &["HLEN", "h1"]).contains(":2"));
}

#[test]
fn hash_hscan_cursor_and_match() {
    let server = TestServer::start();
    let mut s = server.connect();

    send_cmd(&mut s, &["HSET", "hs", "alpha", "1", "beta", "2", "gamma", "3"]);

    let full = send_cmd(&mut s, &["HSCAN", "hs", "0", "COUNT", "100"]);
    assert!(full.contains("alpha"));
    assert!(full.contains("beta"));
    assert!(full.contains("gamma"));

    let matched = send_cmd(&mut s, &["HSCAN", "hs", "0", "MATCH", "a*", "COUNT", "100"]);
    assert!(matched.contains("alpha"));
    assert!(!matched.contains("beta"));

    // Step through with COUNT 1 — three fields require multiple cursors before done.
    let step1 = send_cmd(&mut s, &["HSCAN", "hs", "0", "COUNT", "1"]);
    let step2 = send_cmd(&mut s, &["HSCAN", "hs", "1", "COUNT", "1"]);
    let step3 = send_cmd(&mut s, &["HSCAN", "hs", "2", "COUNT", "1"]);
    let combined = format!("{step1}{step2}{step3}");
    assert!(combined.contains("alpha"));
    assert!(combined.contains("beta"));
    assert!(combined.contains("gamma"));
}