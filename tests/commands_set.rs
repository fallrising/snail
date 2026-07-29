mod common;

use common::{send_cmd, TestServer};

#[test]
fn set_sadd_smembers_sismember() {
    let server = TestServer::start();
    let mut s = server.connect();

    assert!(send_cmd(&mut s, &["SADD", "s1", "a", "b", "c"]).contains(":3"));
    assert!(send_cmd(&mut s, &["SADD", "s1", "a"]).contains(":0"));
    assert!(send_cmd(&mut s, &["SCARD", "s1"]).contains(":3"));
    assert!(send_cmd(&mut s, &["SISMEMBER", "s1", "b"]).contains(":1"));
    assert!(send_cmd(&mut s, &["SISMEMBER", "s1", "z"]).contains(":0"));
    let members = send_cmd(&mut s, &["SMEMBERS", "s1"]);
    assert!(members.contains("a"));
    assert!(members.contains("b"));
    assert!(members.contains("c"));
    assert!(send_cmd(&mut s, &["SREM", "s1", "b"]).contains(":1"));
    assert!(send_cmd(&mut s, &["SCARD", "s1"]).contains(":2"));
}

#[test]
fn set_sinter_same_shard() {
    let server = TestServer::start();
    let mut s = server.connect();

    send_cmd(&mut s, &["SADD", "sa", "1", "2", "3"]);
    send_cmd(&mut s, &["SADD", "sb", "2", "3", "4"]);
    let inter = send_cmd(&mut s, &["SINTER", "sa", "sb"]);
    assert!(inter.contains("2"));
    assert!(inter.contains("3"));
    // "1" and "4" are not in the intersection.
    assert!(!inter.contains("$1\r\n1\r\n"));
    assert!(!inter.contains("$1\r\n4\r\n"));
}
