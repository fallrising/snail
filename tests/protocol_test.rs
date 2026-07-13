use bytes::BytesMut;
use rudis::config::Config;
use rudis::protocol::parser::Parser;

#[test]
fn parse_ping_and_get() {
    let cfg = Config::default();
    let mut p = Parser::new();
    let mut buf = BytesMut::from("*1\r\n$4\r\nPING\r\n");
    let frame = p.next_frame(&mut buf, &cfg).unwrap().unwrap();
    assert_eq!(frame.args[0].as_ref(), b"PING");

    let mut buf2 = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    let frame2 = p.next_frame(&mut buf2, &cfg).unwrap().unwrap();
    assert_eq!(frame2.args[0].as_ref(), b"GET");
    assert_eq!(frame2.args[1].as_ref(), b"foo");
}

#[test]
fn parse_inline_ping() {
    let cfg = Config::default();
    let mut p = Parser::new();
    let mut buf = BytesMut::from("ping\r\n");
    let frame = p.next_frame(&mut buf, &cfg).unwrap().unwrap();
    assert_eq!(frame.args[0].as_ref(), b"ping");
}