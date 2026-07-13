use bytes::{Bytes, BytesMut};

use crate::protocol::frame::{CommandErrKind, Reply};

pub const OK: &[u8] = b"+OK\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";
pub const NULL_BULK: &[u8] = b"$-1\r\n";
pub const ZERO: &[u8] = b":0\r\n";
pub const ONE: &[u8] = b":1\r\n";
pub const EMPTY_ARRAY: &[u8] = b"*0\r\n";

pub fn format_err(msg: &str) -> String {
    format!("-ERR {msg}\r\n")
}

pub fn format_wrongtype() -> String {
    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into()
}

pub fn encode(reply: &Reply, out: &mut BytesMut) {
    match reply {
        Reply::Ok => out.extend_from_slice(OK),
        Reply::Simple(s) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Int(n) => {
            out.extend_from_slice(b":");
            write_i64(*n, out);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Bulk(b) => encode_bulk(b, out),
        Reply::NullBulk => out.extend_from_slice(NULL_BULK),
        Reply::Array(items) => {
            out.extend_from_slice(b"*");
            write_usize(items.len(), out);
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode(item, out);
            }
        }
        Reply::NullArray => out.extend_from_slice(b"$-1\r\n"),
        Reply::Err(kind, msg) => {
            let prefix = match kind {
                CommandErrKind::WrongType => "-WRONGTYPE ",
                CommandErrKind::Generic => "-ERR ",
            };
            out.extend_from_slice(prefix.as_bytes());
            out.extend_from_slice(msg.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

pub fn encode_bulk(b: &Bytes, out: &mut BytesMut) {
    out.extend_from_slice(b"$");
    write_usize(b.len(), out);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
}

fn write_usize(n: usize, out: &mut BytesMut) {
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format(n).as_bytes());
}

fn write_i64(n: i64, out: &mut BytesMut) {
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format_i64(n).as_bytes());
}

mod itoa {
    pub struct Buffer {
        buf: [u8; 22],
    }
    impl Buffer {
        pub fn new() -> Self {
            Self { buf: [0; 22] }
        }
        pub fn format(&mut self, mut n: usize) -> &str {
            if n == 0 {
                self.buf[0] = b'0';
                return std::str::from_utf8(&self.buf[..1]).unwrap();
            }
            let mut i = 21;
            while n > 0 {
                self.buf[i] = b'0' + (n % 10) as u8;
                n /= 10;
                i -= 1;
            }
            std::str::from_utf8(&self.buf[i + 1..]).unwrap()
        }
        pub fn format_i64(&mut self, mut n: i64) -> &str {
            if n == 0 {
                self.buf[0] = b'0';
                return std::str::from_utf8(&self.buf[..1]).unwrap();
            }
            let mut i = 21;
            let neg = n < 0;
            if neg {
                n = -n;
            }
            while n > 0 {
                self.buf[i] = b'0' + (n % 10) as u8;
                n /= 10;
                i -= 1;
            }
            if neg {
                self.buf[i] = b'-';
                std::str::from_utf8(&self.buf[i..]).unwrap()
            } else {
                std::str::from_utf8(&self.buf[i + 1..]).unwrap()
            }
        }
    }
}