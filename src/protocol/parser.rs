use bytes::{Buf, Bytes, BytesMut};

use crate::config::Config;
use crate::error::ProtocolError;
use crate::protocol::frame::Frame;

#[derive(Debug, Default)]
pub struct Parser {
    state: State,
    array_len: usize,
    bulk_remaining: usize,
    current_bulk_len: Option<usize>,
    args: Vec<Bytes>,
    inline_buf: Vec<u8>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum State {
    #[default]
    WaitType,
    ArrayLen,
    BulkLen,
    BulkData,
    Inline,
}

impl Parser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// True when not mid-frame — safe to return the read buffer to the pool.
    pub fn is_idle(&self) -> bool {
        matches!(self.state, State::WaitType) && self.args.is_empty() && self.inline_buf.is_empty()
    }

    pub fn next_frame(
        &mut self,
        buf: &mut BytesMut,
        config: &Config,
    ) -> Result<Option<Frame>, ProtocolError> {
        loop {
            match self.state {
                State::WaitType => {
                    if buf.is_empty() {
                        return Ok(None);
                    }
                    let b = buf[0];
                    if b == b'*' {
                        buf.advance(1);
                        self.state = State::ArrayLen;
                        self.args.clear();
                    } else if b.is_ascii_graphic() || b == b' ' {
                        self.state = State::Inline;
                        self.inline_buf.clear();
                    } else {
                        return Err(ProtocolError::InvalidPrefix(b));
                    }
                }
                State::ArrayLen => {
                    let (n, consumed) = read_usize_line(buf, config.max_multibulk, "multibulk")?;
                    if !consumed {
                        return Ok(None);
                    }
                    if n == 0 {
                        return Ok(Some(Frame { args: vec![] }));
                    }
                    self.array_len = n;
                    self.bulk_remaining = n;
                    self.state = State::BulkLen;
                }
                State::BulkLen => {
                    let (len, consumed) =
                        read_prefixed_usize_line(buf, b'$', config.max_bulk_len, "bulk")?;
                    if !consumed {
                        return Ok(None);
                    }
                    self.current_bulk_len = Some(len);
                    self.state = State::BulkData;
                }
                State::BulkData => {
                    let len = self.current_bulk_len.unwrap_or(0);
                    let need = len + 2;
                    if buf.len() < need {
                        return Ok(None);
                    }
                    if len > 0 {
                        let data = buf.split_to(len).freeze();
                        self.args.push(data);
                    } else {
                        self.args.push(Bytes::new());
                    }
                    if buf[0] != b'\r' || buf[1] != b'\n' {
                        return Err(ProtocolError::MissingCrlf);
                    }
                    buf.advance(2);
                    self.bulk_remaining -= 1;
                    if self.bulk_remaining == 0 {
                        self.state = State::WaitType;
                        return Ok(Some(Frame {
                            args: std::mem::take(&mut self.args),
                        }));
                    }
                    self.state = State::BulkLen;
                }
                State::Inline => {
                    while buf.has_remaining() {
                        let b = buf[0];
                        if b == b'\r' {
                            if buf.len() < 2 || buf[1] != b'\n' {
                                return Err(ProtocolError::MissingCrlf);
                            }
                            buf.advance(2);
                            let line = std::mem::take(&mut self.inline_buf);
                            if line.len() > config.max_inline_len {
                                return Err(ProtocolError::TooLarge("inline"));
                            }
                            self.state = State::WaitType;
                            let args = split_inline(&line);
                            return Ok(Some(Frame { args }));
                        }
                        self.inline_buf.push(b);
                        buf.advance(1);
                        if self.inline_buf.len() > config.max_inline_len {
                            return Err(ProtocolError::TooLarge("inline"));
                        }
                    }
                    return Ok(None);
                }
            }
        }
    }
}

fn split_inline(line: &[u8]) -> Vec<Bytes> {
    let s = std::str::from_utf8(line).unwrap_or("");
    s.split_whitespace()
        .map(|p| Bytes::from(p.as_bytes().to_vec()))
        .collect()
}

fn read_usize_line(
    buf: &mut BytesMut,
    max: usize,
    kind: &'static str,
) -> Result<(usize, bool), ProtocolError> {
    let pos = buf.iter().position(|&b| b == b'\r');
    let Some(pos) = pos else {
        return Ok((0, false));
    };
    if pos + 1 >= buf.len() {
        return Ok((0, false));
    }
    if buf[pos + 1] != b'\n' {
        return Err(ProtocolError::MissingCrlf);
    }
    let line = &buf[..pos];
    if line.is_empty() {
        return Err(ProtocolError::InvalidLength);
    }
    let mut val: usize = 0;
    for &b in line {
        if !b.is_ascii_digit() {
            return Err(ProtocolError::InvalidLength);
        }
        val = val
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as usize))
            .ok_or(ProtocolError::Overflow)?;
        if val > max {
            return Err(ProtocolError::TooLarge(kind));
        }
    }
    buf.advance(pos + 2);
    Ok((val, true))
}

fn read_prefixed_usize_line(
    buf: &mut BytesMut,
    prefix: u8,
    max: usize,
    kind: &'static str,
) -> Result<(usize, bool), ProtocolError> {
    if buf.is_empty() {
        return Ok((0, false));
    }
    if buf[0] != prefix {
        return Err(ProtocolError::InvalidPrefix(buf[0]));
    }

    let Some(relative_pos) = buf[1..].iter().position(|&b| b == b'\r') else {
        return Ok((0, false));
    };
    let line_end = relative_pos + 1;
    if line_end + 1 >= buf.len() {
        return Ok((0, false));
    }
    if buf[line_end + 1] != b'\n' {
        return Err(ProtocolError::MissingCrlf);
    }

    buf.advance(1);
    read_usize_line(buf, max, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config::default()
    }

    #[test]
    fn parse_get() {
        let mut p = Parser::new();
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        let frame = p.next_frame(&mut buf, &cfg()).unwrap().unwrap();
        assert_eq!(frame.args.len(), 2);
        assert_eq!(frame.args[0].as_ref(), b"GET");
        assert_eq!(frame.args[1].as_ref(), b"foo");
    }

    #[test]
    fn parse_inline() {
        let mut p = Parser::new();
        let mut buf = BytesMut::from("ping\r\n");
        let frame = p.next_frame(&mut buf, &cfg()).unwrap().unwrap();
        assert_eq!(frame.args[0].as_ref(), b"ping");
    }

    #[test]
    fn parses_resp_one_byte_at_a_time() {
        let input = b"*3\r\n$3\r\nSET\r\n$13\r\nmetric_key_93\r\n$5\r\nvalue\r\n";
        let mut parser = Parser::new();
        let mut buf = BytesMut::new();
        let mut parsed = None;

        for byte in input {
            buf.extend_from_slice(&[*byte]);
            if let Some(frame) = parser.next_frame(&mut buf, &cfg()).unwrap() {
                parsed = Some(frame);
            }
        }

        let frame = parsed.expect("complete frame");
        assert_eq!(frame.args.len(), 3);
        assert_eq!(frame.args[0].as_ref(), b"SET");
        assert_eq!(frame.args[1].as_ref(), b"metric_key_93");
        assert_eq!(frame.args[2].as_ref(), b"value");
        assert!(buf.is_empty());
    }
}
