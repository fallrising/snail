//! Vectored outbound queue: coalesce small replies, zero-copy large bulks via writev.

use std::collections::VecDeque;
use std::io::{self, ErrorKind, IoSlice, Write};

use bytes::{Bytes, BytesMut};

/// Threshold above which bulk bodies are queued as separate writev segments
/// instead of being copied into the contiguous tail buffer.
const ZC_BULK_THRESH: usize = 64;

pub struct OutBuf {
    /// Contiguous tail for small replies / headers (appended after any `segs`).
    buf: BytesMut,
    /// Frozen / zero-copy segments that precede `buf` in send order.
    segs: VecDeque<Bytes>,
    /// Byte offset into `segs[0]` for a partial write.
    seg_off: usize,
    /// Byte offset into `buf` for a partial write.
    buf_off: usize,
    pending: usize,
}

impl OutBuf {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
            segs: VecDeque::new(),
            seg_off: 0,
            buf_off: 0,
            pending: 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending == 0
    }

    #[inline]
    pub fn pending(&self) -> usize {
        self.pending
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.segs.clear();
        self.seg_off = 0;
        self.buf_off = 0;
        self.pending = 0;
    }

    /// Drop oversized backing storage after a full flush.
    pub fn maybe_shrink(&mut self) {
        if self.pending == 0 && self.buf.capacity() > 64 * 1024 {
            self.buf = BytesMut::with_capacity(4096);
            self.segs = VecDeque::new();
            self.seg_off = 0;
            self.buf_off = 0;
        }
    }

    #[inline]
    pub fn push_static(&mut self, bytes: &'static [u8]) {
        self.buf.extend_from_slice(bytes);
        self.pending += bytes.len();
    }

    #[inline]
    pub fn push_slice(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        self.pending += bytes.len();
    }

    /// Encode a RESP bulk string; large bodies skip the memcpy into `buf`.
    pub fn push_bulk(&mut self, body: &Bytes) {
        let hdr = itoa_usize_bytes(body.len());
        // "$<len>\r\n"
        self.buf.extend_from_slice(b"$");
        self.buf.extend_from_slice(hdr.as_bytes());
        self.buf.extend_from_slice(b"\r\n");
        self.pending += 1 + hdr.len() + 2;

        if body.len() <= ZC_BULK_THRESH {
            self.buf.extend_from_slice(body);
            self.buf.extend_from_slice(b"\r\n");
            self.pending += body.len() + 2;
        } else {
            self.freeze_tail();
            self.segs.push_back(body.clone());
            self.segs.push_back(Bytes::from_static(b"\r\n"));
            self.pending += body.len() + 2;
        }
    }

    /// Move contiguous tail into `segs` so a zero-copy segment can follow in order.
    fn freeze_tail(&mut self) {
        if self.buf_off > 0 {
            let _ = self.buf.split_to(self.buf_off);
            self.buf_off = 0;
        }
        if !self.buf.is_empty() {
            let frozen = std::mem::replace(&mut self.buf, BytesMut::with_capacity(4096)).freeze();
            self.segs.push_back(frozen);
        }
    }

    /// Write pending bytes with writev (falls back to write for a single segment).
    pub fn flush_to<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        while self.pending > 0 {
            let n = self.write_once(w)?;
            if n == 0 {
                return Err(io::Error::new(ErrorKind::WriteZero, "write zero"));
            }
            self.advance(n);
        }
        self.maybe_shrink();
        Ok(())
    }

    fn write_once<W: Write>(&mut self, w: &mut W) -> io::Result<usize> {
        // Fast path: only contiguous tail left.
        if self.segs.is_empty() {
            let slice = &self.buf[self.buf_off..];
            return w.write(slice);
        }

        let mut iov_storage: [IoSlice<'_>; 64] = [IoSlice::new(&[]); 64];
        let mut n_iov = 0usize;

        for (i, seg) in self.segs.iter().enumerate() {
            if n_iov >= 63 {
                break;
            }
            let off = if i == 0 { self.seg_off } else { 0 };
            if off < seg.len() {
                iov_storage[n_iov] = IoSlice::new(&seg[off..]);
                n_iov += 1;
            }
        }
        if n_iov < 64 && self.buf_off < self.buf.len() {
            iov_storage[n_iov] = IoSlice::new(&self.buf[self.buf_off..]);
            n_iov += 1;
        }

        if n_iov == 0 {
            return Ok(0);
        }
        w.write_vectored(&iov_storage[..n_iov])
    }

    fn advance(&mut self, mut n: usize) {
        self.pending = self.pending.saturating_sub(n);
        while n > 0 && !self.segs.is_empty() {
            let front_len = self.segs[0].len() - self.seg_off;
            if n < front_len {
                self.seg_off += n;
                return;
            }
            n -= front_len;
            self.segs.pop_front();
            self.seg_off = 0;
        }
        if n > 0 {
            self.buf_off += n;
            if self.buf_off >= self.buf.len() {
                self.buf.clear();
                self.buf_off = 0;
            }
        }
    }
}

struct UtoaBuf {
    buf: [u8; 20],
    start: usize,
}

impl UtoaBuf {
    fn as_bytes(&self) -> &[u8] {
        &self.buf[self.start..]
    }
    fn len(&self) -> usize {
        20 - self.start
    }
}

fn itoa_usize_bytes(mut n: usize) -> UtoaBuf {
    let mut buf = [0u8; 20];
    if n == 0 {
        buf[19] = b'0';
        return UtoaBuf { buf, start: 19 };
    }
    let mut i = 20;
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    UtoaBuf { buf, start: i }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn flush_static_and_bulk() {
        let mut out = OutBuf::with_capacity(64);
        out.push_static(b"+OK\r\n");
        out.push_bulk(&Bytes::from_static(b"hi"));
        let mut cur = Cursor::new(Vec::new());
        out.flush_to(&mut cur).unwrap();
        assert_eq!(cur.get_ref().as_slice(), b"+OK\r\n$2\r\nhi\r\n");
        assert!(out.is_empty());
    }

    #[test]
    fn writev_large_bulk_roundtrip() {
        let mut out = OutBuf::with_capacity(32);
        let body = Bytes::from(vec![b'x'; 128]);
        out.push_bulk(&body);
        let mut cur = Cursor::new(Vec::new());
        out.flush_to(&mut cur).unwrap();
        let got = cur.get_ref();
        assert!(got.starts_with(b"$128\r\n"));
        assert_eq!(&got[got.len() - 2..], b"\r\n");
        assert_eq!(got.len(), 1 + 3 + 2 + 128 + 2);
    }
}
