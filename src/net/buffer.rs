use bytes::BytesMut;
use std::cell::RefCell;

#[derive(Default)]
pub struct BufferPool {
    small: RefCell<Vec<BytesMut>>,
    medium: RefCell<Vec<BytesMut>>,
}

impl BufferPool {
    pub fn get(&self, hint: usize) -> BytesMut {
        if hint <= 4096 {
            if let Some(b) = self.small.borrow_mut().pop() {
                return b;
            }
            return BytesMut::with_capacity(4096);
        }
        if hint <= 16384 {
            if let Some(b) = self.medium.borrow_mut().pop() {
                return b;
            }
            return BytesMut::with_capacity(16384);
        }
        BytesMut::with_capacity(hint)
    }

    pub fn put(&self, mut buf: BytesMut) {
        let cap = buf.capacity();
        buf.clear();
        if cap <= 4096 {
            if self.small.borrow().len() < 256 {
                self.small.borrow_mut().push(buf);
            }
        } else if cap <= 16384 {
            if self.medium.borrow().len() < 64 {
                self.medium.borrow_mut().push(buf);
            }
        }
    }
}