//! Batched io_uring recv/send for the mio reactor hot path.
//!
//! Sockets stay registered with epoll for readiness; once ready, many
//! connections' read/write syscalls are collapsed into one (or a few)
//! `io_uring_enter` calls.

use std::io;

use io_uring::{opcode, types, IoUring};

use crate::net::connection::{Connection, DriveResult};
use crate::storage::shard::Shard;

const MAX_BATCH: usize = 256;
const MAX_IOV: usize = 16;

const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;

#[inline]
fn pack(op: u64, idx: u32) -> u64 {
    (op << 32) | idx as u64
}

#[inline]
fn unpack(ud: u64) -> (u64, u32) {
    (ud >> 32, ud as u32)
}

pub struct UringBatch {
    ring: IoUring,
    /// Scratch iovec arrays for in-flight writev (stable for the submit..CQE window).
    iov_slots: Vec<[libc::iovec; MAX_IOV]>,
}

impl UringBatch {
    pub fn try_new(entries: u32) -> io::Result<Self> {
        let ring = IoUring::builder().build(entries)?;
        Ok(Self {
            ring,
            iov_slots: Vec::with_capacity(MAX_BATCH),
        })
    }

    /// Recv into each connection's spare read buffer, then process commands.
    /// Returns indices that should be closed.
    ///
    /// Kept for a future always-in-flight completion path; the reactor currently
    /// uses sync reads (edge-triggered epoll must drain until EAGAIN).
    #[allow(dead_code)]
    pub fn recv_ready(
        &mut self,
        conns: &mut [Option<Connection>],
        indices: &[usize],
        shards: &mut [Shard],
    ) -> Vec<usize> {
        let mut to_close = Vec::new();
        if indices.is_empty() {
            return to_close;
        }

        for chunk in indices.chunks(MAX_BATCH) {
            let mut submitted: Vec<usize> = Vec::with_capacity(chunk.len());
            {
                let mut sq = self.ring.submission();
                for &idx in chunk {
                    let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                        continue;
                    };
                    let Some((ptr, len)) = conn.prepare_recv_buf() else {
                        continue;
                    };
                    if len == 0 {
                        continue;
                    }
                    let fd = types::Fd(conn.as_raw_fd());
                    let entry = opcode::Recv::new(fd, ptr, len)
                        .build()
                        .user_data(pack(OP_RECV, idx as u32));
                    // SAFETY: read buffer stays pinned until CQE; we wait before mutating.
                    if unsafe { sq.push(&entry) }.is_err() {
                        break;
                    }
                    submitted.push(idx);
                }
            }

            if submitted.is_empty() {
                continue;
            }

            if let Err(e) = self.ring.submit_and_wait(submitted.len()) {
                tracing::debug!("io_uring recv submit: {e}");
                for &idx in &submitted {
                    if let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) {
                        if matches!(conn.drive(true, true, shards, false), DriveResult::Closed) {
                            to_close.push(idx);
                        }
                    }
                }
                continue;
            }

            let mut cq = self.ring.completion();
            cq.sync();
            for cqe in cq {
                let (op, idx) = unpack(cqe.user_data());
                if op != OP_RECV {
                    continue;
                }
                let idx = idx as usize;
                let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                    continue;
                };
                match conn.complete_recv(cqe.result(), shards) {
                    DriveResult::Closed => to_close.push(idx),
                    DriveResult::Pending => {}
                }
            }
        }

        to_close
    }

    /// Writev pending outbound buffers for `indices`.
    pub fn send_pending(
        &mut self,
        conns: &mut [Option<Connection>],
        indices: &[usize],
    ) -> Vec<usize> {
        let mut to_close = Vec::new();
        if indices.is_empty() {
            return to_close;
        }

        while self.iov_slots.len() < MAX_BATCH {
            self.iov_slots.push(
                [libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_IOV],
            );
        }

        for chunk in indices.chunks(MAX_BATCH) {
            let mut submitted: Vec<usize> = Vec::with_capacity(chunk.len());
            {
                let mut sq = self.ring.submission();
                let mut slot = 0usize;
                for &idx in chunk {
                    let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                        continue;
                    };
                    if !conn.has_pending_out() {
                        continue;
                    }
                    let iov = &mut self.iov_slots[slot];
                    let n_iov = conn.fill_send_iovecs(iov);
                    if n_iov == 0 {
                        continue;
                    }
                    let fd = types::Fd(conn.as_raw_fd());
                    let entry = opcode::Writev::new(fd, iov.as_ptr(), n_iov as u32)
                        .build()
                        .user_data(pack(OP_SEND, idx as u32));
                    // SAFETY: iov_slots[slot] and out_buf stable until CQE.
                    if unsafe { sq.push(&entry) }.is_err() {
                        break;
                    }
                    submitted.push(idx);
                    slot += 1;
                }
            }

            if submitted.is_empty() {
                continue;
            }

            if let Err(e) = self.ring.submit_and_wait(submitted.len()) {
                tracing::debug!("io_uring send submit: {e}");
                for &idx in &submitted {
                    if let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) {
                        if conn.flush_sync().is_err() {
                            to_close.push(idx);
                        } else if conn.should_close_now() {
                            to_close.push(idx);
                        }
                    }
                }
                continue;
            }

            let mut cq = self.ring.completion();
            cq.sync();
            for cqe in cq {
                let (op, idx) = unpack(cqe.user_data());
                if op != OP_SEND {
                    continue;
                }
                let idx = idx as usize;
                let Some(conn) = conns.get_mut(idx).and_then(|c| c.as_mut()) else {
                    continue;
                };
                match conn.complete_send(cqe.result()) {
                    DriveResult::Closed => to_close.push(idx),
                    DriveResult::Pending => {
                        if conn.has_pending_out() {
                            if conn.flush_sync().is_err() {
                                to_close.push(idx);
                            } else if conn.should_close_now() {
                                to_close.push(idx);
                            }
                        }
                    }
                }
            }
        }

        to_close
    }
}
