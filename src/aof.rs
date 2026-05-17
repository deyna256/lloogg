use std::cell::UnsafeCell;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use crate::{
    config::FsyncMode,
    constants::{AOF_BUF_SIZE, AOF_FLUSH_INTERVAL_MS, AOF_OVERFLOW_WARN_S, OPCODE_DEL, OPCODE_PUSH},
    error::Result,
    record::Record,
    time::now_ns,
};

// SAFETY: AOFBuf is shared between event loop (writer) and flush thread (reader).
// The ring buffer protocol guarantees disjoint access:
// - Event loop writes to [head%SIZE .. head%SIZE+n) exclusively
// - Flush thread reads from [tail%SIZE .. tail%SIZE+n) exclusively
// - head - tail <= AOF_BUF_SIZE invariant ensures these ranges never overlap
struct AOFBuf(UnsafeCell<Box<[u8; AOF_BUF_SIZE]>>);
unsafe impl Sync for AOFBuf {}
unsafe impl Send for AOFBuf {}

pub struct AOFWriter {
    buf: Arc<AOFBuf>,
    head: Arc<AtomicU64>,
    tail: Arc<AtomicU64>,
    aof_fd: RawFd,
    fsync_mode: FsyncMode,
    running: Arc<AtomicBool>,
    flush_thread: Option<std::thread::JoinHandle<()>>,
}

impl AOFWriter {
    pub fn open(path: &str, fsync_mode: FsyncMode) -> Result<Self> {
        use std::ffi::CString;
        let cpath = CString::new(path).unwrap();
        let fd = unsafe {
            libc::open(
                cpath.as_ptr(),
                libc::O_CREAT | libc::O_WRONLY | libc::O_APPEND | libc::O_CLOEXEC,
                0o644i32,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        // Allocate 4MB buffer on the heap directly — avoid stack overflow from large array literal.
        // SAFETY: zeroed u8 array is a valid, fully-initialized value.
        let boxed: Box<[u8; AOF_BUF_SIZE]> = unsafe {
            let raw = std::alloc::alloc_zeroed(
                std::alloc::Layout::new::<[u8; AOF_BUF_SIZE]>(),
            ) as *mut [u8; AOF_BUF_SIZE];
            Box::from_raw(raw)
        };
        let buf = Arc::new(AOFBuf(UnsafeCell::new(boxed)));
        let head = Arc::new(AtomicU64::new(0));
        let tail = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));

        let flush_thread = match fsync_mode {
            FsyncMode::Always => None,
            FsyncMode::Everysec | FsyncMode::No => {
                let buf2 = Arc::clone(&buf);
                let head2 = Arc::clone(&head);
                let tail2 = Arc::clone(&tail);
                let running2 = Arc::clone(&running);
                let mode2 = fsync_mode.clone();
                Some(std::thread::spawn(move || {
                    flush_loop(fd, &buf2, &head2, &tail2, &running2, &mode2);
                }))
            }
        };

        Ok(AOFWriter { buf, head, tail, aof_fd: fd, fsync_mode, running, flush_thread })
    }

    /// Write PUSH entry to AOF. Called from event loop thread only.
    pub fn append_push(&mut self, uid: u32, record: Record, write_ts: u64) {
        let mut entry = [0u8; 32]; // 15 header + 17 payload
        entry[0] = OPCODE_PUSH;
        entry[1..5].copy_from_slice(&uid.to_le_bytes());
        entry[5..13].copy_from_slice(&write_ts.to_le_bytes());
        entry[13..15].copy_from_slice(&17u16.to_le_bytes());
        // SAFETY: Record is #[repr(C, packed)] with size==17; byte transmutation is safe
        let rec_bytes: [u8; 17] = unsafe { std::mem::transmute(record) };
        entry[15..32].copy_from_slice(&rec_bytes);
        self.write_entry(&entry);
    }

    /// Write DEL entry to AOF. Called from event loop thread only.
    pub fn append_del(&mut self, uid: u32, write_ts: u64) {
        let mut entry = [0u8; 15];
        entry[0] = OPCODE_DEL;
        entry[1..5].copy_from_slice(&uid.to_le_bytes());
        entry[5..13].copy_from_slice(&write_ts.to_le_bytes());
        entry[13..15].copy_from_slice(&0u16.to_le_bytes());
        self.write_entry(&entry);
    }

    fn write_entry(&mut self, data: &[u8]) {
        match self.fsync_mode {
            FsyncMode::Always => {
                // Synchronous path: write + fdatasync in event loop thread
                // SAFETY: aof_fd is valid and open; data is a valid slice
                checked_write(self.aof_fd, data.as_ptr(), data.len());
                unsafe { libc::fdatasync(self.aof_fd); }
            }
            _ => self.ring_write(data),
        }
    }

    fn ring_write(&mut self, data: &[u8]) {
        let required = data.len() as u64;
        // Spin if ring buffer is full (WAL invariant: never drop an entry)
        let warn_deadline = now_ns() + AOF_OVERFLOW_WARN_S * 1_000_000_000;
        loop {
            let head = self.head.load(Ordering::Relaxed);
            let tail = self.tail.load(Ordering::Acquire);
            if head - tail + required <= AOF_BUF_SIZE as u64 {
                break;
            }
            if now_ns() > warn_deadline {
                log::error!("AOF ring buffer overflow >{}s — client will timeout", AOF_OVERFLOW_WARN_S);
            }
            std::hint::spin_loop();
        }

        let head = self.head.load(Ordering::Relaxed);
        let offset = (head as usize) % AOF_BUF_SIZE;
        let end = offset + data.len();

        // SAFETY: disjoint from flush thread's read range [tail%SIZE..tail%SIZE+pending)
        let buf_ptr = unsafe { (*self.buf.0.get()).as_mut_ptr() };

        if end <= AOF_BUF_SIZE {
            // Single fragment — no wrap
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(offset), data.len()); }
        } else {
            // Two fragments — entry straddles buffer boundary
            let first_len = AOF_BUF_SIZE - offset;
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(offset), first_len);
                std::ptr::copy_nonoverlapping(data[first_len..].as_ptr(), buf_ptr, data.len() - first_len);
            }
        }
        // Release after LAST fragment — flush thread sees complete entry
        self.head.store(head + data.len() as u64, Ordering::Release);
    }

    /// Synchronously flush all pending bytes to disk. Used in tests and shutdown.
    pub fn flush_sync(&mut self) {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Relaxed);
        let pending = (head - tail) as usize;
        if pending == 0 { return; }
        let offset = (tail as usize) % AOF_BUF_SIZE;

        // SAFETY: ranges are within allocated buf; head/tail protocol guarantees validity
        let buf_ptr = unsafe { (*self.buf.0.get()).as_ptr() };

        if offset + pending <= AOF_BUF_SIZE {
            checked_write(self.aof_fd, unsafe { buf_ptr.add(offset) } as *const u8, pending);
        } else {
            let first = AOF_BUF_SIZE - offset;
            checked_write(self.aof_fd, unsafe { buf_ptr.add(offset) } as *const u8, first);
            checked_write(self.aof_fd, buf_ptr as *const u8, pending - first);
        }
        unsafe { libc::fdatasync(self.aof_fd); }
        self.tail.store(head, Ordering::Release);
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(t) = self.flush_thread.take() {
            t.join().ok();
        }
        self.flush_sync();
        if self.aof_fd >= 0 {
            unsafe { libc::close(self.aof_fd); }
            self.aof_fd = -1;
        }
    }
}

impl Drop for AOFWriter {
    fn drop(&mut self) {
        if self.aof_fd >= 0 {
            self.shutdown();
        }
    }
}

fn flush_loop(
    fd: RawFd,
    buf: &AOFBuf,
    head: &AtomicU64,
    tail: &AtomicU64,
    running: &AtomicBool,
    mode: &FsyncMode,
) {
    while running.load(Ordering::Acquire) {
        flush_once(fd, buf, head, tail, mode);
        std::thread::sleep(std::time::Duration::from_millis(AOF_FLUSH_INTERVAL_MS));
    }
    // Final drain
    flush_once(fd, buf, head, tail, mode);
}

fn flush_once(fd: RawFd, buf: &AOFBuf, head: &AtomicU64, tail: &AtomicU64, mode: &FsyncMode) {
    let h = head.load(Ordering::Acquire);
    let t = tail.load(Ordering::Relaxed);
    let pending = (h - t) as usize;
    if pending == 0 { return; }

    let offset = (t as usize) % AOF_BUF_SIZE;
    // SAFETY: flush thread reads [tail%SIZE..tail%SIZE+pending); event loop writes ahead of this
    let buf_ptr = unsafe { (*buf.0.get()).as_ptr() };

    if offset + pending <= AOF_BUF_SIZE {
        checked_write(fd, unsafe { buf_ptr.add(offset) } as *const u8, pending);
    } else {
        let first = AOF_BUF_SIZE - offset;
        checked_write(fd, unsafe { buf_ptr.add(offset) } as *const u8, first);
        checked_write(fd, buf_ptr as *const u8, pending - first);
    }
    if matches!(mode, FsyncMode::Everysec) {
        unsafe { libc::fdatasync(fd); }
    }
    // Release: event loop acquire-loads tail to check free space
    tail.store(h, Ordering::Release);
}

fn checked_write(fd: RawFd, ptr: *const u8, len: usize) {
    // SAFETY: ptr and len are valid — caller ensures this
    let written = unsafe { libc::write(fd, ptr as *const libc::c_void, len) };
    if written < 0 {
        log::error!("AOF write error: {}", std::io::Error::last_os_error());
    } else if written as usize != len {
        log::error!("AOF short write: expected {len}, wrote {written}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    fn make_record() -> Record { Record { event_type: 1, timestamp: 42, url_hash: 99 } }

    #[test]
    fn test_append_push_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        let mut writer = AOFWriter::open(path.to_str().unwrap(), FsyncMode::No).unwrap();
        writer.append_push(1, make_record(), 123);
        writer.flush_sync();
        writer.shutdown();
        assert!(std::fs::metadata(&path).unwrap().len() > 0);
    }

    #[test]
    fn test_del_entry_is_15_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("del.aof");
        let mut writer = AOFWriter::open(path.to_str().unwrap(), FsyncMode::No).unwrap();
        writer.append_del(99, 456);
        writer.flush_sync();
        writer.shutdown();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 15);
        assert_eq!(data[0], OPCODE_DEL);
    }

    #[test]
    fn test_push_entry_layout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("push.aof");
        let mut writer = AOFWriter::open(path.to_str().unwrap(), FsyncMode::No).unwrap();
        let r = Record { event_type: 7, timestamp: 0x0102030405060708, url_hash: 0xDEADBEEFCAFEBABE };
        writer.append_push(0xAABBCCDD, r, 0x1122334455667788);
        writer.flush_sync();
        writer.shutdown();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 32); // 15 + 17
        assert_eq!(data[0], OPCODE_PUSH);
        assert_eq!(&data[1..5], &0xAABBCCDDu32.to_le_bytes());
        assert_eq!(&data[5..13], &0x1122334455667788u64.to_le_bytes());
        assert_eq!(&data[13..15], &17u16.to_le_bytes());
    }

    #[test]
    fn test_always_mode_bypasses_ring_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("always.aof");
        let mut writer = AOFWriter::open(path.to_str().unwrap(), FsyncMode::Always).unwrap();
        writer.append_push(1, make_record(), 1);
        writer.append_del(2, 2);
        writer.shutdown();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 32 + 15); // push + del
    }
}
