use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use lloogg::{
    aof::AOFWriter,
    config::{Config, FsyncMode},
    constants::*,
    event_loop::{bind_listen_socket, EventLoop},
    recovery::recover,
    store::Store,
    COMPILED_N,
};

const TEST_POOL_SLOTS: u32 = 1000;

pub struct TestServer {
    pub port: u16,
    pub shutdown: Arc<AtomicBool>,
    pub thread: Option<std::thread::JoinHandle<()>>,
    pub tmpdir: Option<TempDir>,
}

impl TestServer {
    pub fn start() -> Self {
        Self::start_with_recovery(false)
    }

    pub fn start_recovered(tmpdir: Option<TempDir>) -> Self {
        let tmpdir = tmpdir.expect("tmpdir must be Some");
        let aof_path = tmpdir.path().join("test.aof");
        let snap_path = tmpdir.path().to_str().unwrap().to_string();
        let aof_str = aof_path.to_str().unwrap().to_string();

        let listen_fd = bind_listen_socket(0);
        let port = get_socket_port(listen_fd);

        let config = make_config(port, &aof_str, &snap_path);
        let mut store = Store::with_pool_slots(TEST_POOL_SLOTS);
        recover(&mut store, &snap_path, &aof_str).expect("recovery failed");
        let aof = AOFWriter::open(&aof_str, FsyncMode::No).expect("aof open failed");

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);
        let thread = std::thread::spawn(move || {
            EventLoop::new(config, store, aof, listen_fd, shutdown2).run();
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        TestServer { port, shutdown, thread: Some(thread), tmpdir: Some(tmpdir) }
    }

    fn start_with_recovery(_recover: bool) -> Self {
        let tmpdir = tempfile::tempdir().expect("tempdir");
        let aof_path = tmpdir.path().join("test.aof");
        let snap_path = tmpdir.path().to_str().unwrap().to_string();
        let aof_str = aof_path.to_str().unwrap().to_string();

        let listen_fd = bind_listen_socket(0);
        let port = get_socket_port(listen_fd);

        let config = make_config(port, &aof_str, &snap_path);
        let store = Store::with_pool_slots(TEST_POOL_SLOTS);
        let aof = AOFWriter::open(&aof_str, FsyncMode::No).expect("aof open failed");

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);
        let thread = std::thread::spawn(move || {
            EventLoop::new(config, store, aof, listen_fd, shutdown2).run();
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        TestServer { port, shutdown, thread: Some(thread), tmpdir: Some(tmpdir) }
    }

    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.stop();
        }
    }
}

fn make_config(port: u16, aof_path: &str, snap_path: &str) -> Config {
    Config {
        max_records: COMPILED_N as u16,
        ttl_seconds: 3600,
        aof_path: aof_path.to_string(),
        snapshot_path: snap_path.to_string(),
        snapshot_interval: 3600,
        listen_port: port,
        memory_pool_slots: TEST_POOL_SLOTS,
        aof_fsync: FsyncMode::No,
    }
}

fn get_socket_port(fd: i32) -> u16 {
    let mut addr: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
    unsafe { libc::getsockname(fd, &mut addr as *mut _ as _, &mut len) };
    u16::from_be(addr.sin6_port)
}

pub fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect failed");
    stream.set_read_timeout(Some(std::time::Duration::from_secs(2))).unwrap();
    stream
}

pub fn send_push(stream: &mut TcpStream, uid: u32, ts: u64) {
    let mut frame = [0u8; 26];
    frame[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    frame[2] = OPCODE_PUSH;
    frame[3..7].copy_from_slice(&uid.to_le_bytes());
    frame[7..9].copy_from_slice(&17u16.to_le_bytes());
    frame[9] = 1; // event_type
    frame[10..18].copy_from_slice(&ts.to_le_bytes());
    frame[18..26].copy_from_slice(&(ts * 2).to_le_bytes()); // url_hash
    stream.write_all(&frame).unwrap();
}

pub fn send_get(stream: &mut TcpStream, uid: u32, count: u16) {
    let mut frame = [0u8; 11];
    frame[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    frame[2] = OPCODE_GET;
    frame[3..7].copy_from_slice(&uid.to_le_bytes());
    frame[7..9].copy_from_slice(&2u16.to_le_bytes());
    frame[9..11].copy_from_slice(&count.to_le_bytes());
    stream.write_all(&frame).unwrap();
}

pub fn send_del(stream: &mut TcpStream, uid: u32) {
    let mut frame = [0u8; 9];
    frame[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    frame[2] = OPCODE_DEL;
    frame[3..7].copy_from_slice(&uid.to_le_bytes());
    frame[7..9].copy_from_slice(&0u16.to_le_bytes());
    stream.write_all(&frame).unwrap();
}

/// Read a complete response: 5-byte header + payload_len bytes.
pub fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).expect("read header");
    let payload_len = u16::from_le_bytes(header[3..5].try_into().unwrap()) as usize;
    let mut body = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut body).expect("read body");
    }
    let mut resp = header.to_vec();
    resp.extend(body);
    resp
}
