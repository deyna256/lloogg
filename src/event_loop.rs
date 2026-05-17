use hashbrown::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::{
    aof::AOFWriter,
    commands::{del_command, get_command, push_command},
    config::Config,
    connection::{Connection, State},
    constants::*,
    snapshot::SnapshotManager,
    spsc::SPSCQueue,
    store::Store,
    time::now_ns,
    ttl::{Candidate, TTLEvictionWorker},
};

pub struct EventLoop {
    config: Config,
    store: Store,
    aof: AOFWriter,
    snapshot: SnapshotManager,
    connections: HashMap<i32, Connection>,
    epoll_fd: i32,
    listen_fd: i32,
    ttl_worker: Option<TTLEvictionWorker>,
    candidates_in: Arc<SPSCQueue<Candidate, CANDIDATES_IN_CAP>>,
    expired_out: Arc<SPSCQueue<Candidate, EXPIRED_KEYS_CAP>>,
    last_ttl_collect: u64,
    shutdown_flag: Arc<AtomicBool>,
}

/// Create a non-blocking dual-stack TCP listen socket.
pub fn bind_listen_socket(port: u16) -> i32 {
    unsafe {
        let fd = libc::socket(
            libc::AF_INET6,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        );
        assert!(fd >= 0, "socket() failed: {}", std::io::Error::last_os_error());
        let opt: libc::c_int = 0; // IPV6_V6ONLY=0 — dual-stack
        libc::setsockopt(
            fd, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY,
            &opt as *const _ as _, std::mem::size_of_val(&opt) as _,
        );
        let opt: libc::c_int = 1;
        libc::setsockopt(
            fd, libc::SOL_SOCKET, libc::SO_REUSEADDR,
            &opt as *const _ as _, std::mem::size_of_val(&opt) as _,
        );
        let addr = libc::sockaddr_in6 {
            sin6_family: libc::AF_INET6 as _,
            sin6_port: port.to_be(),
            sin6_flowinfo: 0,
            sin6_addr: libc::in6_addr { s6_addr: [0u8; 16] },
            sin6_scope_id: 0,
        };
        let r = libc::bind(
            fd, &addr as *const _ as _, std::mem::size_of_val(&addr) as _,
        );
        assert_eq!(r, 0, "bind() failed: {}", std::io::Error::last_os_error());
        libc::listen(fd, LISTEN_BACKLOG);
        fd
    }
}

impl EventLoop {
    pub fn new(
        config: Config,
        store: Store,
        aof: AOFWriter,
        listen_fd: i32,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Self {
        let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        assert!(epoll_fd >= 0, "epoll_create1 failed: {}", std::io::Error::last_os_error());

        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: listen_fd as u64 };
        unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, listen_fd, &mut ev) };

        let candidates_in: Arc<SPSCQueue<Candidate, CANDIDATES_IN_CAP>> =
            Arc::new(SPSCQueue::new());
        let expired_out: Arc<SPSCQueue<Candidate, EXPIRED_KEYS_CAP>> =
            Arc::new(SPSCQueue::new());

        let ttl_ns = config.ttl_seconds * 1_000_000_000;
        let ttl_worker = TTLEvictionWorker::new(
            Arc::clone(&candidates_in),
            Arc::clone(&expired_out),
            ttl_ns,
        );

        let snapshot = SnapshotManager::new(&config.snapshot_path);

        Self {
            config,
            store,
            aof,
            snapshot,
            connections: HashMap::new(),
            epoll_fd,
            listen_fd,
            ttl_worker: Some(ttl_worker),
            candidates_in,
            expired_out,
            last_ttl_collect: 0,
            shutdown_flag,
        }
    }

    pub fn run(mut self) {
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; MAX_EPOLL_EVENTS];
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) { break; }

            // Per-spec per-tick order: drain_expired → collect_ttl → snapshot → epoll_wait
            self.drain_expired_keys();
            self.collect_ttl_candidates();
            self.snapshot.maybe_fork(
                &self.store,
                self.config.snapshot_interval as u64 * 1_000_000_000,
            );

            let n = unsafe {
                libc::epoll_wait(
                    self.epoll_fd,
                    events.as_mut_ptr(),
                    MAX_EPOLL_EVENTS as _,
                    TTL_COLLECT_INTERVAL_S as i32 * 1000,
                )
            };

            let ready_count = n.max(0) as usize;
            for i in 0..ready_count {
                let fd = events[i].u64 as i32;
                let flags = events[i].events;
                if fd == self.listen_fd {
                    self.accept_connections();
                } else if flags & libc::EPOLLIN as u32 != 0 {
                    self.handle_read(fd);
                } else if flags & libc::EPOLLOUT as u32 != 0 {
                    self.handle_write(fd);
                }
            }
        }
        self.shutdown();
    }

    fn drain_expired_keys(&mut self) {
        while let Some(c) = self.expired_out.pop() {
            if let Some(buf_ptr) = self.store.get(c.uid) {
                // False-eviction guard: skip if key was written since candidate was collected
                if unsafe { (*buf_ptr).last_write_ts } == c.last_write_ts {
                    self.store.erase(c.uid);
                }
            }
        }
    }

    fn collect_ttl_candidates(&mut self) {
        let now = now_ns();
        if now - self.last_ttl_collect < TTL_COLLECT_INTERVAL_S * 1_000_000_000 {
            return;
        }
        self.last_ttl_collect = now;
        // Clone Arc before for_each to avoid simultaneous borrow of self
        let candidates_in = Arc::clone(&self.candidates_in);
        self.store.for_each(|uid, buf| {
            candidates_in.push(Candidate { uid, last_write_ts: buf.last_write_ts });
        });
    }

    fn accept_connections(&mut self) {
        loop {
            let fd = unsafe {
                libc::accept4(
                    self.listen_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            if fd < 0 {
                let err = std::io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EAGAIN) => break,
                    _ => { log::warn!("accept4: {err}"); break; }
                }
            }
            // TCP_NODELAY: disable Nagle — avoids 40ms delay when sending small responses
            // (Nagle + client delayed-ACK interaction kills pipelined throughput)
            let one: libc::c_int = 1;
            unsafe {
                libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY,
                    &one as *const _ as _, std::mem::size_of_val(&one) as _);
            }
            let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: fd as u64 };
            unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
            self.connections.insert(fd, Connection::new(fd));
        }
    }

    fn handle_read(&mut self, fd: i32) {
        loop {
            let conn = match self.connections.get_mut(&fd) {
                Some(c) => c,
                None => return,
            };

            let needed: usize = match conn.state {
                State::ReadHeader => 9,
                State::ReadPayload => 9 + conn.header_payload_len() as usize,
                State::Process | State::WriteResponse => break,
            };

            if conn.rbuf_pos >= needed { break; }

            let n = unsafe {
                libc::read(
                    fd,
                    conn.rbuf.as_mut_ptr().add(conn.rbuf_pos) as *mut libc::c_void,
                    needed - conn.rbuf_pos,
                )
            };

            if n < 0 {
                let err = std::io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EAGAIN) | Some(libc::EINTR) => return,
                    _ => { self.handle_close(fd); return; }
                }
            }
            if n == 0 { self.handle_close(fd); return; }

            let conn = self.connections.get_mut(&fd).unwrap();
            conn.rbuf_pos += n as usize;

            match conn.state {
                State::ReadHeader if conn.rbuf_pos >= 9 => {
                    match conn.validate_header() {
                        Err(e) => {
                            log::debug!("bad header fd={fd}: {e}");
                            self.handle_close(fd);
                            return;
                        }
                        Ok((_, _, payload_len)) => {
                            conn.state = if payload_len == 0 {
                                State::Process
                            } else {
                                State::ReadPayload
                            };
                        }
                    }
                }
                State::ReadPayload => {
                    let target = 9 + conn.header_payload_len() as usize;
                    if conn.rbuf_pos >= target {
                        conn.state = State::Process;
                    }
                }
                _ => {}
            }
        }

        if self.connections.get(&fd).map_or(false, |c| c.state == State::Process) {
            self.process_command(fd);
        }
    }

    fn process_command(&mut self, fd: i32) {
        // Remove from map to satisfy borrow checker; re-insert after dispatch
        let mut conn = match self.connections.remove(&fd) {
            Some(c) => c,
            None => return,
        };

        match conn.header_opcode() {
            OPCODE_PUSH => push_command(&mut conn, &mut self.store, &mut self.aof),
            OPCODE_GET => {
                if let Err(e) = conn.validate_get_count() {
                    log::debug!("bad GET count fd={fd}: {e}");
                    unsafe {
                        libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
                        libc::close(fd);
                    }
                    return;
                }
                get_command(&mut conn, &self.store);
            }
            OPCODE_DEL => del_command(&mut conn, &mut self.store, &mut self.aof),
            opcode => {
                log::debug!("unknown opcode {opcode:#04x} fd={fd}");
                unsafe {
                    libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
                    libc::close(fd);
                }
                return;
            }
        }

        if conn.state == State::WriteResponse {
            let mut ev = libc::epoll_event { events: libc::EPOLLOUT as u32, u64: fd as u64 };
            unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
        }

        conn.rbuf_pos = 0;
        self.connections.insert(fd, conn);
    }

    fn handle_write(&mut self, fd: i32) {
        loop {
            let conn = match self.connections.get_mut(&fd) {
                Some(c) => c,
                None => return,
            };
            if conn.state != State::WriteResponse { break; }

            let remaining = conn.wbuf_len - conn.wbuf_pos;
            if remaining == 0 { break; }

            let n = unsafe {
                libc::write(
                    fd,
                    conn.wbuf[conn.wbuf_pos..conn.wbuf_len].as_ptr() as *const libc::c_void,
                    remaining,
                )
            };

            if n < 0 {
                let err = std::io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::EAGAIN) | Some(libc::EINTR) => return,
                    _ => { self.handle_close(fd); return; }
                }
            }

            let conn = self.connections.get_mut(&fd).unwrap();
            conn.wbuf_pos += n as usize;

            if conn.wbuf_pos == conn.wbuf_len {
                conn.state = State::ReadHeader;
                conn.rbuf_pos = 0;
                conn.wbuf_pos = 0;
                conn.wbuf_len = 0;
                let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: fd as u64 };
                unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
                break;
            }
        }
    }

    fn handle_close(&mut self, fd: i32) {
        self.connections.remove(&fd);
        unsafe {
            libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
            libc::close(fd);
        }
    }

    fn shutdown(mut self) {
        if let Some(mut w) = self.ttl_worker.take() {
            w.stop();
        }
        self.aof.shutdown();
        let fds: Vec<i32> = self.connections.keys().copied().collect();
        for fd in fds {
            unsafe { libc::close(fd); }
        }
        unsafe {
            libc::close(self.epoll_fd);
            libc::close(self.listen_fd);
        }
    }
}
