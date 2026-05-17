use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use weloxs::{
    aof::AOFWriter,
    config::Config,
    event_loop::{bind_listen_socket, EventLoop},
    recovery::recover,
    store::Store,
};

// Signal handlers cannot capture Arc, so they publish to the active shutdown flag via a raw pointer.
static SHUTDOWN_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(std::ptr::null_mut());

extern "C" fn handle_signal(_: libc::c_int) {
    let ptr = SHUTDOWN_PTR.load(Ordering::SeqCst);
    if !ptr.is_null() {
        unsafe { (*ptr).store(true, Ordering::SeqCst) };
    }
}

fn install_signal_handlers() {
    unsafe {
        // SIGPIPE: ignore — write() returns EPIPE instead of killing the process
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);

        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handle_signal as *const () as libc::sighandler_t;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
    }
}

fn main() {
    env_logger::init();
    log::info!("weloxs starting (COMPILED_N={})", weloxs::COMPILED_N);

    let args: Vec<String> = std::env::args().collect();

    // 1. Config load + validate
    let config = Config::from_args(&args).unwrap_or_else(|e| {
        log::error!("config error: {e}");
        std::process::exit(1);
    });
    log::info!("config loaded: port={} n={}", config.listen_port, config.max_records);

    // 2. Store construction
    let mut store = Store::with_pool_slots(config.memory_pool_slots);

    // 3. Bind listen socket (before recovery — hold the port while we load data)
    let listen_fd = bind_listen_socket(config.listen_port);
    log::info!("listening on :{}", config.listen_port);

    // 4. AOFWriter open
    let aof = AOFWriter::open(&config.aof_path, config.aof_fsync.clone()).unwrap_or_else(|e| {
        log::error!("cannot open AOF {}: {e}", config.aof_path);
        std::process::exit(1);
    });

    // 5. Signal handlers (SIGTERM, SIGINT → shutdown; SIGPIPE → ignore)
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    SHUTDOWN_PTR.store(Arc::as_ptr(&shutdown_flag) as *mut AtomicBool, Ordering::SeqCst);
    install_signal_handlers();

    // 6. Recovery: snapshot load + AOF replay
    recover(&mut store, &config.snapshot_path, &config.aof_path).unwrap_or_else(|e| {
        log::error!("recovery failed: {e}");
        std::process::exit(1);
    });
    log::info!("recovery complete: {} keys loaded", store.len());

    // 7. EventLoop (spawns TTL worker internally)
    let event_loop = EventLoop::new(config, store, aof, listen_fd, shutdown_flag);
    event_loop.run();

    log::info!("weloxs shut down cleanly");
}
