/// WeLoxs benchmark: multi-threaded PUSH throughput + latency percentiles.
///
/// Usage:
///   cargo run --bin bench --release -- [host:port] [options]
///
/// Options:
///   --threads N       OS threads (default: 4)
///   --count N         total PUSH requests (default: 100_000)
///   --warmup N        warmup requests per thread, not counted (default: 500)
///   --uid N           user_id to use (default: 1)
///   --pipeline N      requests pipelined before reading responses (default: 1)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const PROGRESS_GRANULARITY: usize = 256; // increment counter every N requests

// ── Wire protocol ────────────────────────────────────────────────────────────

const MAGIC_WIRE: u16 = 0xAE01;
const OPCODE_PUSH: u8 = 0x01;

fn build_push(uid: u32) -> [u8; 26] {
    let mut f = [0u8; 26];
    f[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    f[2] = OPCODE_PUSH;
    f[3..7].copy_from_slice(&uid.to_le_bytes());
    f[7..9].copy_from_slice(&17u16.to_le_bytes());
    f[9] = 1; // event_type
    f
}

// ── Per-thread work ──────────────────────────────────────────────────────────

struct ThreadResult {
    latencies_ns: Vec<u64>,
    elapsed_ns: u64,
    errors: u64,
}

fn tcp_connect(addr: &str) -> TcpStream {
    let s = TcpStream::connect(addr).unwrap_or_else(|e| {
        eprintln!("connect {addr}: {e}");
        std::process::exit(1);
    });
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(READ_TIMEOUT)).unwrap();
    s
}

fn run_thread(
    addr: String,
    uid: u32,
    warmup: usize,
    count: usize,
    pipeline: usize,
    barrier: Arc<Barrier>,
    warmup_done: Arc<AtomicUsize>,
    measure_done: Arc<AtomicUsize>,
) -> ThreadResult {
    let frame = build_push(uid);
    let mut stream = tcp_connect(&addr);
    let mut rbuf = [0u8; 5];

    // ── Warmup (sequential, not measured) ────────────────────────────────────
    for i in 0..warmup {
        if stream.write_all(&frame).is_err() { break; }
        if stream.read_exact(&mut rbuf).is_err() { break; }
        if i % PROGRESS_GRANULARITY == 0 {
            warmup_done.fetch_add(PROGRESS_GRANULARITY, Ordering::Relaxed);
        }
    }
    warmup_done.fetch_add(warmup % PROGRESS_GRANULARITY, Ordering::Relaxed);

    barrier.wait(); // all threads start measuring simultaneously

    // ── Measurement ──────────────────────────────────────────────────────────
    let mut latencies = Vec::with_capacity(count);
    let mut errors = 0u64;
    let wall = Instant::now();

    if pipeline <= 1 {
        for i in 0..count {
            let t0 = Instant::now();
            if stream.write_all(&frame).is_err() {
                errors += 1;
                stream = tcp_connect(&addr);
                continue;
            }
            match stream.read_exact(&mut rbuf) {
                Ok(_) => latencies.push(t0.elapsed().as_nanos() as u64),
                Err(_) => {
                    errors += 1;
                    stream = tcp_connect(&addr);
                }
            }
            if i % PROGRESS_GRANULARITY == 0 {
                measure_done.fetch_add(PROGRESS_GRANULARITY, Ordering::Relaxed);
            }
        }
    } else {
        let mut i = 0;
        while i < count {
            let batch = pipeline.min(count - i);
            let mut sent = 0usize;
            for _ in 0..batch {
                if stream.write_all(&frame).is_err() { errors += 1; break; }
                sent += 1;
            }
            let t0 = Instant::now();
            let mut received = 0usize;
            for _ in 0..sent {
                match stream.read_exact(&mut rbuf) {
                    Ok(_) => received += 1,
                    Err(e) => {
                        errors += 1;
                        match e.kind() {
                            std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::UnexpectedEof => stream = tcp_connect(&addr),
                            _ => {}
                        }
                        break;
                    }
                }
            }
            if received > 0 {
                let per_req = t0.elapsed().as_nanos() as u64 / received as u64;
                for _ in 0..received { latencies.push(per_req); }
            }
            let advance = sent.max(1);
            measure_done.fetch_add(advance, Ordering::Relaxed);
            i += advance;
        }
    }

    ThreadResult {
        elapsed_ns: wall.elapsed().as_nanos() as u64,
        latencies_ns: latencies,
        errors,
    }
}

// ── Stats ─────────────────────────────────────────────────────────────────────

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64 * p / 100.0).ceil() as usize).min(sorted.len()) - 1;
    sorted[idx]
}

fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 { format!("{ns} ns") }
    else if ns < 1_000_000 { format!("{:.1} µs", ns as f64 / 1_000.0) }
    else { format!("{:.2} ms", ns as f64 / 1_000_000.0) }
}

fn print_progress(label: &str, done: usize, total: usize, elapsed: f64) {
    let pct = (done.min(total) * 100) / total.max(1);
    let done_k = done.min(total);
    print!("\r  {label}  {pct:3}%  ({done_k} / {total})  {elapsed:.1}s   ");
    let _ = std::io::stdout().flush();
}

// ── CLI ───────────────────────────────────────────────────────────────────────

struct Cfg {
    addr: String,
    threads: usize,
    count: usize,
    warmup: usize,
    uid: u32,
    pipeline: usize,
}

fn parse_args() -> Cfg {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cfg = Cfg {
        addr: "127.0.0.1:7379".into(),
        threads: 4,
        count: 100_000,
        warmup: 500,
        uid: 1,
        pipeline: 1,
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--threads"  => { cfg.threads  = args[i+1].parse().unwrap(); i += 2; }
            "--count"    => { cfg.count    = args[i+1].parse().unwrap(); i += 2; }
            "--warmup"   => { cfg.warmup   = args[i+1].parse().unwrap(); i += 2; }
            "--uid"      => { cfg.uid      = args[i+1].parse().unwrap(); i += 2; }
            "--pipeline" => { cfg.pipeline = args[i+1].parse().unwrap(); i += 2; }
            other if !other.starts_with("--") => { cfg.addr = other.to_string(); i += 1; }
            other => { eprintln!("unknown flag: {other}"); i += 1; }
        }
    }
    cfg
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let cfg = parse_args();
    let per_thread = (cfg.count / cfg.threads).max(1);
    let total = per_thread * cfg.threads;
    let warmup_total = cfg.warmup * cfg.threads;

    println!("WeLoxs bench → {}", cfg.addr);
    println!("  threads={} count={} warmup={} pipeline={} uid={}",
        cfg.threads, total, cfg.warmup, cfg.pipeline, cfg.uid);

    let warmup_done  = Arc::new(AtomicUsize::new(0));
    let measure_done = Arc::new(AtomicUsize::new(0));

    // +1 so main participates in the barrier (releases all threads simultaneously)
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));

    let handles: Vec<_> = (0..cfg.threads).map(|_| {
        let addr         = cfg.addr.clone();
        let barrier      = Arc::clone(&barrier);
        let warmup_done  = Arc::clone(&warmup_done);
        let measure_done = Arc::clone(&measure_done);
        let uid      = cfg.uid;
        let warmup   = cfg.warmup;
        let count    = per_thread;
        let pipeline = cfg.pipeline;
        std::thread::spawn(move || {
            run_thread(addr, uid, warmup, count, pipeline, barrier, warmup_done, measure_done)
        })
    }).collect();

    // ── Warmup progress ───────────────────────────────────────────────────────
    let t0 = Instant::now();
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let done = warmup_done.load(Ordering::Relaxed);
        print_progress("warmup  ", done, warmup_total, t0.elapsed().as_secs_f64());
        if done >= warmup_total { break; }
        if t0.elapsed() > Duration::from_secs(120) { eprintln!("\nwarmup timed out"); break; }
    }
    println!("\r  warmup done                                            ");

    barrier.wait(); // release all threads

    // ── Measurement progress ──────────────────────────────────────────────────
    let t1 = Instant::now();
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let done = measure_done.load(Ordering::Relaxed);
        print_progress("measuring", done, total, t1.elapsed().as_secs_f64());
        if done >= total { break; }
        if handles.iter().all(|h| h.is_finished()) { break; }
    }
    println!("\r                                                          ");

    // ── Collect results ───────────────────────────────────────────────────────
    let results: Vec<ThreadResult> = handles
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();

    let mut all_latencies: Vec<u64> = results.iter()
        .flat_map(|r| r.latencies_ns.iter().copied())
        .collect();
    all_latencies.sort_unstable();

    let total_errors: u64 = results.iter().map(|r| r.errors).sum();
    let wall_ns = results.iter().map(|r| r.elapsed_ns).max().unwrap_or(1);
    let successful = all_latencies.len() as u64;
    let rps = successful as f64 / (wall_ns as f64 / 1e9);

    println!("── Results ──────────────────────────────────");
    println!("  Total sent  : {total}");
    println!("  Successful  : {successful}");
    println!("  Errors      : {total_errors}");
    println!("  Wall time   : {}", fmt_ns(wall_ns));
    println!("  Throughput  : {:.0} req/s", rps);
    println!();
    println!("── Latency ──────────────────────────────────");
    println!("  min   : {}", fmt_ns(*all_latencies.first().unwrap_or(&0)));
    println!("  p50   : {}", fmt_ns(percentile(&all_latencies, 50.0)));
    println!("  p90   : {}", fmt_ns(percentile(&all_latencies, 90.0)));
    println!("  p95   : {}", fmt_ns(percentile(&all_latencies, 95.0)));
    println!("  p99   : {}", fmt_ns(percentile(&all_latencies, 99.0)));
    println!("  p99.9 : {}", fmt_ns(percentile(&all_latencies, 99.9)));
    println!("  max   : {}", fmt_ns(*all_latencies.last().unwrap_or(&0)));
}
