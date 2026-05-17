use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use crate::{
    constants::{CANDIDATES_IN_CAP, EXPIRED_KEYS_CAP},
    spsc::SPSCQueue,
    time::now_ns,
};

/// Snapshot of (uid, last_write_ts) sent from EventLoop to TTLEvictionWorker.
/// last_write_ts is compared at drain time — mismatch cancels eviction (false-eviction guard).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Candidate {
    pub uid: u32,
    pub last_write_ts: u64,
}

/// Background TTL checker. NEVER accesses Store directly.
/// Consumes Candidates from candidates_in, forwards expired ones to expired_out.
pub struct TTLEvictionWorker {
    running: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TTLEvictionWorker {
    /// Spawn eviction thread. ttl_ns = Config.ttl_seconds * 1_000_000_000.
    pub fn new(
        candidates_in: Arc<SPSCQueue<Candidate, CANDIDATES_IN_CAP>>,
        expired_out: Arc<SPSCQueue<Candidate, EXPIRED_KEYS_CAP>>,
        ttl_ns: u64,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let running2 = Arc::clone(&running);
        let thread = std::thread::spawn(move || {
            eviction_loop(&candidates_in, &expired_out, ttl_ns, &running2);
        });
        Self { running, thread: Some(thread) }
    }

    /// Signal worker to stop and block until thread exits.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(t) = self.thread.take() {
            t.join().expect("TTL eviction thread panicked");
        }
    }
}

impl Drop for TTLEvictionWorker {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.stop();
        }
    }
}

fn eviction_loop(
    candidates_in: &SPSCQueue<Candidate, CANDIDATES_IN_CAP>,
    expired_out: &SPSCQueue<Candidate, EXPIRED_KEYS_CAP>,
    ttl_ns: u64,
    running: &AtomicBool,
) {
    while running.load(Ordering::Acquire) {
        drain_batch(candidates_in, expired_out, ttl_ns);
        // Yield to avoid burning a full CPU core when queue is empty
        std::thread::yield_now();
    }
    // Final drain after stop signal — process any remaining candidates
    drain_batch(candidates_in, expired_out, ttl_ns);
}

#[inline]
fn drain_batch(
    candidates_in: &SPSCQueue<Candidate, CANDIDATES_IN_CAP>,
    expired_out: &SPSCQueue<Candidate, EXPIRED_KEYS_CAP>,
    ttl_ns: u64,
) {
    while let Some(c) = candidates_in.pop() {
        if now_ns().saturating_sub(c.last_write_ts) > ttl_ns {
            // Silent drop if full — EventLoop will re-sample on next collect cycle
            expired_out.push(c);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_expired_candidate_forwarded() {
        let candidates_in = Arc::new(SPSCQueue::<Candidate, CANDIDATES_IN_CAP>::new());
        let expired_out = Arc::new(SPSCQueue::<Candidate, EXPIRED_KEYS_CAP>::new());
        let ttl_ns = 1_000_000u64; // 1 ms TTL

        let mut worker = TTLEvictionWorker::new(
            Arc::clone(&candidates_in),
            Arc::clone(&expired_out),
            ttl_ns,
        );

        // Push a candidate that expired 2ms ago
        let old_ts = now_ns().saturating_sub(2_000_000);
        candidates_in.push(Candidate { uid: 42, last_write_ts: old_ts });

        std::thread::sleep(std::time::Duration::from_millis(20));
        worker.stop();

        assert_eq!(expired_out.pop(), Some(Candidate { uid: 42, last_write_ts: old_ts }));
    }

    #[test]
    fn test_fresh_candidate_not_forwarded() {
        let candidates_in = Arc::new(SPSCQueue::<Candidate, CANDIDATES_IN_CAP>::new());
        let expired_out = Arc::new(SPSCQueue::<Candidate, EXPIRED_KEYS_CAP>::new());
        let ttl_ns = 60_000_000_000u64; // 60s TTL

        let mut worker = TTLEvictionWorker::new(
            Arc::clone(&candidates_in),
            Arc::clone(&expired_out),
            ttl_ns,
        );

        candidates_in.push(Candidate { uid: 7, last_write_ts: now_ns() });
        std::thread::sleep(std::time::Duration::from_millis(10));
        worker.stop();

        assert_eq!(expired_out.pop(), None);
    }

    #[test]
    fn test_stop_is_idempotent() {
        let candidates_in = Arc::new(SPSCQueue::<Candidate, CANDIDATES_IN_CAP>::new());
        let expired_out = Arc::new(SPSCQueue::<Candidate, EXPIRED_KEYS_CAP>::new());
        let mut worker = TTLEvictionWorker::new(
            Arc::clone(&candidates_in),
            Arc::clone(&expired_out),
            1_000_000,
        );
        worker.stop();
        // Drop should not panic (thread already taken)
    }
}
