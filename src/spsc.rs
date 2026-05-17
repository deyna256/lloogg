use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free single-producer single-consumer queue.
/// CAP must be a power of two (enforced by const assert).
/// push() returns false and discards item if full.
/// pop() returns None if empty. Neither operation ever blocks.
pub struct SPSCQueue<T: Copy, const CAP: usize> {
    buf: UnsafeCell<[MaybeUninit<T>; CAP]>,
    /// Written only by producer. Release store makes item visible to consumer.
    head: AtomicU64,
    /// Written only by consumer. Release store makes free space visible to producer.
    tail: AtomicU64,
}

const fn is_power_of_two(n: usize) -> bool {
    n > 0 && (n & (n - 1)) == 0
}

// SAFETY: exactly one producer and one consumer — atomics with release/acquire guarantee ordering.
unsafe impl<T: Copy, const CAP: usize> Sync for SPSCQueue<T, CAP> {}
unsafe impl<T: Copy, const CAP: usize> Send for SPSCQueue<T, CAP> {}

impl<T: Copy, const CAP: usize> SPSCQueue<T, CAP> {
    pub fn new() -> Self {
        // const block fires at compile time for every monomorphization that calls new()
        const { assert!(is_power_of_two(CAP), "SPSCQueue CAP must be a power of two"); }
        Self {
            buf: UnsafeCell::new(std::array::from_fn(|_| MaybeUninit::uninit())),
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
        }
    }

    /// Producer: enqueue item. Returns false if queue is full (item dropped, no allocation).
    #[inline]
    pub fn push(&self, item: T) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire); // acquire: see consumer's tail update
        if head.wrapping_sub(tail) >= CAP as u64 {
            return false; // full
        }
        let slot = (head as usize) & (CAP - 1);
        // SAFETY: slot is exclusively owned by producer (head not yet published to consumer)
        unsafe { (*self.buf.get())[slot].write(item); }
        // Release: consumer can now see the written item via acquire-load of head
        self.head.store(head.wrapping_add(1), Ordering::Release);
        true
    }

    /// Consumer: dequeue item. Returns None if empty.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire); // acquire: see producer's item write
        if tail == head {
            return None; // empty
        }
        let slot = (tail as usize) & (CAP - 1);
        // SAFETY: slot is exclusively owned by consumer (tail not yet published to producer)
        let item = unsafe { (*self.buf.get())[slot].assume_init_read() };
        // Release: producer can now see the freed space via acquire-load of tail
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    pub fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Relaxed) == self.head.load(Ordering::Acquire)
    }
}

impl<T: Copy, const CAP: usize> Default for SPSCQueue<T, CAP> {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_pop_roundtrip() {
        let q: SPSCQueue<u32, 8> = SPSCQueue::new();
        assert!(q.push(42));
        assert_eq!(q.pop(), Some(42));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_full_returns_false() {
        let q: SPSCQueue<u32, 4> = SPSCQueue::new();
        // CAP=4 means 4 slots; all 4 must be fillable (head - tail <= CAP)
        for i in 0..4u32 {
            assert!(q.push(i), "push {i} should succeed");
        }
        assert!(!q.push(99), "push when full should return false");
    }

    #[test]
    fn test_fifo_order() {
        let q: SPSCQueue<u32, 8> = SPSCQueue::new();
        for i in 0..5u32 { q.push(i); }
        for i in 0..5u32 {
            assert_eq!(q.pop(), Some(i), "expected fifo order");
        }
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_wrap_around() {
        let q: SPSCQueue<u32, 4> = SPSCQueue::new();
        q.push(1); q.push(2); q.push(3);
        q.pop(); q.pop(); // free 2 slots
        q.push(4); q.push(5); // these wrap around
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), Some(4));
        assert_eq!(q.pop(), Some(5));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn test_is_empty() {
        let q: SPSCQueue<u32, 8> = SPSCQueue::new();
        assert!(q.is_empty());
        q.push(1);
        assert!(!q.is_empty());
        q.pop();
        assert!(q.is_empty());
    }

    #[test]
    fn test_concurrent_spsc() {
        use std::sync::Arc;
        let q = Arc::new(SPSCQueue::<u64, 1024>::new());
        let q2 = Arc::clone(&q);
        let producer = std::thread::spawn(move || {
            for i in 0..512u64 {
                while !q2.push(i) { std::hint::spin_loop(); }
            }
        });
        let mut received = Vec::with_capacity(512);
        while received.len() < 512 {
            if let Some(v) = q.pop() { received.push(v); }
            else { std::hint::spin_loop(); }
        }
        producer.join().unwrap();
        assert_eq!(received, (0..512u64).collect::<Vec<_>>());
    }
}
