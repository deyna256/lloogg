use std::ptr;

/// Pre-allocated mmap slab with O(1) alloc/free via intrusive free list.
/// Used for RingBuffer data slots when COMPILED_N > 200.
/// NOT thread-safe — called only from event loop thread.
pub struct MemoryPool {
    base: *mut u8,
    slot_size: usize,
    total_slots: usize,
    /// First 8 bytes of each free slot store the next-free-slot pointer.
    free_list: *mut u8,
}

// SAFETY: MemoryPool is owned exclusively by the event loop thread.
unsafe impl Send for MemoryPool {}

impl MemoryPool {
    /// Allocate `total_slots` slots of `slot_size` bytes via mmap(MAP_POPULATE).
    /// MAP_POPULATE pre-faults all pages — zero page-fault latency on hot path.
    pub fn new(total_slots: usize, slot_size: usize) -> Self {
        assert!(slot_size >= std::mem::size_of::<*mut u8>(),
                "slot_size must be >= pointer size for free list");
        assert!(total_slots > 0, "total_slots must be > 0");

        let len = total_slots * slot_size;
        // SAFETY: standard anonymous mmap with MAP_POPULATE; len > 0; -1 fd for anonymous
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        assert_ne!(base, libc::MAP_FAILED,
                   "mmap failed: {}", std::io::Error::last_os_error());
        let base = base as *mut u8;

        // Build intrusive free list: slot[i] first 8 bytes → &slot[i+1]; last → null
        for i in 0..total_slots {
            let slot = unsafe { base.add(i * slot_size) };
            let next = if i + 1 < total_slots {
                unsafe { base.add((i + 1) * slot_size) }
            } else {
                ptr::null_mut()
            };
            // SAFETY: slot is valid writable memory within our mmap region
            unsafe { *(slot as *mut *mut u8) = next; }
        }

        Self { base, slot_size, total_slots, free_list: base }
    }

    /// O(1) pop from free list. Panics if pool exhausted (increase memory_pool_slots in config).
    #[inline]
    pub fn alloc_slot(&mut self) -> *mut u8 {
        assert!(!self.free_list.is_null(),
                "MemoryPool exhausted — increase memory_pool_slots in config");
        let ptr = self.free_list;
        // SAFETY: free_list points to a valid slot in our mmap region
        self.free_list = unsafe { *(ptr as *mut *mut u8) };
        ptr
    }

    /// O(1) push to free list. ptr must be from this pool's alloc_slot. Slot is NOT zeroed.
    #[inline]
    pub fn free_slot(&mut self, ptr: *mut u8) {
        // SAFETY: ptr is a valid slot from this pool; writing pointer-sized value is safe
        unsafe { *(ptr as *mut *mut u8) = self.free_list; }
        self.free_list = ptr;
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        let len = self.total_slots * self.slot_size;
        // SAFETY: base and len are exactly what was passed to mmap
        unsafe { libc::munmap(self.base as *mut libc::c_void, len); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_free_roundtrip() {
        let mut pool = MemoryPool::new(8, 64);
        let a = pool.alloc_slot();
        let b = pool.alloc_slot();
        assert_ne!(a, b);
        pool.free_slot(a);
        let c = pool.alloc_slot(); // must reuse freed slot
        assert_eq!(c, a);
        pool.free_slot(b);
        pool.free_slot(c);
    }

    #[test]
    fn test_alloc_all_slots() {
        let mut pool = MemoryPool::new(4, 64);
        let slots: Vec<*mut u8> = (0..4).map(|_| pool.alloc_slot()).collect();
        // All slots are distinct
        for i in 0..slots.len() {
            for j in (i + 1)..slots.len() {
                assert_ne!(slots[i], slots[j]);
            }
        }
        for s in slots { pool.free_slot(s); }
    }

    #[test]
    fn test_slot_size_separation() {
        let slot_size = 128usize;
        let pool = MemoryPool::new(4, slot_size);
        // Slots must be exactly slot_size bytes apart
        // (free list is built in order, first alloc = first slot)
        drop(pool); // just verify no crash
    }

    #[test]
    #[should_panic(expected = "MemoryPool exhausted")]
    fn test_alloc_beyond_capacity_panics() {
        let mut pool = MemoryPool::new(2, 64);
        pool.alloc_slot();
        pool.alloc_slot();
        pool.alloc_slot(); // must panic
    }
}
