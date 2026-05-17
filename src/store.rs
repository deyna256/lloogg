use crate::{
    COMPILED_N,
    constants::MEMORY_POOL_SLOTS_DEFAULT,
    memory_pool::MemoryPool,
    record::Record,
    ring_buffer::RingBuffer,
};
use hashbrown::HashMap;
use std::alloc::{alloc, dealloc, Layout};

enum StoreAlloc {
    Slab {
        base: *mut u8,
        layout: Layout,
        free_list: *mut u8,
    },
    Pool(MemoryPool),
}

unsafe impl Send for StoreAlloc {}

impl StoreAlloc {
    fn new(capacity: u16, total_slots: u32) -> Self {
        // Each slot must be large enough to hold both the Record data AND a pointer
        // (the free list stores the next-pointer in the first bytes of each free slot).
        let record_bytes = capacity as usize * std::mem::size_of::<Record>();
        let ptr_size = std::mem::size_of::<*mut u8>();
        // Round up to pointer alignment so the free-list pointer read/write is aligned.
        let slot_size = record_bytes.max(ptr_size).next_multiple_of(ptr_size);
        let total = total_slots as usize;

        if capacity as usize <= 200 {
            let layout = Layout::from_size_align(total * slot_size, 8)
                .expect("invalid slab layout");
            // SAFETY: layout.size() > 0 guaranteed by assertions above
            let base = unsafe { alloc(layout) };
            assert!(!base.is_null(), "slab allocation failed");
            for i in 0..total {
                let slot = unsafe { base.add(i * slot_size) };
                let next = if i + 1 < total {
                    unsafe { base.add((i + 1) * slot_size) }
                } else {
                    std::ptr::null_mut()
                };
                // SAFETY: slot is valid writable memory within the slab
                unsafe { *(slot as *mut *mut u8) = next; }
            }
            StoreAlloc::Slab { base, layout, free_list: base }
        } else {
            StoreAlloc::Pool(MemoryPool::new(total, slot_size))
        }
    }

    #[inline]
    fn alloc(&mut self) -> *mut Record {
        match self {
            StoreAlloc::Slab { free_list, .. } => {
                assert!(!free_list.is_null(), "slab exhausted — increase memory_pool_slots");
                let ptr = *free_list;
                // SAFETY: free_list points to a valid slab slot
                *free_list = unsafe { *(ptr as *mut *mut u8) };
                ptr as *mut Record
            }
            StoreAlloc::Pool(p) => p.alloc_slot() as *mut Record,
        }
    }

    #[inline]
    fn free(&mut self, ptr: *mut Record) {
        match self {
            StoreAlloc::Slab { free_list, .. } => {
                // SAFETY: ptr is a valid slab slot from this allocator
                unsafe { *(ptr as *mut *mut u8) = *free_list; }
                *free_list = ptr as *mut u8;
            }
            StoreAlloc::Pool(p) => p.free_slot(ptr as *mut u8),
        }
    }
}

impl Drop for StoreAlloc {
    fn drop(&mut self) {
        if let StoreAlloc::Slab { base, layout, .. } = self {
            // SAFETY: base and layout match the alloc() call in new()
            unsafe { dealloc(*base, *layout); }
        }
    }
}

/// All per-user ring buffers. Accessed ONLY from the event loop thread.
pub struct Store {
    map: HashMap<u32, RingBuffer>,
    alloc: StoreAlloc,
    capacity: u16,
}

// SAFETY: Store is accessed only from the event loop thread.
unsafe impl Send for Store {}

impl Store {
    pub fn new() -> Self {
        Self::with_pool_slots(MEMORY_POOL_SLOTS_DEFAULT)
    }

    pub fn with_pool_slots(pool_slots: u32) -> Self {
        let capacity = COMPILED_N as u16;
        Store {
            map: HashMap::new(),
            alloc: StoreAlloc::new(capacity, pool_slots),
            capacity,
        }
    }

    /// Get existing or insert new RingBuffer for uid. O(1) amortised.
    #[inline]
    pub fn get_or_create(&mut self, uid: u32) -> *mut RingBuffer {
        let alloc = &mut self.alloc;
        let capacity = self.capacity;
        self.map.entry(uid).or_insert_with(|| {
            let data = alloc.alloc();
            RingBuffer { data, capacity, head: 0, count: 0, last_write_ts: 0 }
        }) as *mut RingBuffer
    }

    /// Returns raw pointer to RingBuffer or None if absent. O(1). Never mutates.
    #[inline]
    pub fn get(&self, uid: u32) -> Option<*mut RingBuffer> {
        self.map.get(&uid).map(|b| b as *const RingBuffer as *mut RingBuffer)
    }

    /// Remove key. Frees data slot BEFORE erasing from map. No-op if absent.
    #[inline]
    pub fn erase(&mut self, uid: u32) {
        if let Some(buf) = self.map.remove(&uid) {
            // Free slot while pointer is still valid (map entry already removed above,
            // but buf.data pointer remains valid until this scope ends)
            self.alloc.free(buf.data);
        }
    }

    /// Read-only iteration. Callback MUST NOT call erase or get_or_create.
    pub fn for_each(&self, mut f: impl FnMut(u32, &RingBuffer)) {
        for (&uid, buf) in &self.map {
            f(uid, buf);
        }
    }

    pub fn len(&self) -> usize { self.map.len() }
    pub fn is_empty(&self) -> bool { self.map.is_empty() }
}

impl Default for Store {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    fn make_record(ts: u64) -> Record {
        Record { event_type: 1, timestamp: ts, url_hash: 0 }
    }

    #[test]
    fn test_get_or_create_returns_same_ptr() {
        let mut store = Store::with_pool_slots(64);
        let a = store.get_or_create(42) as usize;
        let b = store.get_or_create(42) as usize;
        assert_eq!(a, b);
    }

    #[test]
    fn test_get_returns_none_for_missing_key() {
        let store = Store::with_pool_slots(64);
        assert!(store.get(999).is_none());
    }

    #[test]
    fn test_push_and_get_roundtrip() {
        let mut store = Store::with_pool_slots(64);
        let buf = store.get_or_create(1);
        unsafe { (*buf).push(make_record(100), 1000) };
        let buf2 = store.get(1).unwrap();
        let mut out = [Record::default(); 4];
        let n = unsafe { (*buf2).get_last(1, &mut out) };
        assert_eq!(n, 1);
        assert_eq!({ out[0].timestamp }, 100);
    }

    #[test]
    fn test_erase_removes_key() {
        let mut store = Store::with_pool_slots(64);
        store.get_or_create(7);
        store.erase(7);
        assert!(store.get(7).is_none());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_erase_nonexistent_is_noop() {
        let mut store = Store::with_pool_slots(64);
        store.erase(999);
    }

    #[test]
    fn test_for_each_visits_all_keys() {
        let mut store = Store::with_pool_slots(64);
        for uid in [1u32, 2, 3] { store.get_or_create(uid); }
        let mut visited = std::collections::HashSet::new();
        store.for_each(|uid, _| { visited.insert(uid); });
        assert_eq!(visited, [1u32, 2, 3].into_iter().collect());
    }

    #[test]
    fn test_len_tracks_insertions_and_erasures() {
        let mut store = Store::with_pool_slots(64);
        assert_eq!(store.len(), 0);
        store.get_or_create(1);
        store.get_or_create(2);
        assert_eq!(store.len(), 2);
        store.erase(1);
        assert_eq!(store.len(), 1);
    }
}
