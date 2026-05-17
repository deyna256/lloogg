use crate::record::Record;

/// Per-user event ring buffer. data points to a pre-allocated slot (SlabAlloc or MemoryPool).
/// All methods called exclusively from the event loop thread.
pub struct RingBuffer {
    pub data: *mut Record,
    pub capacity: u16,
    pub head: u16,
    pub count: u16,
    /// Server-assigned unix nanoseconds of the last PUSH (from AOFEntry.write_ts).
    /// Never set from Record.timestamp — used as false-eviction guard.
    pub last_write_ts: u64,
}

// SAFETY: RingBuffer is accessed only from the event loop thread.
unsafe impl Send for RingBuffer {}

impl RingBuffer {
    /// Write record at head, advance head, update count and last_write_ts. O(1).
    /// Silently overwrites oldest entry when full — no allocation, no error.
    #[inline]
    pub fn push(&mut self, r: Record, write_ts: u64) {
        // SAFETY: head < capacity invariant is maintained; data is a valid allocated array
        unsafe { *self.data.add(self.head as usize) = r; }
        self.head = (self.head + 1) % self.capacity;
        if self.count < self.capacity {
            self.count += 1;
        }
        self.last_write_ts = write_ts;
    }

    /// Fill out[0..actual] with records newest-first. Returns actual count written.
    /// out[0] = most recently pushed. O(actual_count).
    #[inline]
    pub fn get_last(&self, n: u16, out: &mut [Record]) -> u16 {
        let actual = n.min(self.count);
        for i in 0..actual as usize {
            let idx = (self.head as usize + self.capacity as usize - 1 - i)
                % self.capacity as usize;
            // SAFETY: idx < capacity, data is valid
            out[i] = unsafe { *self.data.add(idx) };
        }
        actual
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    fn make_record(ts: u64) -> Record {
        Record { event_type: 1, timestamp: ts, url_hash: ts * 7 }
    }

    // Helper: allocates a Vec<Record> as backing storage for tests
    fn make_buf(cap: u16) -> (RingBuffer, Vec<Record>) {
        let mut data = vec![Record::default(); cap as usize];
        let buf = RingBuffer {
            data: data.as_mut_ptr(),
            capacity: cap,
            head: 0,
            count: 0,
            last_write_ts: 0,
        };
        (buf, data) // caller must keep data alive
    }

    #[test]
    fn test_push_and_get_last_single() {
        let (mut buf, _data) = make_buf(4);
        buf.push(make_record(100), 999);
        let mut out = [Record::default(); 4];
        let n = buf.get_last(1, &mut out);
        assert_eq!(n, 1);
        let ts = out[0].timestamp; // copy to avoid alignment issue with packed struct
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_get_last_newest_first() {
        let (mut buf, _data) = make_buf(4);
        for ts in [10u64, 20, 30] { buf.push(make_record(ts), ts); }
        let mut out = [Record::default(); 4];
        let n = buf.get_last(3, &mut out);
        assert_eq!(n, 3);
        let ts0 = out[0].timestamp;
        let ts1 = out[1].timestamp;
        let ts2 = out[2].timestamp;
        assert_eq!(ts0, 30);
        assert_eq!(ts1, 20);
        assert_eq!(ts2, 10);
    }

    #[test]
    fn test_push_overflow_overwrites_oldest() {
        let (mut buf, _data) = make_buf(3);
        for ts in [10u64, 20, 30, 40] { buf.push(make_record(ts), ts); }
        assert_eq!(buf.count, 3);
        let mut out = [Record::default(); 3];
        let n = buf.get_last(3, &mut out);
        assert_eq!(n, 3);
        let ts0 = out[0].timestamp;
        let ts1 = out[1].timestamp;
        let ts2 = out[2].timestamp;
        assert_eq!(ts0, 40);
        assert_eq!(ts1, 30);
        assert_eq!(ts2, 20);
    }

    #[test]
    fn test_last_write_ts_is_server_ts_not_record_ts() {
        let (mut buf, _data) = make_buf(4);
        buf.push(make_record(9999), 42); // client ts=9999, server write_ts=42
        assert_eq!(buf.last_write_ts, 42);
    }

    #[test]
    fn test_get_last_empty_returns_zero() {
        let (buf, _data) = make_buf(4);
        let mut out = [Record::default(); 4];
        let n = buf.get_last(2, &mut out);
        assert_eq!(n, 0);
    }

    #[test]
    fn test_get_last_clamps_to_count() {
        let (mut buf, _data) = make_buf(4);
        buf.push(make_record(1), 1);
        let mut out = [Record::default(); 4];
        let n = buf.get_last(4, &mut out);
        assert_eq!(n, 1);
    }

    #[test]
    fn test_head_wraps_correctly() {
        let (mut buf, _data) = make_buf(2);
        buf.push(make_record(1), 1);
        buf.push(make_record(2), 2);
        buf.push(make_record(3), 3); // overwrites slot 0
        assert_eq!(buf.head, 1); // head is back at 1
        assert_eq!(buf.count, 2);
    }
}
