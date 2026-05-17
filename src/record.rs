/// Wire-format event record. sizeof == 17, align == 1 (packed, no padding).
/// user_id is NOT stored here — it lives as the HashMap key in Store.
/// timestamp is client-supplied unix seconds — NOT used for server-side TTL.
#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Record {
    pub event_type: u8,
    pub timestamp: u64,
    pub url_hash: u64,
}

const _: () = assert!(std::mem::size_of::<Record>() == 17);
const _: () = assert!(std::mem::align_of::<Record>() == 1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_three_records_fit_in_cache_line() {
        // 3 × 17 = 51 < 64 bytes (one cache line)
        assert!(3 * std::mem::size_of::<Record>() <= 64);
    }

    #[test]
    fn test_record_field_access() {
        let r = Record { event_type: 7, timestamp: 12345, url_hash: 99999 };
        // packed fields must be read via copy to avoid unaligned references
        let et = r.event_type;
        let ts = r.timestamp;
        let uh = r.url_hash;
        assert_eq!(et, 7);
        assert_eq!(ts, 12345);
        assert_eq!(uh, 99999);
    }
}
