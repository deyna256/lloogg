use crate::{
    aof::AOFWriter,
    connection::{Connection, State},
    constants::MAGIC_WIRE,
    record::Record,
    store::Store,
    time::now_ns,
};

/// PUSH: WAL → apply. Steps in this exact order per spec.
/// write_ts captured ONCE and passed to both AOF and RingBuffer.
pub fn push_command(conn: &mut Connection, store: &mut Store, aof: &mut AOFWriter) {
    let uid = conn.header_user_id();
    // SAFETY: rbuf[9..26] contains a valid packed Record (validated by payload_len check)
    let rec: Record = unsafe {
        std::mem::transmute::<[u8; 17], Record>(conn.rbuf[9..26].try_into().unwrap())
    };
    let write_ts = now_ns(); // captured ONCE — same value for AOF and RingBuffer
    aof.append_push(uid, rec, write_ts); // WAL before apply
    let buf = store.get_or_create(uid);
    unsafe { (*buf).push(rec, write_ts); }
    conn.write_ok_response(); // sets state = WriteResponse
}

/// GET: read-only, no AOF write.
pub fn get_command(conn: &mut Connection, store: &Store) {
    let uid = conn.header_user_id();
    let requested = u16::from_le_bytes(conn.rbuf[9..11].try_into().unwrap());
    match store.get(uid) {
        None => conn.write_not_found_response(),
        Some(buf_ptr) => {
            let actual_count = unsafe { (*buf_ptr).count.min(requested) };
            let payload_len = 2 + actual_count as usize * 17;
            // Response: [magic:2][STATUS_OK:1][payload_len:2][actual_count:2][records:actual*17]
            conn.wbuf[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
            conn.wbuf[2] = crate::constants::STATUS_OK;
            conn.wbuf[3..5].copy_from_slice(&(payload_len as u16).to_le_bytes());
            conn.wbuf[5..7].copy_from_slice(&actual_count.to_le_bytes());
            if actual_count > 0 {
                // SAFETY: wbuf capacity = WBUF_SIZE = ceil((7+N*17)/64)*64 >= 7+N*17
                // actual_count <= COMPILED_N, so 7 + actual*17 <= WBUF_SIZE
                let records_dst = conn.wbuf[7..7 + actual_count as usize * 17].as_mut_ptr() as *mut Record;
                let records_slice = unsafe {
                    std::slice::from_raw_parts_mut(records_dst, actual_count as usize)
                };
                unsafe { (*buf_ptr).get_last(actual_count, records_slice); }
            }
            conn.wbuf_len = 7 + actual_count as usize * 17;
            conn.wbuf_pos = 0;
            conn.state = State::WriteResponse;
        }
    }
}

/// DEL: WAL → erase. Idempotent (STATUS_OK even if uid absent).
pub fn del_command(conn: &mut Connection, store: &mut Store, aof: &mut AOFWriter) {
    let uid = conn.header_user_id();
    aof.append_del(uid, now_ns()); // WAL before erase
    store.erase(uid);
    conn.write_ok_response(); // sets state = WriteResponse
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::FsyncMode,
        constants::*,
        record::Record,
        store::Store,
    };

    fn make_push_conn(uid: u32, r: Record) -> Connection {
        let mut conn = Connection::new(0);
        conn.rbuf[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
        conn.rbuf[2] = OPCODE_PUSH;
        conn.rbuf[3..7].copy_from_slice(&uid.to_le_bytes());
        conn.rbuf[7..9].copy_from_slice(&17u16.to_le_bytes());
        // SAFETY: Record is packed size=17; byte copy is safe
        let bytes: [u8; 17] = unsafe { std::mem::transmute(r) };
        conn.rbuf[9..26].copy_from_slice(&bytes);
        conn
    }

    fn make_get_conn(uid: u32, count: u16) -> Connection {
        let mut conn = Connection::new(0);
        conn.rbuf[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
        conn.rbuf[2] = OPCODE_GET;
        conn.rbuf[3..7].copy_from_slice(&uid.to_le_bytes());
        conn.rbuf[7..9].copy_from_slice(&2u16.to_le_bytes());
        conn.rbuf[9..11].copy_from_slice(&count.to_le_bytes());
        conn
    }

    fn make_aof() -> (AOFWriter, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        let writer = AOFWriter::open(path.to_str().unwrap(), FsyncMode::No).unwrap();
        (writer, dir)
    }

    #[test]
    fn test_push_creates_entry_responds_ok() {
        let mut store = Store::with_pool_slots(64);
        let (mut aof, _dir) = make_aof();
        let r = Record { event_type: 5, timestamp: 1000, url_hash: 2000 };
        let mut conn = make_push_conn(99, r);
        push_command(&mut conn, &mut store, &mut aof);
        assert_eq!(conn.wbuf[2], STATUS_OK);
        assert!(store.get(99).is_some());
    }

    #[test]
    fn test_get_not_found_for_missing_uid() {
        let store = Store::with_pool_slots(64);
        let mut conn = make_get_conn(404, 1);
        get_command(&mut conn, &store);
        assert_eq!(conn.wbuf[2], STATUS_NOT_FOUND);
    }

    #[test]
    fn test_push_then_get_newest_first() {
        let mut store = Store::with_pool_slots(64);
        let (mut aof, _dir) = make_aof();
        for ts in [10u64, 20, 30] {
            let r = Record { event_type: 1, timestamp: ts, url_hash: 0 };
            push_command(&mut make_push_conn(1, r), &mut store, &mut aof);
        }
        let mut conn = make_get_conn(1, 3);
        get_command(&mut conn, &store);
        assert_eq!(conn.wbuf[2], STATUS_OK);
        let actual = u16::from_le_bytes(conn.wbuf[5..7].try_into().unwrap());
        assert_eq!(actual, 3);
        // First record = newest (ts=30)
        let rec: Record = unsafe {
            std::mem::transmute::<[u8; 17], Record>(conn.wbuf[7..24].try_into().unwrap())
        };
        let ts = rec.timestamp; // copy to avoid alignment issue
        assert_eq!(ts, 30);
    }

    #[test]
    fn test_del_removes_entry_and_responds_ok() {
        let mut store = Store::with_pool_slots(64);
        let (mut aof, _dir) = make_aof();
        push_command(&mut make_push_conn(5, Record::default()), &mut store, &mut aof);
        assert!(store.get(5).is_some());
        let mut conn = Connection::new(0);
        conn.rbuf[3..7].copy_from_slice(&5u32.to_le_bytes()); // uid=5
        del_command(&mut conn, &mut store, &mut aof);
        assert_eq!(conn.wbuf[2], STATUS_OK);
        assert!(store.get(5).is_none());
    }

    #[test]
    fn test_del_nonexistent_is_ok() {
        let mut store = Store::with_pool_slots(64);
        let (mut aof, _dir) = make_aof();
        let mut conn = Connection::new(0);
        conn.rbuf[3..7].copy_from_slice(&999u32.to_le_bytes());
        del_command(&mut conn, &mut store, &mut aof);
        assert_eq!(conn.wbuf[2], STATUS_OK);
    }
}
