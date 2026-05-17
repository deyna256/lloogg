use crate::{
    constants::{OPCODE_DEL, OPCODE_PUSH},
    error::Result,
    record::Record,
    snapshot::SnapshotManager,
    store::Store,
};

/// Full recovery: snapshot load → AOF replay.
/// Subsystems (flush thread, TTL worker, EventLoop) start ONLY after this completes.
pub fn recover(store: &mut Store, snapshot_path: &str, aof_path: &str) -> Result<()> {
    let snapshot_ts = SnapshotManager::load(snapshot_path, store)?;
    log::info!("snapshot loaded: ts={snapshot_ts}, keys={}", store.len());

    replay_aof(store, aof_path, snapshot_ts)?;
    log::info!("recovery complete: keys={}", store.len());
    Ok(())
}

fn replay_aof(store: &mut Store, path: &str, snapshot_ts: u64) -> Result<()> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::info!("no AOF file found at {path}, starting fresh");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let mut pos = 0usize;
    let mut replayed = 0u64;

    loop {
        // Need at least 15 bytes for a header
        if pos + 15 > data.len() {
            break;
        }

        let opcode = data[pos];
        let uid = u32::from_le_bytes(data[pos + 1..pos + 5].try_into().unwrap());
        let write_ts = u64::from_le_bytes(data[pos + 5..pos + 13].try_into().unwrap());
        let payload_len = u16::from_le_bytes(data[pos + 13..pos + 15].try_into().unwrap());
        pos += 15;

        // Truncated payload — stop (client never received ACK for this entry)
        if pos + payload_len as usize > data.len() {
            break;
        }
        let payload = &data[pos..pos + payload_len as usize];
        pos += payload_len as usize;

        // Skip entries already covered by snapshot
        if write_ts <= snapshot_ts {
            continue;
        }

        match opcode {
            OPCODE_PUSH if payload.len() == 17 => {
                // SAFETY: payload is exactly 17 bytes; Record is #[repr(C,packed)] size=17
                let rec: Record = unsafe {
                    std::mem::transmute::<[u8; 17], Record>(payload.try_into().unwrap())
                };
                let buf = store.get_or_create(uid);
                unsafe { (*buf).push(rec, write_ts); }
                replayed += 1;
            }
            OPCODE_DEL => {
                store.erase(uid);
                replayed += 1;
            }
            _ => {
                // Unknown opcode — skip (forward-compatible, future version may add ops)
                log::warn!("unknown AOF opcode {opcode:#04x}, skipping");
            }
        }
    }

    log::info!("AOF replay: {replayed} entries applied");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        aof::AOFWriter,
        config::FsyncMode,
        record::Record,
        snapshot::SnapshotManager,
        store::Store,
    };

    fn make_record(ts: u64) -> Record {
        Record { event_type: 1, timestamp: ts, url_hash: 0 }
    }

    #[test]
    fn test_recovery_from_snapshot_and_aof() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().to_str().unwrap();
        let aof_path = dir.path().join("test.aof");
        let aof_str = aof_path.to_str().unwrap();

        // Write snapshot with uid=1 at ts=100_000
        let mut store = Store::with_pool_slots(64);
        let buf = store.get_or_create(1);
        unsafe { (*buf).push(make_record(100), 100_000) };
        SnapshotManager::dump_sync(&store, snap_path, 100_000);

        // Write AOF entries AFTER snapshot_ts
        let mut aof = AOFWriter::open(aof_str, FsyncMode::No).unwrap();
        aof.append_push(1, make_record(200), 200_000); // uid=1, second record
        aof.append_push(2, make_record(300), 300_000); // uid=2, new user
        aof.flush_sync();
        aof.shutdown();

        // Recover
        let mut store2 = Store::with_pool_slots(64);
        recover(&mut store2, snap_path, aof_str).unwrap();

        // uid=1: 2 records (100 from snapshot + 200 from AOF)
        let buf1 = store2.get(1).unwrap();
        assert_eq!(unsafe { (*buf1).count }, 2);

        // uid=2: 1 record from AOF only
        let buf2 = store2.get(2).unwrap();
        assert_eq!(unsafe { (*buf2).count }, 1);
    }

    #[test]
    fn test_recovery_skips_aof_entries_before_snapshot_ts() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().to_str().unwrap();
        let aof_path = dir.path().join("test.aof");
        let aof_str = aof_path.to_str().unwrap();

        // Snapshot at ts=200_000
        let mut store = Store::with_pool_slots(64);
        let buf = store.get_or_create(1);
        unsafe { (*buf).push(make_record(100), 200_000) };
        SnapshotManager::dump_sync(&store, snap_path, 200_000);

        // AOF has entries both before and after snapshot_ts
        let mut aof = AOFWriter::open(aof_str, FsyncMode::No).unwrap();
        aof.append_push(1, make_record(50), 100_000); // BEFORE snapshot_ts — skip
        aof.append_push(1, make_record(150), 300_000); // AFTER snapshot_ts — apply
        aof.flush_sync();
        aof.shutdown();

        let mut store2 = Store::with_pool_slots(64);
        recover(&mut store2, snap_path, aof_str).unwrap();

        let buf1 = store2.get(1).unwrap();
        // snapshot had 1 record, +1 from AOF after snapshot_ts = 2
        assert_eq!(unsafe { (*buf1).count }, 2);
    }

    #[test]
    fn test_recovery_del_removes_key() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().to_str().unwrap();
        let aof_path = dir.path().join("test.aof");
        let aof_str = aof_path.to_str().unwrap();

        // Snapshot with uid=1
        let mut store = Store::with_pool_slots(64);
        let buf = store.get_or_create(1);
        unsafe { (*buf).push(make_record(100), 100_000) };
        SnapshotManager::dump_sync(&store, snap_path, 100_000);

        // AOF: DEL uid=1 after snapshot
        let mut aof = AOFWriter::open(aof_str, FsyncMode::No).unwrap();
        aof.append_del(1, 200_000);
        aof.flush_sync();
        aof.shutdown();

        let mut store2 = Store::with_pool_slots(64);
        recover(&mut store2, snap_path, aof_str).unwrap();
        assert!(store2.get(1).is_none());
    }

    #[test]
    fn test_recovery_no_snapshot_no_aof() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().to_str().unwrap();
        let aof_path = dir.path().join("nonexistent.aof");

        let mut store = Store::with_pool_slots(64);
        recover(&mut store, snap_path, aof_path.to_str().unwrap()).unwrap();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_recovery_skips_unknown_aof_opcode() {
        use crate::constants::OPCODE_PUSH;

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().to_str().unwrap();
        let aof_path = dir.path().join("unknown_op.aof");

        // Build AOF manually: unknown opcode entry followed by valid PUSH
        let mut data = Vec::<u8>::new();
        data.push(0xFF); // unknown opcode
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&200_000u64.to_le_bytes());
        data.extend_from_slice(&0u16.to_le_bytes()); // no payload
        // Valid PUSH after the unknown entry
        let rec = make_record(42);
        let rec_bytes: [u8; 17] = unsafe { std::mem::transmute(rec) };
        data.push(OPCODE_PUSH);
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&300_000u64.to_le_bytes());
        data.extend_from_slice(&17u16.to_le_bytes());
        data.extend_from_slice(&rec_bytes);
        std::fs::write(&aof_path, &data).unwrap();

        let mut store = Store::with_pool_slots(64);
        recover(&mut store, snap_path, aof_path.to_str().unwrap()).unwrap();
        // Unknown opcode skipped; valid PUSH still applied
        let buf = store.get(1).unwrap();
        assert_eq!(unsafe { (*buf).count }, 1);
    }
}
