use super::helpers::*;
use weloxs::{constants::*, snapshot::SnapshotManager, store::Store, time::now_ns};

#[test]
fn test_snapshot_recovery_restores_records() {
    // Build store, write snapshot manually, then start server with recovery
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let snap_path = tmpdir.path().to_str().unwrap();
    // Write snapshot directly (no server needed).
    // Use now_ns() as write_ts so TTL eviction doesn't immediately expire them.
    let write_ts = now_ns();
    {
        let mut store = Store::with_pool_slots(1000);
        let buf = store.get_or_create(99);
        for ts in [10u64, 20, 30] {
            unsafe { (*buf).push(weloxs::record::Record { event_type: 1, timestamp: ts, url_hash: 0 }, write_ts) };
        }
        SnapshotManager::dump_sync(&store, snap_path, write_ts);
    }

    // Start server with recovery from snapshot (no AOF)
    let mut server = TestServer::start_recovered(Some(tmpdir));
    let mut s = connect(server.port);

    send_get(&mut s, 99, 3);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK, "records should be restored from snapshot");
    let actual = u16::from_le_bytes(resp[5..7].try_into().unwrap());
    assert_eq!(actual, 3, "all 3 snapshot records should be present");

    server.stop();
}
