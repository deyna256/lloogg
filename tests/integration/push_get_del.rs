use super::helpers::*;
use lloogg::constants::*;

#[test]
fn test_push_get_del_roundtrip() {
    let mut server = TestServer::start();
    let mut s = connect(server.port);

    // PUSH 3 records for uid=1 with timestamps 10, 20, 30
    for ts in [10u64, 20, 30] {
        send_push(&mut s, 1, ts);
        let resp = read_response(&mut s);
        assert_eq!(resp[2], STATUS_OK, "PUSH should return STATUS_OK");
    }

    // GET 3 records — should come back newest-first (ts=30, 20, 10)
    send_get(&mut s, 1, 3);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK);
    let actual_count = u16::from_le_bytes(resp[5..7].try_into().unwrap());
    assert_eq!(actual_count, 3);
    // First record timestamp (bytes 7..15 of response, field offset 10..18 = ts)
    // Response layout: [magic:2][status:1][payload_len:2][actual_count:2][records:count*17]
    // Record layout: [event_type:1][timestamp:8][url_hash:8] — packed
    let ts_bytes: [u8; 8] = resp[8..16].try_into().unwrap(); // record[0].timestamp
    let ts0 = u64::from_le_bytes(ts_bytes);
    assert_eq!(ts0, 30, "newest record should be first");

    // DEL uid=1
    send_del(&mut s, 1);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK);

    // GET after DEL — should return STATUS_NOT_FOUND
    send_get(&mut s, 1, 1);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_NOT_FOUND);

    // DEL on nonexistent uid — idempotent, STATUS_OK
    send_del(&mut s, 999);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK);

    server.stop();
}

#[test]
fn test_get_returns_actual_count_when_fewer_records_than_requested() {
    let mut server = TestServer::start();
    let mut s = connect(server.port);

    // Push only 2 records, request 5
    for ts in [1u64, 2] {
        send_push(&mut s, 42, ts);
        read_response(&mut s);
    }
    send_get(&mut s, 42, 5);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK);
    let actual = u16::from_le_bytes(resp[5..7].try_into().unwrap());
    assert_eq!(actual, 2);

    server.stop();
}
