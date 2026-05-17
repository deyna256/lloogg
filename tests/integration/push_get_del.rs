use super::helpers::*;
use lloogg::constants::*;

#[test]
fn test_push_and_get_returns_records_newest_first() {
    let mut server = TestServer::start();
    let mut s = connect(server.port);

    for ts in [10u64, 20, 30] {
        send_push(&mut s, 1, ts);
        let resp = read_response(&mut s);
        assert_eq!(resp[2], STATUS_OK);
    }

    send_get(&mut s, 1, 3);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK);
    let actual_count = u16::from_le_bytes(resp[5..7].try_into().unwrap());
    assert_eq!(actual_count, 3);
    // Response layout: [magic:2][status:1][payload_len:2][actual_count:2][records:count*17]
    // Record layout: [event_type:1][timestamp:8][url_hash:8] — packed
    let ts0 = u64::from_le_bytes(resp[8..16].try_into().unwrap());
    assert_eq!(ts0, 30, "newest record should be first");

    server.stop();
}

#[test]
fn test_del_clears_user_records() {
    let mut server = TestServer::start();
    let mut s = connect(server.port);

    send_push(&mut s, 1, 100);
    read_response(&mut s);

    send_del(&mut s, 1);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_OK);

    send_get(&mut s, 1, 1);
    let resp = read_response(&mut s);
    assert_eq!(resp[2], STATUS_NOT_FOUND);

    server.stop();
}

#[test]
fn test_del_nonexistent_uid_returns_ok() {
    let mut server = TestServer::start();
    let mut s = connect(server.port);

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
