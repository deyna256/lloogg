use super::helpers::*;
use weloxs::constants::*;

#[test]
fn test_aof_recovery_restores_pushed_records() {
    // Phase 1: start server, push records, shut down
    let mut server = TestServer::start();
    let mut s = connect(server.port);

    for ts in [100u64, 200, 300] {
        send_push(&mut s, 7, ts);
        let resp = read_response(&mut s);
        assert_eq!(resp[2], STATUS_OK);
    }
    drop(s);
    server.stop();

    // Phase 2: restart from same tmpdir — recovery should replay AOF
    let tmpdir = server.tmpdir.take();
    let mut server2 = TestServer::start_recovered(tmpdir);
    let mut s2 = connect(server2.port);

    send_get(&mut s2, 7, 3);
    let resp = read_response(&mut s2);
    assert_eq!(resp[2], STATUS_OK, "records should be restored after AOF replay");
    let actual = u16::from_le_bytes(resp[5..7].try_into().unwrap());
    assert_eq!(actual, 3, "all 3 records should be restored");

    server2.stop();
}

#[test]
fn test_aof_recovery_del_removes_key() {
    let mut server = TestServer::start();
    let mut s = connect(server.port);

    send_push(&mut s, 55, 1000);
    read_response(&mut s);
    send_del(&mut s, 55);
    read_response(&mut s);
    drop(s);
    server.stop();

    let tmpdir = server.tmpdir.take();
    let mut server2 = TestServer::start_recovered(tmpdir);
    let mut s2 = connect(server2.port);

    send_get(&mut s2, 55, 1);
    let resp = read_response(&mut s2);
    assert_eq!(resp[2], STATUS_NOT_FOUND, "DEL should be replayed from AOF");

    server2.stop();
}
