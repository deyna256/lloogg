use lloogg::constants::{MAGIC_WIRE, OPCODE_DEL, OPCODE_GET, OPCODE_PUSH};
use std::io::Read;
use lloogg::constants::{STATUS_NOT_FOUND, STATUS_OK};

fn build_push_frame(uid: u32, ev: u8, ts: u64, hash: u64) -> [u8; 26] {
    let mut f = [0u8; 26];
    f[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    f[2] = OPCODE_PUSH;
    f[3..7].copy_from_slice(&uid.to_le_bytes());
    f[7..9].copy_from_slice(&17u16.to_le_bytes());
    f[9] = ev;
    f[10..18].copy_from_slice(&ts.to_le_bytes());
    f[18..26].copy_from_slice(&hash.to_le_bytes());
    f
}

fn build_get_frame(uid: u32, count: u16) -> [u8; 11] {
    let mut f = [0u8; 11];
    f[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    f[2] = OPCODE_GET;
    f[3..7].copy_from_slice(&uid.to_le_bytes());
    f[7..9].copy_from_slice(&2u16.to_le_bytes());
    f[9..11].copy_from_slice(&count.to_le_bytes());
    f
}

fn build_del_frame(uid: u32) -> [u8; 9] {
    let mut f = [0u8; 9];
    f[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
    f[2] = OPCODE_DEL;
    f[3..7].copy_from_slice(&uid.to_le_bytes());
    f[7..9].copy_from_slice(&0u16.to_le_bytes());
    f
}

#[derive(Debug)]
struct RecordData {
    event_type: u8,
    timestamp: u64,
    url_hash: u64,
}

#[derive(Debug)]
enum ServerResponse {
    Ok,
    NotFound,
    GetData { count: u16, records: Vec<RecordData> },
}

fn read_response(stream: &mut impl Read) -> Result<ServerResponse, String> {
    let mut hdr = [0u8; 5];
    stream.read_exact(&mut hdr).map_err(|e| e.to_string())?;

    let magic = u16::from_le_bytes(hdr[0..2].try_into().unwrap());
    if magic != MAGIC_WIRE {
        return Err(format!("bad magic: {magic:#06x}"));
    }

    let status = hdr[2];
    let payload_len = u16::from_le_bytes(hdr[3..5].try_into().unwrap()) as usize;

    if status == STATUS_NOT_FOUND {
        return Ok(ServerResponse::NotFound);
    }
    if status != STATUS_OK {
        return Err(format!("unexpected status: {status:#04x}"));
    }
    if payload_len == 0 {
        return Ok(ServerResponse::Ok);
    }

    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).map_err(|e| e.to_string())?;

    let actual_count = u16::from_le_bytes(payload[0..2].try_into().unwrap());
    let mut records = Vec::with_capacity(actual_count as usize);
    for i in 0..actual_count as usize {
        let b = 2 + i * 17;
        records.push(RecordData {
            event_type: payload[b],
            timestamp:  u64::from_le_bytes(payload[b+1..b+9].try_into().unwrap()),
            url_hash:   u64::from_le_bytes(payload[b+9..b+17].try_into().unwrap()),
        });
    }
    Ok(ServerResponse::GetData { count: actual_count, records })
}

fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use lloogg::constants::{MAGIC_WIRE, OPCODE_DEL, OPCODE_GET, OPCODE_PUSH, STATUS_NOT_FOUND, STATUS_OK};

    #[test]
    fn push_frame_has_correct_header() {
        let f = build_push_frame(42, 3, 1000, 0xdeadbeef);
        assert_eq!(u16::from_le_bytes(f[0..2].try_into().unwrap()), MAGIC_WIRE);
        assert_eq!(f[2], OPCODE_PUSH);
        assert_eq!(u32::from_le_bytes(f[3..7].try_into().unwrap()), 42u32);
        assert_eq!(u16::from_le_bytes(f[7..9].try_into().unwrap()), 17u16);
    }

    #[test]
    fn push_frame_has_correct_payload() {
        let f = build_push_frame(1, 5, 9999, 0xcafe);
        assert_eq!(f[9], 5u8);
        assert_eq!(u64::from_le_bytes(f[10..18].try_into().unwrap()), 9999u64);
        assert_eq!(u64::from_le_bytes(f[18..26].try_into().unwrap()), 0xcafeu64);
    }

    #[test]
    fn get_frame_correct() {
        let f = build_get_frame(7, 3);
        assert_eq!(u16::from_le_bytes(f[0..2].try_into().unwrap()), MAGIC_WIRE);
        assert_eq!(f[2], OPCODE_GET);
        assert_eq!(u32::from_le_bytes(f[3..7].try_into().unwrap()), 7u32);
        assert_eq!(u16::from_le_bytes(f[7..9].try_into().unwrap()), 2u16);
        assert_eq!(u16::from_le_bytes(f[9..11].try_into().unwrap()), 3u16);
    }

    #[test]
    fn del_frame_correct() {
        let f = build_del_frame(99);
        assert_eq!(f[2], OPCODE_DEL);
        assert_eq!(u32::from_le_bytes(f[3..7].try_into().unwrap()), 99u32);
        assert_eq!(u16::from_le_bytes(f[7..9].try_into().unwrap()), 0u16);
    }

    #[test]
    fn read_response_ok_simple() {
        let mut wire: &[u8] = &{
            let mut b = [0u8; 5];
            b[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
            b[2] = STATUS_OK;
            b[3..5].copy_from_slice(&0u16.to_le_bytes());
            b
        };
        matches!(read_response(&mut wire).unwrap(), ServerResponse::Ok);
    }

    #[test]
    fn read_response_not_found() {
        let mut wire: &[u8] = &{
            let mut b = [0u8; 5];
            b[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
            b[2] = STATUS_NOT_FOUND;
            b
        };
        matches!(read_response(&mut wire).unwrap(), ServerResponse::NotFound);
    }

    #[test]
    fn read_response_get_data_one_record() {
        // payload_len = 2 + 1*17 = 19
        let mut buf = [0u8; 5 + 19];
        buf[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
        buf[2] = STATUS_OK;
        buf[3..5].copy_from_slice(&19u16.to_le_bytes());
        buf[5..7].copy_from_slice(&1u16.to_le_bytes()); // actual_count = 1
        buf[7] = 2u8;                                   // event_type
        buf[8..16].copy_from_slice(&12345u64.to_le_bytes()); // timestamp
        buf[16..24].copy_from_slice(&0xdeadbeefu64.to_le_bytes()); // url_hash
        let mut wire: &[u8] = &buf;
        let resp = read_response(&mut wire).unwrap();
        if let ServerResponse::GetData { count, records } = resp {
            assert_eq!(count, 1);
            assert_eq!(records[0].event_type, 2);
            assert_eq!(records[0].timestamp, 12345);
            assert_eq!(records[0].url_hash, 0xdeadbeef);
        } else {
            panic!("expected GetData");
        }
    }

    #[test]
    fn read_response_bad_magic_returns_err() {
        let mut wire: &[u8] = &[0xFF, 0xFF, STATUS_OK, 0, 0];
        assert!(read_response(&mut wire).is_err());
    }
}
