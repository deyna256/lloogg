use crate::{
    COMPILED_N,
    constants::{MAGIC_WIRE, OPCODE_DEL, OPCODE_GET, OPCODE_PUSH, RBUF_SIZE,
                STATUS_NOT_FOUND, STATUS_OK, WBUF_SIZE},
    error::{Result, WeloxsError},
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    ReadHeader,
    ReadPayload,
    Process,
    WriteResponse,
}

pub struct Connection {
    pub fd: i32,
    pub state: State,
    pub rbuf: [u8; RBUF_SIZE],
    pub rbuf_pos: usize,
    /// Pre-allocated to WBUF_SIZE — no allocation on hot path
    pub wbuf: Vec<u8>,
    pub wbuf_len: usize,
    pub wbuf_pos: usize,
}

impl Connection {
    pub fn new(fd: i32) -> Self {
        let mut wbuf = Vec::with_capacity(WBUF_SIZE);
        // SAFETY: capacity is allocated; set_len to WBUF_SIZE so indexing works
        unsafe { wbuf.set_len(WBUF_SIZE); }
        Self {
            fd,
            state: State::ReadHeader,
            rbuf: [0; RBUF_SIZE],
            rbuf_pos: 0,
            wbuf,
            wbuf_len: 0,
            wbuf_pos: 0,
        }
    }

    /// Validate 9-byte header in rbuf[0..9].
    /// Returns (opcode, user_id, payload_len) on success.
    /// Returns Err on magic/opcode/payload_len violations → caller must close connection.
    pub fn validate_header(&self) -> Result<(u8, u32, u16)> {
        let magic = u16::from_le_bytes(self.rbuf[0..2].try_into().unwrap());
        if magic != MAGIC_WIRE {
            return Err(WeloxsError::Protocol(format!(
                "bad magic: {magic:#06x}, expected {MAGIC_WIRE:#06x}"
            )));
        }
        let opcode = self.rbuf[2];
        let user_id = u32::from_le_bytes(self.rbuf[3..7].try_into().unwrap());
        let payload_len = u16::from_le_bytes(self.rbuf[7..9].try_into().unwrap());

        let expected_payload: u16 = match opcode {
            OPCODE_PUSH => 17,
            OPCODE_GET  => 2,
            OPCODE_DEL  => 0,
            _ => return Err(WeloxsError::Protocol(format!("unknown opcode: {opcode:#04x}"))),
        };

        if payload_len != expected_payload {
            return Err(WeloxsError::Protocol(format!(
                "opcode {opcode:#04x}: expected payload_len={expected_payload}, got {payload_len}"
            )));
        }

        Ok((opcode, user_id, payload_len))
    }

    /// Validate GET count field after ReadPayload completes.
    /// count must be in [1, COMPILED_N].
    pub fn validate_get_count(&self) -> Result<u16> {
        let count = u16::from_le_bytes(self.rbuf[9..11].try_into().unwrap());
        if count == 0 || count as usize > COMPILED_N {
            return Err(WeloxsError::Protocol(format!(
                "GET count={count} out of range [1, {COMPILED_N}]"
            )));
        }
        Ok(count)
    }

    /// Write 5-byte STATUS_OK response into wbuf.
    pub fn write_ok_response(&mut self) {
        self.wbuf[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
        self.wbuf[2] = STATUS_OK;
        self.wbuf[3..5].copy_from_slice(&0u16.to_le_bytes());
        self.wbuf_len = 5;
        self.wbuf_pos = 0;
        self.state = State::WriteResponse;
    }

    /// Write 5-byte STATUS_NOT_FOUND response into wbuf.
    pub fn write_not_found_response(&mut self) {
        self.wbuf[0..2].copy_from_slice(&MAGIC_WIRE.to_le_bytes());
        self.wbuf[2] = STATUS_NOT_FOUND;
        self.wbuf[3..5].copy_from_slice(&0u16.to_le_bytes());
        self.wbuf_len = 5;
        self.wbuf_pos = 0;
        self.state = State::WriteResponse;
    }

    /// Returns (opcode, user_id) from current rbuf (after header is read).
    pub fn header_opcode(&self) -> u8 { self.rbuf[2] }
    pub fn header_user_id(&self) -> u32 {
        u32::from_le_bytes(self.rbuf[3..7].try_into().unwrap())
    }
    pub fn header_payload_len(&self) -> u16 {
        u16::from_le_bytes(self.rbuf[7..9].try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header(magic: u16, opcode: u8, uid: u32, payload_len: u16) -> [u8; 9] {
        let mut h = [0u8; 9];
        h[0..2].copy_from_slice(&magic.to_le_bytes());
        h[2] = opcode;
        h[3..7].copy_from_slice(&uid.to_le_bytes());
        h[7..9].copy_from_slice(&payload_len.to_le_bytes());
        h
    }

    #[test]
    fn test_validate_push_header_ok() {
        let mut conn = Connection::new(0);
        conn.rbuf[..9].copy_from_slice(&make_header(MAGIC_WIRE, OPCODE_PUSH, 42, 17));
        let (opcode, uid, plen) = conn.validate_header().unwrap();
        assert_eq!(opcode, OPCODE_PUSH);
        assert_eq!(uid, 42);
        assert_eq!(plen, 17);
    }

    #[test]
    fn test_bad_magic_rejected() {
        let mut conn = Connection::new(0);
        conn.rbuf[..9].copy_from_slice(&make_header(0xDEAD, OPCODE_PUSH, 1, 17));
        assert!(conn.validate_header().is_err());
    }

    #[test]
    fn test_unknown_opcode_rejected() {
        let mut conn = Connection::new(0);
        conn.rbuf[..9].copy_from_slice(&make_header(MAGIC_WIRE, 0xFF, 1, 0));
        assert!(conn.validate_header().is_err());
    }

    #[test]
    fn test_wrong_payload_len_rejected() {
        let mut conn = Connection::new(0);
        // PUSH expects payload_len=17; send 0
        conn.rbuf[..9].copy_from_slice(&make_header(MAGIC_WIRE, OPCODE_PUSH, 1, 0));
        assert!(conn.validate_header().is_err());
    }

    #[test]
    fn test_del_header_zero_payload() {
        let mut conn = Connection::new(0);
        conn.rbuf[..9].copy_from_slice(&make_header(MAGIC_WIRE, OPCODE_DEL, 5, 0));
        let (opcode, uid, plen) = conn.validate_header().unwrap();
        assert_eq!(opcode, OPCODE_DEL);
        assert_eq!(uid, 5);
        assert_eq!(plen, 0);
    }

    #[test]
    fn test_ok_response_layout() {
        let mut conn = Connection::new(0);
        conn.write_ok_response();
        assert_eq!(&conn.wbuf[..2], &MAGIC_WIRE.to_le_bytes());
        assert_eq!(conn.wbuf[2], STATUS_OK);
        assert_eq!(&conn.wbuf[3..5], &0u16.to_le_bytes());
        assert_eq!(conn.wbuf_len, 5);
        assert_eq!(conn.state, State::WriteResponse);
    }

    #[test]
    fn test_not_found_response_layout() {
        let mut conn = Connection::new(0);
        conn.write_not_found_response();
        assert_eq!(conn.wbuf[2], STATUS_NOT_FOUND);
        assert_eq!(conn.wbuf_len, 5);
    }

    #[test]
    fn test_validate_get_count_valid() {
        let mut conn = Connection::new(0);
        conn.rbuf[..9].copy_from_slice(&make_header(MAGIC_WIRE, OPCODE_GET, 1, 2));
        conn.rbuf[9..11].copy_from_slice(&1u16.to_le_bytes());
        assert_eq!(conn.validate_get_count().unwrap(), 1);
    }

    #[test]
    fn test_validate_get_count_zero_rejected() {
        let mut conn = Connection::new(0);
        conn.rbuf[9..11].copy_from_slice(&0u16.to_le_bytes());
        assert!(conn.validate_get_count().is_err());
    }

    #[test]
    fn test_validate_get_count_over_n_rejected() {
        let mut conn = Connection::new(0);
        let over = (COMPILED_N + 1) as u16;
        conn.rbuf[9..11].copy_from_slice(&over.to_le_bytes());
        assert!(conn.validate_get_count().is_err());
    }
}
