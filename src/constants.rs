use crate::COMPILED_N;

pub const PORT_DEFAULT: u16 = 7379;
pub const AOF_BUF_SIZE: usize = 4_194_304;
pub const AOF_FLUSH_INTERVAL_MS: u64 = 1_000;
pub const AOF_OVERFLOW_WARN_S: u64 = 5;
pub const SNAPSHOT_INTERVAL_S: u32 = 120;
pub const TTL_COLLECT_INTERVAL_S: u64 = 10;
pub const CANDIDATES_IN_CAP: usize = 8_192;
pub const EXPIRED_KEYS_CAP: usize = 4_096;
pub const MAX_EPOLL_EVENTS: usize = 1_024;
pub const RBUF_SIZE: usize = 32;
pub const MAGIC_WIRE: u16 = 0xAE01;
pub const MAGIC_SNAPSHOT: u32 = 0x574C4F58;
pub const SNAPSHOT_VERSION: u8 = 1;
pub const OPCODE_PUSH: u8 = 0x01;
pub const OPCODE_GET: u8 = 0x02;
pub const OPCODE_DEL: u8 = 0x03;
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_NOT_FOUND: u8 = 0x02;
pub const MEMORY_POOL_SLOTS_DEFAULT: u32 = 1_200_000;
pub const MAX_RECORDS_PROTOCOL_LIMIT: u16 = 3_854;
pub const LISTEN_BACKLOG: libc::c_int = 128;
pub const CONFIG_PATH_DEFAULT: &str = "weloxs.toml";

// Compile-time: ceil((7 + N*17) / 64) * 64
pub const WBUF_SIZE: usize = (7 + COMPILED_N * 17).div_ceil(64) * 64;

// Static asserts
const _: () = assert!(
    COMPILED_N <= MAX_RECORDS_PROTOCOL_LIMIT as usize,
    "COMPILED_N exceeds MAX_RECORDS_PROTOCOL_LIMIT (3854)"
);
