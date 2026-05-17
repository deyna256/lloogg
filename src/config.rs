use serde::Deserialize;
use crate::{
    COMPILED_N,
    constants::{MAX_RECORDS_PROTOCOL_LIMIT, MEMORY_POOL_SLOTS_DEFAULT, PORT_DEFAULT, SNAPSHOT_INTERVAL_S, CONFIG_PATH_DEFAULT},
    error::{WeloxsError, Result},
};

#[derive(Debug, Clone, PartialEq)]
pub enum FsyncMode {
    Always,
    Everysec,
    No,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_records: u16,
    pub ttl_seconds: u64,
    pub aof_path: String,
    pub snapshot_path: String,
    pub snapshot_interval: u32,
    pub listen_port: u16,
    pub memory_pool_slots: u32,
    pub aof_fsync: FsyncMode,
}

/// Raw TOML deserialization target. Validated into Config via validate().
#[derive(Deserialize)]
struct RawConfig {
    max_records: u16,
    ttl_seconds: u64,
    aof_path: String,
    snapshot_path: String,
    #[serde(default = "default_snapshot_interval")]
    snapshot_interval: u32,
    #[serde(default = "default_listen_port")]
    listen_port: u16,
    #[serde(default = "default_pool_slots")]
    memory_pool_slots: u32,
    #[serde(default = "default_fsync")]
    aof_fsync: String,
}

fn default_snapshot_interval() -> u32 { SNAPSHOT_INTERVAL_S }
fn default_listen_port() -> u16 { PORT_DEFAULT }
fn default_pool_slots() -> u32 { MEMORY_POOL_SLOTS_DEFAULT }
fn default_fsync() -> String { "everysec".to_string() }

impl Config {
    pub fn from_args(args: &[String]) -> Result<Self> {
        let path = args.get(1).map(|s| s.as_str()).unwrap_or(CONFIG_PATH_DEFAULT);
        Self::from_file(path)
    }

    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| WeloxsError::Config(format!("cannot read {path}: {e}")))?;
        let raw: RawConfig = toml::from_str(&content)
            .map_err(|e| WeloxsError::Config(format!("TOML parse error: {e}")))?;
        Self::validate(raw)
    }

    fn validate(raw: RawConfig) -> Result<Self> {
        if raw.max_records as usize != COMPILED_N {
            return Err(WeloxsError::Config(format!(
                "max_records={} does not match COMPILED_N={} — rebuild binary or fix config",
                raw.max_records, COMPILED_N
            )));
        }
        if raw.max_records > MAX_RECORDS_PROTOCOL_LIMIT {
            return Err(WeloxsError::Config(format!(
                "max_records={} exceeds MAX_RECORDS_PROTOCOL_LIMIT={}",
                raw.max_records, MAX_RECORDS_PROTOCOL_LIMIT
            )));
        }
        if raw.ttl_seconds == 0 {
            return Err(WeloxsError::Config("ttl_seconds must be > 0".into()));
        }
        if raw.aof_path.is_empty() {
            return Err(WeloxsError::Config("aof_path is required".into()));
        }
        if raw.snapshot_path.is_empty() {
            return Err(WeloxsError::Config("snapshot_path is required".into()));
        }
        let fsync = match raw.aof_fsync.as_str() {
            "always"   => FsyncMode::Always,
            "everysec" => FsyncMode::Everysec,
            "no"       => FsyncMode::No,
            other => return Err(WeloxsError::Config(
                format!("unknown aof_fsync: '{other}' — expected always|everysec|no")
            )),
        };
        Ok(Config {
            max_records: raw.max_records,
            ttl_seconds: raw.ttl_seconds,
            aof_path: raw.aof_path,
            snapshot_path: raw.snapshot_path,
            snapshot_interval: raw.snapshot_interval,
            listen_port: raw.listen_port,
            memory_pool_slots: raw.memory_pool_slots,
            aof_fsync: fsync,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_toml(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn valid_toml() -> String {
        format!(
            "max_records = {}\nttl_seconds = 3600\naof_path = \"/tmp/test.aof\"\nsnapshot_path = \"/tmp\"\n",
            COMPILED_N
        )
    }

    #[test]
    fn test_parse_valid_config() {
        let f = write_toml(&valid_toml());
        let cfg = Config::from_file(f.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.max_records, COMPILED_N as u16);
        assert_eq!(cfg.ttl_seconds, 3600);
        assert_eq!(cfg.aof_fsync, FsyncMode::Everysec);
        assert_eq!(cfg.listen_port, PORT_DEFAULT);
        assert_eq!(cfg.snapshot_interval, SNAPSHOT_INTERVAL_S);
    }

    #[test]
    fn test_max_records_mismatch_is_error() {
        let bad_n = if COMPILED_N == 100 { 99u16 } else { 100u16 };
        let f = write_toml(&format!(
            "max_records = {bad_n}\nttl_seconds = 1\naof_path = \"/tmp/a\"\nsnapshot_path = \"/tmp\"\n"
        ));
        let err = Config::from_file(f.path().to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("max_records"));
    }

    #[test]
    fn test_zero_ttl_is_error() {
        let f = write_toml(&format!(
            "max_records = {}\nttl_seconds = 0\naof_path = \"/tmp/a\"\nsnapshot_path = \"/tmp\"\n",
            COMPILED_N
        ));
        let err = Config::from_file(f.path().to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("ttl_seconds"));
    }

    #[test]
    fn test_always_fsync_mode() {
        let f = write_toml(&format!(
            "max_records = {}\nttl_seconds = 1\naof_path = \"/tmp/a\"\nsnapshot_path = \"/tmp\"\naof_fsync = \"always\"\n",
            COMPILED_N
        ));
        let cfg = Config::from_file(f.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.aof_fsync, FsyncMode::Always);
    }

    #[test]
    fn test_unknown_fsync_is_error() {
        let f = write_toml(&format!(
            "max_records = {}\nttl_seconds = 1\naof_path = \"/tmp/a\"\nsnapshot_path = \"/tmp\"\naof_fsync = \"maybe\"\n",
            COMPILED_N
        ));
        assert!(Config::from_file(f.path().to_str().unwrap()).is_err());
    }
}
