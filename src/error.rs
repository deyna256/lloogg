use thiserror::Error;

#[derive(Debug, Error)]
pub enum LlooggError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("memory pool exhausted")]
    PoolExhausted,
}

pub type Result<T> = std::result::Result<T, LlooggError>;
