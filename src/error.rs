use thiserror::Error;

#[derive(Debug, Error)]
pub enum WeloxsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("protocol error: {0}")]
    Protocol(String),

}

pub type Result<T> = std::result::Result<T, WeloxsError>;
