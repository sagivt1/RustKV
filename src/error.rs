use thiserror::Error;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Error, Debug)]
pub enum KvsError {
    #[error("IO error {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error {0}")]
    Serde(#[from] Box<bincode::ErrorKind>),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Internal error {0}")]
    Internal(String),
}

