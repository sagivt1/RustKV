pub mod error;
pub mod kv;

pub use error::{KvsError, Result};
pub use kv::KvStore;