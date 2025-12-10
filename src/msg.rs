use serde::{Deserialize, Serialize};

/// Represents a request sent from a client to the key-value store server.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Get the value of a key.
    Get { key: String },
    /// Set the value of a key.
    Set { key: String, value: String },
    /// Remove a key.
    Remove { key: String },
}

/// Represents a response sent from the server back to the client.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    /// A successful operation. Contains the value for `Get`, `None` otherwise.
    Success(Option<String>),
    /// An error occurred during the operation.
    Error(String),
}
