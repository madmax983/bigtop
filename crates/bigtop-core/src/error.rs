//! The shared error type.

/// Errors produced by `BigTop` core logic and surfaced over the API.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A referenced object does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// A job spec failed validation.
    #[error("invalid job spec: {0}")]
    InvalidJobSpec(String),
    /// A state transition or mutation is not allowed.
    #[error("conflict: {0}")]
    Conflict(String),
    /// `JSON` (de)serialization failed.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}
