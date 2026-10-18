use crate::protocol::error::RakCodecError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RakSessionError {
    #[error("invalid session configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("RakCodecError: {0}")]
    RakCodecError(#[from] RakCodecError),
    #[error("Closed")]
    Closed,
    #[error("message of {0} bytes exceeds the send limit")]
    MessageSizeExceeded(usize),
}
