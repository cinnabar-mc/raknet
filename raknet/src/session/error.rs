use crate::protocol::error::RakCodecError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RakSessionError {
    #[error("RakCodecError: {0}")]
    RakCodecError(#[from] RakCodecError),
    #[error("Closed")]
    Closed,
    #[error("message of {0} bytes exceeds the send limit")]
    MessageSizeExceeded(usize),
}
