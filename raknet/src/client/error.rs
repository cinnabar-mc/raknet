// Modified by Cinnabar contributors for caller-driven connection negotiation.
use crate::protocol::error::RakCodecError;
use crate::session::error::RakSessionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RakClientError {
    #[error(transparent)]
    InvalidSnapshot(#[from] crate::session::snapshot::RakSessionRestoreError),
    /// The socket adapter could not generate a client identity for an operation.
    #[error("client identity generation failed: {0}")]
    IdentityGeneration(#[source] std::io::Error),
    #[error("Invalid client configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("RakCodecError: {0}")]
    RakCodecError(#[from] RakCodecError),
    #[error("RakSessionError: {0}")]
    RakSessionError(#[from] RakSessionError),
    #[error("Security Unsupported")]
    SecurityUnsupported,
    /// Complete early application packets exceeded the configured handshake FIFO.
    #[error("Handshake receive buffer exceeded")]
    HandshakeReceiveBufferExceeded,
    /// The local socket owner already retains the maximum pending discovery replies.
    #[error("Pending ping capacity exceeded")]
    PendingPingCapacityExceeded,
    #[error("Connection Failed")]
    ConnectionFailed,
    #[error("Connection Attempt Failed")]
    ConnectionAttemptFailed,
    #[error("Incompatible Protocol")]
    IncompatibleProtocol,
    #[error("Already Connected")]
    AlreadyConnected,
    #[error("Recently Connected")]
    RecentlyConnected,
    #[error("No Free Incoming Connections")]
    NoFreeIncomingConnections,
    #[error("Invalid MTU: {0}")]
    InvalidMtu(u16),
    #[error("Timeout")]
    Timeout,
    #[error("Closed")]
    Closed,
}
