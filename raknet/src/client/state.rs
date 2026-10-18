// Modified by Cinnabar contributors for explicit per-attempt client metadata.
use super::ClientGuid;
use std::net::SocketAddr;

/// Immutable identity and endpoint metadata owned by a connection attempt.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub(super) struct ClientPeer {
    pub(super) remote: SocketAddr,
    pub(super) local: SocketAddr,
    pub(super) guid: ClientGuid,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub(super) enum RakClientState {
    Unconnected,
    Handshake1(ClientPeer),
    Handshake2(ClientPeer),
    HandshakeCompleted(ClientPeer),
    Connected(SocketAddr),
}
