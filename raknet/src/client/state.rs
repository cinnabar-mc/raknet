// Modified by Cinnabar contributors for explicit connection endpoints and wire address bounds.
use std::net::SocketAddr;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum RakClientState {
    Unconnected,
    Handshake1 {
        remote: SocketAddr,
        local: SocketAddr,
    },
    Handshake2 {
        remote: SocketAddr,
        local: SocketAddr,
    },
    HandshakeCompleted {
        remote: SocketAddr,
        local: SocketAddr,
    },
    Connected(SocketAddr),
}
