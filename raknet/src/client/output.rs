// Modified by Cinnabar contributors for caller-driven connection negotiation.
use super::MtuProbeId;
use crate::session::RakSession;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum RakClientOutput {
    /// Write one probe, then report its token and measured write result as `MtuProbeResult`.
    MtuProbe {
        id: MtuProbeId,
        datagram: Box<[u8]>,
        peer: SocketAddr,
    },
    SocketDatagram(Box<[u8]>, SocketAddr),
    SessionDatagram(Box<[u8]>),
    SessionConnected(Box<RakSession>),
    Wait(Duration),
    /// A reply with the echoed wire counter and measured monotonic latency.
    Pong {
        addr: SocketAddr,
        message: Box<[u8]>,
        echoed_timestamp: u64,
        latency: Duration,
    },
}
