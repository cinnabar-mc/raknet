// Modified by Cinnabar contributors for caller-driven connection negotiation.
use super::MtuProbeId;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::SystemTime;

pub enum RakClientInput {
    /// Completes a probe write. `elapsed` covers the socket write only, including failures.
    MtuProbeResult {
        id: MtuProbeId,
        now: SystemTime,
        elapsed: Duration,
        too_large: bool,
    },
    Ping(SocketAddr, SystemTime),
    Connect(SocketAddr, SystemTime),
    Datagram(Box<[u8]>, SocketAddr, SystemTime),
    Update(SystemTime),
    Disconnect,
}
