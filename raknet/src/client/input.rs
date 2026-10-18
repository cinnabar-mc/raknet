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
    /// Starts negotiation with the remote endpoint and the adapter's bound local address.
    ///
    /// Supply the route-selected address of a connected socket when available.
    /// IPv4-mapped addresses are normalized; the two endpoint families must agree.
    Connect {
        remote: SocketAddr,
        local: SocketAddr,
        now: SystemTime,
    },
    Datagram(Box<[u8]>, SocketAddr, SystemTime),
    Update(SystemTime),
    Disconnect,
}
