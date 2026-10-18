// Modified by Cinnabar contributors for caller-driven connection negotiation.
use super::{ClientGuid, MtuProbeId};
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
    /// Sends one ping using the identity generated for this operation by the adapter.
    Ping {
        remote: SocketAddr,
        guid: ClientGuid,
        now: SystemTime,
    },
    /// Starts negotiation with the remote endpoint and the adapter's bound local address.
    ///
    /// Supply the route-selected address of a connected socket when available.
    /// Generate a fresh identity for this attempt; retries retain it automatically.
    /// IPv4-mapped addresses are normalized; the two endpoint families must agree.
    Connect {
        remote: SocketAddr,
        local: SocketAddr,
        guid: ClientGuid,
        now: SystemTime,
    },
    Datagram(Box<[u8]>, SocketAddr, SystemTime),
    Update(SystemTime),
    Disconnect,
}
