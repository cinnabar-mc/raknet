// Modified by Cinnabar contributors for caller-driven connection negotiation.
use super::{ClientGuid, MtuProbeId};
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

pub(crate) enum ClientInput {
    /// Completes a probe write. `elapsed` covers the socket write only, including failures.
    MtuProbeResult {
        id: MtuProbeId,
        now: Instant,
        elapsed: Duration,
        too_large: bool,
    },
    /// Sends one ping using the identity generated for this operation by the adapter.
    Ping {
        remote: SocketAddr,
        guid: ClientGuid,
        now: Instant,
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
        now: Instant,
    },
    Datagram(Box<[u8]>, SocketAddr, Instant),
    Update(Instant),
    Disconnect,
}
