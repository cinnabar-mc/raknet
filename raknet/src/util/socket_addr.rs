// Modified by Cinnabar contributors for explicit connection endpoints and wire address bounds.
use std::net::SocketAddr;

/// Unmaps IPv4 endpoints while retaining routing metadata on native IPv6 endpoints.
pub fn canonical_socket_addr(addr: SocketAddr) -> SocketAddr {
    if let SocketAddr::V6(v6) = addr
        && let Some(v4) = v6.ip().to_ipv4_mapped()
    {
        return SocketAddr::new(v4.into(), v6.port());
    }
    addr
}

/// Returns the IP header allowance for the endpoint's actual address family.
pub fn get_overhead(addr: &SocketAddr) -> u16 {
    match canonical_socket_addr(*addr) {
        SocketAddr::V4(_) => 20,
        SocketAddr::V6(_) => 40,
    }
}
