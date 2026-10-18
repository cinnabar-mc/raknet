use crate::session::RakSessionId;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

pub enum RakServerInput {
    SetMessage(Box<[u8]>),
    SetMaxConnections(usize),
    Datagram(Box<[u8]>, SocketAddr, Instant),
    RemoveSession(RakSessionId, Instant),
    Update(Instant),
    Block(IpAddr, Duration, Instant),
    Unblock(IpAddr),
}
