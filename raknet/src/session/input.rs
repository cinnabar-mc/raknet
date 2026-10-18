use crate::types::RakReliability;
use crate::types::priority::RakPriority;
use std::time::Instant;

#[derive(Clone, Debug)]
pub enum RakSessionInput {
    Datagram(Box<[u8]>, Instant),
    Send(Box<[u8]>, RakReliability, RakPriority, Instant),
    Update(Instant),
    Disconnect(Instant),
    DisconnectNow(Instant),
}
