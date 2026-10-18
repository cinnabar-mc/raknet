use crate::session::{RakSessionId, SessionEngine};
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub(crate) enum ServerOutput {
    SocketDatagram(Box<[u8]>, SocketAddr),
    SessionDatagram(Box<[u8]>, RakSessionId),
    SessionConnected(Box<SessionEngine>),
}
