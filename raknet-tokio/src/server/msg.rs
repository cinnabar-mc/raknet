use crate::session::RestoredSession;
use raknet::prelude::RestoredSession as CoreRestoredSession;
use std::net::IpAddr;
use std::time::Duration;
use tokio::sync::oneshot;

pub enum RakServerMsg {
    SetMessage(Box<[u8]>),
    SetMaxConnections(usize),
    Block(IpAddr, Duration),
    Unblock(IpAddr),
    Stop,
    Adopt(
        CoreRestoredSession,
        oneshot::Sender<Result<RestoredSession, super::error::RakServerError>>,
    ),
}
