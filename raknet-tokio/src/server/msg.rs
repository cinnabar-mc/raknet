use crate::session::RestoredSession;
use raknet::prelude::RestoredSession as CoreRestoredSession;
use std::net::IpAddr;
use std::time::Duration;
use tokio::sync::oneshot;

pub enum RakServerMsg {
    #[cfg(test)]
    InspectCommands(oneshot::Sender<crate::endpoint_command_tests::QueueState>),
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
