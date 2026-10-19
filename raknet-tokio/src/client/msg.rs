use crate::session::RakSession;
use raknet::prelude::{Established, RakClientError, RestoredSession};
use std::net::SocketAddr;
use tokio::sync::oneshot;

pub enum RakClientMsg {
    Connect(
        SocketAddr,
        oneshot::Sender<Result<RakSession<Established>, RakClientError>>,
    ),
    Ping(SocketAddr, oneshot::Sender<super::PingResult>),
    #[cfg(test)]
    InspectPings(oneshot::Sender<super::bounded_pings_tests::PingState>),
    #[cfg(test)]
    InspectCommands(oneshot::Sender<crate::endpoint_command_tests::QueueState>),
    Adopt(
        RestoredSession,
        oneshot::Sender<Result<crate::session::RestoredSession, RakClientError>>,
    ),
}
