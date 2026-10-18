use crate::session::RakSession;
use raknet::prelude::{RakClientError, RakSession as RakSessionIntl};
use std::net::SocketAddr;
use tokio::sync::oneshot;

pub enum RakClientMsg {
    Connect(
        SocketAddr,
        oneshot::Sender<Result<RakSession, RakClientError>>,
    ),
    Ping(SocketAddr, oneshot::Sender<super::PingResult>),
    Adopt(
        Box<RakSessionIntl>,
        oneshot::Sender<Result<RakSession, RakClientError>>,
    ),
    Stop,
}
