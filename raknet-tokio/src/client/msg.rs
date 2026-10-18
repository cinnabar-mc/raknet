use crate::session::RakSession;
use raknet::prelude::{RakClientError, RakSessionSnapshot};
use std::net::SocketAddr;
use tokio::sync::oneshot;

pub enum RakClientMsg {
    Connect(
        SocketAddr,
        oneshot::Sender<Result<RakSession, RakClientError>>,
    ),
    Ping(SocketAddr, oneshot::Sender<super::PingResult>),
    Adopt(
        Box<RakSessionSnapshot>,
        oneshot::Sender<Result<RakSession, RakClientError>>,
    ),
    Stop,
}
