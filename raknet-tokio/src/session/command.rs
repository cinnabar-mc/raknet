use super::admission::SendCredit;
use raknet::prelude::{
    RakPriority, RakReliability, RakSessionCaptureError, RakSessionError, RakSessionSnapshot,
};
use tokio::sync::oneshot::Sender;

pub(super) enum RakSessionMsg {
    Send(
        Box<[u8]>,
        RakReliability,
        RakPriority,
        Sender<Result<(), RakSessionError>>,
        SendCredit,
    ),
    IsClosed(Sender<bool>),
    /// Capture the session's protocol state, leaving the live task running.
    Snapshot(Sender<Result<RakSessionSnapshot, RakSessionCaptureError>>),
    #[cfg(test)]
    InspectCommands(Sender<usize>),
}
