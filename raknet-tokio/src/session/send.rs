//! Owned queue admission without sharing receive or lifecycle ownership.

use super::{Admission, RakSessionError, RakSessionMsg, SendCredit, Task};
use raknet::prelude::{RakPriority, RakReliability};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot};

/// Send-only access to one original established session's bounded command queue.
/// Closing that session synchronously revokes every outstanding reservation.
#[derive(Clone)]
pub struct SessionSender {
    task: Weak<Task>,
    commands: mpsc::Sender<RakSessionMsg>,
    admission: Arc<Admission>,
    maximum_message: usize,
}

impl SessionSender {
    pub(super) fn new(session: &super::RakSession<raknet::prelude::Established>) -> Self {
        Self {
            task: session.task.clone(),
            commands: session.msg_tx.clone(),
            admission: Arc::clone(&session.admission),
            maximum_message: session.max_message_len,
        }
    }

    /// Reserves byte and command capacity without admitting a message.
    /// Dropping this wait or its returned permit releases both reservations.
    pub async fn reserve_send(&self, bytes: usize) -> Result<SendPermit, RakSessionError> {
        let credit = self.admission.reserve(bytes, self.maximum_message).await?;
        let command = tokio::select! {
            biased;
            _ = self.admission.closed() => return Err(RakSessionError::Closed),
            command = self.commands.clone().reserve_owned() => {
                command.map_err(|_| RakSessionError::Closed)?
            },
        };
        Ok(SendPermit {
            task: self.task.clone(),
            command,
            credit,
            maximum_bytes: bytes,
        })
    }
}

/// One original command slot and byte reservation, with no admitted payload yet.
#[must_use = "commit one message or drop its uncommitted reservation"]
pub struct SendPermit {
    task: Weak<Task>,
    command: mpsc::OwnedPermit<RakSessionMsg>,
    credit: SendCredit,
    maximum_bytes: usize,
}

impl SendPermit {
    /// Admits one bounded body synchronously against the original close fence.
    /// The receipt reports local core admission, not physical transmission or peer delivery.
    pub fn commit(
        mut self,
        bytes: Box<[u8]>,
        reliability: RakReliability,
        priority: RakPriority,
    ) -> Result<SendReceipt, RakSessionError> {
        if bytes.len() > self.maximum_bytes {
            return Err(RakSessionError::MessageSizeExceeded(bytes.len()));
        }
        let (reply, received) = oneshot::channel();
        self.credit.commit()?;
        Task::drive(&self.task, || {
            self.command.send(RakSessionMsg::Send(
                bytes,
                reliability,
                priority,
                reply,
                self.credit,
            ));
        });
        Ok(SendReceipt {
            received: Some(received),
        })
    }
}

/// The retained local admission result for one already committed command.
#[must_use = "observe the original local admission result"]
pub struct SendReceipt {
    received: Option<oneshot::Receiver<Result<(), RakSessionError>>>,
}

impl SendReceipt {
    /// Observes the result once, retaining it across cancellation of this borrowed wait.
    /// Repeated observation after completion returns `Closed` rather than repolling the channel.
    pub async fn admitted(&mut self) -> Result<(), RakSessionError> {
        let received = self.received.as_mut().ok_or(RakSessionError::Closed)?;
        let result = received.await;
        self.received = None;
        result.map_err(|_| RakSessionError::Closed)?
    }
}
