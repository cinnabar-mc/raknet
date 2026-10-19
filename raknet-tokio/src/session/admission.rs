//! Admission metadata for the original bounded command FIFO and its first close.

use raknet::prelude::RakSessionError;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

pub(super) struct Admission {
    maximum: Option<usize>,
    close_timeout: Duration,
    state: Mutex<State>,
    changed: Notify,
}

struct State {
    open: bool,
    retired: bool,
    bytes: usize,
    committed: usize,
    close: Option<Close>,
}

pub(super) struct Close {
    pub(super) started: Instant,
    pub(super) reply: oneshot::Sender<Result<(), RakSessionError>>,
}

/// Credit follows the same body until the actor transfers or rejects it.
pub(super) struct SendCredit {
    admission: Arc<Admission>,
    bytes: usize,
    committed: bool,
}

/// Seals blocked callers even if the original actor is aborted or unwinds.
pub(super) struct ActorAdmission(pub(super) Arc<Admission>);

impl Admission {
    /// Retains the original byte policy without semaphore platform restrictions.
    pub(super) fn new(maximum: i32, close_timeout: Duration, open: bool) -> Arc<Self> {
        Arc::new(Self {
            maximum: usize::try_from(maximum)
                .ok()
                .filter(|maximum| *maximum != 0),
            close_timeout,
            state: Mutex::new(State {
                open,
                retired: false,
                bytes: 0,
                committed: 0,
                close: None,
            }),
            changed: Notify::new(),
        })
    }

    /// Reserves actual body bytes while leaving uncommitted bodies caller-owned.
    pub(super) async fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        message_limit: usize,
    ) -> Result<SendCredit, RakSessionError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().expect("session admission mutex poisoned");
                if !state.open {
                    return Err(RakSessionError::Closed);
                }
                let maximum = self.maximum.ok_or(RakSessionError::InvalidConfig(
                    "session byte capacities must be positive",
                ))?;
                if bytes > message_limit || bytes > maximum {
                    return Err(RakSessionError::MessageSizeExceeded(bytes));
                }
                if bytes <= maximum - state.bytes {
                    state.bytes = state
                        .bytes
                        .checked_add(bytes)
                        .expect("reserved bytes fit their budget");
                    return Ok(SendCredit {
                        admission: Arc::clone(self),
                        bytes,
                        committed: false,
                    });
                }
            }
            notified.await;
        }
    }

    /// Waits for sealing independently of available command or byte capacity.
    pub(super) async fn closed(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self
                .state
                .lock()
                .expect("session admission mutex poisoned")
                .open
            {
                return;
            }
            notified.await;
        }
    }

    /// Captures the sole close before returning the locally closing capability.
    pub(super) fn request_close(
        &self,
        reply: oneshot::Sender<Result<(), RakSessionError>>,
    ) -> Result<(), RakSessionError> {
        {
            let mut state = self.state.lock().expect("session admission mutex poisoned");
            if state.retired || state.close.is_some() {
                return Err(RakSessionError::Closed);
            }
            state.open = false;
            state.close = Some(Close {
                started: Instant::now(),
                reply,
            });
        }
        self.changed.notify_waiters();
        Ok(())
    }

    /// Drains earlier committed sends under the original close duration.
    pub(super) async fn close_ready(&self) -> Close {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let remaining = {
                let mut state = self.state.lock().expect("session admission mutex poisoned");
                let remaining = state
                    .close
                    .as_ref()
                    .map(|close| self.close_timeout.saturating_sub(close.started.elapsed()));
                if remaining.is_some_and(|remaining| remaining.is_zero() || state.committed == 0) {
                    return state.close.take().expect("the original close is pending");
                }
                remaining
            };
            match remaining {
                Some(remaining) => {
                    // A huge configured duration need not fit an absolute Instant.
                    match Instant::now().checked_add(remaining) {
                        Some(deadline) => tokio::select! {
                            _ = &mut notified => {},
                            _ = tokio::time::sleep_until(deadline) => {},
                        },
                        None => notified.await,
                    }
                }
                None => notified.await,
            }
        }
    }

    /// Stops future send commitment without discarding committed FIFO entries.
    pub(super) fn seal(&self) {
        self.state
            .lock()
            .expect("session admission mutex poisoned")
            .open = false;
        self.changed.notify_waiters();
    }
}

impl SendCredit {
    /// Linearizes commitment against the consuming close's admission fence.
    pub(super) fn commit(&mut self) -> Result<(), RakSessionError> {
        let mut state = self
            .admission
            .state
            .lock()
            .expect("session admission mutex poisoned");
        if !state.open {
            return Err(RakSessionError::Closed);
        }
        state.committed = state
            .committed
            .checked_add(1)
            .expect("committed commands fit their bounded FIFO");
        self.committed = true;
        Ok(())
    }
}

impl Drop for SendCredit {
    fn drop(&mut self) {
        {
            let mut state = self
                .admission
                .state
                .lock()
                .expect("session admission mutex poisoned");
            state.bytes = state
                .bytes
                .checked_sub(self.bytes)
                .expect("each body returns its original byte credit once");
            if self.committed {
                state.committed = state
                    .committed
                    .checked_sub(1)
                    .expect("each committed body leaves the FIFO once");
            }
        }
        self.admission.changed.notify_waiters();
    }
}

impl Drop for ActorAdmission {
    fn drop(&mut self) {
        let close = {
            let mut state = self
                .0
                .state
                .lock()
                .expect("session admission mutex poisoned");
            state.open = false;
            state.retired = true;
            state.close.take()
        };
        self.0.changed.notify_waiters();
        drop(close);
    }
}
