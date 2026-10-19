//! One idempotent endpoint stop request, independent of bounded ordinary commands.

use raknet::prelude::constants;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Default)]
pub(crate) struct StopState {
    deadline: OnceLock<Instant>,
    changed: Notify,
}

impl StopState {
    /// Captures the first original close allowance and wakes all admission/actor waits.
    pub(crate) fn request(&self) {
        self.deadline
            .get_or_init(|| Instant::now() + constants::CLOSE_TIMEOUT + Duration::from_secs(1));
        self.changed.notify_waiters();
    }

    /// Reads the original cutoff without moving or resetting it.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline.get().copied()
    }

    /// Observes synchronous admission withdrawal without borrowing an observer lock.
    pub(crate) fn requested(&self) -> bool {
        self.deadline.get().is_some()
    }

    /// Registers before checking state so a request cannot strand a capacity waiter.
    pub(crate) async fn wait(&self) -> Instant {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(deadline) = self.deadline() {
                return deadline;
            }
            notified.await;
        }
    }
}
