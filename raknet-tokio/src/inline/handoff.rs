//! One atomic orders readiness against transfer back to the scheduled owner.

use std::sync::atomic::{AtomicU8, Ordering};

const DRIVING: u8 = 1;
const NOTIFIED: u8 = 2;

#[derive(Default)]
pub(super) struct Handoff(AtomicU8);

impl Handoff {
    /// Starts inline ownership while the future's mutex is exclusively held.
    pub(super) fn enter(&self) {
        self.0.fetch_or(DRIVING, Ordering::AcqRel);
    }

    /// Consumes earlier wakes immediately before polling their retained readiness.
    pub(super) fn prepare(&self) {
        self.0.fetch_and(!NOTIFIED, Ordering::AcqRel);
    }

    /// Observes whether the last poll generated another required poll.
    pub(super) fn notified(&self) -> bool {
        self.0.load(Ordering::Acquire) & NOTIFIED != 0
    }

    /// Transfers readiness to the runner before releasing the future's mutex.
    pub(super) fn leave(&self) -> bool {
        self.0.fetch_and(!DRIVING, Ordering::AcqRel) & NOTIFIED != 0
    }

    /// Either records readiness for the inline owner or asks the runner to poll.
    pub(super) fn wake(&self) -> bool {
        self.0.fetch_or(NOTIFIED, Ordering::AcqRel) & DRIVING == 0
    }
}
