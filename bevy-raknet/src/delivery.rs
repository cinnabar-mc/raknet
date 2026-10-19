//! Accounting for bodies in the adapter's original final queues, without another payload owner.

use raknet::prelude::{RakDisconnectReason, constants};

pub(crate) struct ReceiveBudget {
    max_bytes: usize,
    packets: usize,
    bytes: usize,
    open: bool,
}

impl ReceiveBudget {
    /// Captures the original positive byte policy; the complete-message count has one core source.
    pub(crate) fn new(max_bytes: i32) -> Self {
        Self {
            max_bytes: usize::try_from(max_bytes).expect("session byte policy was validated"),
            packets: 0,
            bytes: 0,
            open: true,
        }
    }

    /// Charges a complete body before moving it into the existing FIFO, including empty bodies.
    pub(crate) fn admit(&mut self, bytes: usize) -> bool {
        if !self.open || self.packets >= constants::MAX_RECEIVE_MESSAGES {
            return false;
        }
        let Some(total) = self.bytes.checked_add(bytes) else {
            return false;
        };
        if total > self.max_bytes {
            return false;
        }
        self.packets += 1;
        self.bytes = total;
        true
    }

    /// Releases only the body that public receive just moved out of that same FIFO.
    pub(crate) fn release(&mut self, bytes: usize) {
        self.packets = self.packets.checked_sub(1).expect("body was charged");
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .expect("body bytes were charged");
    }

    /// Permanently stops a reliable prefix after its first rejected body.
    pub(crate) fn seal(&mut self) -> bool {
        std::mem::replace(&mut self.open, false)
    }

    /// Capacity retirement remains explicit even if best-effort protocol abort itself failed.
    pub(crate) fn failure(&self) -> Option<RakDisconnectReason> {
        (!self.open).then_some(RakDisconnectReason::ReceiveBufferExceeded)
    }

    /// Retains the original failure, while an ordinary EOF cannot hide a rejected prefix.
    pub(crate) fn reason(&self, reason: RakDisconnectReason) -> RakDisconnectReason {
        if !self.open
            && matches!(
                reason,
                RakDisconnectReason::Requested | RakDisconnectReason::Remote
            )
        {
            RakDisconnectReason::ReceiveBufferExceeded
        } else {
            reason
        }
    }

    /// Retirement keeps its reservation while any original accepted body remains owned.
    pub(crate) fn is_empty(&self) -> bool {
        self.packets == 0
    }
}
