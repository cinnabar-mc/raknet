//! Peer inactivity admission and the original bounded graceful-close drain.

use super::{Duration, Instant, VecDeque, remaining};
use super::{
    RakDisconnectReason, RakSessionError, RakSessionOutput, RakSessionState, SessionEngine,
};

const PEER_SILENCE: Duration = Duration::from_secs(5);
const INITIAL_RTT: Duration = Duration::from_millis(50);

impl SessionEngine {
    /// Allows two round trips beyond the silence interval, including before the first sample.
    pub(super) fn peer_timeout(&self) -> Duration {
        let rtt = if self.congestion_controller.has_rtt_sample() {
            self.rtt()
        } else {
            INITIAL_RTT
        };
        PEER_SILENCE.saturating_add(rtt.saturating_mul(2))
    }

    /// Peer silence starts a close; it cannot shorten an already-started drain.
    pub(super) fn peer_deadline(&self) -> Option<Instant> {
        (self.state != RakSessionState::Closing)
            .then(|| self.last_recv.checked_add(self.peer_timeout()))
            .flatten()
    }

    /// Retains the first close origin and validated grace interval.
    pub(super) fn close_deadline(&self, since: Instant) -> Option<Instant> {
        since.checked_add(self.config.close_timeout)
    }

    /// Starts one close drain without changing a previously admitted origin or cause.
    pub(super) fn close(
        &mut self,
        reason: RakDisconnectReason,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        if self.state == RakSessionState::Closing {
            return Ok(());
        }

        self.state = RakSessionState::Closing;
        self.closing_since = Some(now);
        self.closing_for_timeout = reason == RakDisconnectReason::Timeout;
        self.last_tick = now;
        self.tick(now)?;

        if self.nothing_left_to_send() {
            self.send_disconnect(now)?;
        }

        self.output
            .push_back(RakSessionOutput::Wait(self.closing_wait(now)));
        Ok(())
    }

    /// Includes the reliable final notification until its actual receipt is acknowledged.
    fn nothing_left_to_send(&self) -> bool {
        self.queue.is_empty()
            && self.outbound_cache.is_empty()
            && self.outbound_queue.iter().all(VecDeque::is_empty)
    }

    /// Retries the same accepted prefix and final notice until receipt or the first cutoff.
    pub(super) fn update_closing(&mut self, now: Instant) -> Result<(), RakSessionError> {
        let since = self.closing_since.unwrap_or(now);
        let reason = if self.closing_for_timeout {
            RakDisconnectReason::Timeout
        } else {
            RakDisconnectReason::Requested
        };

        self.tick(now)?;
        self.last_tick = now;

        if self
            .close_deadline(since)
            .is_some_and(|deadline| now >= deadline)
        {
            return self.disconnect(!self.disconnect_sent, reason, now);
        }
        if self.nothing_left_to_send() {
            if self.disconnect_sent {
                return self.disconnect(false, reason, now);
            }
            self.send_disconnect(now)?;
        }

        self.output
            .push_back(RakSessionOutput::Wait(self.closing_wait(now)));
        Ok(())
    }

    /// Keeps the adapter cadence bounded by the first close cutoff alone.
    fn closing_wait(&self, now: Instant) -> Duration {
        remaining(
            self.closing_since.unwrap_or(now),
            self.config.close_timeout,
            now,
        )
        .min(self.config.autoflush_interval_ms)
    }
}
