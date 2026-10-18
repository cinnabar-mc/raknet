//! Caller-driven MTU probe scheduling. See NOTICE for the Go behavior reference.

use super::{RakClientConfig, RakClientError};
use crate::util::constants;
use std::time::{Duration, Instant};

use constants::MTU_PROBE_ATTEMPTS_PER_SIZE as ATTEMPTS_PER_SIZE;
const URGENT_DELAY: Duration = Duration::from_millis(10);

/// Identifies one outstanding MTU probe write, including across reconnects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MtuProbeId(pub(super) u64);

#[derive(Clone, Debug)]
struct PendingProbe {
    id: MtuProbeId,
    started: Instant,
    rung: usize,
}

#[derive(Clone, Debug)]
pub(super) struct MtuProbes {
    sizes: Vec<u16>,
    count: usize,
    limit: usize,
    interval: Duration,
    next: Option<Instant>,
    pending: Option<PendingProbe>,
}

impl MtuProbes {
    /// Derives distinct descending probe sizes within the validated caller's bounds.
    pub(super) fn new(config: &RakClientConfig, now: Instant) -> Self {
        let floor = config.min_mtu_size.max(constants::MIN_PROBE_MTU_SIZE);
        let mut sizes = vec![config.max_mtu_size];
        for size in constants::MTU_SIZES
            .iter()
            .rev()
            .map(|size| (*size).max(floor))
        {
            if size < *sizes.last().expect("the caller cap is always present") {
                sizes.push(size);
            }
        }
        let limit = (ATTEMPTS_PER_SIZE * sizes.len() + 1).min(config.conn_attempt_max);
        Self {
            sizes,
            count: 0,
            limit,
            interval: config.conn_attempt_interval,
            next: Some(now),
            pending: None,
        }
    }

    /// Takes one due probe; its completion must arrive before another is emitted.
    pub(super) fn take(&mut self, now: Instant, id: MtuProbeId) -> Option<u16> {
        if self.pending.is_some() || self.count >= self.limit || self.next? > now {
            return None;
        }
        let rung = (self.count / ATTEMPTS_PER_SIZE).min(self.sizes.len() - 1);
        self.count += 1;
        self.next = None;
        self.pending = Some(PendingProbe {
            id,
            started: now,
            rung,
        });
        Some(self.sizes[rung])
    }

    /// Applies a matching write result; returns false without changing state for stale IDs.
    pub(super) fn completed(
        &mut self,
        id: MtuProbeId,
        now: Instant,
        elapsed: Duration,
        too_large: bool,
    ) -> Result<bool, RakClientError> {
        if self.pending.as_ref().is_none_or(|pending| pending.id != id) {
            return Ok(false);
        }
        let pending = self.pending.take().expect("the completion token matched");
        let urgent = if too_large {
            self.count = ATTEMPTS_PER_SIZE * (pending.rung + 1);
            true
        } else if elapsed.as_millis() > 100 {
            let final_start = ATTEMPTS_PER_SIZE * (self.sizes.len() - 1);
            if self.count < final_start {
                self.count = final_start;
            } else {
                self.count = self.limit;
            }
            true
        } else {
            false
        };
        if self.count < self.limit {
            let yield_at = deadline(now, URGENT_DELAY)?;
            self.next = Some(if urgent {
                yield_at
            } else {
                deadline(pending.started, self.interval)?.max(yield_at)
            });
        }
        Ok(true)
    }

    /// Returns the next retry deadline, excluding a write already owned by the adapter.
    pub(super) fn next(&self) -> Option<Instant> {
        self.next
    }
}

/// Rejects unrepresentable domain deadlines instead of panicking on caller input.
pub(super) fn deadline(now: Instant, after: Duration) -> Result<Instant, RakClientError> {
    now.checked_add(after).ok_or(RakClientError::InvalidConfig(
        "connection deadline is too large",
    ))
}
