//! Monotonic transport time and its relative wire counter.

use std::time::{Duration, Instant};

/// The caller-selected start of a RakNet endpoint's uptime.
///
/// Reuse this value for connection attempts and their sessions. All supplied
/// Instants must use the same monotonic clock and be at or after this origin.
#[derive(Clone, Copy, Debug)]
pub struct RakClock {
    origin: Instant,
}

impl RakClock {
    /// Starts the wire counter at zero without reading the clock.
    pub const fn new(origin: Instant) -> Self {
        Self { origin }
    }

    /// Returns elapsed monotonic time, saturating for a caller time before the origin.
    pub fn elapsed(self, now: Instant) -> Duration {
        now.saturating_duration_since(self.origin)
    }

    /// Returns relative wire milliseconds, saturating if the counter cannot fit.
    pub fn timestamp(self, now: Instant) -> u64 {
        u64::try_from(self.elapsed(now).as_millis()).unwrap_or(u64::MAX)
    }

    /// Reconstructs a saved local timestamp without panicking on an invalid snapshot.
    pub(crate) fn at(self, milliseconds: u64) -> Option<Instant> {
        self.origin.checked_add(Duration::from_millis(milliseconds))
    }

    /// Rebases a transferred wire counter onto the destination's monotonic clock.
    pub(crate) fn restore(uptime_ms: u64, now: Instant, elapsed: Duration) -> Option<Self> {
        let age = Duration::from_millis(uptime_ms).checked_add(elapsed)?;
        now.checked_sub(age).map(Self::new)
    }
}

#[cfg(test)]
/// Shares a synthetic-time anchor between the core's test helpers.
pub(crate) fn test_now() -> Instant {
    static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}
