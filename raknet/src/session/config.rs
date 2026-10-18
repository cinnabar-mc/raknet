use crate::util::constants::{
    AUTOFLUSH, AUTOFLUSH_INTERVAL_MS, CLOSE_TIMEOUT, MAX_ORDERING_CHANNELS, MAX_QUEUED_BYTES,
};
use std::time::Duration;

#[derive(Clone, Debug, facet::Facet)]
pub struct RakSessionConfig {
    pub ordering_channels: i32,
    pub autoflush: bool,
    pub autoflush_interval_ms: Duration,
    pub max_queued_bytes: i32,
    pub max_outbound_bytes: i32,
    pub close_timeout: Duration,
}

impl Default for RakSessionConfig {
    fn default() -> Self {
        Self {
            ordering_channels: MAX_ORDERING_CHANNELS,
            autoflush: AUTOFLUSH,
            autoflush_interval_ms: Duration::from_millis(AUTOFLUSH_INTERVAL_MS as u64),
            max_queued_bytes: MAX_QUEUED_BYTES,
            max_outbound_bytes: MAX_QUEUED_BYTES,
            close_timeout: CLOSE_TIMEOUT,
        }
    }
}

impl RakSessionConfig {
    /// Validates local capacities and cadence before session establishment.
    pub fn validate(&self) -> Result<(), super::error::RakSessionError> {
        use super::error::RakSessionError;
        if !(1..=MAX_ORDERING_CHANNELS).contains(&self.ordering_channels) {
            return Err(RakSessionError::InvalidConfig(
                "ordering channel count is outside its supported range",
            ));
        }
        if self.max_queued_bytes <= 0 || self.max_outbound_bytes <= 0 {
            return Err(RakSessionError::InvalidConfig(
                "session byte capacities must be positive",
            ));
        }
        if self.autoflush && self.autoflush_interval_ms.is_zero() {
            return Err(RakSessionError::InvalidConfig(
                "automatic flush cadence must be positive",
            ));
        }
        Ok(())
    }
}
