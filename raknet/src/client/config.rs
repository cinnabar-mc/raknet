// Modified by Cinnabar contributors for caller-driven connection negotiation.
use super::RakClientError;
use crate::util::constants;
use rand::random;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RakClientConfig {
    pub local_port: u16,
    pub guid: u64,
    pub protocol: u8,
    pub min_mtu_size: u16,
    pub max_mtu_size: u16,
    /// Maximum complete application bytes retained before session handoff.
    pub max_handshake_bytes: usize,
    /// Maximum complete application messages retained before session handoff.
    pub max_handshake_messages: usize,
    pub conn_attempt_timeout: Duration,
    pub conn_attempt_interval: Duration,
    /// Maximum ordinary Request 1 probes; Request 2 retries are bounded by the overall timeout.
    pub conn_attempt_max: usize,
}

impl RakClientConfig {
    /// Validates capacities and cadence before an adapter opens its socket.
    pub fn validate(&self) -> Result<(), RakClientError> {
        if self.max_mtu_size < self.min_mtu_size.max(constants::MIN_PROBE_MTU_SIZE)
            || self.max_mtu_size > constants::MAX_MTU_SIZE
        {
            return Err(RakClientError::InvalidMtu(self.max_mtu_size));
        }
        if self.max_handshake_bytes == 0 || self.max_handshake_messages == 0 {
            return Err(RakClientError::InvalidConfig(
                "handshake receive bounds must be positive",
            ));
        }
        if self.conn_attempt_interval.is_zero() || self.conn_attempt_max == 0 {
            return Err(RakClientError::InvalidConfig(
                "probe cadence and attempt count must be positive",
            ));
        }
        Ok(())
    }
}

impl Default for RakClientConfig {
    fn default() -> Self {
        Self {
            local_port: 0,
            min_mtu_size: constants::MIN_MTU_SIZE,
            max_mtu_size: constants::MAX_MTU_SIZE,
            protocol: constants::PROTOCOL,
            guid: random::<u64>() | (1 << 63),
            max_handshake_bytes: usize::try_from(constants::MAX_QUEUED_BYTES)
                .expect("default queued byte bound is positive"),
            max_handshake_messages: constants::MAX_RECEIVE_MESSAGES,
            conn_attempt_timeout: constants::CONNECTION_ATTEMPT_TIMEOUT,
            conn_attempt_interval: constants::CONNECTION_ATTEMPT_INTERVAL,
            conn_attempt_max: constants::CONNECTION_ATTEMPT_MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_guid_is_negative_as_i64() {
        for _ in 0..256 {
            let guid = RakClientConfig::default().guid;
            assert!((guid as i64) < 0, "guid {guid:#x} has the sign bit clear");
        }
    }
}
