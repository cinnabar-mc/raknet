//! Complete application packets retained until the connection owner transfers.

use super::{RakClientConfig, RakClientError};
use std::collections::VecDeque;

#[derive(Clone, Debug, Default)]
pub(super) struct PendingPackets {
    packets: VecDeque<Box<[u8]>>,
    bytes: usize,
}

impl PendingPackets {
    /// Admits an owned packet without copying bytes or exceeding either configured bound.
    pub(super) fn push(
        &mut self,
        packet: Box<[u8]>,
        config: &RakClientConfig,
    ) -> Result<(), RakClientError> {
        if self.packets.len() >= config.max_handshake_messages
            || packet.len() > config.max_handshake_bytes.saturating_sub(self.bytes)
        {
            return Err(RakClientError::HandshakeReceiveBufferExceeded);
        }
        self.bytes += packet.len();
        self.packets.push_back(packet);
        Ok(())
    }

    /// Transfers the FIFO to its established session and resets byte accounting.
    pub(super) fn take(&mut self) -> VecDeque<Box<[u8]>> {
        self.bytes = 0;
        std::mem::take(&mut self.packets)
    }

    /// Releases payloads belonging to an aborted or cancelled connection attempt.
    pub(super) fn clear(&mut self) {
        self.packets.clear();
        self.bytes = 0;
    }
}
