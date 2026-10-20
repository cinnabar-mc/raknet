//! Optional application admission and wire progress on the original session queues.

use super::input::SessionInput;
use super::lifecycle::RakSessionInput;
use super::output::RakSessionOutput;
use super::ranges;
use super::{Frame, ORDER_WINDOW, RakSessionError, RakSessionState, Sans, SessionEngine};
use crate::util::{flags, packet_id, u24};
use std::{io::Cursor, time::Instant};
use tracing::debug;

/// The same connection controls used by dispatch and admission classification.
pub(super) enum ConnectionControl {
    Ping,
    Pong,
    DetectLost,
    Disconnect,
}

impl ConnectionControl {
    /// Recognizes only controls owned by the existing connected-session handler.
    pub(super) fn classify(payload: &[u8]) -> Option<Self> {
        match *payload.first()? {
            packet_id::CONNECTED_PING => Some(Self::Ping),
            packet_id::CONNECTED_PONG => Some(Self::Pong),
            packet_id::DETECT_LOST_CONNECTIONS => Some(Self::DetectLost),
            packet_id::DISCONNECT => Some(Self::Disconnect),
            _ => None,
        }
    }
}

impl SessionEngine {
    /// Uses the canonical parser while the caller's application queue cannot admit.
    pub(super) fn handle_backpressured(
        &mut self,
        input: RakSessionInput,
    ) -> Result<(), RakSessionError> {
        if self.state == RakSessionState::Disconnected {
            return Err(RakSessionError::Closed);
        }
        match input {
            RakSessionInput::Datagram(bytes, now) => self.handle_datagram(bytes, now, false),
            RakSessionInput::Update(now) => self.handle(SessionInput::Update(now)),
        }
    }

    /// Shares complete datagram validation and ACK handling with ordinary admission.
    pub(super) fn handle_datagram(
        &mut self,
        bytes: Box<[u8]>,
        now: Instant,
        admit_application: bool,
    ) -> Result<(), RakSessionError> {
        self.last_recv = now;
        let Some(&header) = bytes.first() else {
            return Ok(());
        };
        let mut cursor = Cursor::new(bytes.as_ref());
        match header {
            _ if header & flags::VALID == 0 => debug!(
                "received unknown online packet {:#04X} from {}",
                header, self.addr
            ),
            _ if header & (flags::ACK | flags::NACK) != 0 => self.handle_ack(&mut cursor, now)?,
            _ => self.handle_frame_set(&mut cursor, now, admit_application)?,
        }
        Ok(())
    }

    /// Skips only the already-seen application prefix, never rescanning it per tick.
    pub(super) fn poll_control(&mut self) -> Option<RakSessionOutput> {
        while let Some(output) = self.output.get(self.control_cursor) {
            if matches!(output, RakSessionOutput::Packet(_)) {
                self.control_cursor += 1;
            } else {
                return self.output.remove(self.control_cursor);
            }
        }
        None
    }

    /// Observes duplicate receipt before an optional refusal can affect its ACK.
    pub(super) fn reliable_seen(&self, index: u32) -> bool {
        let ahead = u24::distance(self.inbound_rel_base, index);
        ahead < 0
            || (ahead > 0
                && ranges::contains(&self.inbound_rel_ahead, self.inbound_rel_base, index))
    }

    /// Refuses only a new valid application frame before reliable receipt changes.
    pub(super) fn defer_application(&self, frame: &Frame) -> bool {
        if self.state == RakSessionState::Connecting
            || (frame.reliability.is_reliable() && self.reliable_seen(frame.reliable_index))
        {
            return false;
        }
        if frame.reliability.is_ordered() || frame.reliability.is_sequenced() {
            let channel = frame.order_channel as usize;
            let channels = (self.config.ordering_channels as usize).min(self.inbound_ord_idx.len());
            if channel >= channels
                || u24::distance(self.inbound_ord_idx[channel], frame.order_index) < 0
                || (frame.reliability.is_ordered()
                    && u24::distance(self.inbound_ord_idx[channel], frame.order_index)
                        >= ORDER_WINDOW as i32)
                || (frame.reliability.is_sequenced()
                    && u24::distance(self.inbound_seq_idx[channel], frame.sequence_index) < 0)
            {
                return false;
            }
        }
        let payload = if frame.is_split() {
            if frame.split_index >= frame.split_size || self.split_violation(frame).is_some() {
                return false;
            }
            let held = self.inbound_spl_queue.get(&frame.split_id);
            if held.is_some_and(|fragments| fragments.contains_key(&frame.split_index))
                || held.map_or(0, |fragments| fragments.len()) + 1 != frame.split_size as usize
            {
                return false;
            }
            if frame.split_index == 0 {
                frame.payload.as_ref()
            } else {
                let Some(first) = held.and_then(|fragments| fragments.get(&0)) else {
                    return false;
                };
                first.payload.as_ref()
            }
        } else {
            frame.payload.as_ref()
        };
        !payload.is_empty() && ConnectionControl::classify(payload).is_none()
    }
}

#[cfg(test)]
mod tests;
