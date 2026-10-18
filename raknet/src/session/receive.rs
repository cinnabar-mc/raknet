//! Inbound datagram, reliability, ordering and fragment ownership.

use super::error::RakSessionError;
use super::output::RakDisconnectReason;
use super::state::RakSessionState;
use super::{
    DATAGRAM_WINDOW, MAX_CONCURRENT_SPLITS, MAX_SPLIT_COUNT, ORDER_WINDOW, RELIABLE_WINDOW,
    RakSession, SESSION_TIMEOUT,
};
use crate::protocol::codec::RakCodec;
use crate::protocol::packets::frame_set::FrameSet;
use crate::protocol::types::frame::Frame;
use crate::util::u24;
use std::io::Cursor;
use std::time::SystemTime;
use tracing::debug;

impl RakSession {
    /// Releases fragment payloads whose reassemblies have stopped making progress.
    pub(super) fn evict_stale_splits(&mut self, now: SystemTime) {
        let last = &self.inbound_spl_last;
        let mut freed = 0;
        self.inbound_spl_queue.retain(|split_id, fragments| {
            let fresh = last
                .get(split_id)
                .is_some_and(|&at| now < at + SESSION_TIMEOUT);
            if !fresh {
                freed += fragments.values().map(|f| f.payload.len()).sum::<usize>();
            }
            fresh
        });
        let queue = &self.inbound_spl_queue;
        self.inbound_spl_last
            .retain(|split_id, _| queue.contains_key(split_id));
        self.inbound_spl_bytes = self.inbound_spl_bytes.saturating_sub(freed);

        if freed > 0 {
            debug!(
                "evicted {} bytes of stale partial splits from {}",
                freed, self.addr
            );
        }
    }

    /// Tracks the datagram receive window before dispatching its encapsulated frames.
    pub(super) fn handle_frame_set(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        let set = FrameSet::deserialize(buf)?;

        let ahead = u24::distance(self.inbound_seq, set.sequence);
        if ahead >= DATAGRAM_WINDOW as i32 {
            debug!(
                "dropping FrameSet {} from {}, too far ahead of {}",
                set.sequence, self.addr, self.inbound_seq
            );
            return Ok(());
        }

        if !self.sequences_recv.insert(set.sequence) {
            debug!(
                "received duplicate FrameSet {} from {}",
                set.sequence, self.addr
            );
            return Ok(());
        }

        self.sequences_lost.remove(&set.sequence);

        if ahead < 0 {
            debug!(
                "received out of order FrameSet {} from {}, expected {}",
                set.sequence, self.addr, self.inbound_seq
            );
        } else {
            let mut missing = self.inbound_seq;
            while missing != set.sequence && self.sequences_lost.len() < DATAGRAM_WINDOW {
                self.sequences_lost.insert(missing);
                missing = u24::add(missing, 1);
            }
            self.inbound_seq = u24::add(set.sequence, 1);
        }

        for frame in set.frames {
            if matches!(self.state, RakSessionState::Disconnected) {
                break;
            }
            self.handle_frame(frame, now)?;
        }
        Ok(())
    }

    /// Filters duplicate reliable frames and invalid channels before payload admission.
    pub(super) fn handle_frame(
        &mut self,
        frame: Frame,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        if frame.reliability.is_reliable() && !self.track_reliable(frame.reliable_index) {
            debug!(
                "received duplicate reliable frame {} from {}",
                frame.reliable_index, self.addr
            );
            return Ok(());
        }

        let max_channels = (self.config.ordering_channels as usize).min(self.inbound_ord_idx.len());
        if (frame.reliability.is_ordered() || frame.reliability.is_sequenced())
            && frame.order_channel as usize >= max_channels
        {
            debug!(
                "received frame with out of range order channel {} from {}",
                frame.order_channel, self.addr
            );
            return Ok(());
        }

        match frame.is_split() {
            true => self.handle_split_frame(frame, now)?,
            false => self.handle_full_frame(frame, now)?,
        }
        Ok(())
    }

    /// Retains a bounded duplicate-detection window and reports whether an index is new.
    pub(super) fn track_reliable(&mut self, index: u32) -> bool {
        if !self.inbound_rel_seen.insert(index) {
            return false;
        }

        self.inbound_rel_order.push_back(index);

        while self.inbound_rel_order.len() > RELIABLE_WINDOW {
            if let Some(old) = self.inbound_rel_order.pop_front() {
                self.inbound_rel_seen.remove(&old);
            }
        }

        true
    }

    /// Delivers complete payloads in sequence, retaining bounded ordered gaps.
    pub(super) fn handle_full_frame(
        &mut self,
        frame: Frame,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        let channel = frame.order_channel as usize;

        if frame.reliability.is_sequenced() {
            if u24::distance(self.inbound_seq_idx[channel], frame.sequence_index) < 0
                || u24::distance(self.inbound_ord_idx[channel], frame.order_index) < 0
            {
                debug!(
                    "dropped stale sequenced frame on channel {} from {}",
                    frame.order_channel, self.addr
                );
                return Ok(());
            }

            self.inbound_seq_idx[channel] = u24::add(frame.sequence_index, 1);

            return self.handle_packet(frame.payload, now);
        }

        if frame.reliability.is_ordered() {
            let ahead = u24::distance(self.inbound_ord_idx[channel], frame.order_index);

            if ahead == 0 {
                self.inbound_seq_idx[channel] = 0;
                self.inbound_ord_idx[channel] = u24::add(frame.order_index, 1);

                self.handle_packet(frame.payload, now)?;

                let mut idx = self.inbound_ord_idx[channel];

                let mut packets = Vec::new();
                {
                    let unord_queue = self
                        .inbound_ord_queue
                        .entry(frame.order_channel)
                        .or_default();
                    while let Some(unord_frame) = unord_queue.remove(&idx) {
                        self.inbound_ord_bytes = self
                            .inbound_ord_bytes
                            .saturating_sub(unord_frame.payload.len());
                        packets.push(unord_frame.payload);

                        idx = u24::add(idx, 1);
                    }
                }
                self.inbound_ord_idx[channel] = idx;

                for packet in packets {
                    self.handle_packet(packet, now)?;
                }
                return Ok(());
            }

            if ahead >= ORDER_WINDOW as i32 {
                debug!(
                    "closing session with {}, order index {} too far ahead of {}",
                    self.addr, frame.order_index, self.inbound_ord_idx[channel]
                );
                return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
            }

            if ahead > 0 {
                if self.exceeds_queued_bytes(frame.payload.len()) {
                    debug!(
                        "closing session with {}, buffered ordered bytes would exceed max_queued_bytes",
                        self.addr
                    );
                    return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
                }

                self.inbound_ord_bytes += frame.payload.len();
                if let Some(replaced) = self
                    .inbound_ord_queue
                    .entry(frame.order_channel)
                    .or_default()
                    .insert(frame.order_index, frame)
                {
                    self.inbound_ord_bytes = self
                        .inbound_ord_bytes
                        .saturating_sub(replaced.payload.len());
                }
            }
            return Ok(());
        }

        self.handle_packet(frame.payload, now)?;
        Ok(())
    }

    /// Owns admitted fragments until the complete payload can be delivered once.
    pub(super) fn handle_split_frame(
        &mut self,
        frame: Frame,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        if frame.split_index >= frame.split_size {
            debug!(
                "received split frame with out of range index {} (size {}) from {}",
                frame.split_index, frame.split_size, self.addr
            );
            return Ok(());
        }

        if let Some(reason) = self.split_violation(&frame) {
            debug!("closing session with {}, {}", self.addr, reason);
            return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
        }

        if self.split_slots_full(frame.split_id) {
            debug!(
                "dropping split frame from {}, {} splits already in reassembly",
                self.addr, MAX_CONCURRENT_SPLITS
            );
            return Ok(());
        }

        if self.exceeds_queued_bytes(frame.payload.len()) {
            debug!(
                "dropping split frame from {}, buffered split bytes would exceed max_queued_bytes",
                self.addr
            );
            return Ok(());
        }

        let split_id = frame.split_id;
        let split_size = frame.split_size;

        let fragments = self.inbound_spl_queue.entry(split_id).or_default();
        if fragments.contains_key(&frame.split_index) {
            return Ok(());
        }
        self.inbound_spl_bytes += frame.payload.len();
        fragments.insert(frame.split_index, frame);
        self.inbound_spl_last.insert(split_id, now);

        if fragments.len() as u32 != split_size {
            return Ok(());
        }

        self.inbound_spl_last.remove(&split_id);
        let Some(mut fragments) = self.inbound_spl_queue.remove(&split_id) else {
            return Ok(());
        };
        let buffered: usize = fragments.values().map(|f| f.payload.len()).sum();
        self.inbound_spl_bytes = self.inbound_spl_bytes.saturating_sub(buffered);

        let mut payload = Vec::with_capacity(buffered);
        let mut whole = None;
        for i in 0..split_size {
            let Some(fragment) = fragments.remove(&i) else {
                return Ok(());
            };
            payload.extend_from_slice(&fragment.payload);
            whole.get_or_insert(fragment);
        }
        let Some(mut whole) = whole else {
            return Ok(());
        };

        whole.payload = payload.into_boxed_slice();
        whole.split_size = 0;
        whole.split_id = 0;
        whole.split_index = 0;

        self.handle_full_frame(whole, now)
    }

    /// Checks the shared payload budget for fragments and future ordered messages.
    pub(super) fn exceeds_queued_bytes(&self, additional: usize) -> bool {
        self.inbound_spl_bytes + self.inbound_ord_bytes + additional
            > self.config.max_queued_bytes as usize
    }

    /// Identifies invalid fragment metadata against the current reassembly.
    pub(super) fn split_violation(&self, frame: &Frame) -> Option<&'static str> {
        if frame.payload.is_empty() {
            return Some("empty split fragment");
        }
        if frame.split_size > MAX_SPLIT_COUNT {
            return Some("split count exceeds the maximum");
        }
        self.inbound_spl_queue
            .get(&frame.split_id)?
            .values()
            .next()
            .is_some_and(|f| f.split_size != frame.split_size)
            .then_some("split count changed mid-split")
    }

    /// Protects existing reassembly progress when the concurrent packet limit is full.
    pub(super) fn split_slots_full(&self, split_id: u16) -> bool {
        self.inbound_spl_queue.len() >= MAX_CONCURRENT_SPLITS
            && !self.inbound_spl_queue.contains_key(&split_id)
    }
}
