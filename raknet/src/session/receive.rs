//! Inbound datagram, reliability, ordering and fragment ownership.

use super::error::RakSessionError;
use super::output::RakDisconnectReason;
use super::state::RakSessionState;
use super::{
    DATAGRAM_WINDOW, MAX_CONCURRENT_SPLITS, MAX_DATAGRAM_GAP, MAX_DATAGRAM_SKIPS, MAX_SPLIT_COUNT,
    ORDER_WINDOW, SESSION_TIMEOUT, SessionEngine, ranges,
};
use crate::protocol::codec::RakCodec;
use crate::protocol::packets::frame_set::FrameSet;
use crate::protocol::types::frame::Frame;
use crate::util::u24;
use std::collections::HashMap;
use std::io::Cursor;
use std::time::Instant;
use tracing::debug;

enum ReliableArrival {
    New,
    Duplicate,
    OverBudget,
}

/// Charges a retained frame's inline storage as well as its owned payload.
pub(super) fn frame_cost(frame: &Frame) -> usize {
    size_of::<Frame>() + frame.payload.len()
}

impl SessionEngine {
    /// Releases abandoned reassemblies without ending an otherwise active session.
    pub(super) fn evict_stale_splits(&mut self, now: Instant) {
        let last = &self.inbound_spl_last;
        let mut freed = 0;
        self.inbound_spl_queue.retain(|split_id, fragments| {
            let fresh = now < split_deadline(last.get(split_id), now);
            if !fresh {
                freed += fragments.values().map(frame_cost).sum::<usize>();
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

    /// Acknowledges a datagram only when every reliable frame was retained or delivered.
    pub(super) fn handle_frame_set(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
        admit_application: bool,
    ) -> Result<(), RakSessionError> {
        let set = FrameSet::deserialize(buf)?;

        let ahead = u24::distance(self.inbound_seq, set.sequence);
        if ahead > MAX_DATAGRAM_GAP {
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
            let skipped = ahead.min(MAX_DATAGRAM_SKIPS) as u32;
            let mut missing = set.sequence.wrapping_sub(skipped) & u24::MASK;
            while missing != set.sequence && self.sequences_lost.len() < DATAGRAM_WINDOW {
                self.sequences_lost.insert(missing);
                missing = u24::add(missing, 1);
            }
            self.inbound_seq = u24::add(set.sequence, 1);
        }

        self.inbound_deferred = false;
        let mut result = Ok(());
        let mut frames = set.frames.into_iter();
        while let Some(frame) = frames.next() {
            if matches!(self.state, RakSessionState::Disconnected) {
                self.inbound_deferred |= frame.reliability.is_reliable()
                    || frames.any(|frame| frame.reliability.is_reliable());
                break;
            }
            if !admit_application && self.defer_application(&frame) {
                self.inbound_deferred |= frame.reliability.is_reliable();
                continue;
            }
            result = self.handle_frame(frame, now);
            if result.is_err() {
                break;
            }
        }
        if self.inbound_deferred || result.is_err() {
            self.sequences_recv.remove(&set.sequence);
        }
        if matches!(
            self.output.back(),
            Some(super::output::RakSessionOutput::Disconnected(
                _,
                RakDisconnectReason::Remote
            ))
        ) {
            // Receipt ACKs precede terminal output only after the full datagram is accounted for.
            let terminal = self.output.pop_back();
            let received = self.sequences_recv.drain().collect();
            self.queue_acks(received, false)?;
            self.flush();
            self.output.extend(terminal);
        }
        self.refresh_stall(now);
        result
    }

    /// Retains a deferred reliable obligation until delivery advances, including across control traffic.
    fn refresh_stall(&mut self, now: Instant) {
        let waiting = self.inbound_deferred
            || !self.inbound_rel_ahead.is_empty()
            || self.inbound_ord_bytes > 0;
        self.inbound_stall = match (waiting, self.inbound_stall) {
            (_, Some((since, progress))) if progress == self.inbound_progress => {
                Some((since, progress))
            }
            (false, _) => None,
            (true, _) => Some((now, self.inbound_progress)),
        };
    }

    /// Observes only a gap whose original delivery progress has not advanced.
    fn stall_deadline(&self) -> Option<Instant> {
        self.inbound_stall
            .filter(|(_, progress)| *progress == self.inbound_progress)
            .map(|(since, _)| since + SESSION_TIMEOUT)
    }

    /// Reports the terminal reliable-gap deadline; split eviction does not terminate the session.
    pub(super) fn receive_deadline(&self) -> Option<Instant> {
        self.stall_deadline()
    }

    /// Reports a gap with no delivery progress for the session timeout.
    pub(super) fn inbound_stalled(&mut self, now: Instant) -> bool {
        self.refresh_stall(now);
        self.stall_deadline()
            .is_some_and(|deadline| now >= deadline)
    }

    /// Admits buffering before committing reliable duplicate-detection state.
    pub(super) fn handle_frame(
        &mut self,
        frame: Frame,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        let cost = self.admission_cost(&frame);
        if cost > 0
            && (self.exceeds_queued_bytes(cost)
                || (frame.is_split() && self.split_slots_full(frame.split_id)))
        {
            debug!(
                "deferring frame {} from {}, buffering it would exceed max_queued_bytes",
                frame.reliable_index, self.addr
            );
            self.inbound_deferred |= frame.reliability.is_reliable();
            return Ok(());
        }

        if frame.reliability.is_reliable() {
            match self.track_reliable(frame.reliable_index) {
                ReliableArrival::New => {}
                ReliableArrival::Duplicate => {
                    debug!(
                        "received duplicate reliable frame {} from {}",
                        frame.reliable_index, self.addr
                    );
                    return Ok(());
                }
                ReliableArrival::OverBudget => {
                    debug!(
                        "closing session with {}, tracking reliable frame {} would exceed max_queued_bytes",
                        self.addr, frame.reliable_index
                    );
                    return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
                }
            }
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

    /// Estimates additional retained frame and range storage before admission.
    fn admission_cost(&self, frame: &Frame) -> usize {
        let mut cost = 0;

        if frame.reliability.is_reliable() {
            let ahead = u24::distance(self.inbound_rel_base, frame.reliable_index);
            if self.reliable_seen(frame.reliable_index) {
                return 0;
            }
            if ahead > 0 {
                cost += size_of::<ranges::Range>();
            }
        }

        let max_channels = (self.config.ordering_channels as usize).min(self.inbound_ord_idx.len());
        let buffered = frame.reliability.is_ordered() || frame.reliability.is_sequenced();
        if buffered && frame.order_channel as usize >= max_channels {
            return cost;
        }

        if frame.is_split() {
            if self.split_violation(frame).is_some() {
                return 0;
            }
            let fragments = self.inbound_spl_queue.get(&frame.split_id);
            let held = fragments.map_or(0, HashMap::len);
            let duplicate = fragments.is_some_and(|f| f.contains_key(&frame.split_index));
            if !duplicate && held + 1 < frame.split_size as usize {
                cost += frame_cost(frame);
            } else if !duplicate
                && frame.reliability.is_ordered()
                && u24::distance(
                    self.inbound_ord_idx[frame.order_channel as usize],
                    frame.order_index,
                ) > 0
            {
                // Completion replaces retained fragments with one future ordered frame.
                cost += frame_cost(frame).saturating_sub(held * size_of::<Frame>());
            }
        } else if frame.reliability.is_ordered() {
            let channel = frame.order_channel as usize;
            if u24::distance(self.inbound_ord_idx[channel], frame.order_index) > 0 {
                cost += frame_cost(frame);
            }
        }
        cost
    }

    /// Advances contiguous receipt and retains compact ranges across gaps.
    fn track_reliable(&mut self, index: u32) -> ReliableArrival {
        let ahead = u24::distance(self.inbound_rel_base, index);
        if ahead < 0 {
            return ReliableArrival::Duplicate;
        }
        if ahead == 0 {
            self.inbound_progress += 1;
            let next = u24::add(self.inbound_rel_base, 1);
            self.inbound_rel_base = ranges::take_contiguous(&mut self.inbound_rel_ahead, next);
            return ReliableArrival::New;
        }
        if ranges::contains(&self.inbound_rel_ahead, self.inbound_rel_base, index) {
            return ReliableArrival::Duplicate;
        }
        if self.exceeds_queued_bytes(size_of::<ranges::Range>()) {
            return ReliableArrival::OverBudget;
        }
        match ranges::insert(&mut self.inbound_rel_ahead, self.inbound_rel_base, index) {
            true => ReliableArrival::New,
            false => ReliableArrival::Duplicate,
        }
    }

    /// Delivers ordered payloads while retaining bounded future messages.
    pub(super) fn handle_full_frame(
        &mut self,
        frame: Frame,
        now: Instant,
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
                self.inbound_progress += 1;
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
                            .saturating_sub(frame_cost(&unord_frame));
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
                return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
            }

            if ahead > 0 {
                if self.exceeds_queued_bytes(frame_cost(&frame)) {
                    debug!(
                        "closing session with {}, buffered ordered bytes would exceed max_queued_bytes",
                        self.addr
                    );
                    return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
                }

                self.inbound_ord_bytes += frame_cost(&frame);
                if let Some(replaced) = self
                    .inbound_ord_queue
                    .entry(frame.order_channel)
                    .or_default()
                    .insert(frame.order_index, frame)
                {
                    self.inbound_ord_bytes =
                        self.inbound_ord_bytes.saturating_sub(frame_cost(&replaced));
                }
            }
            return Ok(());
        }

        self.handle_packet(frame.payload, now)?;
        Ok(())
    }

    /// Owns admitted fragments until complete reassembly and delivery.
    pub(super) fn handle_split_frame(
        &mut self,
        frame: Frame,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        if frame.split_index >= frame.split_size {
            debug!(
                "received split frame with out of range index {} (size {}) from {}",
                frame.split_index, frame.split_size, self.addr
            );
            return Ok(());
        }

        if let Some(reason) = self.split_violation(&frame) {
            if frame.split_size > MAX_SPLIT_COUNT {
                return self.disconnect(true, RakDisconnectReason::ProtocolViolation, now);
            }
            debug!("dropping fragment from {}, {}", self.addr, reason);
            return Ok(());
        }

        let split_id = frame.split_id;
        let split_size = frame.split_size;

        let fragments = self.inbound_spl_queue.entry(split_id).or_default();
        if fragments.contains_key(&frame.split_index) {
            return Ok(());
        }
        self.inbound_spl_bytes += frame_cost(&frame);
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
        let cost: usize = fragments.values().map(frame_cost).sum();
        self.inbound_spl_bytes = self.inbound_spl_bytes.saturating_sub(cost);

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

    /// Checks the shared frame, payload and reliable-range storage budget.
    pub(super) fn exceeds_queued_bytes(&self, additional: usize) -> bool {
        self.inbound_spl_bytes
            + self.inbound_ord_bytes
            + self.inbound_rel_ahead.len() * size_of::<ranges::Range>()
            + additional
            > self.config.max_queued_bytes as usize
    }

    /// Identifies invalid fragment metadata against an existing reassembly.
    pub(super) fn split_violation(&self, frame: &Frame) -> Option<&'static str> {
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

/// Missing fragment timing is already stale; present timing uses the original cutoff.
fn split_deadline(last: Option<&Instant>, now: Instant) -> Instant {
    last.map_or(now, |&at| at + SESSION_TIMEOUT)
}
