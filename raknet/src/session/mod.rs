// Modified by Cinnabar contributors for caller-driven connection negotiation.
mod backpressure;
pub mod config;
pub mod congestion_controller;
pub mod error;
pub mod input;
pub(crate) mod lifecycle;
pub mod output;
mod ranges;
mod receive;
pub(crate) mod snapshot;
pub mod state;

use crate::clock::RakClock;
use crate::protocol::codec::{RakCodec, RakCodecHelper};
use crate::protocol::packets::ack::Ack;
use crate::protocol::packets::connected_ping::ConnectedPing;
use crate::protocol::packets::connected_pong::ConnectedPong;
use crate::protocol::packets::disconnect::Disconnect;
use crate::protocol::packets::frame_set::FrameSet;
use crate::protocol::types::frame::Frame;
use crate::sans::Sans;
use crate::session::congestion_controller::RakCongestionController;
use crate::session::error::RakSessionError;
use crate::session::input::SessionInput;
use crate::session::output::{RakDisconnectReason, RakSessionOutput};
use crate::types::priority::RakPriority;
use crate::types::reliability::RakReliability;
use crate::util::constants::{DGRAM_HEADER_SIZE, DGRAM_MTU_OVERHEAD, UDP_HEADER_SIZE};
use crate::util::socket_addr::get_overhead;
use crate::util::u24;
#[cfg(test)]
use crate::util::{flags, packet_id};
use backpressure::ConnectionControl;
use config::RakSessionConfig;
use state::RakSessionState;
use std::cmp::{Reverse, min};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::mem::take;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::debug;

#[derive(Default, Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct RakSessionId(pub u64);

impl RakSessionId {
    pub fn increment(&mut self) -> RakSessionId {
        let old = *self;
        self.0 += 1;
        old
    }
}

const DATAGRAM_WINDOW: usize = 2048;
const ORDER_WINDOW: usize = 2048;
const MAX_SPLIT_COUNT: u32 = 8192;
const MAX_OUTBOUND_SPLIT_COUNT: usize = 2048;
const MAX_CONCURRENT_SPLITS: usize = 256;
const MAX_UNACKED_RELIABLE: i32 = 4096;
const MAX_SPLITS_IN_FLIGHT: usize = 256;
const SESSION_TIMEOUT: Duration = Duration::from_millis(15000);

#[derive(Clone, Debug)]
pub(crate) struct SessionEngine {
    pub id: RakSessionId,
    pub addr: SocketAddr,
    pub state: RakSessionState,
    pub guid: u64,
    pub mtu: u16,
    config: RakSessionConfig,
    pub(crate) clock: RakClock,

    last_tick: Instant,
    last_ping: Instant,
    last_recv: Instant,
    last_pong: Instant,
    closing_since: Option<Instant>,

    congestion_controller: RakCongestionController,
    bandwidth_limited: bool,
    wire_continuous: bool,

    queue: VecDeque<(Box<[u8]>, SocketAddr)>,

    sequences_recv: HashSet<u32>,
    sequences_lost: HashSet<u32>,

    outbound_seq: u32,
    outbound_spl: u16,
    pub(crate) outbound_rel: u32,
    outbound_queue: [VecDeque<Frame>; 4],
    outbound_cache: HashMap<u32, FrameSet>,
    outbound_bytes: usize,
    outbound_resend: BinaryHeap<(Reverse<Instant>, u32)>,
    outbound_ord_idx: [u32; 32],
    outbound_seq_idx: [u32; 32],

    inbound_seq: u32,
    inbound_rel_base: u32,
    inbound_rel_ahead: Vec<ranges::Range>,
    inbound_spl_queue: HashMap<u16, HashMap<u32, Frame>>,
    inbound_spl_bytes: usize,
    inbound_spl_last: HashMap<u16, Instant>,
    inbound_ord_queue: HashMap<u8, HashMap<u32, Frame>>,
    inbound_ord_bytes: usize,
    inbound_ord_idx: [u32; 32],
    inbound_seq_idx: [u32; 32],
    inbound_progress: u64,
    inbound_deferred: bool,
    inbound_stall: Option<(Instant, u64)>,

    output: VecDeque<RakSessionOutput>,
    control_cursor: usize,
}

impl Sans for SessionEngine {
    type Input = SessionInput;
    type Output = RakSessionOutput;
    type Error = RakSessionError;

    fn handle(&mut self, msg: Self::Input) -> Result<(), Self::Error> {
        if matches!(self.state, RakSessionState::Disconnected) {
            return Err(RakSessionError::Closed);
        }

        match msg {
            SessionInput::Datagram(buf, now) => self.handle_datagram(buf, now, true)?,
            SessionInput::Send(..) if self.state == RakSessionState::Closing => {
                return Err(RakSessionError::Closed);
            }
            SessionInput::Send(buf, ..) if buf.len() > self.max_message_len() => {
                return Err(RakSessionError::MessageSizeExceeded(buf.len()));
            }
            SessionInput::Send(buf, _, _, now)
                if self.outbound_bytes + buf.len() > self.config.max_outbound_bytes as usize =>
            {
                debug!(
                    "closing session with {}, queued outbound bytes would exceed max_outbound_bytes",
                    self.addr
                );
                self.disconnect(true, RakDisconnectReason::OutboundBytesExceeded, now)?
            }
            SessionInput::Send(buf, reliability, priority, now) => {
                self.send_frame(Frame::new(reliability, buf), priority, now)?
            }
            SessionInput::Update(now) => self.handle_timeout(now)?,
            SessionInput::Disconnect(now) => self.close(now)?,
            SessionInput::DisconnectNow(now) => {
                self.send_queue(now)?;
                self.flush();
                self.disconnect(true, RakDisconnectReason::Requested, now)?
            }
        }
        Ok(())
    }

    fn poll(&mut self) -> Option<Self::Output> {
        let output = self.output.pop_front()?;
        self.control_cursor = self.control_cursor.saturating_sub(1);
        Some(output)
    }
}

impl SessionEngine {
    /// Creates session timers in the same domain clock used by its subsequent inputs.
    pub fn new(
        id: RakSessionId,
        addr: SocketAddr,
        guid: u64,
        mtu: u16,
        now: Instant,
        clock: RakClock,
        config: RakSessionConfig,
    ) -> Self {
        let mtu = mtu - UDP_HEADER_SIZE - get_overhead(&addr);

        Self {
            id,
            addr,
            guid,
            mtu,
            config,
            clock,

            last_tick: now,
            last_ping: now,
            last_recv: now,
            last_pong: now,
            closing_since: None,

            state: RakSessionState::Connected,
            congestion_controller: RakCongestionController::new(mtu as usize),
            bandwidth_limited: false,
            wire_continuous: false,

            sequences_recv: HashSet::new(),
            sequences_lost: HashSet::new(),

            queue: VecDeque::new(),

            outbound_seq: 0,
            outbound_spl: 0,
            outbound_rel: 0,
            outbound_queue: [
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
            ],
            outbound_cache: HashMap::new(),
            outbound_bytes: 0,
            outbound_resend: BinaryHeap::new(),
            outbound_ord_idx: [0; 32],
            outbound_seq_idx: [0; 32],

            inbound_seq: 0,
            inbound_rel_base: 0,
            inbound_rel_ahead: Vec::new(),
            inbound_spl_queue: HashMap::new(),
            inbound_spl_bytes: 0,
            inbound_spl_last: HashMap::new(),
            inbound_ord_queue: HashMap::new(),
            inbound_ord_bytes: 0,
            inbound_ord_idx: [0; 32],
            inbound_seq_idx: [0; 32],
            inbound_progress: 0,
            inbound_deferred: false,
            inbound_stall: None,

            output: VecDeque::new(),
            control_cursor: 0,
        }
    }

    pub fn get_state(&self) -> RakSessionState {
        self.state
    }

    pub fn rtt(&self) -> Duration {
        self.congestion_controller.rtt()
    }

    pub fn outbound_bytes(&self) -> usize {
        self.outbound_bytes
    }

    pub fn max_message_len(&self) -> usize {
        let payload_per_fragment = (self.mtu - DGRAM_MTU_OVERHEAD) as usize;
        let storage_per_fragment = payload_per_fragment + size_of::<Frame>();
        let budget = usize::try_from(self.config.max_queued_bytes).unwrap_or(0);
        let payload_budget = (budget / storage_per_fragment) * payload_per_fragment
            + (budget % storage_per_fragment).saturating_sub(size_of::<Frame>());
        let split_limit = MAX_OUTBOUND_SPLIT_COUNT * payload_per_fragment;
        payload_budget.min(split_limit).max(self.mtu as usize)
    }

    /// Shares the original terminal deadlines with a backpressured delivery edge.
    fn expiry_deadline(&self, now: Instant) -> Option<Instant> {
        if self.state == RakSessionState::Disconnected {
            return None;
        }
        let close = if self.state == RakSessionState::Closing {
            self.closing_since
                .and_then(|since| self.close_deadline(since))
        } else {
            None
        };
        [self.peer_deadline(), self.receive_deadline(now), close]
            .into_iter()
            .flatten()
            .min()
    }

    /// Uses the same last admitted input and timeout as ordinary maintenance.
    fn peer_deadline(&self) -> Option<Instant> {
        self.last_recv.checked_add(SESSION_TIMEOUT)
    }

    /// Retains the original first close origin and validated grace interval.
    fn close_deadline(&self, since: Instant) -> Option<Instant> {
        since.checked_add(self.config.close_timeout)
    }

    fn handle_timeout(&mut self, now: Instant) -> Result<(), RakSessionError> {
        if self.peer_deadline().is_some_and(|deadline| now >= deadline) {
            debug!(
                "detected stale connection from {}, disconnecting...",
                self.addr
            );

            self.disconnect(true, RakDisconnectReason::Timeout, now)?;
            return Ok(());
        }

        if self.evict_stale_splits(now) || self.inbound_stalled(now) {
            return self.disconnect(true, RakDisconnectReason::Timeout, now);
        }

        if self.state == RakSessionState::Closing {
            return self.update_closing(now);
        }

        if self.config.autoflush
            && now
                .checked_duration_since(self.last_tick)
                .is_some_and(|elapsed| elapsed >= self.config.autoflush_interval_ms)
        {
            self.tick(now)?;

            self.last_tick = now;
        }

        let ping_interval = Duration::from_millis(2000);
        if now
            .checked_duration_since(self.last_ping)
            .is_some_and(|elapsed| elapsed >= ping_interval)
        {
            self.send_connected_ping(now, RakReliability::Unreliable)?;

            self.last_ping = now;
        }

        let mut duration = min(
            remaining(self.last_ping, ping_interval, now),
            remaining(self.last_recv, SESSION_TIMEOUT, now),
        );
        if self.config.autoflush {
            duration = min(
                duration,
                remaining(self.last_tick, self.config.autoflush_interval_ms, now),
            );
        }

        self.output.push_back(RakSessionOutput::Wait(duration));
        Ok(())
    }

    fn close(&mut self, now: Instant) -> Result<(), RakSessionError> {
        if self.state == RakSessionState::Closing {
            return Ok(());
        }

        self.state = RakSessionState::Closing;
        self.closing_since = Some(now);
        self.last_tick = now;
        self.tick(now)?;

        if self.nothing_left_to_send() {
            return self.disconnect(true, RakDisconnectReason::Requested, now);
        }

        self.output
            .push_back(RakSessionOutput::Wait(self.closing_wait(now)));
        Ok(())
    }

    fn nothing_left_to_send(&self) -> bool {
        self.queue.is_empty()
            && self.outbound_cache.is_empty()
            && self.outbound_queue.iter().all(VecDeque::is_empty)
    }

    fn update_closing(&mut self, now: Instant) -> Result<(), RakSessionError> {
        let since = self.closing_since.unwrap_or(now);

        self.tick(now)?;
        self.last_tick = now;

        if self.nothing_left_to_send()
            || self
                .close_deadline(since)
                .is_some_and(|deadline| now >= deadline)
        {
            return self.disconnect(true, RakDisconnectReason::Requested, now);
        }

        self.output
            .push_back(RakSessionOutput::Wait(self.closing_wait(now)));
        Ok(())
    }

    /// Bounds both the first close wait and later updates by the original session policy.
    fn closing_wait(&self, now: Instant) -> Duration {
        min(
            min(
                remaining(
                    self.closing_since.unwrap_or(now),
                    self.config.close_timeout,
                    now,
                ),
                remaining(self.last_recv, SESSION_TIMEOUT, now),
            ),
            self.config.autoflush_interval_ms,
        )
    }

    pub fn tick(&mut self, now: Instant) -> Result<(), RakSessionError> {
        if matches!(self.state, RakSessionState::Disconnected) {
            return Ok(());
        }
        self.wire_continuous = self.bandwidth_limited;

        let received = self.sequences_recv.drain().collect();
        self.queue_acks(received, false)?;
        let lost = self.sequences_lost.drain().collect();
        self.queue_acks(lost, true)?;

        self.send_stale(now)?;
        self.send_queue(now)?;
        self.flush();
        Ok(())
    }

    fn queue_acks(&mut self, sequences: Vec<u32>, is_nack: bool) -> Result<(), RakSessionError> {
        for ack in Ack::split(sequences, is_nack, self.mtu as usize) {
            self.queue.push_back((ack.serialized()?, self.addr));
        }
        Ok(())
    }

    fn send_stale(&mut self, now: Instant) -> Result<(), RakSessionError> {
        let mut pending = Vec::new();

        let mut bandwidth = self.congestion_controller.retransmission_bandwidth();

        while let Some(&(Reverse(sent), seq)) = self.outbound_resend.peek() {
            if sent > now {
                break;
            }

            let Some(set) = self.outbound_cache.get(&seq) else {
                self.outbound_resend.pop();
                continue;
            };

            let size = set.encapsulated_size();
            if size > bandwidth {
                break;
            }
            bandwidth -= size;

            self.outbound_resend.pop();

            self.congestion_controller
                .resent(self.outbound_seq, self.bandwidth_limited);

            let mut set = self.outbound_cache.remove(&seq).expect("unreachable");
            self.outbound_bytes = self.outbound_bytes.saturating_sub(set.size_hint());
            set.sequence = self.outbound_seq;
            self.outbound_seq = u24::add(self.outbound_seq, 1);
            self.congestion_controller.resent_as(seq, set.sequence);
            pending.push(set);
        }

        for set in pending {
            self.send_frame_set(set, false, false, now)?;
        }
        Ok(())
    }

    fn send_queue(&mut self, now: Instant) -> Result<(), RakSessionError> {
        let mut bandwidth = self.congestion_controller.transmission_bandwidth();
        let oldest_reliable = self.oldest_unacked_reliable();
        let oldest_ordered = self.oldest_unacked_ordered();
        let mut splits_in_flight = self.splits_in_flight();

        let mut frames = Vec::new();
        for queue in &mut self.outbound_queue {
            while let Some(frame) = queue.pop_front_if(|f| {
                f.size_hint() <= bandwidth
                    && within_reliable_window(f, oldest_reliable)
                    && within_ordered_window(f, &oldest_ordered)
                    && (!f.is_split()
                        || splits_in_flight.contains(&f.split_id)
                        || splits_in_flight.len() < MAX_SPLITS_IN_FLIGHT)
            }) {
                if frame.is_split() {
                    splits_in_flight.insert(frame.split_id);
                }
                bandwidth -= frame.size_hint();
                self.outbound_bytes = self.outbound_bytes.saturating_sub(frame.size_hint());
                frames.push(frame);
            }
        }
        self.bandwidth_limited = self.outbound_queue.iter().any(|queue| !queue.is_empty());

        if frames.is_empty() {
            return Ok(());
        };

        let sets = self.make_sets(frames);
        for set in sets {
            self.send_frame_set(set, false, true, now)?;
        }
        Ok(())
    }

    fn oldest_unacked_reliable(&self) -> Option<u32> {
        let cached = self.outbound_cache.values().flat_map(|set| &set.frames);
        let queued = self.outbound_queue.iter().flatten();
        cached
            .chain(queued)
            .filter(|f| f.reliability.is_reliable())
            .map(|f| f.reliable_index)
            .max_by_key(|&index| u24::distance(index, self.outbound_rel))
    }

    /// Finds each channel's oldest outstanding ordered message, including queued fragments.
    fn oldest_unacked_ordered(&self) -> [Option<u32>; 32] {
        let mut oldest = [None; 32];
        let cached = self.outbound_cache.values().flat_map(|set| &set.frames);
        let queued = self.outbound_queue.iter().flatten();
        for frame in cached.chain(queued).filter(|f| f.reliability.is_ordered()) {
            let channel = frame.order_channel as usize;
            let next = self.outbound_ord_idx[channel];
            if oldest[channel].is_none_or(|index| {
                u24::distance(frame.order_index, next) > u24::distance(index, next)
            }) {
                oldest[channel] = Some(frame.order_index);
            }
        }
        oldest
    }

    fn splits_in_flight(&self) -> HashSet<u16> {
        self.outbound_cache
            .values()
            .flat_map(|set| &set.frames)
            .filter(|f| f.is_split())
            .map(|f| f.split_id)
            .collect()
    }

    /// Packs frames and assigns sequences; emission installs the current feedback flags.
    fn make_sets(&mut self, frames: Vec<Frame>) -> Vec<FrameSet> {
        let mut sets = Vec::new();

        let max = (self.mtu - DGRAM_HEADER_SIZE) as usize;

        let mut batch = Vec::new();
        let mut size = DGRAM_HEADER_SIZE as usize;

        for frame in frames {
            let frame_size = frame.size_hint();

            if frame_size > max {
                // TODO: make this an error instead of panic
                panic!(
                    "Frame too large for FrameSet, size: {}, max size: {}",
                    frame_size, max
                );
            }

            if size + frame_size > max {
                sets.push(FrameSet {
                    sequence: self.outbound_seq,
                    frames: take(&mut batch),
                    continuous_send: false,
                    needs_b_and_as: false,
                    is_pair: false,
                });
                self.outbound_seq = u24::add(self.outbound_seq, 1);

                size = DGRAM_HEADER_SIZE as usize;
            }

            size += frame_size;
            batch.push(frame);
        }

        if !batch.is_empty() {
            sets.push(FrameSet {
                sequence: self.outbound_seq,
                frames: batch,
                continuous_send: false,
                needs_b_and_as: false,
                is_pair: false,
            });
            self.outbound_seq = u24::add(self.outbound_seq, 1);
        }

        sets
    }

    /// Installs live feedback flags and charges retained reliable encapsulations once.
    fn send_frame_set(
        &mut self,
        mut frameset: FrameSet,
        immediate: bool,
        first_send: bool,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        frameset.continuous_send = self.wire_continuous;
        frameset.needs_b_and_as = self.congestion_controller.slow_start();
        let buf = frameset.serialized()?;
        self.wire_continuous = true;

        match immediate {
            true => self
                .output
                .push_back(RakSessionOutput::Datagram(buf, self.addr)),
            false => {
                self.queue.push_back((buf, self.addr));
            }
        }

        let reliable = frameset.frames.iter().any(|f| f.reliability.is_reliable());
        if reliable {
            let resend = now + self.congestion_controller.retransmission_timeout();

            if first_send {
                self.congestion_controller.sent(
                    frameset.sequence,
                    frameset.encapsulated_size(),
                    now,
                );
            }
            self.outbound_resend
                .push((Reverse(resend), frameset.sequence));
            self.outbound_bytes += frameset.size_hint();
            self.outbound_cache.insert(frameset.sequence, frameset);
        }
        Ok(())
    }

    fn flush(&mut self) {
        for (buf, addr) in self.queue.drain(..) {
            self.output.push_back(RakSessionOutput::Datagram(buf, addr));
        }
    }

    /// Schedules application traffic while keeping Immediate sends inside flow limits.
    fn send_frame(
        &mut self,
        frame: Frame,
        priority: RakPriority,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        let frames = self.prepare_frames(frame);
        if priority == RakPriority::Immediate {
            self.outbound_bytes += frames.iter().map(Frame::size_hint).sum::<usize>();
            self.outbound_queue[priority as usize].extend(frames);
            self.wire_continuous = self.bandwidth_limited;
            self.send_queue(now)?;
            self.flush();
        } else {
            self.queue_frames(frames, priority, now)?;
        }
        Ok(())
    }

    /// Assigns wire counters and fragments one payload independently from scheduling.
    fn prepare_frames(&mut self, frame: Frame) -> Vec<Frame> {
        let max_size = (self.mtu - DGRAM_MTU_OVERHEAD) as usize;

        let order_channel = frame.order_channel;

        let mut reliability = frame.reliability;
        let mut split_id = 0;

        let payloads = if frame.size_hint() > max_size {
            reliability = match reliability {
                RakReliability::Unreliable => RakReliability::Reliable,
                RakReliability::UnreliableSequenced => RakReliability::ReliableSequenced,
                RakReliability::UnreliableWithAckReceipt => RakReliability::ReliableWithAckReceipt,
                val => val,
            };
            split_id = self.outbound_spl;
            self.outbound_spl = self.outbound_spl.wrapping_add(1);

            let split_size = frame.payload.len().div_ceil(max_size);

            let mut payloads = Vec::with_capacity(split_size);
            for i in 0..split_size {
                let start = i * max_size;
                let end = min(start + max_size, frame.payload.len());

                payloads.push(frame.payload[start..end].into());
            }
            payloads
        } else {
            vec![frame.payload]
        };

        let mut ord_idx = 0;
        let mut seq_idx = 0;
        if frame.reliability.is_sequenced() {
            ord_idx = self.outbound_ord_idx[order_channel as usize];
            seq_idx = {
                let r = &mut self.outbound_seq_idx[order_channel as usize];
                let val = *r;
                *r = u24::add(val, 1);
                val
            };
        } else if frame.reliability.is_ordered() {
            ord_idx = {
                let r = &mut self.outbound_ord_idx[order_channel as usize];
                let val = *r;
                *r = u24::add(val, 1);
                val
            };
            self.outbound_seq_idx[order_channel as usize] = 0;
        }

        let split_size = payloads.len();
        payloads
            .into_iter()
            .enumerate()
            .map(|(i, payload)| Frame {
                reliability,
                payload,
                reliable_index: match reliability.is_reliable() {
                    true => {
                        let val = self.outbound_rel;
                        self.outbound_rel = u24::add(val, 1);
                        val
                    }
                    false => 0,
                },
                sequence_index: seq_idx,
                order_index: ord_idx,
                order_channel,
                split_size: if split_size > 1 { split_size as u32 } else { 0 },
                split_id,
                split_index: i as u32,
            })
            .collect()
    }

    /// Emits unreliable or terminal control without waiting for application congestion.
    fn send_control(&mut self, frame: Frame, now: Instant) -> Result<(), RakSessionError> {
        let frames = self.prepare_frames(frame);
        self.queue_frames(frames, RakPriority::Immediate, now)
    }

    fn queue_frames(
        &mut self,
        frames: Vec<Frame>,
        priority: RakPriority,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        match priority {
            RakPriority::Immediate => {
                let sets = self.make_sets(frames);
                for set in sets {
                    self.send_frame_set(set, true, true, now)?;
                }
            }
            _ => {
                self.outbound_bytes += frames.iter().map(Frame::size_hint).sum::<usize>();
                self.outbound_queue[priority as usize].extend(frames);
            }
        }
        Ok(())
    }

    fn handle_ack(&mut self, buf: &mut Cursor<&[u8]>, now: Instant) -> Result<(), RakSessionError> {
        let ack = Ack::deserialize(buf)?;

        for seq in ack.sequences {
            let Some(set) = self.outbound_cache.remove(&seq) else {
                continue;
            };
            self.outbound_bytes = self.outbound_bytes.saturating_sub(set.size_hint());
            match ack.is_nack {
                true => {
                    self.congestion_controller
                        .nacked(self.outbound_seq, self.bandwidth_limited);
                    self.congestion_controller
                        .lost(set.sequence, set.encapsulated_size());
                    self.queue_frames(set.frames, RakPriority::Immediate, now)?;
                }
                false => {
                    self.congestion_controller.acked(
                        now,
                        set.sequence,
                        set.encapsulated_size(),
                        self.bandwidth_limited,
                    );
                }
            }
        }
        Ok(())
    }

    fn handle_packet(&mut self, buf: Box<[u8]>, now: Instant) -> Result<(), RakSessionError> {
        if buf.is_empty() {
            return Ok(());
        }
        if self.state != RakSessionState::Connecting && self.try_handle_control(&buf, now)? {
            return Ok(());
        }
        self.output.push_back(RakSessionOutput::Packet(buf));
        Ok(())
    }

    /// Handles common connection controls, returning false for an application or role-specific packet.
    ///
    /// The connecting client calls this in output order so acceptance and peer close
    /// remain ordered with application packets from the same datagram.
    pub(crate) fn try_handle_control(
        &mut self,
        buf: &[u8],
        now: Instant,
    ) -> Result<bool, RakSessionError> {
        let Some(control) = ConnectionControl::classify(buf) else {
            return Ok(false);
        };
        let mut cursor = Cursor::new(buf);
        match control {
            ConnectionControl::Ping => self.handle_connected_ping(&mut cursor, now)?,
            ConnectionControl::Pong => self.handle_connected_pong(&mut cursor, now)?,
            ConnectionControl::DetectLost if self.state == RakSessionState::Closing => {}
            ConnectionControl::DetectLost => {
                self.send_connected_ping(now, RakReliability::ReliableOrdered)?
            }
            ConnectionControl::Disconnect => self.handle_disconnect(&mut cursor, now)?,
        }
        Ok(true)
    }

    /// Emits a liveness ping using the caller's time and selected control reliability.
    fn send_connected_ping(
        &mut self,
        now: Instant,
        reliability: RakReliability,
    ) -> Result<(), RakSessionError> {
        let ping = ConnectedPing {
            timestamp: self.clock.timestamp(now),
        };
        let payload = ping.serialized()?;
        if reliability.is_reliable() {
            // Reliable liveness needs the same retention and delivery limits as application data.
            self.handle(SessionInput::Send(
                payload,
                reliability,
                RakPriority::Immediate,
                now,
            ))
        } else {
            self.send_control(Frame::new(reliability, payload), now)
        }
    }

    /// Restores the completed handshake prefix before an optional terminal notification.
    pub(crate) fn restore_handshake_outputs(
        &mut self,
        packets: VecDeque<Box<[u8]>>,
        terminal: Option<(RakSessionId, RakDisconnectReason)>,
    ) {
        for packet in packets {
            self.output.push_back(RakSessionOutput::Packet(packet));
        }
        if let Some((id, reason)) = terminal {
            self.output
                .push_back(RakSessionOutput::Disconnected(id, reason));
        }
    }

    fn handle_connected_ping(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        let ping = ConnectedPing::deserialize(buf)?;

        let pong = ConnectedPong {
            ping_timestamp: ping.timestamp,
            timestamp: self.clock.timestamp(now),
        };

        if self.state != RakSessionState::Closing {
            self.send_control(
                Frame::new(RakReliability::Unreliable, pong.serialized()?),
                now,
            )?;
        }
        Ok(())
    }

    fn handle_connected_pong(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        let pong = ConnectedPong::deserialize(buf)?;

        if pong.ping_timestamp >= self.clock.timestamp(self.last_ping) {
            self.last_pong = now;
        }
        Ok(())
    }

    fn handle_disconnect(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        Disconnect::deserialize(buf)?;

        debug!("session closed by {}", self.addr);

        self.disconnect(false, RakDisconnectReason::Remote, now)?;
        Ok(())
    }

    fn disconnect(
        &mut self,
        send: bool,
        reason: RakDisconnectReason,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        if matches!(self.state, RakSessionState::Disconnected) {
            return Err(RakSessionError::Closed);
        }

        if send {
            let disconnect = Disconnect;

            let frame = Frame::new(RakReliability::ReliableOrdered, disconnect.serialized()?);

            // Terminal control must leave before the adapter receives Disconnected.
            self.send_control(frame, now)?;
        }

        self.state = RakSessionState::Disconnected;

        self.output
            .push_back(RakSessionOutput::Disconnected(self.id, reason));

        Ok(())
    }
}

/// Computes a relative wait without making a future caller timestamp prematurely due.
fn remaining(since: Instant, interval: Duration, now: Instant) -> Duration {
    match now.checked_duration_since(since) {
        Some(elapsed) => interval.saturating_sub(elapsed),
        None => interval.saturating_add(since.duration_since(now)),
    }
}

fn within_reliable_window(frame: &Frame, oldest: Option<u32>) -> bool {
    match oldest {
        Some(oldest) if frame.reliability.is_reliable() => {
            u24::distance(oldest, frame.reliable_index) < MAX_UNACKED_RELIABLE
        }
        _ => true,
    }
}

/// Keeps new ordered messages inside the peer's window while an earlier message is unacknowledged.
fn within_ordered_window(frame: &Frame, oldest: &[Option<u32>; 32]) -> bool {
    match oldest[frame.order_channel as usize] {
        Some(index) if frame.reliability.is_ordered() => {
            u24::distance(index, frame.order_index) < ORDER_WINDOW as i32
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests;
