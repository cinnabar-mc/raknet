// Modified by Cinnabar contributors for caller-driven connection negotiation.
pub mod config;
pub mod congestion_controller;
pub mod error;
pub mod input;
pub mod output;
mod receive;
pub(crate) mod snapshot;
pub mod state;

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
use crate::session::input::RakSessionInput;
use crate::session::output::{RakDisconnectReason, RakSessionOutput};
use crate::types::priority::RakPriority;
use crate::types::reliability::RakReliability;
use crate::util::constants::{DGRAM_HEADER_SIZE, DGRAM_MTU_OVERHEAD, UDP_HEADER_SIZE};
use crate::util::socket_addr::get_overhead;
use crate::util::{flags, packet_id, u24};
use config::RakSessionConfig;
use state::RakSessionState;
use std::cmp::{Reverse, min};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::mem::take;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

const RELIABLE_WINDOW: usize = 8192;
const DATAGRAM_WINDOW: usize = 2048;
const ORDER_WINDOW: usize = 2048;
const MAX_SPLIT_COUNT: u32 = 8192;
const MAX_OUTBOUND_SPLIT_COUNT: usize = 2048;
const MAX_CONCURRENT_SPLITS: usize = 256;
const MAX_UNACKED_RELIABLE: i32 = 4096;
const MAX_SPLITS_IN_FLIGHT: usize = 256;
const SESSION_TIMEOUT: Duration = Duration::from_millis(15000);

#[derive(Clone, Debug)]
pub struct RakSession {
    pub id: RakSessionId,
    pub addr: SocketAddr,
    pub state: RakSessionState,
    pub guid: u64,
    pub mtu: u16,
    config: RakSessionConfig,

    last_tick: SystemTime,
    last_ping: SystemTime,
    last_recv: SystemTime,
    last_pong: SystemTime,
    closing_since: Option<SystemTime>,

    congestion_controller: RakCongestionController,
    bandwidth_limited: bool,

    queue: VecDeque<(Box<[u8]>, SocketAddr)>,

    sequences_recv: HashSet<u32>,
    sequences_lost: HashSet<u32>,

    outbound_seq: u32,
    outbound_spl: u16,
    pub(crate) outbound_rel: u32,
    outbound_queue: [VecDeque<Frame>; 4],
    outbound_cache: HashMap<u32, FrameSet>,
    outbound_bytes: usize,
    outbound_resend: BinaryHeap<(Reverse<SystemTime>, u32)>,
    outbound_ord_idx: [u32; 32],
    outbound_seq_idx: [u32; 32],

    inbound_seq: u32,
    inbound_rel_seen: HashSet<u32>,
    inbound_rel_order: VecDeque<u32>,
    inbound_spl_queue: HashMap<u16, HashMap<u32, Frame>>,
    inbound_spl_bytes: usize,
    inbound_spl_last: HashMap<u16, SystemTime>,
    inbound_ord_queue: HashMap<u8, HashMap<u32, Frame>>,
    inbound_ord_bytes: usize,
    inbound_ord_idx: [u32; 32],
    inbound_seq_idx: [u32; 32],

    output: VecDeque<RakSessionOutput>,
}

impl Sans for RakSession {
    type Input = RakSessionInput;
    type Output = RakSessionOutput;
    type Error = RakSessionError;

    fn handle(&mut self, msg: Self::Input) -> Result<(), Self::Error> {
        if matches!(self.state, RakSessionState::Disconnected) {
            return Err(RakSessionError::Closed);
        }

        match msg {
            RakSessionInput::Datagram(buf, now) => {
                self.last_recv = now;

                let Some(&b) = buf.first() else {
                    return Ok(());
                };

                let mut cursor = Cursor::new(buf.as_ref());
                match b {
                    _ if b & flags::VALID == 0 => debug!(
                        "received unknown online packet {:#04X} from {}",
                        b, self.addr
                    ),
                    _ if b & (flags::ACK | flags::NACK) != 0 => {
                        self.handle_ack(&mut cursor, now)?
                    }
                    _ => self.handle_frame_set(&mut cursor, now)?,
                }
            }
            RakSessionInput::Send(..) if self.state == RakSessionState::Closing => {
                return Err(RakSessionError::Closed);
            }
            RakSessionInput::Send(buf, ..) if buf.len() > self.max_message_len() => {
                return Err(RakSessionError::MessageSizeExceeded(buf.len()));
            }
            RakSessionInput::Send(buf, _, _, now)
                if self.outbound_bytes + buf.len() > self.config.max_outbound_bytes as usize =>
            {
                debug!(
                    "closing session with {}, queued outbound bytes would exceed max_outbound_bytes",
                    self.addr
                );
                self.disconnect(true, RakDisconnectReason::OutboundBytesExceeded, now)?
            }
            RakSessionInput::Send(buf, reliability, priority, now) => {
                self.send_frame(Frame::new(reliability, buf), priority, now)?
            }
            RakSessionInput::Update(now) => self.handle_timeout(now)?,
            RakSessionInput::Disconnect(now) => self.close(now)?,
            RakSessionInput::DisconnectNow(now) => {
                self.send_queue(now)?;
                self.flush();
                self.disconnect(true, RakDisconnectReason::Requested, now)?
            }
        }
        Ok(())
    }

    fn poll(&mut self) -> Option<Self::Output> {
        self.output.pop_front()
    }
}

impl RakSession {
    /// Creates session timers in the same domain clock used by its subsequent inputs.
    pub fn new<F>(
        id: RakSessionId,
        addr: SocketAddr,
        guid: u64,
        mtu: u16,
        now: SystemTime,
        conf: F,
    ) -> Self
    where
        F: FnOnce(&mut RakSessionConfig),
    {
        let mtu = mtu - UDP_HEADER_SIZE - get_overhead(&addr);
        let mut config = RakSessionConfig::default();
        conf(&mut config);

        Self {
            id,
            addr,
            guid,
            mtu,
            config,

            last_tick: now,
            last_ping: now,
            last_recv: now,
            last_pong: now,
            closing_since: None,

            state: RakSessionState::Connected,
            congestion_controller: RakCongestionController::new(mtu as usize),
            bandwidth_limited: false,

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
            inbound_rel_seen: HashSet::new(),
            inbound_rel_order: VecDeque::new(),
            inbound_spl_queue: HashMap::new(),
            inbound_spl_bytes: 0,
            inbound_spl_last: HashMap::new(),
            inbound_ord_queue: HashMap::new(),
            inbound_ord_bytes: 0,
            inbound_ord_idx: [0; 32],
            inbound_seq_idx: [0; 32],

            output: VecDeque::new(),
        }
    }

    pub fn get_addr(self) -> SocketAddr {
        self.addr
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
        let split_limit = MAX_OUTBOUND_SPLIT_COUNT * (self.mtu - DGRAM_MTU_OVERHEAD) as usize;
        usize::try_from(self.config.max_queued_bytes)
            .unwrap_or(0)
            .min(split_limit)
            .max(self.mtu as usize)
    }

    fn handle_timeout(&mut self, now: SystemTime) -> Result<(), RakSessionError> {
        if now >= self.last_recv + SESSION_TIMEOUT {
            debug!(
                "detected stale connection from {}, disconnecting...",
                self.addr
            );

            self.disconnect(true, RakDisconnectReason::Timeout, now)?;
            return Ok(());
        }

        self.evict_stale_splits(now);

        if self.state == RakSessionState::Closing {
            return self.update_closing(now);
        }

        if self.config.autoflush && now >= self.last_tick + self.config.autoflush_interval_ms {
            self.tick(now)?;

            self.last_tick = now;
        }

        if now >= self.last_ping + Duration::from_millis(2000) {
            self.send_connected_ping(now, RakReliability::Unreliable)?;

            self.last_ping = now;
        }

        let mut next = min(
            self.last_ping + Duration::from_millis(2000),
            self.last_recv + SESSION_TIMEOUT,
        );
        if self.config.autoflush {
            next = min(next, self.last_tick + self.config.autoflush_interval_ms);
        }

        let duration = next.duration_since(now).unwrap_or(Duration::from_secs(0));

        self.output.push_back(RakSessionOutput::Wait(duration));
        Ok(())
    }

    fn close(&mut self, now: SystemTime) -> Result<(), RakSessionError> {
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
            .push_back(RakSessionOutput::Wait(self.config.autoflush_interval_ms));
        Ok(())
    }

    fn nothing_left_to_send(&self) -> bool {
        self.queue.is_empty()
            && self.outbound_cache.is_empty()
            && self.outbound_queue.iter().all(VecDeque::is_empty)
    }

    fn update_closing(&mut self, now: SystemTime) -> Result<(), RakSessionError> {
        let deadline = self.closing_since.unwrap_or(now) + self.config.close_timeout;

        self.tick(now)?;
        self.last_tick = now;

        if self.nothing_left_to_send() || now >= deadline {
            return self.disconnect(true, RakDisconnectReason::Requested, now);
        }

        let next = min(
            min(deadline, self.last_recv + SESSION_TIMEOUT),
            now + self.config.autoflush_interval_ms,
        );
        let duration = next.duration_since(now).unwrap_or(Duration::ZERO);

        self.output.push_back(RakSessionOutput::Wait(duration));
        Ok(())
    }

    pub fn tick(&mut self, now: SystemTime) -> Result<(), RakSessionError> {
        if matches!(self.state, RakSessionState::Disconnected) {
            return Ok(());
        }

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

    fn send_stale(&mut self, now: SystemTime) -> Result<(), RakSessionError> {
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

            let size = set.size_hint();
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

    fn send_queue(&mut self, now: SystemTime) -> Result<(), RakSessionError> {
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
                let continuous_send = batch.iter().any(Frame::is_split);

                sets.push(FrameSet {
                    sequence: self.outbound_seq,
                    frames: take(&mut batch),
                    continuous_send,
                    needs_b_and_as: true,
                    is_pair: false,
                });
                self.outbound_seq = u24::add(self.outbound_seq, 1);

                size = DGRAM_HEADER_SIZE as usize;
            }

            size += frame_size;
            batch.push(frame);
        }

        if !batch.is_empty() {
            let continuous_send = batch.iter().any(Frame::is_split);

            sets.push(FrameSet {
                sequence: self.outbound_seq,
                frames: batch,
                continuous_send,
                needs_b_and_as: true,
                is_pair: false,
            });
            self.outbound_seq = u24::add(self.outbound_seq, 1);
        }

        sets
    }

    fn send_frame_set(
        &mut self,
        frameset: FrameSet,
        immediate: bool,
        first_send: bool,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        let buf = frameset.serialized()?;

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
                self.congestion_controller
                    .sent(frameset.sequence, frameset.size_hint(), now);
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

    fn send_frame(
        &mut self,
        frame: Frame,
        priority: RakPriority,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
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
        let frames = payloads
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
            .collect();

        self.queue_frames(frames, priority, now)?;
        Ok(())
    }

    fn queue_frames(
        &mut self,
        frames: Vec<Frame>,
        priority: RakPriority,
        now: SystemTime,
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

    fn handle_ack(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        let ack = Ack::deserialize(buf)?;

        for seq in ack.sequences {
            let Some(set) = self.outbound_cache.remove(&seq) else {
                continue;
            };
            self.outbound_bytes = self.outbound_bytes.saturating_sub(set.size_hint());
            match ack.is_nack {
                true => {
                    self.congestion_controller
                        .lost(set.sequence, set.size_hint());
                    self.queue_frames(set.frames, RakPriority::Immediate, now)?;
                    self.congestion_controller.nacked(self.bandwidth_limited);
                }
                false => {
                    self.congestion_controller.acked(
                        now,
                        set.sequence,
                        set.size_hint(),
                        self.bandwidth_limited,
                    );
                }
            }
        }
        Ok(())
    }

    fn handle_packet(&mut self, buf: Box<[u8]>, now: SystemTime) -> Result<(), RakSessionError> {
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
        now: SystemTime,
    ) -> Result<bool, RakSessionError> {
        let Some(&id) = buf.first() else {
            return Ok(false);
        };
        let mut cursor = Cursor::new(buf);
        match id {
            packet_id::CONNECTED_PING => self.handle_connected_ping(&mut cursor, now)?,
            packet_id::CONNECTED_PONG => self.handle_connected_pong(&mut cursor, now)?,
            packet_id::DETECT_LOST_CONNECTIONS if self.state == RakSessionState::Closing => {}
            packet_id::DETECT_LOST_CONNECTIONS => {
                self.send_connected_ping(now, RakReliability::ReliableOrdered)?
            }
            packet_id::DISCONNECT => self.handle_disconnect(&mut cursor, now)?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Emits a liveness ping using the caller's time and selected control reliability.
    fn send_connected_ping(
        &mut self,
        now: SystemTime,
        reliability: RakReliability,
    ) -> Result<(), RakSessionError> {
        let ping = ConnectedPing {
            timestamp: now.duration_since(UNIX_EPOCH)?.as_millis() as u64,
        };
        self.handle(RakSessionInput::Send(
            ping.serialized()?,
            reliability,
            RakPriority::Immediate,
            now,
        ))
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
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        let ping = ConnectedPing::deserialize(buf)?;

        let pong = ConnectedPong {
            ping_timestamp: ping.timestamp,
            timestamp: now.duration_since(UNIX_EPOCH)?.as_millis() as u64,
        };

        let reliability = RakReliability::Unreliable;
        let priority = RakPriority::Immediate;
        _ = self.handle(RakSessionInput::Send(
            pong.serialized()?,
            reliability,
            priority,
            now,
        ));
        Ok(())
    }

    fn handle_connected_pong(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        let pong = ConnectedPong::deserialize(buf)?;

        if UNIX_EPOCH + Duration::from_millis(pong.ping_timestamp) >= self.last_ping {
            self.last_pong = now;
        }
        Ok(())
    }

    fn handle_disconnect(
        &mut self,
        buf: &mut Cursor<&[u8]>,
        now: SystemTime,
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
        now: SystemTime,
    ) -> Result<(), RakSessionError> {
        if matches!(self.state, RakSessionState::Disconnected) {
            return Err(RakSessionError::Closed);
        }

        if send {
            let disconnect = Disconnect;

            let frame = Frame::new(RakReliability::ReliableOrdered, disconnect.serialized()?);

            self.send_frame(frame, RakPriority::Immediate, now)?;
        }

        self.state = RakSessionState::Disconnected;

        self.output
            .push_back(RakSessionOutput::Disconnected(self.id, reason));

        Ok(())
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
