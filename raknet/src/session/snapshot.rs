//! Capture and restoration of owned session state.

use super::config::RakSessionConfig;
use super::congestion_controller::{RakCongestionController, RakCongestionSnapshot};
use super::state::RakSessionState;
use super::{RakSession, RakSessionId};
use crate::protocol::codec::RakCodec;
use crate::protocol::packets::frame_set::FrameSet;
use crate::protocol::types::frame::Frame;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::AddrParseError;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// [`RakSession`] in a serializable form, so a session can be moved to another process
/// and resumed there.
///
/// Times are milliseconds from [`RakSessionSnapshot::epoch_ms`], and the queues are
/// `Vec`s, since `SystemTime`, `VecDeque` and `BinaryHeap` have no `Facet` impl. Pending
/// output is not carried - drain it with `poll()` before taking a snapshot.
#[derive(Clone, Debug, facet::Facet)]
pub struct RakSessionSnapshot {
    pub id: u64,
    /// Text form: facet-json cannot construct a `SocketAddr` on the way back in.
    pub addr: String,
    pub state: RakSessionState,
    pub guid: u64,
    pub mtu: u16,
    pub config: RakSessionConfig,

    /// Wall clock the offsets below are measured from.
    pub epoch_ms: u64,
    pub last_tick_ms: u64,
    pub last_ping_ms: u64,
    pub last_recv_ms: u64,
    pub last_pong_ms: u64,

    pub congestion_controller: RakCongestionSnapshot,

    pub queue: Vec<(Box<[u8]>, String)>,

    pub sequences_recv: HashSet<u32>,
    pub sequences_lost: HashSet<u32>,

    pub outbound_seq: u32,
    pub outbound_spl: u16,
    pub outbound_rel: u32,
    pub outbound_queue: Vec<Vec<Frame>>,
    pub outbound_cache: Vec<(u32, FrameSet)>,
    pub outbound_resend: Vec<(u64, u32)>,
    pub outbound_ord_idx: [u32; 32],
    pub outbound_seq_idx: [u32; 32],

    pub inbound_seq: u32,
    pub inbound_rel_seen: HashSet<u32>,
    pub inbound_rel_order: Vec<u32>,
    pub inbound_spl_queue: Vec<(u16, Vec<(u32, Frame)>)>,
    pub inbound_ord_queue: Vec<(u8, Vec<(u32, Frame)>)>,
    pub inbound_ord_idx: [u32; 32],
    pub inbound_seq_idx: [u32; 32],
}

impl RakSession {
    /// Captures this session's protocol state.
    pub fn snapshot(&self) -> RakSessionSnapshot {
        let epoch = UNIX_EPOCH;
        RakSessionSnapshot {
            id: self.id.0,
            addr: self.addr.to_string(),
            state: self.state,
            guid: self.guid,
            mtu: self.mtu,
            config: self.config.clone(),

            epoch_ms: 0,
            last_tick_ms: millis_since(epoch, self.last_tick),
            last_ping_ms: millis_since(epoch, self.last_ping),
            last_recv_ms: millis_since(epoch, self.last_recv),
            last_pong_ms: millis_since(epoch, self.last_pong),

            congestion_controller: self.congestion_controller.snapshot(epoch),

            queue: self
                .queue
                .iter()
                .map(|(bytes, addr)| (bytes.clone(), addr.to_string()))
                .collect(),

            sequences_recv: self.sequences_recv.clone(),
            sequences_lost: self.sequences_lost.clone(),

            outbound_seq: self.outbound_seq,
            outbound_spl: self.outbound_spl,
            outbound_rel: self.outbound_rel,
            outbound_queue: self
                .outbound_queue
                .iter()
                .map(|q| q.iter().cloned().collect())
                .collect(),
            outbound_cache: self.outbound_cache.clone().into_iter().collect(),
            outbound_resend: self
                .outbound_resend
                .iter()
                .map(|(at, seq)| (millis_since(epoch, at.0), *seq))
                .collect(),
            outbound_ord_idx: self.outbound_ord_idx,
            outbound_seq_idx: self.outbound_seq_idx,

            inbound_seq: self.inbound_seq,
            inbound_rel_seen: self.inbound_rel_seen.clone(),
            inbound_rel_order: self.inbound_rel_order.iter().copied().collect(),
            inbound_spl_queue: flatten_nested(&self.inbound_spl_queue),
            inbound_ord_queue: flatten_nested(&self.inbound_ord_queue),
            inbound_ord_idx: self.inbound_ord_idx,
            inbound_seq_idx: self.inbound_seq_idx,
        }
    }

    /// Rebuilds a session from [`RakSession::snapshot`], using caller time for a closing session.
    pub fn restore(snapshot: RakSessionSnapshot, now: SystemTime) -> Result<Self, AddrParseError> {
        let epoch = UNIX_EPOCH + Duration::from_millis(snapshot.epoch_ms);
        let at = |offset: u64| epoch + Duration::from_millis(offset);

        let mut outbound_queue: [VecDeque<Frame>; 4] = Default::default();
        for (channel, frames) in snapshot.outbound_queue.into_iter().enumerate().take(4) {
            outbound_queue[channel] = frames.into();
        }

        let outbound_cache: HashMap<u32, FrameSet> = snapshot.outbound_cache.into_iter().collect();
        let outbound_bytes = outbound_queue
            .iter()
            .flatten()
            .map(Frame::size_hint)
            .chain(outbound_cache.values().map(FrameSet::size_hint))
            .sum();

        let inbound_spl_queue = rebuild_nested(snapshot.inbound_spl_queue);
        let inbound_spl_bytes = payload_bytes(&inbound_spl_queue);
        let inbound_spl_last = inbound_spl_queue
            .keys()
            .map(|&split_id| (split_id, at(snapshot.last_recv_ms)))
            .collect();
        let inbound_ord_queue = rebuild_nested(snapshot.inbound_ord_queue);
        let inbound_ord_bytes = payload_bytes(&inbound_ord_queue);

        let queue = snapshot
            .queue
            .into_iter()
            .map(|(bytes, addr)| Ok((bytes, addr.parse()?)))
            .collect::<Result<VecDeque<_>, AddrParseError>>()?;

        Ok(Self {
            id: RakSessionId(snapshot.id),
            addr: snapshot.addr.parse()?,
            state: snapshot.state,
            guid: snapshot.guid,
            mtu: snapshot.mtu,
            config: snapshot.config,

            last_tick: at(snapshot.last_tick_ms),
            last_ping: at(snapshot.last_ping_ms),
            last_recv: at(snapshot.last_recv_ms),
            last_pong: at(snapshot.last_pong_ms),
            closing_since: (snapshot.state == RakSessionState::Closing).then_some(now),

            congestion_controller: RakCongestionController::restore(
                snapshot.congestion_controller,
                epoch,
            ),
            bandwidth_limited: false,

            queue,

            sequences_recv: snapshot.sequences_recv,
            sequences_lost: snapshot.sequences_lost,

            outbound_seq: snapshot.outbound_seq,
            outbound_spl: snapshot.outbound_spl,
            outbound_rel: snapshot.outbound_rel,
            outbound_queue,
            outbound_cache,
            outbound_bytes,
            outbound_resend: snapshot
                .outbound_resend
                .into_iter()
                .map(|(offset, seq)| (Reverse(at(offset)), seq))
                .collect(),
            outbound_ord_idx: snapshot.outbound_ord_idx,
            outbound_seq_idx: snapshot.outbound_seq_idx,

            inbound_seq: snapshot.inbound_seq,
            inbound_rel_seen: snapshot.inbound_rel_seen,
            inbound_rel_order: snapshot.inbound_rel_order.into(),
            inbound_spl_queue,
            inbound_spl_bytes,
            inbound_spl_last,
            inbound_ord_queue,
            inbound_ord_bytes,
            inbound_ord_idx: snapshot.inbound_ord_idx,
            inbound_seq_idx: snapshot.inbound_seq_idx,

            output: VecDeque::new(),
        })
    }
}

fn payload_bytes<K>(map: &HashMap<K, HashMap<u32, Frame>>) -> usize {
    map.values()
        .flat_map(HashMap::values)
        .map(|f| f.payload.len())
        .sum()
}

fn millis_since(epoch: SystemTime, at: SystemTime) -> u64 {
    at.duration_since(epoch).unwrap_or_default().as_millis() as u64
}

/// JSON object keys are strings, so maps keyed by an integer are carried as pairs.
fn flatten_nested<K: Copy + Eq + std::hash::Hash>(
    map: &HashMap<K, HashMap<u32, Frame>>,
) -> Vec<(K, Vec<(u32, Frame)>)> {
    map.iter()
        .map(|(key, inner)| (*key, inner.clone().into_iter().collect()))
        .collect()
}

fn rebuild_nested<K: Eq + std::hash::Hash>(
    pairs: Vec<(K, Vec<(u32, Frame)>)>,
) -> HashMap<K, HashMap<u32, Frame>> {
    pairs
        .into_iter()
        .map(|(key, inner)| (key, inner.into_iter().collect()))
        .collect()
}
