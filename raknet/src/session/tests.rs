use super::*;

/// Uses one explicit domain clock for session construction and protocol events.
fn domain_time() -> Instant {
    crate::clock::test_now() + Duration::from_secs(1000)
}
use crate::protocol::packets::ack::MAX_ACK_ENTRIES;

#[test]
fn transferred_snapshot_preserves_wire_uptime_and_inflight_timers() {
    let origin = crate::clock::test_now();
    let start = origin + Duration::from_secs(5);
    let mut session = RakSession::new(
        RakSessionId(42),
        "127.0.0.1:19132".parse().unwrap(),
        7,
        1492,
        start,
        RakClock::new(origin),
        Default::default(),
    );
    send(&mut session, 4, start).unwrap();
    session.tick(start).unwrap();
    drain_outputs(&mut session);
    let capture = start + Duration::from_millis(40);
    let json = facet_json::to_string(&session.snapshot(capture)).unwrap();
    let destination = origin + Duration::from_secs(3600);
    let mut resumed = RakSession::restore(
        facet_json::from_str(&json).unwrap(),
        destination,
        Duration::from_millis(60),
    )
    .unwrap();
    assert_eq!(resumed.clock.timestamp(destination), 5100);
    assert_eq!(
        destination.duration_since(resumed.last_recv),
        Duration::from_millis(100)
    );
    let due = resumed.outbound_resend.peek().unwrap().0.0;
    assert_eq!(due.duration_since(destination), Duration::from_millis(1900));
    resumed.tick(due - Duration::from_millis(1)).unwrap();
    assert!(
        drain_outputs(&mut resumed).is_empty(),
        "resend must retain its remaining deadline"
    );
    resumed.tick(due).unwrap();
    assert_eq!(sequences_of(&drain_outputs(&mut resumed)).len(), 1);

    let mut acknowledged = RakSession::restore(
        facet_json::from_str(&json).unwrap(),
        destination,
        Duration::from_millis(60),
    )
    .unwrap();
    let ack = Ack::new(vec![0], false);
    acknowledged
        .handle(RakSessionInput::Datagram(
            ack.serialized().unwrap(),
            destination,
        ))
        .unwrap();
    assert_eq!(acknowledged.rtt(), Duration::from_millis(100));
    assert_eq!(acknowledged.outbound_bytes(), 0);

    let snapshot = session.snapshot(capture);
    assert!(matches!(
        RakSession::restore(snapshot, destination, Duration::MAX),
        Err(snapshot::RakSessionRestoreError::TimeOutOfRange)
    ));
}

#[test]
fn a_snapshot_round_trips_through_json_and_resumes_where_it_left_off() {
    let mut session = RakSession::new(
        RakSessionId(42),
        "127.0.0.1:19132".parse().unwrap(),
        0xDEAD_BEEF,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    let now = domain_time();
    session
        .handle(RakSessionInput::Send(
            Box::from(*b"hello"),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))
        .unwrap();
    while session.poll().is_some() {}

    let before_seq = session.outbound_rel;
    assert!(
        before_seq > 0,
        "sending a reliable frame should advance outbound_rel"
    );

    let json =
        facet_json::to_string(&session.snapshot(domain_time())).expect("snapshot must serialize");
    let mut resumed = RakSession::restore(
        facet_json::from_str(&json).expect("snapshot must deserialize"),
        domain_time(),
        Duration::ZERO,
    )
    .expect("a snapshot's own address must parse");

    assert_eq!(resumed.id, session.id);
    assert_eq!(resumed.addr, session.addr);
    assert_eq!(resumed.guid, session.guid);
    assert_eq!(resumed.outbound_rel, before_seq);

    resumed
        .handle(RakSessionInput::Send(
            Box::from(*b"world"),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))
        .unwrap();
    assert_eq!(resumed.outbound_rel, before_seq + 1);
}

/// JSON has no infinity, and an unmeasured RTT is infinite - it must not come back
/// as zero.
#[test]
fn an_unmeasured_rtt_survives_a_json_round_trip() {
    let session = RakSession::new(
        RakSessionId(1),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    let json = facet_json::to_string(&session.snapshot(domain_time())).unwrap();
    let resumed = RakSession::restore(
        facet_json::from_str(&json).unwrap(),
        domain_time(),
        Duration::ZERO,
    )
    .unwrap();

    assert_eq!(
        resumed.congestion_controller.retransmission_timeout(),
        session.congestion_controller.retransmission_timeout()
    );
}

#[test]
fn out_of_range_order_channel_does_not_panic() {
    let mut session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    let mut frame = Frame::new(RakReliability::ReliableOrdered, Box::new([]));
    frame.order_channel = u8::MAX;

    session.handle_frame(frame, domain_time()).unwrap();
}

#[test]
fn max_message_len_is_the_outbound_split_limit() {
    let session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    assert_eq!(
        session.max_message_len(),
        MAX_OUTBOUND_SPLIT_COUNT * (session.mtu - DGRAM_MTU_OVERHEAD) as usize
    );
}

#[test]
fn max_message_len_is_at_least_one_unsplit_frame() {
    let session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        RakSessionConfig {
            max_queued_bytes: 4,
            ..Default::default()
        },
    );

    assert_eq!(session.max_message_len(), session.mtu as usize);
}

#[test]
fn honors_configured_ordering_channels() {
    let mut session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        RakSessionConfig {
            ordering_channels: 4,
            ..Default::default()
        },
    );

    let mut frame = Frame::new(RakReliability::ReliableOrdered, Box::new([1]));
    frame.order_channel = 4;

    session.handle_frame(frame, domain_time()).unwrap();

    assert_eq!(session.inbound_ord_idx[4], 0);
}

#[test]
fn priority_routes_to_separate_queues() {
    let mut session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    let now = domain_time();
    session
        .send_frame(
            Frame::new(RakReliability::Unreliable, Box::new([1])),
            RakPriority::Low,
            now,
        )
        .unwrap();
    session
        .send_frame(
            Frame::new(RakReliability::Unreliable, Box::new([2])),
            RakPriority::High,
            now,
        )
        .unwrap();

    assert_eq!(session.outbound_queue[RakPriority::High as usize].len(), 1);
    assert_eq!(session.outbound_queue[RakPriority::Low as usize].len(), 1);
}

#[test]
fn rejects_out_of_range_split_index() {
    let mut session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    let mut frame = Frame::new(RakReliability::Reliable, Box::new([1, 2, 3]));
    frame.split_size = 2;
    frame.split_index = 5;
    frame.split_id = 1;

    session.handle_frame(frame, domain_time()).unwrap();

    assert!(session.inbound_spl_queue.is_empty());
}

#[test]
fn drops_split_frame_exceeding_max_queued_bytes() {
    let mut session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        RakSessionConfig {
            max_queued_bytes: 4,
            ..Default::default()
        },
    );

    let mut frame = Frame::new(RakReliability::Reliable, vec![0u8; 8].into_boxed_slice());
    frame.split_size = 2;
    frame.split_index = 0;
    frame.split_id = 1;

    session.handle_frame(frame, domain_time()).unwrap();

    assert!(session.inbound_spl_queue.is_empty());
}

#[test]
fn disconnect_reason_matches_cause() {
    let mut session = RakSession::new(
        RakSessionId(0),
        "127.0.0.1:19132".parse().unwrap(),
        0,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    );

    session
        .handle(RakSessionInput::Disconnect(domain_time()))
        .unwrap();

    let reason = session
        .output
        .iter()
        .find_map(|out| match out {
            RakSessionOutput::Disconnected(_, reason) => Some(*reason),
            _ => None,
        })
        .unwrap();

    assert_eq!(reason, RakDisconnectReason::Requested);
}

fn session(id: u64, port: u16) -> RakSession {
    RakSession::new(
        RakSessionId(id),
        SocketAddr::from(([127, 0, 0, 1], port)),
        id,
        crate::util::constants::MAX_MTU_SIZE,
        domain_time(),
        crate::clock::RakClock::new(crate::clock::test_now()),
        Default::default(),
    )
}

fn deliver(from: &mut RakSession, to: &mut RakSession, now: Instant) {
    let datagrams: Vec<Box<[u8]>> = std::iter::from_fn(|| from.poll())
        .filter_map(|out| match out {
            RakSessionOutput::Datagram(buf, _) => Some(buf),
            _ => None,
        })
        .collect();
    for buf in datagrams {
        to.handle(RakSessionInput::Datagram(buf, now)).unwrap();
    }
}

fn packets(session: &mut RakSession) -> Vec<Box<[u8]>> {
    std::iter::from_fn(|| session.poll())
        .filter_map(|out| match out {
            RakSessionOutput::Packet(buf) => Some(buf),
            _ => None,
        })
        .collect()
}

fn datagram(sequence: u32, frames: Vec<Frame>) -> RakSessionInput {
    let set = FrameSet::new(sequence, frames, false, true, false);

    RakSessionInput::Datagram(set.serialized().unwrap(), domain_time())
}

fn unreliable(byte: u8) -> Vec<Frame> {
    vec![Frame::new(
        RakReliability::Unreliable,
        Box::new([0xFE, byte]),
    )]
}

#[test]
fn datagram_gap_within_window_is_nacked() {
    let mut session = session(0, 1);

    session.handle(datagram(0, unreliable(0))).unwrap();
    session.handle(datagram(3, unreliable(3))).unwrap();

    assert_eq!(session.sequences_lost, HashSet::from([1, 2]));
    assert_eq!(session.inbound_seq, 4);
}

#[test]
fn datagram_beyond_window_is_dropped() {
    let mut session = session(0, 1);

    session
        .handle(datagram(DATAGRAM_WINDOW as u32 * 50, unreliable(0)))
        .unwrap();

    assert!(
        session.sequences_lost.is_empty(),
        "a datagram far ahead must not mark {} sequences lost",
        session.sequences_lost.len()
    );
    assert_eq!(session.inbound_seq, 0);
    assert!(packets(&mut session).is_empty());
}

#[test]
fn late_datagram_does_not_move_inbound_seq_backwards() {
    let mut session = session(0, 1);

    session.handle(datagram(0, unreliable(0))).unwrap();
    session.handle(datagram(5, unreliable(5))).unwrap();
    session.handle(datagram(3, unreliable(3))).unwrap();

    assert_eq!(session.inbound_seq, 6);
    assert_eq!(session.sequences_lost, HashSet::from([1, 2, 4]));
    assert_eq!(packets(&mut session).len(), 3);
}

#[test]
fn lost_sequences_stay_bounded_under_repeated_gaps() {
    let mut session = session(0, 1);

    let step = DATAGRAM_WINDOW as u32 - 1;
    for i in 1..=16 {
        session.handle(datagram(i * step, unreliable(0))).unwrap();
    }

    assert!(session.sequences_lost.len() <= DATAGRAM_WINDOW);
}

fn ordered(order_index: u32) -> Frame {
    let mut frame = Frame::new(RakReliability::ReliableOrdered, Box::new([0xFE]));
    frame.order_index = order_index;
    frame
}

fn disconnect_reason(session: &RakSession) -> Option<RakDisconnectReason> {
    session.output.iter().find_map(|out| match out {
        RakSessionOutput::Disconnected(_, reason) => Some(*reason),
        _ => None,
    })
}

#[test]
fn ordered_frame_within_window_is_queued() {
    let mut session = session(0, 1);

    session
        .handle_frame(ordered(ORDER_WINDOW as u32 - 1), domain_time())
        .unwrap();

    assert_eq!(session.inbound_ord_queue[&0].len(), 1);
    assert_eq!(session.state, RakSessionState::Connected);
}

#[test]
fn ordered_frame_beyond_window_closes_the_session() {
    let mut session = session(0, 1);

    session
        .handle_frame(ordered(ORDER_WINDOW as u32), domain_time())
        .unwrap();

    assert!(
        session.inbound_ord_queue.values().all(HashMap::is_empty),
        "a frame {ORDER_WINDOW} ahead must not be buffered"
    );
    assert_eq!(session.state, RakSessionState::Disconnected);
    assert_eq!(
        disconnect_reason(&session),
        Some(RakDisconnectReason::ProtocolViolation)
    );
}

fn sequenced(sequence_index: u32, payload: &[u8]) -> Frame {
    let mut frame = Frame::new(RakReliability::UnreliableSequenced, payload.into());
    frame.sequence_index = sequence_index;
    frame
}

#[test]
fn stale_sequenced_frame_is_dropped() {
    let mut session = session(0, 1);
    let now = domain_time();

    session.handle_frame(sequenced(5, &[0xFE, 1]), now).unwrap();
    session.handle_frame(sequenced(3, &[0xFE, 2]), now).unwrap();
    session.handle_frame(sequenced(6, &[0xFE, 3]), now).unwrap();

    let delivered = packets(&mut session);
    assert_eq!(
        delivered,
        vec![Box::<[u8]>::from([0xFE, 1]), Box::<[u8]>::from([0xFE, 3])],
        "a sequenced frame older than the last delivered one must be dropped"
    );
}

fn fragment(split_id: u16, split_size: u32, split_index: u32, payload: &[u8]) -> Frame {
    let mut frame = Frame::new(RakReliability::Unreliable, payload.into());
    frame.split_id = split_id;
    frame.split_size = split_size;
    frame.split_index = split_index;
    frame
}

fn assert_closed_for_violation(session: &RakSession) {
    assert_eq!(session.state, RakSessionState::Disconnected);
    assert_eq!(
        disconnect_reason(session),
        Some(RakDisconnectReason::ProtocolViolation)
    );
}

#[test]
fn split_count_above_limit_closes_the_session() {
    let mut session = session(0, 1);

    session
        .handle_frame(fragment(1, MAX_SPLIT_COUNT + 1, 0, &[0xFE]), domain_time())
        .unwrap();

    assert!(session.inbound_spl_queue.is_empty());
    assert_closed_for_violation(&session);
}

#[test]
fn concurrent_splits_above_limit_are_dropped() {
    let mut session = session(0, 1);

    let now = domain_time();
    for split_id in 0..=MAX_CONCURRENT_SPLITS as u16 {
        session
            .handle_frame(fragment(split_id, 2, 0, &[0xFE]), now)
            .unwrap();
    }

    assert_eq!(session.inbound_spl_queue.len(), MAX_CONCURRENT_SPLITS);
    assert!(
        !session
            .inbound_spl_queue
            .contains_key(&(MAX_CONCURRENT_SPLITS as u16))
    );
    assert_eq!(
        session.inbound_spl_bytes,
        MAX_CONCURRENT_SPLITS * (size_of::<Frame>() + 1)
    );
    assert_eq!(disconnect_reason(&session), None);
}

#[test]
fn a_hundred_interleaved_splits_are_reassembled() {
    let mut session = session(0, 1);

    let now = domain_time();
    for split_index in 0..2 {
        for split_id in 0..100 {
            session
                .handle_frame(fragment(split_id, 2, split_index, &[0xFE]), now)
                .unwrap();
        }
    }

    assert_eq!(packets(&mut session).len(), 100);
    assert_eq!(disconnect_reason(&session), None);
}

#[test]
fn empty_split_fragment_closes_the_session() {
    let mut session = session(0, 1);

    session
        .handle_frame(fragment(1, 2, 0, &[]), domain_time())
        .unwrap();

    assert!(session.inbound_spl_queue.is_empty());
    assert_closed_for_violation(&session);
}

#[test]
fn split_size_mismatch_closes_the_session() {
    let mut session = session(0, 1);

    let now = domain_time();
    session
        .handle_frame(fragment(1, 3, 0, &[0xFE]), now)
        .unwrap();
    session
        .handle_frame(fragment(1, 2, 1, &[0xFF]), now)
        .unwrap();

    assert_closed_for_violation(&session);
}

#[test]
fn completed_split_is_delivered_and_releases_its_byte_budget() {
    let mut session = session(0, 1);
    session.config.max_queued_bytes = (2 * size_of::<Frame>() + 4) as i32;

    let now = domain_time();
    session
        .handle_frame(fragment(1, 2, 0, &[0xFE, 1]), now)
        .unwrap();
    session
        .handle_frame(fragment(1, 2, 1, &[2, 3]), now)
        .unwrap();
    session
        .handle_frame(fragment(2, 2, 0, &[0xFE, 4, 5, 6]), now)
        .unwrap();

    assert_eq!(packets(&mut session), vec![Box::from([0xFE, 1, 2, 3])]);
    assert_eq!(session.inbound_spl_queue[&2].len(), 1);
}

#[test]
fn completing_a_split_reclaims_its_fragment_storage() {
    let mut receiver = session(0, 1);
    receiver.config.max_queued_bytes = (2 * size_of::<Frame>() + 3) as i32;
    let now = domain_time();
    receiver
        .handle_frame(fragment(1, 2, 0, &[0xfe]), now)
        .unwrap();
    receiver
        .handle_frame(fragment(2, 2, 0, &[0xfe]), now)
        .unwrap();
    receiver.handle_frame(fragment(1, 2, 1, &[1]), now).unwrap();
    assert_eq!(receiver.state, RakSessionState::Connected);
    assert_eq!(packets(&mut receiver), vec![Box::<[u8]>::from([0xfe, 1])]);
    assert_eq!(receiver.inbound_spl_queue[&2].len(), 1);
    receiver
        .handle_frame(fragment(2, 2, 0, &[0xfe]), now)
        .unwrap();
    assert_eq!(receiver.state, RakSessionState::Connected);
}

#[test]
fn counters_wrap_at_24_bits_in_both_directions() {
    let near_top = u24::MASK - 1;

    let mut sender = session(1, 1);
    sender.outbound_seq = near_top;
    sender.outbound_rel = near_top;
    sender.outbound_ord_idx[0] = near_top;

    let mut receiver = session(2, 2);
    receiver.inbound_seq = near_top;
    receiver.inbound_rel_base = near_top;
    receiver.inbound_ord_idx[0] = near_top;

    let now = domain_time();
    for i in 0..4u8 {
        sender
            .handle(RakSessionInput::Send(
                Box::new([0xFE, i]),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                now,
            ))
            .unwrap();
        deliver(&mut sender, &mut receiver, now);
    }

    let received = packets(&mut receiver);
    let expected: Vec<Box<[u8]>> = (0..4u8).map(|i| Box::from([0xFE, i])).collect();
    assert_eq!(
        received, expected,
        "every packet across the wrap must arrive in order"
    );
    assert_eq!(sender.outbound_seq, 2);
    assert_eq!(sender.outbound_rel, 2);
    assert_eq!(sender.outbound_ord_idx[0], 2);
    assert_eq!(receiver.inbound_ord_idx[0], 2);
}

fn outbound_acks(session: &mut RakSession) -> Vec<Box<[u8]>> {
    std::iter::from_fn(|| session.poll())
        .filter_map(|out| match out {
            RakSessionOutput::Datagram(buf, _) if buf[0] & (flags::ACK | flags::NACK) != 0 => {
                Some(buf)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn outbound_acks_fit_the_mtu() {
    let mut session = session(0, 1);

    let received: Vec<u32> = (0..DATAGRAM_WINDOW as u32).map(|i| i * 2).collect();
    for &sequence in &received {
        session.handle(datagram(sequence, unreliable(0))).unwrap();
    }
    session.tick(domain_time()).unwrap();

    let mut acked = Vec::new();
    let mut nacked = Vec::new();
    for buf in outbound_acks(&mut session) {
        assert!(
            buf.len() <= session.mtu as usize,
            "an ACK of {} bytes exceeds the {} byte MTU",
            buf.len(),
            session.mtu
        );
        let ack = Ack::deserialize(&mut buf.as_ref()).unwrap();
        match ack.is_nack {
            true => nacked.extend(ack.sequences),
            false => acked.extend(ack.sequences),
        }
    }
    acked.sort_unstable();
    nacked.sort_unstable();

    assert_eq!(acked, received);
    assert_eq!(nacked.len(), DATAGRAM_WINDOW - 1);
}

#[test]
fn outbound_acks_respect_the_entry_cap() {
    let mut session = session(0, 1);

    let received = MAX_ACK_ENTRIES as u32 * 2 + 1;
    for sequence in 0..received {
        session.handle(datagram(sequence, unreliable(0))).unwrap();
    }
    session.tick(domain_time()).unwrap();

    let mut acked = Vec::new();
    for buf in outbound_acks(&mut session) {
        let ack = Ack::deserialize(&mut buf.as_ref());
        assert!(
            ack.is_ok(),
            "every outbound ACK must decode under the entry cap, got {ack:?}"
        );
        acked.extend(ack.unwrap().sequences);
    }
    acked.sort_unstable();

    assert_eq!(acked, (0..received).collect::<Vec<_>>());
}

fn ordered_payload(order_index: u32, payload: &[u8]) -> Frame {
    let mut frame = Frame::new(RakReliability::ReliableOrdered, payload.into());
    frame.order_index = order_index;
    frame.reliable_index = order_index;
    frame
}

#[test]
fn deferred_reliable_datagrams_are_retried_after_the_receive_budget_frees() {
    let mut receiver = session(0, 1);
    receiver.config.max_queued_bytes = (size_of::<Frame>() + 2 + 2 * size_of::<u32>()) as i32;
    let now = domain_time();
    receiver
        .handle(datagram_at(0, vec![ordered_payload(1, &[0xfe, 1])], now))
        .unwrap();
    receiver
        .handle(datagram_at(1, vec![ordered_payload(2, &[0xfe, 2])], now))
        .unwrap();
    receiver.tick(now).unwrap();
    let acknowledgments = outbound_acks(&mut receiver)
        .into_iter()
        .map(|bytes| Ack::deserialize(&mut bytes.as_ref()).unwrap())
        .filter(|ack| !ack.is_nack)
        .flat_map(|ack| ack.sequences)
        .collect::<Vec<_>>();
    assert!(acknowledgments.contains(&0));
    assert!(
        !acknowledgments.contains(&1),
        "unretained reliable data must not be acknowledged"
    );
    let saved = receiver.snapshot(now);
    let mut receiver = RakSession::restore(saved, now, Duration::ZERO).unwrap();
    receiver
        .handle(datagram_at(2, vec![ordered_payload(0, &[0xfe, 0])], now))
        .unwrap();
    receiver
        .handle(datagram_at(1, vec![ordered_payload(2, &[0xfe, 2])], now))
        .unwrap();
    assert_eq!(
        packets(&mut receiver),
        vec![
            vec![0xfe, 0].into_boxed_slice(),
            vec![0xfe, 1].into_boxed_slice(),
            vec![0xfe, 2].into_boxed_slice()
        ]
    );
    assert_eq!(receiver.state, RakSessionState::Connected);
}

#[test]
fn reliable_duplicates_remain_harmless_under_receive_pressure() {
    let mut receiver = session(0, 1);
    receiver.config.max_queued_bytes = (size_of::<Frame>() + 2 + 2 * size_of::<u32>()) as i32;
    let now = domain_time();
    receiver
        .handle(datagram_at(0, vec![ordered_payload(1, &[0xfe, 1])], now))
        .unwrap();
    receiver
        .handle(datagram_at(1, vec![ordered_payload(1, &[0xfe, 1])], now))
        .unwrap();
    assert_eq!(receiver.state, RakSessionState::Connected);
    receiver
        .handle(datagram_at(2, vec![ordered_payload(0, &[0xfe, 0])], now))
        .unwrap();
    assert_eq!(
        packets(&mut receiver),
        vec![Box::<[u8]>::from([0xfe, 0]), Box::from([0xfe, 1])]
    );
}

#[test]
fn control_decode_errors_cancel_acknowledgments_under_receive_pressure() {
    let mut receiver = session(0, 1);
    receiver.config.max_queued_bytes = (size_of::<Frame>() + 2 + 2 * size_of::<u32>()) as i32;
    let now = domain_time();
    let frames = vec![
        ordered_payload(1, &[0xfe, 1]),
        ordered_payload(2, &[0xfe, 2]),
        Frame::new(
            RakReliability::Unreliable,
            Box::new([packet_id::CONNECTED_PING]),
        ),
    ];
    assert!(receiver.handle(datagram_at(0, frames, now)).is_err());
    receiver.tick(now).unwrap();
    assert!(
        outbound_acks(&mut receiver)
            .into_iter()
            .map(|bytes| Ack::deserialize(&mut bytes.as_ref()).unwrap())
            .filter(|ack| !ack.is_nack)
            .all(|ack| !ack.sequences.contains(&0))
    );
}

#[test]
fn immediate_reliable_sends_obey_the_outstanding_window() {
    let mut sender = session(0, 1);
    let now = domain_time();
    for _ in 0..MAX_UNACKED_RELIABLE + 20 {
        sender
            .handle(RakSessionInput::Send(
                Box::new([0xfe]),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                now,
            ))
            .unwrap();
    }
    let oldest = sender.oldest_unacked_reliable().unwrap();
    assert!(
        sender
            .outbound_cache
            .values()
            .flat_map(|set| &set.frames)
            .all(|frame| u24::distance(oldest, frame.reliable_index) < MAX_UNACKED_RELIABLE)
    );
    assert!(!sender.outbound_queue[RakPriority::Immediate as usize].is_empty());
}

#[test]
fn terminal_notification_precedes_stopping_an_exhausted_sender() {
    for immediate in [false, true] {
        let mut sender = session(0, 1);
        let now = domain_time();
        for _ in 0..MAX_UNACKED_RELIABLE + 20 {
            sender
                .handle(RakSessionInput::Send(
                    Box::new([0xfe]),
                    RakReliability::ReliableOrdered,
                    RakPriority::Immediate,
                    now,
                ))
                .unwrap();
        }
        assert!(!sender.outbound_queue[RakPriority::Immediate as usize].is_empty());
        drain_outputs(&mut sender);
        let ping = ConnectedPing { timestamp: 42 }.serialized().unwrap();
        sender
            .handle(datagram_at(
                0,
                vec![Frame::new(RakReliability::Unreliable, ping)],
                now,
            ))
            .unwrap();
        let replies = payloads_of(&drain_outputs(&mut sender));
        assert!(
            replies
                .iter()
                .any(|payload| payload.first() == Some(&packet_id::CONNECTED_PONG)),
            "protocol replies must progress while application sends are blocked"
        );
        if immediate {
            sender.handle(RakSessionInput::DisconnectNow(now)).unwrap();
        } else {
            sender.handle(RakSessionInput::Disconnect(now)).unwrap();
            sender
                .handle(RakSessionInput::Update(
                    now + sender.config.close_timeout + Duration::from_millis(1),
                ))
                .unwrap();
        }
        let outputs = drain_outputs(&mut sender);
        assert!(
            payloads_of(&outputs)
                .iter()
                .any(|payload| payload.as_ref() == [packet_id::DISCONNECT]),
            "termination must emit its notification even when application sends are blocked"
        );
        assert!(matches!(
            outputs.last(),
            Some(RakSessionOutput::Disconnected(
                _,
                RakDisconnectReason::Requested
            ))
        ));
    }
}

#[test]
fn receive_gap_and_split_deadlines_survive_snapshot_transfer() {
    for split in [false, true] {
        let mut receiver = session(0, 1);
        let start = domain_time();
        let frame = if split {
            let mut frame = fragment(1, 2, 0, &[0xfe]);
            frame.reliability = RakReliability::Reliable;
            frame
        } else {
            ordered_payload(1, &[0xfe])
        };
        receiver.handle(datagram_at(0, vec![frame], start)).unwrap();
        let capture = start + SESSION_TIMEOUT / 2;
        receiver
            .handle(datagram_at(1, unreliable(0), capture))
            .unwrap();
        drain_outputs(&mut receiver);
        let json = facet_json::to_string(&receiver.snapshot(capture)).unwrap();
        let destination = crate::clock::test_now() + Duration::from_secs(3600);
        let mut receiver = RakSession::restore(
            facet_json::from_str(&json).unwrap(),
            destination,
            SESSION_TIMEOUT / 2 + Duration::from_millis(1),
        )
        .unwrap();
        receiver
            .handle(RakSessionInput::Update(destination))
            .unwrap();
        assert_eq!(
            disconnect_reason(&receiver),
            Some(RakDisconnectReason::Timeout)
        );
    }
}

#[test]
fn ordered_frames_beyond_max_queued_bytes_are_deferred() {
    let mut session = session(0, 1);
    session.config.max_queued_bytes = 4;

    session
        .handle_frame(ordered_payload(1, &[0xFE, 1, 2, 3, 4]), domain_time())
        .unwrap();

    assert!(session.inbound_ord_queue.values().all(HashMap::is_empty));
    assert_eq!(session.state, RakSessionState::Connected);
}

#[test]
fn ordered_queue_shares_the_byte_budget_with_split_reassembly() {
    let mut session = session(0, 1);
    session.config.max_queued_bytes = (size_of::<Frame>() + 3) as i32;

    let now = domain_time();
    session
        .handle_frame(fragment(1, 2, 0, &[0xFE, 1, 2]), now)
        .unwrap();
    session
        .handle_frame(ordered_payload(1, &[0xFE, 1]), now)
        .unwrap();

    assert_eq!(session.state, RakSessionState::Connected);
    assert_eq!(session.inbound_spl_queue[&1].len(), 1);
    assert!(session.inbound_ord_queue.values().all(HashMap::is_empty));
}

#[test]
fn delivered_ordered_frames_release_their_byte_budget() {
    let mut session = session(0, 1);
    session.config.max_queued_bytes = (size_of::<Frame>() + 3 + 2 * size_of::<u32>()) as i32;

    let now = domain_time();
    session
        .handle_frame(ordered_payload(1, &[0xFE, 1, 2]), now)
        .unwrap();
    session
        .handle_frame(ordered_payload(0, &[0xFE]), now)
        .unwrap();
    session
        .handle_frame(ordered_payload(3, &[0xFE, 3, 4]), now)
        .unwrap();

    assert_eq!(session.state, RakSessionState::Connected);
    assert_eq!(packets(&mut session).len(), 2);
    assert_eq!(session.inbound_ord_queue[&0].len(), 1);
}

fn datagram_at(sequence: u32, frames: Vec<Frame>, now: Instant) -> RakSessionInput {
    let RakSessionInput::Datagram(buf, _) = datagram(sequence, frames) else {
        unreachable!()
    };
    RakSessionInput::Datagram(buf, now)
}

#[test]
fn partial_split_without_a_fragment_for_the_session_timeout_is_evicted() {
    let mut session = session(0, 1);
    let start = domain_time();

    session
        .handle(datagram_at(0, vec![fragment(1, 2, 0, &[0xFE, 1])], start))
        .unwrap();
    session
        .handle(datagram_at(1, unreliable(0), start + SESSION_TIMEOUT / 2))
        .unwrap();
    session
        .handle(RakSessionInput::Update(
            start + SESSION_TIMEOUT + Duration::from_millis(1),
        ))
        .unwrap();

    assert_eq!(session.state, RakSessionState::Connected);
    assert!(
        session.inbound_spl_queue.is_empty(),
        "a split idle for the session timeout must be evicted"
    );
    assert_eq!(session.inbound_spl_bytes, 0);
}

#[test]
fn partial_split_receiving_fragments_is_kept() {
    let mut session = session(0, 1);
    let start = domain_time();

    session
        .handle(datagram_at(0, vec![fragment(1, 3, 0, &[0xFE])], start))
        .unwrap();
    session
        .handle(datagram_at(
            1,
            vec![fragment(1, 3, 1, &[1])],
            start + SESSION_TIMEOUT / 2,
        ))
        .unwrap();
    session
        .handle(RakSessionInput::Update(
            start + SESSION_TIMEOUT + Duration::from_millis(1),
        ))
        .unwrap();

    assert_eq!(session.inbound_spl_queue[&1].len(), 2);
    assert_eq!(session.inbound_spl_bytes, 2 * size_of::<Frame>() + 2);
}

#[test]
fn large_sends_split_into_datagrams_within_the_mtu() {
    let reliabilities = [
        RakReliability::Unreliable,
        RakReliability::UnreliableSequenced,
        RakReliability::Reliable,
        RakReliability::ReliableOrdered,
        RakReliability::ReliableSequenced,
        RakReliability::UnreliableWithAckReceipt,
        RakReliability::ReliableWithAckReceipt,
        RakReliability::ReliableOrderedWithAckReceipt,
    ];
    let now = domain_time();

    for mtu in crate::util::constants::MTU_SIZES {
        for reliability in reliabilities {
            let mut session = RakSession::new(
                RakSessionId(0),
                "[::1]:19132".parse().unwrap(),
                0,
                mtu,
                domain_time(),
                crate::clock::RakClock::new(crate::clock::test_now()),
                Default::default(),
            );
            for priority in [RakPriority::Immediate, RakPriority::Normal] {
                session
                    .handle(RakSessionInput::Send(
                        vec![0xFE; 64 * 1024].into_boxed_slice(),
                        reliability,
                        priority,
                        now,
                    ))
                    .unwrap();
            }
            session.tick(now).unwrap();

            let max = session.mtu as usize;
            for out in std::iter::from_fn(|| session.poll()) {
                if let RakSessionOutput::Datagram(buf, _) = out {
                    assert!(
                        buf.len() <= max,
                        "{reliability:?} at mtu {mtu} sent {} bytes",
                        buf.len()
                    );
                }
            }
        }
    }
}

fn drain_outputs(session: &mut RakSession) -> Vec<RakSessionOutput> {
    std::iter::from_fn(|| session.poll()).collect()
}

fn datagrams_of(outputs: &[RakSessionOutput]) -> Vec<Box<[u8]>> {
    outputs
        .iter()
        .filter_map(|out| match out {
            RakSessionOutput::Datagram(buf, _) => Some(buf.clone()),
            _ => None,
        })
        .collect()
}

fn payloads_of(outputs: &[RakSessionOutput]) -> Vec<Box<[u8]>> {
    datagrams_of(outputs)
        .iter()
        .filter(|buf| buf[0] & flags::VALID != 0 && buf[0] & (flags::ACK | flags::NACK) == 0)
        .flat_map(|buf| {
            FrameSet::deserialize(&mut Cursor::new(buf.as_ref()))
                .unwrap()
                .frames
        })
        .map(|frame| frame.payload)
        .collect()
}

fn disconnect_reason_of(outputs: &[RakSessionOutput]) -> Option<RakDisconnectReason> {
    outputs.iter().find_map(|out| match out {
        RakSessionOutput::Disconnected(_, reason) => Some(*reason),
        _ => None,
    })
}

#[test]
fn close_sends_queued_data_and_waits_for_ack() {
    let mut a = session(1, 1);
    let mut b = session(2, 2);
    let t0 = domain_time();
    let payload: Box<[u8]> = vec![0xFE, 1, 2, 3].into();

    a.handle(RakSessionInput::Send(
        payload.clone(),
        RakReliability::ReliableOrdered,
        RakPriority::Normal,
        t0,
    ))
    .unwrap();
    a.handle(RakSessionInput::Disconnect(t0)).unwrap();

    let outputs = drain_outputs(&mut a);
    assert!(
        payloads_of(&outputs).contains(&payload),
        "queued payload was not sent"
    );
    assert_eq!(disconnect_reason_of(&outputs), None);
    assert_eq!(a.state, RakSessionState::Closing);

    for buf in datagrams_of(&outputs) {
        b.handle(RakSessionInput::Datagram(buf, t0)).unwrap();
    }
    b.tick(t0).unwrap();
    deliver(&mut b, &mut a, t0);

    a.handle(RakSessionInput::Update(t0 + Duration::from_millis(20)))
        .unwrap();
    let outputs = drain_outputs(&mut a);
    assert!(
        payloads_of(&outputs)
            .iter()
            .any(|payload| payload.first() == Some(&0x15)),
        "disconnect notice was not sent"
    );
    assert_eq!(
        disconnect_reason_of(&outputs),
        Some(RakDisconnectReason::Requested)
    );
}

#[test]
fn close_gives_up_after_close_timeout() {
    let mut a = session(1, 1);
    let t0 = domain_time();

    a.handle(RakSessionInput::Send(
        vec![0xFE, 1].into(),
        RakReliability::ReliableOrdered,
        RakPriority::Normal,
        t0,
    ))
    .unwrap();
    a.handle(RakSessionInput::Disconnect(t0)).unwrap();
    drain_outputs(&mut a);

    a.handle(RakSessionInput::Update(t0 + Duration::from_secs(4)))
        .unwrap();
    assert_eq!(disconnect_reason_of(&drain_outputs(&mut a)), None);

    a.handle(RakSessionInput::Update(t0 + Duration::from_millis(5001)))
        .unwrap();
    assert_eq!(
        disconnect_reason_of(&drain_outputs(&mut a)),
        Some(RakDisconnectReason::Requested)
    );
}

#[test]
fn close_resends_unacknowledged_data() {
    let mut a = session(1, 1);
    let t0 = domain_time();
    let payload: Box<[u8]> = vec![0xFE, 9, 9].into();

    a.handle(RakSessionInput::Send(
        payload.clone(),
        RakReliability::ReliableOrdered,
        RakPriority::Immediate,
        t0,
    ))
    .unwrap();
    a.handle(RakSessionInput::Disconnect(t0)).unwrap();
    drain_outputs(&mut a);

    let result = a.handle(RakSessionInput::Update(t0 + Duration::from_millis(2100)));

    assert!(result.is_ok(), "update while closing failed: {result:?}");
    assert!(payloads_of(&drain_outputs(&mut a)).contains(&payload));
}

#[test]
fn unacknowledged_application_and_liveness_sends_respect_outbound_memory_limit() {
    for liveness in [false, true] {
        let mut sender = session(0, 1);
        sender.config.max_outbound_bytes = 1024;
        let now = domain_time();
        for seq in 0..256 {
            let result = if liveness {
                sender.handle(datagram_at(
                    seq,
                    vec![Frame::new(
                        RakReliability::Unreliable,
                        Box::new([packet_id::DETECT_LOST_CONNECTIONS]),
                    )],
                    now,
                ))
            } else {
                send(&mut sender, 128, now)
            };
            if result.is_err() || sender.state == RakSessionState::Disconnected {
                break;
            }
            sender.tick(now).unwrap();
            drain_outputs(&mut sender);
        }
        assert_eq!(sender.state, RakSessionState::Disconnected);
        assert_eq!(
            disconnect_reason(&sender),
            Some(RakDisconnectReason::OutboundBytesExceeded)
        );
        assert!(
            payloads_of(&drain_outputs(&mut sender))
                .iter()
                .any(|payload| payload.as_ref() == [packet_id::DISCONNECT])
        );
    }
}

fn send(session: &mut RakSession, len: usize, now: Instant) -> Result<(), RakSessionError> {
    session.handle(RakSessionInput::Send(
        vec![0xFE; len].into_boxed_slice(),
        RakReliability::ReliableOrdered,
        RakPriority::Normal,
        now,
    ))
}

#[test]
fn a_maximum_message_fits_the_receivers_fragment_storage_budget() {
    let mut a = session(0, 1);
    let mut b = session(1, 2);
    a.config.max_queued_bytes = 1 << 20;
    b.config.max_queued_bytes = a.config.max_queued_bytes;
    let now = domain_time();

    let length = a.max_message_len();
    send(&mut a, length, now).unwrap();

    let mut received = Vec::new();
    let mut reason = None;
    for step in 0..10_000 {
        let now = now + Duration::from_millis(step * 10);
        a.tick(now).unwrap();
        deliver(&mut a, &mut b, now);
        b.tick(now).unwrap();
        let outputs = drain_outputs(&mut b);
        reason = reason.or(disconnect_reason_of(&outputs));
        for buf in datagrams_of(&outputs) {
            a.handle(RakSessionInput::Datagram(buf, now)).unwrap();
        }
        received.extend(outputs.into_iter().filter_map(|out| match out {
            RakSessionOutput::Packet(buf) => Some(buf),
            _ => None,
        }));
        if !received.is_empty() || b.state == RakSessionState::Disconnected {
            break;
        }
    }

    assert_eq!(reason, None);
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].len(), length);
}

#[test]
fn sends_over_max_message_len_are_refused() {
    let mut session = session(0, 1);
    let now = domain_time();
    let len = session.max_message_len() + 1;

    assert!(matches!(
        send(&mut session, len, now),
        Err(RakSessionError::MessageSizeExceeded(n)) if n == len
    ));
    assert_eq!(session.outbound_bytes(), 0);
    assert_ne!(session.state, RakSessionState::Disconnected);
    send(&mut session, len - 1, now).unwrap();
}

fn sequences_of(outputs: &[RakSessionOutput]) -> Vec<u32> {
    datagrams_of(outputs)
        .iter()
        .filter(|buf| buf[0] & flags::VALID != 0 && buf[0] & (flags::ACK | flags::NACK) == 0)
        .map(|buf| {
            FrameSet::deserialize(&mut Cursor::new(buf.as_ref()))
                .unwrap()
                .sequence
        })
        .collect()
}

#[test]
fn resends_use_a_fresh_sequence_number() {
    let mut a = session(0, 1);
    let now = domain_time();

    send(&mut a, 16, now).unwrap();
    a.tick(now).unwrap();
    let first = sequences_of(&drain_outputs(&mut a));
    let queued = a.outbound_bytes();

    let later = now + Duration::from_secs(5);
    a.tick(later).unwrap();
    let resent = sequences_of(&drain_outputs(&mut a));

    assert_eq!(first.len(), 1);
    assert_eq!(resent.len(), 1);
    assert_ne!(resent[0], first[0]);
    assert_eq!(a.outbound_bytes(), queued);
}

#[test]
fn acks_of_resent_datagrams_do_not_sample_the_rtt() {
    let mut a = session(0, 1);
    let mut b = session(1, 2);
    let now = domain_time();

    send(&mut a, 16, now).unwrap();
    a.tick(now).unwrap();
    drain_outputs(&mut a);

    let later = now + Duration::from_secs(5);
    a.tick(later).unwrap();
    deliver(&mut a, &mut b, later);
    b.tick(later).unwrap();
    deliver(&mut b, &mut a, later);

    assert_eq!(a.outbound_bytes(), 0);
    assert_eq!(a.rtt(), Duration::ZERO);
}

#[test]
fn congestion_reference_avoidance_grows_once_per_ack_independent_of_datagram_size() {
    let now = domain_time();
    let clock = RakClock::new(now);
    for size in [100, 1200] {
        let mut snapshot = RakCongestionController::new(1200).snapshot(clock);
        snapshot.congestion_window = 3000.0;
        snapshot.slow_start_threshold = 2400.0;
        let mut controller = RakCongestionController::restore(snapshot, clock).unwrap();
        controller.sent(1, size, now);
        controller.acked(now + Duration::from_millis(10), 1, size, true);
        assert_eq!(
            controller.snapshot(clock).congestion_window,
            3480.0,
            "one admitted ACK must advance avoidance by the same amount for either size"
        );
        controller.acked(now + Duration::from_millis(20), 1, size, true);
        assert_eq!(controller.snapshot(clock).congestion_window, 3480.0);
        controller.sent(2, size, now + Duration::from_millis(20));
        controller.acked(now + Duration::from_millis(30), 2, size, false);
        assert_eq!(
            controller.snapshot(clock).congestion_window,
            3480.0,
            "an idle sender must not grow its congestion window"
        );
    }
}

#[test]
fn congestion_reference_nack_recovery_does_not_back_off_again_for_same_block_timeout() {
    let now = domain_time();
    for first_sequence in [0, u24::MASK - 1] {
        let mut sender = RakSession::new(
            RakSessionId(0),
            "127.0.0.1:19132".parse().unwrap(),
            7,
            1228,
            now,
            RakClock::new(crate::clock::test_now()),
            Default::default(),
        );
        assert_eq!(sender.mtu, 1200);
        let mut snapshot = sender.congestion_controller.snapshot(sender.clock);
        snapshot.congestion_window = 4800.0;
        sender.congestion_controller =
            RakCongestionController::restore(snapshot, sender.clock).unwrap();
        sender.outbound_seq = first_sequence;
        for _ in 0..12 {
            send(&mut sender, 1000, now).unwrap();
        }
        sender.tick(now).unwrap();
        let initial_outputs = drain_outputs(&mut sender);
        let initial = datagrams_of(&initial_outputs);
        let initial_frames = frames_of(&initial);
        let initial_sequences = sequences_of(&initial_outputs);
        assert_eq!(initial_sequences.len(), 4);
        assert!(sender.bandwidth_limited);
        let before = sender.congestion_controller.snapshot(sender.clock);
        let nack_at = now + Duration::from_millis(10);
        sender
            .handle(RakSessionInput::Datagram(
                Ack::new(vec![initial_sequences[0]], true)
                    .serialized()
                    .unwrap(),
                nack_at,
            ))
            .unwrap();
        let nack_outputs = drain_outputs(&mut sender);
        let nack_sequences = sequences_of(&nack_outputs);
        assert_eq!(nack_sequences.len(), 1);
        assert_eq!(nack_sequences[0], u24::add(first_sequence, 4));
        assert_eq!(
            frames_of(&datagrams_of(&nack_outputs)),
            initial_frames[..1],
            "NACK recovery keeps the original reliable and ordered frame identity"
        );
        let after_nack = sender.congestion_controller.snapshot(sender.clock);
        assert_eq!(after_nack.congestion_window, 4800.0);
        assert_eq!(after_nack.slow_start_threshold, 2400.0);
        assert_eq!(
            after_nack.bytes_not_acknowledged,
            before.bytes_not_acknowledged
        );

        // The other original datagrams expire before the NACK retransmission does.
        sender.tick(now + Duration::from_secs(2)).unwrap();
        let timeout_outputs = drain_outputs(&mut sender);
        let timeout_sequences = sequences_of(&timeout_outputs);
        assert_eq!(timeout_sequences.len(), 3);
        assert!(
            timeout_sequences
                .iter()
                .all(|seq| !initial_sequences.contains(seq))
        );
        let mut recovered = frames_of(&datagrams_of(&timeout_outputs));
        recovered.sort_by_key(|frame| frame.order_index);
        assert_eq!(recovered, initial_frames[1..]);
        let after_timeout = sender.congestion_controller.snapshot(sender.clock);
        assert_eq!(
            after_timeout.bytes_not_acknowledged, before.bytes_not_acknowledged,
            "resending retained datagrams must neither duplicate nor release in-flight bytes"
        );
        assert_eq!(
            after_timeout.congestion_window, 4800.0,
            "a NACK recovery block must not also collapse the window for a due retransmission"
        );
    }
}

#[test]
fn congestion_reference_accounting_excludes_the_datagram_header_for_mixed_frames() {
    let now = domain_time();
    let mut sender = session(0, 1);
    for reliability in [RakReliability::Reliable, RakReliability::Unreliable] {
        sender
            .handle(RakSessionInput::Send(
                vec![0xfe; 100].into_boxed_slice(),
                reliability,
                RakPriority::Normal,
                now,
            ))
            .unwrap();
    }
    sender.tick(now).unwrap();
    let outputs = drain_outputs(&mut sender);
    let datagrams = datagrams_of(&outputs);
    assert_eq!(datagrams.len(), 1);
    let set = FrameSet::deserialize(&mut Cursor::new(datagrams[0].as_ref())).unwrap();
    assert_eq!(set.frames.len(), 2);
    let before = sender.congestion_controller.snapshot(sender.clock);
    assert_eq!(
        before.bytes_not_acknowledged,
        datagrams[0].len() - DGRAM_HEADER_SIZE as usize,
        "congestion counts the encapsulated bytes of the retained datagram"
    );
    assert_eq!(sender.outbound_bytes(), datagrams[0].len());
    let ack = Ack::new(vec![set.sequence], false).serialized().unwrap();
    sender
        .handle(RakSessionInput::Datagram(
            ack.clone(),
            now + Duration::from_millis(10),
        ))
        .unwrap();
    assert_eq!(sender.outbound_bytes(), 0);
    let after = sender.congestion_controller.snapshot(sender.clock);
    assert_eq!(after.bytes_not_acknowledged, 0);
    sender
        .handle(RakSessionInput::Datagram(
            ack,
            now + Duration::from_millis(20),
        ))
        .unwrap();
    let duplicate = sender.congestion_controller.snapshot(sender.clock);
    assert_eq!(duplicate.bytes_not_acknowledged, 0);
    assert_eq!(duplicate.congestion_window, after.congestion_window);
}

#[test]
fn congestion_reference_feedback_bits_follow_the_actual_window_and_send_burst() {
    let now = domain_time();
    let mut sender = session(0, 1);
    let mut snapshot = sender.congestion_controller.snapshot(sender.clock);
    snapshot.congestion_window = 4.0 * sender.mtu as f64;
    snapshot.slow_start_threshold = 2.0 * sender.mtu as f64;
    sender.congestion_controller =
        RakCongestionController::restore(snapshot, sender.clock).unwrap();
    for _ in 0..2 {
        send(&mut sender, 1000, now).unwrap();
    }
    sender.tick(now).unwrap();
    let sets: Vec<_> = datagrams_of(&drain_outputs(&mut sender))
        .iter()
        .map(|bytes| FrameSet::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap())
        .collect();
    assert_eq!(sets.len(), 2);
    assert!(!sets[0].continuous_send);
    assert!(sets[1].continuous_send);
    assert!(sets.iter().all(|set| !set.needs_b_and_as));
    assert!(
        sets.iter()
            .flat_map(|set| &set.frames)
            .all(|frame| !frame.is_split())
    );

    let mut slow = session(1, 2);
    send(&mut slow, 100, now).unwrap();
    slow.tick(now).unwrap();
    let first = datagrams_of(&drain_outputs(&mut slow));
    let set = FrameSet::deserialize(&mut Cursor::new(first[0].as_ref())).unwrap();
    assert!(set.needs_b_and_as);
    assert!(!set.continuous_send);
}

#[test]
fn congestion_reference_recovery_and_queued_demand_survive_json_transfer() {
    let now = domain_time();
    for first_sequence in [0, u24::MASK - 2] {
        let mut sender = session(0, 1);
        let mut snapshot = sender.congestion_controller.snapshot(sender.clock);
        snapshot.congestion_window = 4.0 * sender.mtu as f64;
        sender.congestion_controller =
            RakCongestionController::restore(snapshot, sender.clock).unwrap();
        sender.outbound_seq = first_sequence;
        for _ in 0..12 {
            send(&mut sender, 1000, now).unwrap();
        }
        sender.tick(now).unwrap();
        let initial = sequences_of(&drain_outputs(&mut sender));
        assert!(initial.len() >= 2);
        let boundary = sender.outbound_seq;
        let nack_at = now + Duration::from_millis(10);
        sender
            .handle(RakSessionInput::Datagram(
                Ack::new(vec![initial[0]], true).serialized().unwrap(),
                nack_at,
            ))
            .unwrap();
        let nacked = drain_outputs(&mut sender);
        assert_eq!(sequences_of(&nacked), [boundary]);
        let before = sender.congestion_controller.snapshot(sender.clock);
        let json = facet_json::to_string(&sender.snapshot(nack_at)).unwrap();
        let destination = now + Duration::from_secs(10);
        let mut resumed = RakSession::restore(
            facet_json::from_str(&json).unwrap(),
            destination,
            Duration::from_millis(10),
        )
        .unwrap();
        // An ACK equal to the recovery boundary grows a busy window without ending its block.
        resumed
            .handle(RakSessionInput::Datagram(
                Ack::new(vec![boundary], false).serialized().unwrap(),
                destination,
            ))
            .unwrap();
        let after_ack = resumed.congestion_controller.snapshot(resumed.clock);
        assert!(after_ack.congestion_window > before.congestion_window);
        assert_eq!(after_ack.congestion_recovery_sequence, Some(boundary));
        resumed
            .handle(RakSessionInput::Datagram(
                Ack::new(vec![initial[1]], true).serialized().unwrap(),
                destination + Duration::from_millis(1),
            ))
            .unwrap();
        let repeated = datagrams_of(&drain_outputs(&mut resumed));
        assert_eq!(repeated.len(), 1);
        let set = FrameSet::deserialize(&mut Cursor::new(repeated[0].as_ref())).unwrap();
        assert!(set.continuous_send);
        let after_nack = resumed.congestion_controller.snapshot(resumed.clock);
        assert_eq!(after_nack.slow_start_threshold, before.slow_start_threshold);
        assert_eq!(after_nack.congestion_recovery_sequence, Some(boundary));
        resumed.tick(destination + Duration::from_secs(2)).unwrap();
        assert_eq!(
            resumed
                .congestion_controller
                .snapshot(resumed.clock)
                .congestion_window,
            after_ack.congestion_window,
            "transfer must preserve the original loss block across a due retransmission"
        );
    }
}

#[test]
fn congestion_reference_newer_ack_opens_the_next_wrapping_loss_block() {
    let now = domain_time();
    let clock = RakClock::new(now);
    for boundary in [20, u24::MASK] {
        let mut snapshot = RakCongestionController::new(1200).snapshot(clock);
        snapshot.congestion_window = 4800.0;
        let mut controller = RakCongestionController::restore(snapshot, clock).unwrap();
        controller.nacked(boundary, true);
        controller.resent(u24::add(boundary, 1), true);
        assert_eq!(controller.snapshot(clock).congestion_window, 4800.0);
        let newer = u24::add(boundary, 1);
        controller.sent(newer, 100, now);
        controller.acked(now + Duration::from_millis(10), newer, 100, true);
        assert!(controller.snapshot(clock).congestion_window > 4800.0);
        controller.resent(u24::add(newer, 1), true);
        assert_eq!(
            controller.snapshot(clock).congestion_window,
            1200.0,
            "an admitted ACK past the old boundary permits a new timeout backoff"
        );
    }
}

fn frames_of(datagrams: &[Box<[u8]>]) -> Vec<Frame> {
    datagrams
        .iter()
        .filter(|buf| buf[0] & flags::VALID != 0 && buf[0] & (flags::ACK | flags::NACK) == 0)
        .flat_map(|buf| {
            FrameSet::deserialize(&mut Cursor::new(buf.as_ref()))
                .unwrap()
                .frames
        })
        .collect()
}

#[test]
fn ordered_sends_wait_for_a_lost_message_before_exceeding_the_receive_window() {
    for first_index in [0, u24::MASK - 100] {
        let mut a = session(0, 1);
        let mut b = session(1, 2);
        let now = domain_time();
        // Establish enough congestion capacity for later datagrams to pass the loss.
        for _ in 0..3000 {
            a.handle(RakSessionInput::Send(
                Box::new([0xfe]),
                RakReliability::Reliable,
                RakPriority::Normal,
                now,
            ))
            .unwrap();
        }
        for _ in 0..100 {
            a.tick(now).unwrap();
            deliver(&mut a, &mut b, now);
            packets(&mut b);
            b.tick(now).unwrap();
            deliver(&mut b, &mut a, now);
            if a.outbound_bytes() == 0 {
                break;
            }
        }
        assert_eq!(a.outbound_bytes(), 0);
        a.outbound_ord_idx[0] = first_index;
        b.inbound_ord_idx[0] = first_index;
        let total = ORDER_WINDOW + 1000;
        for index in 0..total {
            let mut message = vec![0xfe];
            message.extend_from_slice(&(index as u32).to_le_bytes());
            a.handle(RakSessionInput::Send(
                message.into_boxed_slice(),
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
                now,
            ))
            .unwrap();
        }

        let mut withheld = Vec::new();
        let mut highest = 0;
        for _ in 0..500 {
            a.tick(now).unwrap();
            for datagram in datagrams_of(&drain_outputs(&mut a)) {
                let frames = frames_of(std::slice::from_ref(&datagram));
                if frames.iter().any(|f| f.order_index == first_index) {
                    withheld.push(datagram);
                    continue;
                }
                for frame in frames {
                    highest = highest.max(u24::distance(first_index, frame.order_index));
                }
                b.handle(RakSessionInput::Datagram(datagram, now)).unwrap();
                assert_eq!(b.get_state(), RakSessionState::Connected);
            }
            b.tick(now).unwrap();
            deliver(&mut b, &mut a, now);
        }
        assert!(highest > ORDER_WINDOW as i32 - 100, "window never filled");
        assert!(
            highest < ORDER_WINDOW as i32,
            "ordered gap exceeded receiver window"
        );
        assert!(packets(&mut b).is_empty());

        for datagram in withheld {
            b.handle(RakSessionInput::Datagram(datagram, now)).unwrap();
        }
        let mut received = packets(&mut b);
        for _ in 0..500 {
            b.tick(now).unwrap();
            deliver(&mut b, &mut a, now);
            a.tick(now).unwrap();
            deliver(&mut a, &mut b, now);
            received.extend(packets(&mut b));
            if received.len() == total {
                break;
            }
        }
        assert_eq!(received.len(), total);
        for (index, message) in received.iter().enumerate() {
            assert_eq!(&message[1..], &(index as u32).to_le_bytes());
        }
    }
}

#[test]
fn reliable_sends_stop_4096_indices_past_the_oldest_unacknowledged() {
    let mut a = session(0, 1);
    let mut b = session(1, 2);
    let now = domain_time();

    for _ in 0..6000 {
        a.handle(RakSessionInput::Send(
            Box::new([0xFE]),
            RakReliability::Reliable,
            RakPriority::Normal,
            now,
        ))
        .unwrap();
    }

    let mut highest = 0;
    for _ in 0..500 {
        a.tick(now).unwrap();
        let mut datagrams = datagrams_of(&drain_outputs(&mut a));
        datagrams.retain(|buf| {
            !frames_of(std::slice::from_ref(buf))
                .iter()
                .any(|f| f.reliable_index == 0)
        });
        for frame in frames_of(&datagrams) {
            highest = highest.max(frame.reliable_index);
        }
        for buf in datagrams {
            b.handle(RakSessionInput::Datagram(buf, now)).unwrap();
        }
        b.tick(now).unwrap();
        deliver(&mut b, &mut a, now);
    }

    assert!(highest >= 4000, "the window never filled: {highest}");
    assert!(
        highest < 4096,
        "sent reliable index {highest} with index 0 unacknowledged"
    );
}

#[test]
fn no_new_split_starts_while_256_are_in_flight() {
    let mut session = session(0, 1);
    let now = domain_time();

    for split_id in 0..256u16 {
        let mut frame = Frame::new(RakReliability::Reliable, Box::new([0]));
        frame.split_id = 1_000 + split_id;
        frame.split_size = 2;
        let sequence = 1_000 + split_id as u32;
        session.outbound_cache.insert(
            sequence,
            FrameSet::new(sequence, vec![frame], false, true, false),
        );
    }

    let len = 4 * session.mtu as usize;
    send(&mut session, len, now).unwrap();
    session.tick(now).unwrap();

    assert!(frames_of(&datagrams_of(&drain_outputs(&mut session))).is_empty());
}

#[test]
fn acknowledged_sends_release_outbound_bytes() {
    let mut a = session(0, 1);
    let mut b = session(1, 2);
    let now = domain_time();

    a.handle(RakSessionInput::Send(
        vec![0xFE; 1024].into_boxed_slice(),
        RakReliability::ReliableOrdered,
        RakPriority::Normal,
        now,
    ))
    .unwrap();
    assert!(a.outbound_bytes() >= 1024);

    a.tick(now).unwrap();
    deliver(&mut a, &mut b, now);
    b.tick(now).unwrap();
    deliver(&mut b, &mut a, now);

    assert_eq!(a.outbound_bytes(), 0);
}

#[test]
fn deferred_reliable_before_admission_times_out_despite_live_control_traffic() {
    let mut receiver = session(0, 1);
    receiver.config.max_queued_bytes = 1024;
    let start = domain_time();
    let mut frame = fragment(1, 2, 0, &[0xfe; 1200]);
    frame.reliability = RakReliability::Reliable;
    receiver
        .handle(datagram_at(0, vec![frame.clone()], start))
        .unwrap();
    assert!(receiver.inbound_spl_queue.is_empty());
    assert!(receiver.inbound_rel_ahead.is_empty());
    assert_eq!(receiver.state, RakSessionState::Connected);
    for (sequence, after) in [
        (1, SESSION_TIMEOUT / 3),
        (2, SESSION_TIMEOUT * 2 / 3),
        (3, SESSION_TIMEOUT - Duration::from_millis(1)),
    ] {
        let now = start + after;
        receiver
            .handle(datagram_at(0, vec![frame.clone()], now))
            .unwrap();
        let ping = ConnectedPing {
            timestamp: u64::from(sequence),
        }
        .serialized()
        .unwrap();
        receiver
            .handle(datagram_at(
                sequence,
                vec![Frame::new(RakReliability::Unreliable, ping)],
                now,
            ))
            .unwrap();
        receiver.handle(RakSessionInput::Update(now)).unwrap();
        assert_eq!(receiver.state, RakSessionState::Connected);
    }
    receiver
        .handle(RakSessionInput::Update(start + SESSION_TIMEOUT))
        .unwrap();
    assert_eq!(
        disconnect_reason(&receiver),
        Some(RakDisconnectReason::Timeout),
        "retransmission and live control traffic must not erase an undelivered reliable obligation"
    );
}
