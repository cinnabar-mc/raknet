use crate::manual_handshake_tests::{Pair, negotiate_after_loss};
use crate::prelude::*;
use crate::protocol::codec::{RakCodec, RakCodecHelper};
use crate::protocol::error::RakCodecError;
use crate::protocol::packets::ack::Ack;
use crate::protocol::packets::connected_ping::ConnectedPing;
use crate::protocol::packets::disconnect::Disconnect;
use crate::protocol::packets::frame_set::FrameSet;
use crate::util::{flags, packet_id};
use std::io::Cursor;
use std::time::Instant;

/// Negotiates actual public endpoint owners before exercising established input.
fn connected_pair() -> Pair {
    let (left, right, now) =
        negotiate_after_loss(true, packet_id::NEW_INCOMING_CONNECTION).unwrap();
    assert_eq!(left.outbound_bytes(), 0);
    assert_eq!(right.outbound_bytes(), 0);
    (left, right, now)
}

/// Takes emitted wire output without manufacturing packet or phase state.
fn wire<S: SessionPhase>(session: &mut RakSession<S>) -> Vec<Box<[u8]>> {
    let mut datagrams = Vec::new();
    while let Some(output) = session.poll() {
        match output {
            RakSessionOutput::Datagram(bytes, _) => datagrams.push(bytes),
            RakSessionOutput::Wait(_) => {}
            output => panic!("unexpected output while taking wire: {output:?}"),
        }
    }
    datagrams
}

/// Delivers actual control output and returns the peer-visible ACK and pong facts.
fn forward_control(
    from: &mut RakSession<Established>,
    to: &mut RakSession<Established>,
    now: Instant,
) -> (Vec<u32>, usize) {
    let mut acknowledged = Vec::new();
    let mut pongs = 0;
    while let Some(output) = from.poll_control() {
        match output {
            RakSessionOutput::Datagram(bytes, _) => {
                if bytes[0] & (flags::ACK | flags::NACK) != 0 {
                    let ack = Ack::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap();
                    if !ack.is_nack {
                        acknowledged.extend(ack.sequences);
                    }
                } else {
                    let set = FrameSet::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap();
                    pongs += set
                        .frames
                        .iter()
                        .filter(|frame| frame.payload.first() == Some(&packet_id::CONNECTED_PONG))
                        .count();
                }
                to.handle_backpressured(RakSessionInput::Datagram(bytes, now))
                    .unwrap();
            }
            RakSessionOutput::Wait(_) => {}
            output => panic!("unexpected terminal during control exchange: {output:?}"),
        }
    }
    (acknowledged, pongs)
}

/// Retires the same pure owner, draining every application and terminal output.
fn retire<S: SessionPhase>(session: RakSession<S>, now: Instant) {
    let mut closing = match session.abort(now) {
        Ok(closing) => closing,
        Err(error) => error.into_parts().0,
    };
    while closing.poll().is_some() {}
    assert!(closing.finish().is_ok());
}

#[test]
fn backpressure_withholds_ack_then_accepts_exact_retry_and_acks_duplicates() {
    for reliability in [RakReliability::Reliable, RakReliability::ReliableOrdered] {
        let (mut sender, mut receiver, now) = connected_pair();
        let payload: Box<[u8]> = Box::from([0xfe, 1, 2]);
        sender
            .send(payload.clone(), reliability, RakPriority::Immediate, now)
            .unwrap();
        let datagrams = wire(&mut sender);
        assert_eq!(datagrams.len(), 1);
        let original = &datagrams[0];
        let set = FrameSet::deserialize(&mut Cursor::new(original.as_ref())).unwrap();
        receiver
            .handle_backpressured(RakSessionInput::Datagram(original.clone(), now))
            .unwrap();
        receiver.tick(now).unwrap();
        let retained = sender.outbound_bytes();
        assert!(retained > 0);
        let (acknowledged, _) = forward_control(&mut receiver, &mut sender, now);
        assert!(!acknowledged.contains(&set.sequence));
        assert_eq!(sender.outbound_bytes(), retained);
        assert!(receiver.poll().is_none());

        receiver
            .handle(RakSessionInput::Datagram(original.clone(), now))
            .unwrap();
        receiver.tick(now).unwrap();
        let (acknowledged, _) = forward_control(&mut receiver, &mut sender, now);
        assert!(acknowledged.contains(&set.sequence));
        assert_eq!(sender.outbound_bytes(), 0);
        assert!(
            matches!(receiver.poll(), Some(RakSessionOutput::Packet(bytes)) if bytes == payload)
        );
        assert!(receiver.poll().is_none());

        receiver
            .handle_backpressured(RakSessionInput::Datagram(original.clone(), now))
            .unwrap();
        receiver.tick(now).unwrap();
        let (acknowledged, _) = forward_control(&mut receiver, &mut sender, now);
        assert!(acknowledged.contains(&set.sequence));
        assert!(receiver.poll().is_none());
        retire(sender, now);
        retire(receiver, now);
    }
}

#[test]
fn backpressure_services_control_but_never_acks_a_partly_refused_datagram() {
    let (mut sender, mut receiver, now) = connected_pair();
    let payload: Box<[u8]> = Box::from([0xfe, 7]);
    for bytes in [
        payload.clone(),
        ConnectedPing { timestamp: 0 }.serialized().unwrap(),
    ] {
        let frames = sender
            .engine
            .prepare_frames(crate::protocol::types::frame::Frame::new(
                RakReliability::Reliable,
                bytes,
            ));
        sender
            .engine
            .queue_frames(frames, RakPriority::Normal, now)
            .unwrap();
    }
    sender.tick(now).unwrap();
    let datagrams = wire(&mut sender);
    assert_eq!(datagrams.len(), 1);
    let original = &datagrams[0];
    let set = FrameSet::deserialize(&mut Cursor::new(original.as_ref())).unwrap();
    assert_eq!(set.frames.len(), 2);
    receiver
        .handle_backpressured(RakSessionInput::Datagram(original.clone(), now))
        .unwrap();
    receiver.tick(now).unwrap();
    let (acknowledged, pongs) = forward_control(&mut receiver, &mut sender, now);
    assert_eq!(pongs, 1);
    assert!(!acknowledged.contains(&set.sequence));
    assert!(sender.outbound_bytes() > 0);
    assert!(receiver.poll().is_none());

    receiver
        .handle(RakSessionInput::Datagram(original.clone(), now))
        .unwrap();
    receiver.tick(now).unwrap();
    let (acknowledged, pongs) = forward_control(&mut receiver, &mut sender, now);
    assert!(acknowledged.contains(&set.sequence));
    assert_eq!(
        pongs, 0,
        "the already-received reliable ping must not repeat"
    );
    assert_eq!(sender.outbound_bytes(), 0);
    assert!(matches!(receiver.poll(), Some(RakSessionOutput::Packet(bytes)) if bytes == payload));
    assert!(receiver.poll().is_none());
    retire(sender, now);
    retire(receiver, now);
}

#[test]
fn backpressure_retains_acknowledged_fragments_until_final_fragment_retry() {
    let (mut sender, mut receiver, now) = connected_pair();
    let payload = vec![0xfe; sender.mtu() as usize + 17].into_boxed_slice();
    sender
        .send(
            payload.clone(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    let mut completing = None;
    let mut retained_fragments = 0;
    for _ in 0..4 {
        for bytes in wire(&mut sender) {
            let set = FrameSet::deserialize(&mut Cursor::new(bytes.as_ref())).unwrap();
            assert!(set.frames.iter().all(|frame| frame.is_split()));
            if set
                .frames
                .iter()
                .any(|frame| frame.split_index + 1 == frame.split_size)
            {
                assert!(completing.replace((bytes, set.sequence)).is_none());
            } else {
                receiver
                    .handle_backpressured(RakSessionInput::Datagram(bytes, now))
                    .unwrap();
                receiver.tick(now).unwrap();
                let (acknowledged, _) = forward_control(&mut receiver, &mut sender, now);
                assert!(acknowledged.contains(&set.sequence));
                retained_fragments += set.frames.len();
            }
        }
        if completing.is_some() {
            break;
        }
        sender.tick(now).unwrap();
    }
    assert!(retained_fragments > 0);
    let (original, sequence) = completing.expect("the real sender must emit the final fragment");
    assert!(receiver.poll().is_none());
    receiver
        .handle_backpressured(RakSessionInput::Datagram(original.clone(), now))
        .unwrap();
    receiver.tick(now).unwrap();
    let (acknowledged, _) = forward_control(&mut receiver, &mut sender, now);
    assert!(!acknowledged.contains(&sequence));
    assert!(sender.outbound_bytes() > 0);
    assert!(receiver.poll().is_none());

    receiver
        .handle(RakSessionInput::Datagram(original, now))
        .unwrap();
    receiver.tick(now).unwrap();
    let (acknowledged, _) = forward_control(&mut receiver, &mut sender, now);
    assert!(acknowledged.contains(&sequence));
    assert_eq!(sender.outbound_bytes(), 0);
    assert!(matches!(receiver.poll(), Some(RakSessionOutput::Packet(bytes)) if bytes == payload));
    assert!(receiver.poll().is_none());
    retire(sender, now);
    retire(receiver, now);
}

#[test]
fn backpressure_preserves_canonical_invalid_ack_and_control_errors() {
    for backpressured in [false, true] {
        let (mut sender, mut receiver, now) = connected_pair();
        let invalid_ack = Box::from([flags::VALID | flags::ACK | flags::NACK]);
        let input = RakSessionInput::Datagram(invalid_ack, now);
        let error = if backpressured {
            receiver.handle_backpressured(input)
        } else {
            receiver.handle(input)
        };
        assert!(matches!(
            error,
            Err(RakSessionError::RakCodecError(
                RakCodecError::UnexpectedHeader(_)
            ))
        ));
        sender
            .send(
                Box::from([packet_id::CONNECTED_PING]),
                RakReliability::Reliable,
                RakPriority::Immediate,
                now,
            )
            .unwrap();
        let mut datagrams = wire(&mut sender);
        assert_eq!(datagrams.len(), 1);
        let input = RakSessionInput::Datagram(datagrams.pop().unwrap(), now);
        let error = if backpressured {
            receiver.handle_backpressured(input)
        } else {
            receiver.handle(input)
        };
        assert!(matches!(
            error,
            Err(RakSessionError::RakCodecError(RakCodecError::IOError(_)))
        ));
        assert!(receiver.poll().is_none());
        retire(sender, now);
        retire(receiver, now);
    }
}

#[test]
fn backpressure_keeps_invalid_split_and_order_metadata_terminal() {
    for split in [false, true] {
        let (mut sender, mut receiver, now) = connected_pair();
        sender
            .send(
                Box::from([0xfe, 4]),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                now,
            )
            .unwrap();
        let datagrams = wire(&mut sender);
        assert_eq!(datagrams.len(), 1);
        let mut set = FrameSet::deserialize(&mut Cursor::new(datagrams[0].as_ref())).unwrap();
        let frame = &mut set.frames[0];
        if split {
            frame.split_size = super::super::MAX_SPLIT_COUNT + 1;
        } else {
            frame.order_index =
                crate::util::u24::add(frame.order_index, super::super::ORDER_WINDOW as u32);
        }
        receiver
            .handle_backpressured(RakSessionInput::Datagram(set.serialized().unwrap(), now))
            .unwrap();
        let mut terminal = None;
        while let Some(output) = receiver.poll_control() {
            if let RakSessionOutput::Disconnected(_, reason) = output {
                terminal = Some(reason);
            }
        }
        assert_eq!(terminal, Some(RakDisconnectReason::ProtocolViolation));
        assert!(receiver.poll().is_none());
        assert_eq!(receiver.finish().unwrap().reason(), terminal);
        retire(sender, now);
    }
}

#[test]
fn backpressure_keeps_ordered_disconnect_behind_the_refused_application() {
    let (mut sender, mut receiver, now) = connected_pair();
    sender
        .send(
            Box::from([0xfe, 9]),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    let mut datagrams = wire(&mut sender);
    assert_eq!(datagrams.len(), 1);
    let original = datagrams.pop().unwrap();
    receiver
        .handle_backpressured(RakSessionInput::Datagram(original.clone(), now))
        .unwrap();
    sender
        .send(
            Disconnect.serialized().unwrap(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    let datagrams = wire(&mut sender);
    assert_eq!(datagrams.len(), 1);
    for bytes in datagrams {
        receiver
            .handle_backpressured(RakSessionInput::Datagram(bytes, now))
            .unwrap();
    }
    receiver.tick(now).unwrap();
    forward_control(&mut receiver, &mut sender, now);
    assert_eq!(receiver.state(), RakSessionState::Connected);
    assert!(receiver.poll().is_none());
    receiver
        .handle(RakSessionInput::Datagram(original, now))
        .unwrap();
    assert!(matches!(
        receiver.poll(),
        Some(RakSessionOutput::Packet(bytes)) if bytes.as_ref() == [0xfe, 9]
    ));
    assert!(matches!(
        receiver.poll_control(),
        Some(RakSessionOutput::Disconnected(
            _,
            RakDisconnectReason::Remote
        ))
    ));
    assert!(receiver.poll().is_none());
    assert_eq!(
        receiver.finish().unwrap().reason(),
        Some(RakDisconnectReason::Remote)
    );
    retire(sender, now);
}

#[test]
fn control_poll_preserves_packet_fifo_and_terminal_capture_ownership() {
    let (mut sender, mut receiver, now) = connected_pair();
    for tag in 1..=3 {
        sender
            .send(
                Box::from([0xfe, tag]),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                now,
            )
            .unwrap();
        for bytes in wire(&mut sender) {
            receiver
                .handle(RakSessionInput::Datagram(bytes, now))
                .unwrap();
        }
        receiver.tick(now).unwrap();
        forward_control(&mut receiver, &mut sender, now);
        assert!(receiver.poll_control().is_none());
        if tag == 1 {
            assert!(
                matches!(receiver.poll(), Some(RakSessionOutput::Packet(bytes)) if bytes.as_ref() == [0xfe, 1])
            );
        }
    }
    receiver
        .handle_backpressured(RakSessionInput::Update(now))
        .unwrap();
    assert!(matches!(
        receiver.poll_control(),
        Some(RakSessionOutput::Wait(_))
    ));
    assert!(receiver.poll_control().is_none());
    sender
        .send(
            Disconnect.serialized().unwrap(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    for bytes in wire(&mut sender) {
        receiver
            .handle_backpressured(RakSessionInput::Datagram(bytes, now))
            .unwrap();
    }
    assert!(matches!(
        receiver.poll_control(),
        Some(RakSessionOutput::Disconnected(
            _,
            RakDisconnectReason::Remote
        ))
    ));
    assert!(receiver.poll_control().is_none());
    assert!(matches!(
        receiver.snapshot(now),
        Err(RakSessionCaptureError::PendingOutput)
    ));
    let mut receiver = receiver.finish().unwrap_err();
    for tag in 2..=3 {
        assert!(
            matches!(receiver.poll(), Some(RakSessionOutput::Packet(bytes)) if bytes.as_ref() == [0xfe, tag])
        );
    }
    assert!(receiver.poll().is_none());
    assert_eq!(
        receiver.finish().unwrap().reason(),
        Some(RakDisconnectReason::Remote)
    );
    retire(sender, now);
}
