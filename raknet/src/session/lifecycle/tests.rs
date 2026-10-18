use super::*;
use crate::clock::RakClock;
use crate::protocol::codec::RakCodecHelper;
use crate::protocol::packets::disconnect::Disconnect;

/// Constructs private engine fixtures without exposing a public handoff bypass.
fn engine(now: Instant, id: u64) -> SessionEngine {
    SessionEngine::new(
        RakSessionId(id),
        "127.0.0.1:19132".parse().unwrap(),
        id,
        crate::util::constants::MAX_MTU_SIZE,
        now,
        RakClock::new(now),
        Default::default(),
    )
}

#[test]
fn capture_rejects_produced_output_and_leaves_the_owner_drainable() {
    let now = crate::clock::test_now();
    let mut session = RakSession::<Established>::from_engine(Box::new(engine(now, 1)));
    session
        .send(
            Box::from([0xfe, 9]),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        )
        .unwrap();
    assert!(matches!(
        session.snapshot(now),
        Err(RakSessionCaptureError::PendingOutput)
    ));
    assert!(matches!(
        session.poll(),
        Some(RakSessionOutput::Datagram(..))
    ));
    while session.poll().is_some() {}
    assert!(session.snapshot(now).is_ok());
}

#[test]
fn restoring_a_locally_closing_session_preserves_its_phase() {
    let now = crate::clock::test_now();
    let mut session = RakSession::<Established>::from_engine(Box::new(engine(now, 1)));
    session
        .send(
            Box::from([0xfe, 9]),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
            now,
        )
        .unwrap();
    let mut closing = session.close(now).unwrap();
    while closing.poll().is_some() {}
    let snapshot = closing.snapshot(now).unwrap();
    let restored = snapshot.restore(now, Duration::ZERO).unwrap();
    assert!(
        matches!(restored, RestoredSession::Closing(session) if session.state() == RakSessionState::Closing)
    );
}

#[test]
fn capture_preserves_consumed_permission_before_peer_state_changes() {
    let now = crate::clock::test_now();
    // A fallible immediate-retirement operation can retain Connected peer state.
    let closing = RakSession::<Closing>::from_engine(Box::new(engine(now, 1)));
    let restored = closing
        .snapshot(now)
        .unwrap()
        .restore(now, Duration::ZERO)
        .unwrap();
    assert!(matches!(restored, RestoredSession::Closing(_)));
}

#[test]
fn a_close_error_retains_the_remote_terminal_packet_prefix() {
    let now = crate::clock::test_now();
    let mut peer = engine(now, 2);
    let mut session = RakSession::<Established>::from_engine(Box::new(engine(now, 1)));
    for bytes in [
        Box::from([0xfe, 9].as_slice()),
        Disconnect.serialized().unwrap(),
    ] {
        peer.handle(SessionInput::Send(
            bytes,
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))
        .unwrap();
        while let Some(output) = peer.poll() {
            if let RakSessionOutput::Datagram(bytes, _) = output {
                session
                    .handle(RakSessionInput::Datagram(bytes, now))
                    .unwrap();
            }
        }
    }
    let error = session.close(now).unwrap_err();
    let (mut closing, cause) = error.into_parts();
    assert!(matches!(cause, RakSessionError::Closed));
    let mut saw_packet = false;
    let mut saw_terminal = false;
    while let Some(output) = closing.poll() {
        match output {
            RakSessionOutput::Packet(bytes) => {
                assert_eq!(bytes.as_ref(), &[0xfe, 9]);
                assert!(!saw_terminal);
                saw_packet = true;
            }
            RakSessionOutput::Disconnected(_, RakDisconnectReason::Remote) => {
                assert!(saw_packet);
                saw_terminal = true;
            }
            _ => {}
        }
    }
    assert!(saw_packet && saw_terminal);
}
