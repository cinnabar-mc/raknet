use super::*;

/// Retires the same pure owner and drains all of its original pending output.
fn retire(engine: &mut SessionEngine, now: Instant) {
    if engine.state != RakSessionState::Disconnected {
        engine.handle(SessionInput::DisconnectNow(now)).unwrap();
    }
    drain_outputs(engine);
    assert_eq!(engine.state, RakSessionState::Disconnected);
}

/// Obtains an RTT sample by acknowledging an actual original outbound datagram.
fn sample_roundtrip(engine: &mut SessionEngine, start: Instant, roundtrip: Duration) -> Instant {
    engine
        .handle(SessionInput::Send(
            Box::from([0xfe, 1]),
            RakReliability::Reliable,
            RakPriority::Immediate,
            start,
        ))
        .unwrap();
    let sent = drain_outputs(engine);
    let sequences = sequences_of(&sent);
    assert!(!sequences.is_empty());
    let observed = start + roundtrip;
    engine
        .handle(SessionInput::Datagram(
            Ack::new(sequences, false).serialized().unwrap(),
            observed,
        ))
        .unwrap();
    assert_eq!(engine.rtt(), roundtrip);
    assert_eq!(engine.outbound_bytes(), 0);
    observed
}

/// Go6124 permits five seconds plus two measured round trips before initiating close.
#[test]
fn measured_rtt_silence_allowance_survives_before_go_close_admission() {
    let mut sender = session(1, 1);
    let last_input = sample_roundtrip(&mut sender, domain_time(), Duration::from_secs(3));
    // Ten seconds is still inside the selected Go five-plus-two-RTT allowance.
    let observed_at = last_input + Duration::from_secs(10);
    sender.handle(SessionInput::Update(observed_at)).unwrap();
    let state = sender.state;
    let terminal = disconnect_reason_of(&drain_outputs(&mut sender));
    retire(&mut sender, observed_at);
    assert_eq!(
        (state, terminal),
        (RakSessionState::Connected, None),
        "a measured high-RTT peer retired before its selected Go silence allowance"
    );
}

/// Peer inactivity must not replace an already-started local close's drain and cause.
#[test]
fn outstanding_close_keeps_its_original_drain_after_peer_silence() {
    let mut sender = session(1, 1);
    let last_input = sample_roundtrip(&mut sender, domain_time(), Duration::from_millis(40));
    let began = last_input + Duration::from_secs(9);
    let body: Box<[u8]> = Box::from([0xfe, 7].as_slice());
    sender
        .handle(SessionInput::Send(
            body.clone(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            began,
        ))
        .unwrap();
    sender.handle(SessionInput::Disconnect(began)).unwrap();
    let first = drain_outputs(&mut sender);
    assert!(payloads_of(&first).contains(&body));
    assert_eq!(sender.state, RakSessionState::Closing);
    assert_eq!(disconnect_reason_of(&first), None);
    let cutoff = began + sender.config.close_timeout;
    let observed_at = last_input + Duration::from_secs(10);
    assert!(observed_at < cutoff);
    sender.handle(SessionInput::Update(observed_at)).unwrap();
    let state = sender.state;
    let terminal = disconnect_reason_of(&drain_outputs(&mut sender));
    retire(&mut sender, observed_at);
    assert_eq!(
        (state, terminal),
        (RakSessionState::Closing, None),
        "peer silence cut short the original requested close drain"
    );
}

/// An ACKed reliable prefix cannot disappear silently while ordinary input keeps the peer alive.
#[test]
fn acknowledged_reassembly_reports_completion_or_failure_after_expiry() {
    let mut receiver = session(1, 1);
    let start = domain_time();
    let mut first = fragment(1, 2, 0, &[0xfe, 7]);
    first.reliability = RakReliability::Reliable;
    receiver.handle(datagram_at(0, vec![first], start)).unwrap();
    receiver.tick(start).unwrap();
    let receipt = drain_outputs(&mut receiver);
    assert!(
        datagrams_of(&receipt).iter().any(|bytes| {
            Ack::deserialize(&mut std::io::Cursor::new(bytes.as_ref()))
                .is_ok_and(|ack| !ack.is_nack && ack.sequences.contains(&0))
        }),
        "original accepted fragment was not actually acknowledged"
    );
    receiver
        .handle(datagram_at(
            1,
            unreliable(9),
            start + Duration::from_secs(9),
        ))
        .unwrap();
    assert_eq!(
        packets(&mut receiver),
        vec![Box::from([0xfe, 9].as_slice())]
    );
    let now = start + Duration::from_millis(10_500);
    receiver.handle(SessionInput::Update(now)).unwrap();
    let expired = drain_outputs(&mut receiver);
    let mut terminal = disconnect_reason_of(&expired);
    let mut delivered = false;
    if receiver.state != RakSessionState::Disconnected {
        let mut last = fragment(1, 2, 1, &[8]);
        last.reliability = RakReliability::Reliable;
        last.reliable_index = 1;
        receiver.handle(datagram_at(2, vec![last], now)).unwrap();
        let output = drain_outputs(&mut receiver);
        terminal = terminal.or(disconnect_reason_of(&output));
        delivered = output.iter().any(|output| {
            matches!(output, RakSessionOutput::Packet(bytes) if bytes.as_ref() == [0xfe, 7, 8])
        });
    }
    retire(&mut receiver, now);
    assert!(
        delivered || terminal.is_some(),
        "acknowledged reliable bytes vanished without a completed message or terminal failure"
    );
}
