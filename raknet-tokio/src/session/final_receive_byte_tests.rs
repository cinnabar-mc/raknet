//! Aggregate retention and exclusive single-body admission at the final receiver.

use super::*;

#[derive(Debug)]
struct Drained {
    bodies: Vec<Box<[u8]>>,
    reason: Option<RakDisconnectReason>,
}

/// Installs the existing post-delivery observer on a genuinely negotiated owner.
fn gated_original(
    local: raknet::prelude::RakSession<Established>,
    peer: raknet::prelude::RakSession<Established>,
    attempts: usize,
    entered: oneshot::Sender<(usize, RakSessionState)>,
    release: oneshot::Receiver<()>,
) -> Original {
    let address = local.addr();
    let id = local.id();
    let limit = local.max_message_len();
    let mtu = local.mtu();
    let mut driver = Driver::established(local);
    driver.receive_gate = Some(ReceiveGate {
        remaining: attempts,
        entered: Some(entered),
        release: Some(release),
    });
    let (datagram_tx, datagrams) = wire::datagrams();
    let (disconnect_tx, disconnected) = unbounded_channel();
    let (mut session, input) = spawn(driver, address, id, limit, mtu, datagram_tx, disconnect_tx);
    let actor = session.actor.take();
    Original {
        session: Some(session),
        actor,
        input,
        datagrams,
        disconnected,
        peer: Some(peer),
        address,
        id,
        received: Vec::new(),
    }
}

/// Transfers each original encoded datagram without decoding or rebuilding it.
async fn route_peer_output(original: &mut Original) -> Result<usize, String> {
    let mut count = 0;
    while let Some(output) = original.peer.as_mut().unwrap().poll() {
        match output {
            RakSessionOutput::Datagram(bytes, _) => {
                original
                    .input
                    .send(SessionEvent::Io(RakSessionInput::Datagram(
                        bytes,
                        Instant::now().into_std(),
                    )))
                    .await
                    .map_err(|error| format!("original peer route: {error}"))?;
                count += 1;
            }
            RakSessionOutput::Wait(_) => {}
            _ => {
                return Err(
                    "the sending peer produced unexpected application or terminal output"
                        .to_owned(),
                );
            }
        }
    }
    Ok(count)
}

/// Requests the same public close, joins its actor and drains its retained prefix.
async fn close_and_drain(original: &mut Original) -> Result<Drained, String> {
    let session = original
        .session
        .take()
        .expect("the public owner is retained");
    let mut closing = match session.close() {
        Ok(closing) => closing,
        Err(error) => error.into_parts().0,
    };
    let mut actor = original
        .actor
        .take()
        .expect("the original actor is retained");
    let cutoff = Instant::now() + constants::CLOSE_TIMEOUT + constants::CLOSE_TIMEOUT;
    let mut wire_open = true;
    let mut errors = Vec::new();
    loop {
        tokio::select! {
            result = &mut actor => {
                if let Err(error) = result {
                    errors.push(format!("original actor join: {error}"));
                }
                break;
            }
            frame = original.datagrams.recv(), if wire_open => {
                match frame {
                    Some(frame) => {
                        if let Err(error) = original.forward(frame, false, true) {
                            errors.push(error);
                        }
                    }
                    None => wire_open = false,
                }
            }
            _ = tokio::time::sleep_until(cutoff) => {
                actor.abort();
                let _ = actor.await;
                errors.push("original actor needed abort after public close".to_owned());
                break;
            }
        }
    }
    while let Ok(frame) = original.datagrams.try_recv() {
        if let Err(error) = original.forward(frame, false, true) {
            errors.push(error);
        }
    }
    if original.disconnected.try_recv() != Ok(original.id) {
        errors.push("original actor did not publish its exact route retirement".to_owned());
    }
    let mut bodies = Vec::new();
    let drained = tokio::time::timeout_at(cutoff, async {
        while let Ok(body) = closing.recv::<Box<[u8]>>().await {
            bodies.push(body);
        }
    })
    .await;
    if drained.is_err() {
        errors.push("original receive prefix did not end after its actor joined".to_owned());
    }
    let mut reason = None;
    match closing.finish() {
        Ok(terminal) if terminal.id() == original.id => reason = terminal.reason(),
        Ok(_) => errors.push("original terminal changed its route identity".to_owned()),
        Err(closing) => {
            drop(closing);
            errors.push("original terminal was unavailable after the actor joined".to_owned());
        }
    }
    if let Some(peer) = original.peer.take() {
        let mut closing = match peer.abort(Instant::now().into_std()) {
            Ok(closing) => closing,
            Err(error) => error.into_parts().0,
        };
        while closing.poll().is_some() {}
        if closing.finish().is_err() {
            errors.push("original peer did not finish after explicit abort".to_owned());
        }
    }
    if errors.is_empty() {
        Ok(Drained { bodies, reason })
    } else {
        Err(errors.join("; "))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn final_receive_byte_refusal_preserves_the_prefix_after_an_old_body_is_popped() {
    let now = Instant::now().into_std();
    let config = RakSessionConfig {
        max_queued_bytes: OUTBOUND_BYTES,
        ..Default::default()
    };
    let budget = usize::try_from(config.max_queued_bytes).unwrap();
    let body_bytes = count_body(0).len();
    let prefix = budget / body_bytes;
    assert!(prefix > 0 && prefix + 2 < constants::MAX_RECEIVE_MESSAGES);
    let (local, peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        config,
    );
    let (healthy_local, healthy_peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54322".parse().unwrap(),
        "127.0.0.1:19133".parse().unwrap(),
        now,
        now,
        Default::default(),
    );
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let mut release = Some(release);
    let mut original = gated_original(local, peer, prefix + 1, entered, release_rx);
    let mut healthy = Original::new(healthy_local, healthy_peer);
    let cutoff = Instant::now() + constants::CLOSE_TIMEOUT + constants::CLOSE_TIMEOUT;
    let before = b"\xfehealthy neighbor before final byte pressure";
    let during = b"\xfehealthy neighbor while final byte delivery is gated";
    let observed = tokio::time::timeout_at(cutoff, async {
        let before_echo = healthy.echo(before, cutoff).await?;
        for sequence in 0..prefix {
            original
                .peer
                .as_mut()
                .unwrap()
                .send(
                    count_body(sequence),
                    RakReliability::ReliableOrdered,
                    RakPriority::Normal,
                    Instant::now().into_std(),
                )
                .map_err(|error| format!("original byte prefix send: {error}"))?;
        }
        original
            .peer
            .as_mut()
            .unwrap()
            .tick(Instant::now().into_std())
            .map_err(|error| format!("original byte prefix flush: {error}"))?;
        route_peer_output(&mut original).await?;
        // Drain real ACKs without consuming any final application body.
        while original.peer.as_ref().unwrap().outbound_bytes() != 0 {
            let frame = original
                .datagrams
                .recv()
                .await
                .ok_or_else(|| "original acknowledgement route closed".to_owned())?;
            original.forward(frame, false, false)?;
        }
        let peer = original.peer.as_mut().unwrap();
        for sequence in prefix..prefix + 2 {
            peer.send(
                count_body(sequence),
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
                Instant::now().into_std(),
            )
            .map_err(|error| format!("original byte tail send: {error}"))?;
        }
        peer.tick(Instant::now().into_std())
            .map_err(|error| format!("original byte tail flush: {error}"))?;
        if route_peer_output(&mut original).await? != 1 {
            return Err("the two original tail bodies did not share one datagram".to_owned());
        }
        let full = entered_rx
            .await
            .map_err(|_| "the original final byte delivery gate was not reached".to_owned())?;
        let first = original
            .session
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|error| format!("original accepted front: {error}"))?;
        let during_echo = healthy.echo(during, cutoff).await?;
        release
            .take()
            .unwrap()
            .send(())
            .map_err(|_| "the original receive gate disappeared before release".to_owned())?;
        Ok::<_, String>((full, first, before_echo, during_echo))
    })
    .await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    // Both the connected baseline and the refused branch use the original close owner.
    let original_cleanup = close_and_drain(&mut original).await;
    let healthy_cleanup = healthy.finish().await;
    drop(original);
    drop(healthy);
    assert!(
        healthy_cleanup.is_ok(),
        "healthy cleanup: {healthy_cleanup:?}"
    );
    let mut drained = original_cleanup.expect("original close, prefix, actor and peer must finish");
    let ((queued, state), first, before_echo, during_echo) = observed
        .expect("the finite delivery observation must complete")
        .expect("the actual gate and healthy neighbor must progress");
    assert_eq!(before_echo.0.as_ref(), before);
    assert_eq!(before_echo.1.as_ref(), before);
    assert_eq!(during_echo.0.as_ref(), during);
    assert_eq!(during_echo.1.as_ref(), during);
    assert_eq!(drained.reason, Some(RakDisconnectReason::Requested));
    drained.bodies.insert(0, first);
    assert!(drained.bodies.len() >= prefix);
    for (sequence, body) in drained.bodies[..prefix].iter().enumerate() {
        assert_eq!(body, &count_body(sequence), "the accepted prefix changed");
    }
    let queued_bytes = queued.checked_mul(body_bytes).unwrap();
    assert!(
        queued_bytes <= budget,
        "final delivery retained {queued_bytes} bytes across {queued} bodies beyond its {budget}-byte budget"
    );
    assert_eq!(state, RakSessionState::Disconnected);
    assert!(!drained.bodies.contains(&count_body(prefix)));
    assert!(!drained.bodies.contains(&count_body(prefix + 1)));
    assert_eq!(
        drained.bodies.len(),
        prefix,
        "final delivery admitted a successor after byte refusal and a freed prefix slot"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn final_receive_byte_exclusive_body_credit_ends_at_pop_not_caller_drop() {
    let now = Instant::now().into_std();
    let config = RakSessionConfig {
        max_queued_bytes: 1,
        ..Default::default()
    };
    let budget = usize::try_from(config.max_queued_bytes).unwrap();
    assert!(count_body(0).len() > budget);
    let (local, peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        config,
    );
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let mut release = Some(release);
    let mut original = gated_original(local, peer, 1, entered, release_rx);
    let cutoff = Instant::now() + constants::CLOSE_TIMEOUT + constants::CLOSE_TIMEOUT;
    let observed = tokio::time::timeout_at(cutoff, async {
        original
            .peer
            .as_mut()
            .unwrap()
            .send(
                count_body(0),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                Instant::now().into_std(),
            )
            .map_err(|error| format!("original exclusive body send: {error}"))?;
        route_peer_output(&mut original).await?;
        let first_admission = entered_rx
            .await
            .map_err(|_| "the original exclusive body gate was not reached".to_owned())?;
        let first = original
            .session
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|error| format!("original exclusive body receive: {error}"))?;
        release
            .take()
            .unwrap()
            .send(())
            .map_err(|_| "the original exclusive body gate disappeared".to_owned())?;
        original
            .peer
            .as_mut()
            .unwrap()
            .send(
                count_body(1),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
                Instant::now().into_std(),
            )
            .map_err(|error| format!("original successor send: {error}"))?;
        route_peer_output(&mut original).await?;
        let second = original
            .session
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|error| {
                format!("original successor receive while first is retained: {error}")
            })?;
        Ok::<_, String>((first_admission, first, second))
    })
    .await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    let cleanup = close_and_drain(&mut original).await;
    drop(original);
    let drained = cleanup.expect("the original exclusive body owner must finish");
    let (first_admission, first, second) = observed
        .expect("the finite exclusive body observation must complete")
        .expect("both already-valid full bodies must be received");
    assert_eq!(first_admission, (1, RakSessionState::Connected));
    assert_eq!(first, count_body(0));
    assert_eq!(second, count_body(1));
    assert!(drained.bodies.is_empty());
    assert_eq!(drained.reason, Some(RakDisconnectReason::Requested));
}
