//! The original actor's wire output remains bounded while its consumer is held.

use super::*;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};

const BODY_BYTES: usize = 128;
const OUTBOUND_BYTES: i32 = 2048;

type Echo = (Box<[u8]>, Box<[u8]>);

/// Owns one genuinely negotiated actor and its original wire consumer and peer.
struct Original {
    session: Option<RakSession<Established>>,
    actor: Option<tokio::task::JoinHandle<()>>,
    input: Sender<SessionEvent>,
    datagrams: Receiver<wire::Datagram>,
    disconnected: UnboundedReceiver<RakSessionId>,
    peer: Option<raknet::prelude::RakSession<Established>>,
    address: SocketAddr,
    id: RakSessionId,
    received: Vec<Box<[u8]>>,
}

impl Original {
    /// Starts the actual actor only after both public handshake owners completed.
    fn new(
        local: raknet::prelude::RakSession<Established>,
        peer: raknet::prelude::RakSession<Established>,
    ) -> Self {
        let address = local.addr();
        let id = local.id();
        let (datagram_tx, datagrams) = wire::datagrams();
        let (disconnect_tx, disconnected) = unbounded_channel();
        let (mut session, input) = spawn_established(local, datagram_tx, disconnect_tx);
        let actor = session.actor.take();
        Self {
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

    /// Delivers a datagram to a live peer and returns its protocol output.
    fn forward(
        &mut self,
        datagram: wire::Datagram,
        echo: bool,
        closing: bool,
    ) -> Result<(), String> {
        let (bytes, address, _credit) = datagram.into_parts();
        if address != self.address {
            return Err("original wire frame changed its destination".to_owned());
        }
        let peer = self
            .peer
            .as_mut()
            .ok_or_else(|| "original peer was already consumed".to_owned())?;
        if closing && peer.state() == RakSessionState::Disconnected {
            return Ok(());
        }
        peer.handle(RakSessionInput::Datagram(bytes, Instant::now().into_std()))
            .map_err(|error| format!("original peer receive: {error}"))?;
        peer.tick(Instant::now().into_std())
            .map_err(|error| format!("original peer tick: {error}"))?;
        while let Some(output) = peer.poll() {
            match output {
                RakSessionOutput::Datagram(bytes, _) => {
                    match self
                        .input
                        .try_send(SessionEvent::Io(RakSessionInput::Datagram(
                            bytes,
                            Instant::now().into_std(),
                        ))) {
                        Ok(()) => {}
                        Err(TrySendError::Closed(_)) if closing => {}
                        Err(error) => return Err(format!("original peer return route: {error}")),
                    }
                }
                RakSessionOutput::Packet(bytes) => {
                    if echo {
                        peer.send(
                            bytes.clone(),
                            RakReliability::ReliableOrdered,
                            RakPriority::Immediate,
                            Instant::now().into_std(),
                        )
                        .map_err(|error| format!("original peer echo: {error}"))?;
                    }
                    self.received.push(bytes);
                }
                RakSessionOutput::Wait(_) | RakSessionOutput::Disconnected(..) => {}
            }
        }
        Ok(())
    }

    /// Proves useful reliable traffic while the other original consumer stays held.
    async fn echo(&mut self, marker: &[u8], deadline: Instant) -> Result<Echo, String> {
        tokio::time::timeout_at(deadline, async {
            let before = self.received.len();
            self.session
                .as_ref()
                .ok_or_else(|| "original public handle was consumed".to_owned())?
                .send(
                    marker.to_vec().into_boxed_slice(),
                    RakReliability::ReliableOrdered,
                    RakPriority::Immediate,
                )
                .await
                .map_err(|error| format!("healthy original send: {error}"))?;
            while self.received.len() == before {
                let frame = self
                    .datagrams
                    .recv()
                    .await
                    .ok_or_else(|| "healthy original wire consumer closed".to_owned())?;
                self.forward(frame, true, false)?;
            }
            let response = self
                .session
                .as_mut()
                .ok_or_else(|| "original public handle was consumed".to_owned())?
                .recv::<Box<[u8]>>()
                .await
                .map_err(|error| format!("healthy original response: {error}"))?;
            Ok((self.received[before].clone(), response))
        })
        .await
        .map_err(|_| "healthy original exchange reached its observation cutoff".to_owned())?
    }

    /// Closes the same public owner, drains its wire prefix and joins its original task.
    async fn finish(&mut self) -> Result<(), String> {
        let session = self
            .session
            .take()
            .ok_or_else(|| "original public handle was already consumed".to_owned())?;
        let closing = match session.close() {
            Ok(closing) => closing,
            Err(error) => error.into_parts().0,
        };
        let mut actor = self
            .actor
            .take()
            .ok_or_else(|| "original spawned actor was not retained".to_owned())?;
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
                frame = self.datagrams.recv(), if wire_open => {
                    match frame {
                        Some(frame) => {
                            if let Err(error) = self.forward(frame, false, true) {
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
        while let Ok(frame) = self.datagrams.try_recv() {
            if let Err(error) = self.forward(frame, false, true) {
                errors.push(error);
            }
        }
        if self.disconnected.try_recv() != Ok(self.id) {
            errors.push("original actor did not publish its exact route retirement".to_owned());
        }
        match closing.finish() {
            Ok(terminal) if terminal.id() == self.id => {}
            Ok(_) => errors.push("original terminal changed its route identity".to_owned()),
            Err(closing) => {
                drop(closing);
                errors.push("original terminal was unavailable after the actor joined".to_owned());
            }
        }
        if let Some(peer) = self.peer.take() {
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
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl Drop for Original {
    fn drop(&mut self) {
        if let Some(actor) = self.actor.take() {
            actor.abort();
        }
    }
}

/// Builds only the fixed legal application body; framing stays with the real core.
fn body(sequence: usize) -> Box<[u8]> {
    let mut bytes = vec![0x41; BODY_BYTES];
    bytes[0] = 0xfe;
    bytes[1] = u8::try_from(sequence).expect("the finite fixture fits one marker byte");
    bytes.into_boxed_slice()
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retained_original_wire_consumer_bounds_actual_datagram_bytes() {
    let now = Instant::now().into_std();
    let config = RakSessionConfig {
        max_outbound_bytes: OUTBOUND_BYTES,
        ..Default::default()
    };
    let (local, peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        config.clone(),
    );
    let (healthy_local, healthy_peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54322".parse().unwrap(),
        "127.0.0.1:19133".parse().unwrap(),
        now,
        now,
        config.clone(),
    );
    let wire_budget = (config.max_outbound_bytes as usize).max(usize::from(local.mtu()));
    let attempts = wire_budget / BODY_BYTES + 1;
    assert!(attempts < u8::MAX as usize);
    assert!(local.max_message_len() >= BODY_BYTES);
    let mut original = Original::new(local, peer);
    let mut healthy = Original::new(healthy_local, healthy_peer);
    let mut completed = 0;
    let observed = async {
        let before = b"\xfehealthy original neighbor before retained wire output";
        let before_echo = healthy
            .echo(before, Instant::now() + constants::CLOSE_TIMEOUT)
            .await?;
        let production = tokio::time::timeout_at(Instant::now() + config.close_timeout, async {
            for sequence in 0..attempts {
                original
                    .session
                    .as_ref()
                    .ok_or_else(|| "original public handle was consumed".to_owned())?
                    .send(
                        body(sequence),
                        RakReliability::Unreliable,
                        RakPriority::Immediate,
                    )
                    .await
                    .map_err(|error| format!("original held-consumer send: {error}"))?;
                completed += 1;
            }
            Ok::<_, String>(())
        })
        .await;
        if let Ok(result) = production {
            result?;
        }
        let after = b"\xfehealthy original neighbor while the other wire consumer stays held";
        let after_echo = healthy
            .echo(after, Instant::now() + constants::CLOSE_TIMEOUT)
            .await?;

        // Current-thread delivery cannot interleave actor work during this original drain.
        let queued = original.datagrams.len();
        let mut wire_bytes = 0_usize;
        let mut drained = 0_usize;
        while let Ok(frame) = original.datagrams.try_recv() {
            wire_bytes = wire_bytes
                .checked_add(frame.bytes().len())
                .ok_or_else(|| "actual wire byte sum overflowed".to_owned())?;
            drained += 1;
            original.forward(frame, false, false)?;
        }
        Ok::<_, String>((
            queued,
            drained,
            wire_bytes,
            before_echo,
            after_echo,
            before,
            after,
        ))
    }
    .await;

    let original_cleanup = original.finish().await;
    let healthy_cleanup = healthy.finish().await;
    assert!(
        original_cleanup.is_ok(),
        "original cleanup: {original_cleanup:?}"
    );
    assert!(
        healthy_cleanup.is_ok(),
        "healthy cleanup: {healthy_cleanup:?}"
    );
    let (queued, drained, wire_bytes, before_echo, after_echo, before, after) =
        observed.expect("setup: original wire observation and independent traffic must complete");
    assert_eq!(before_echo.0.as_ref(), before);
    assert_eq!(before_echo.1.as_ref(), before);
    assert_eq!(after_echo.0.as_ref(), after);
    assert_eq!(after_echo.1.as_ref(), after);
    assert_eq!(queued, drained);
    assert!(completed != 0 && completed <= original.received.len());
    assert!(original.received.len() <= attempts);
    for (sequence, received) in original.received.iter().enumerate() {
        assert_eq!(
            received,
            &body(sequence),
            "original accepted wire prefix changed"
        );
    }
    assert!(
        queued <= constants::MAX_RECEIVE_MESSAGES && wire_bytes <= wire_budget,
        "retained original wire consumer exceeded finite route admission: queued={queued}, wire_bytes={wire_bytes}, wire_budget={wire_budget}, completed={completed}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn original_peer_expiry_retires_an_actor_with_retained_wire_credit() {
    let now = Instant::now();
    let config = RakSessionConfig {
        max_outbound_bytes: OUTBOUND_BYTES,
        ..Default::default()
    };
    let (local, peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now.into_std(),
        now.into_std(),
        config.clone(),
    );
    let expiry = Instant::from_std(local.expiry_deadline(now.into_std()).unwrap());
    let budget = (config.max_outbound_bytes as usize).max(usize::from(local.mtu()));
    let mut original = Original::new(local, peer);
    let mut actor = original.actor.take().unwrap();
    let mut held = Vec::new();
    let observed = tokio::time::timeout_at(expiry + constants::CLOSE_TIMEOUT, async {
        original
            .session
            .as_ref()
            .unwrap()
            .send(body(0), RakReliability::Unreliable, RakPriority::Immediate)
            .await
            .map_err(|error| format!("first original send: {error}"))?;
        let first = original
            .datagrams
            .recv()
            .await
            .ok_or_else(|| "first original wire output closed".to_owned())?;
        let frame_bytes = first.bytes().len();
        held.push(first);
        let slots = budget / frame_bytes;
        for sequence in 1..slots {
            original
                .session
                .as_ref()
                .unwrap()
                .send(
                    body(sequence),
                    RakReliability::Unreliable,
                    RakPriority::Immediate,
                )
                .await
                .map_err(|error| format!("original held-credit send: {error}"))?;
            held.push(
                original
                    .datagrams
                    .recv()
                    .await
                    .ok_or_else(|| "original held-credit output closed".to_owned())?,
            );
        }
        original
            .session
            .as_ref()
            .unwrap()
            .send(
                body(slots),
                RakReliability::Unreliable,
                RakPriority::Immediate,
            )
            .await
            .map_err(|error| format!("original blocked-output send: {error}"))?;
        // Actual dequeued frames retain credit, as does the real socket-send head.
        // The same-sized next encoded frame cannot fit that original route budget.
        let equal_frames = held.iter().all(|frame| frame.bytes().len() == frame_bytes);
        let retained_bytes: usize = held.iter().map(|frame| frame.bytes().len()).sum();
        tokio::time::advance(expiry.saturating_duration_since(Instant::now())).await;
        (&mut actor)
            .await
            .map_err(|error| format!("original expiry join: {error}"))?;
        let terminal_read = original.session.as_mut().unwrap().recv::<Box<[u8]>>().await;
        let session = original.session.take().unwrap();
        let reason = match session.finish() {
            Ok(terminal) => terminal.reason(),
            Err(session) => {
                drop(session);
                return Err("original terminal ownership was not available".to_owned());
            }
        };
        Ok::<_, String>((
            equal_frames,
            retained_bytes,
            frame_bytes,
            terminal_read,
            reason,
        ))
    })
    .await;
    if !actor.is_finished() {
        actor.abort();
        let _ = actor.await;
    }
    let peer_finished = original.peer.take().map(|peer| {
        let mut closing = match peer.abort(Instant::now().into_std()) {
            Ok(closing) => closing,
            Err(error) => error.into_parts().0,
        };
        while closing.poll().is_some() {}
        closing.finish().is_ok()
    });
    drop(original);
    drop(held);
    assert_eq!(peer_finished, Some(true), "original peer cleanup failed");
    let (equal_frames, retained_bytes, frame_bytes, terminal_read, reason) = observed
        .expect("original blocked actor must retire under its unchanged peer deadline")
        .expect("original wire and terminal owners must complete");
    assert!(equal_frames && retained_bytes <= budget && retained_bytes + frame_bytes > budget);
    assert!(matches!(terminal_read, Err(RakSessionError::Closed)));
    assert_eq!(reason, Some(RakDisconnectReason::Timeout));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dropping_public_session_owners_forwards_the_original_peer_disconnect() {
    let mut observations = Vec::new();
    for closing_first in [false, true] {
        let now = Instant::now();
        let config = RakSessionConfig {
            max_outbound_bytes: OUTBOUND_BYTES,
            ..Default::default()
        };
        let (local, peer) = crate::test_support::negotiated_pair(
            "127.0.0.1:54321".parse().unwrap(),
            "127.0.0.1:19132".parse().unwrap(),
            now.into_std(),
            now.into_std(),
            config.clone(),
        );
        let peer_expiry = Instant::from_std(peer.expiry_deadline(now.into_std()).unwrap());
        let cutoff = now + config.close_timeout;
        let mut original = Original::new(local, peer);
        let mut actor = original.actor.take().unwrap();
        let session = original.session.take().unwrap();
        let close_admitted = if closing_first {
            match session.close() {
                Ok(closing) => {
                    drop(closing);
                    true
                }
                Err(error) => {
                    drop(error.into_parts().0);
                    false
                }
            }
        } else {
            drop(session);
            true
        };
        // Keep the actual wire consumer and input route alive after the public owner drops.
        let observed = tokio::time::timeout_at(cutoff, async {
            let mut joined = false;
            let mut wire_open = true;
            let mut wire_frames = 0;
            let mut remote_reason = None;
            while !joined || wire_open {
                tokio::select! {
                    result = &mut actor, if !joined => {
                        result.map_err(|error| format!("original dropped-owner join: {error}"))?;
                        joined = true;
                    }
                    frame = original.datagrams.recv(), if wire_open => {
                        let Some(frame) = frame else {
                            wire_open = false;
                            continue;
                        };
                        let (bytes, address, _credit) = frame.into_parts();
                        if address != original.address {
                            return Err("original dropped-owner wire destination changed".to_owned());
                        }
                        wire_frames += 1;
                        let peer = original.peer.as_mut().unwrap();
                        peer.handle(RakSessionInput::Datagram(bytes, Instant::now().into_std()))
                            .map_err(|error| format!("original dropped-owner peer input: {error}"))?;
                        while let Some(output) = peer.poll() {
                            if let RakSessionOutput::Disconnected(_, reason) = output {
                                remote_reason = Some(reason);
                            }
                        }
                    }
                }
            }
            Ok::<_, String>((
                wire_frames,
                remote_reason,
                original.peer.as_ref().unwrap().state(),
                original.disconnected.try_recv(),
                Instant::now() < peer_expiry && cutoff < peer_expiry,
            ))
        }).await;
        if !actor.is_finished() {
            actor.abort();
            let _ = actor.await;
        }
        let peer = original.peer.take().unwrap();
        let mut cleanup = match peer.abort(Instant::now().into_std()) {
            Ok(closing) => closing,
            Err(error) => error.into_parts().0,
        };
        while cleanup.poll().is_some() {}
        let peer_finished = cleanup.finish().is_ok();
        let id = original.id;
        drop(original);
        observations.push((closing_first, close_admitted, observed, peer_finished, id));
    }
    let mut missing = Vec::new();
    for (closing_first, close_admitted, observed, peer_finished, id) in observations {
        assert!(
            peer_finished,
            "original peer cleanup failed: closing_first={closing_first}"
        );
        assert!(close_admitted, "the original local close was not admitted");
        let (wire_frames, reason, state, retired, before_stale) = observed
            .expect("the original dropped actor and wire consumer retire before their cutoff")
            .expect("original route and peer input remain valid");
        assert_eq!(retired, Ok(id), "original route retirement changed");
        assert!(
            before_stale,
            "the peer's own stale timer cannot explain retirement"
        );
        if wire_frames == 0
            || reason != Some(RakDisconnectReason::Remote)
            || state != RakSessionState::Disconnected
        {
            missing.push(format!("closing_first={closing_first}, wire_frames={wire_frames}, reason={reason:?}, state={state:?}"));
        }
    }
    assert!(
        missing.is_empty(),
        "dropped public owners did not forward their actual peer disconnect: {}",
        missing.join("; ")
    );
}

/// Holds only this actor after one completed final delivery attempt and its original failure path.
pub(super) struct ReceiveGate {
    remaining: usize,
    entered: Option<oneshot::Sender<(usize, RakSessionState)>>,
    release: Option<oneshot::Receiver<()>>,
}

impl ReceiveGate {
    /// Observes no payload; the original try_send result and failed body have already been disposed.
    pub(super) async fn after_attempt(&mut self, queued: usize, state: RakSessionState) {
        if self.entered.is_none() {
            return;
        }
        self.remaining -= 1;
        if self.remaining == 0 {
            let _ = self.entered.take().unwrap().send((queued, state));
            if let Some(release) = self.release.take() {
                let _ = release.await;
            }
        }
    }
}

/// Gives every legal short message a distinct identity through the full canonical receive count.
fn count_body(sequence: usize) -> Box<[u8]> {
    let mut bytes = vec![0x41; BODY_BYTES];
    bytes[0] = 0xfe;
    bytes[1..5].copy_from_slice(&u32::try_from(sequence).unwrap().to_le_bytes());
    bytes.into_boxed_slice()
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn final_receive_refusal_cannot_admit_a_successor_after_a_freed_prefix_slot() {
    let now = Instant::now().into_std();
    let (local, peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        Default::default(),
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
    let address = local.addr();
    let id = local.id();
    let limit = local.max_message_len();
    let mtu = local.mtu();
    let mut driver = Driver::established(local);
    driver.receive_gate = Some(ReceiveGate {
        remaining: constants::MAX_RECEIVE_MESSAGES + 1,
        entered: Some(entered),
        release: Some(release_rx),
    });
    let (datagram_tx, datagrams) = wire::datagrams();
    let (disconnect_tx, disconnected) = unbounded_channel();
    let (mut session, input) = spawn(driver, address, id, limit, mtu, datagram_tx, disconnect_tx);
    let actor = session.actor.take();
    let mut original = Original {
        session: Some(session),
        actor,
        input,
        datagrams,
        disconnected,
        peer: Some(peer),
        address,
        id,
        received: Vec::new(),
    };
    let mut healthy = Original::new(healthy_local, healthy_peer);
    let cutoff = Instant::now() + constants::CLOSE_TIMEOUT + constants::CLOSE_TIMEOUT;
    let observed = tokio::time::timeout_at(cutoff, async {
        let before = b"\xfehealthy neighbor before held final receive";
        let before_echo = healthy.echo(before, cutoff).await?;
        for sequence in 0..constants::MAX_RECEIVE_MESSAGES {
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
                .map_err(|error| format!("original prefix send: {error}"))?;
        }
        original
            .peer
            .as_mut()
            .unwrap()
            .tick(Instant::now().into_std())
            .map_err(|error| format!("original prefix flush: {error}"))?;
        while let Some(output) = original.peer.as_mut().unwrap().poll() {
            if let RakSessionOutput::Datagram(bytes, _) = output {
                original
                    .input
                    .send(SessionEvent::Io(RakSessionInput::Datagram(
                        bytes,
                        Instant::now().into_std(),
                    )))
                    .await
                    .map_err(|error| format!("original prefix route: {error}"))?;
            }
        }
        // Real acknowledgements advance the original reliable window without reading the final FIFO.
        while original.peer.as_ref().unwrap().outbound_bytes() != 0 {
            let frame = original
                .datagrams
                .recv()
                .await
                .ok_or_else(|| "original prefix acknowledgement route closed".to_owned())?;
            original.forward(frame, false, false)?;
        }
        let peer = original.peer.as_mut().unwrap();
        for sequence in constants::MAX_RECEIVE_MESSAGES..constants::MAX_RECEIVE_MESSAGES + 2 {
            peer.send(
                count_body(sequence),
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
                Instant::now().into_std(),
            )
            .map_err(|error| format!("original coalesced send: {error}"))?;
        }
        peer.tick(Instant::now().into_std())
            .map_err(|error| format!("original coalesced flush: {error}"))?;
        let mut pair = None;
        while let Some(output) = peer.poll() {
            if let RakSessionOutput::Datagram(bytes, _) = output
                && pair.replace(bytes).is_some()
            {
                return Err("the original two-message tail used more than one datagram".to_owned());
            }
        }
        original
            .input
            .send(SessionEvent::Io(RakSessionInput::Datagram(
                pair.ok_or_else(|| "the original two-message datagram was not emitted".to_owned())?,
                Instant::now().into_std(),
            )))
            .await
            .map_err(|error| format!("original coalesced route: {error}"))?;
        let full = entered_rx
            .await
            .map_err(|_| "the original final delivery gate was not reached".to_owned())?;
        let first = original
            .session
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|error| format!("original accepted front: {error}"))?;
        let during = b"\xfehealthy neighbor while original final receive stays gated";
        let during_echo = healthy.echo(during, cutoff).await?;
        release
            .take()
            .unwrap()
            .send(())
            .map_err(|_| "the original receive gate disappeared before release".to_owned())?;
        let mut delivered = vec![first];
        while let Ok(bytes) = original.session.as_mut().unwrap().recv::<Box<[u8]>>().await {
            delivered.push(bytes);
        }
        Ok::<_, String>((full, delivered, before_echo, during_echo, before, during))
    })
    .await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    let original_cleanup = original.finish().await;
    let healthy_cleanup = healthy.finish().await;
    drop(original);
    drop(healthy);
    assert!(
        original_cleanup.is_ok(),
        "original cleanup: {original_cleanup:?}"
    );
    assert!(
        healthy_cleanup.is_ok(),
        "healthy cleanup: {healthy_cleanup:?}"
    );
    let ((queued, state), delivered, before_echo, during_echo, before, during) = observed
        .expect("the bounded original delivery observation must complete")
        .expect("the actual peer, gate and healthy neighbor must progress");
    assert_eq!(queued, constants::MAX_RECEIVE_MESSAGES);
    assert_eq!(state, RakSessionState::Disconnected);
    assert_eq!(before_echo.0.as_ref(), before);
    assert_eq!(before_echo.1.as_ref(), before);
    assert_eq!(during_echo.0.as_ref(), during);
    assert_eq!(during_echo.1.as_ref(), during);
    assert!(delivered.len() >= constants::MAX_RECEIVE_MESSAGES);
    for (sequence, bytes) in delivered[..constants::MAX_RECEIVE_MESSAGES]
        .iter()
        .enumerate()
    {
        assert_eq!(
            bytes,
            &count_body(sequence),
            "the original accepted prefix changed"
        );
    }
    assert!(!delivered.contains(&count_body(constants::MAX_RECEIVE_MESSAGES)));
    assert_eq!(
        delivered.len(),
        constants::MAX_RECEIVE_MESSAGES,
        "final delivery resumed after its refused front when an old prefix slot was freed"
    );
}

#[path = "final_receive_byte_tests.rs"]
mod final_receive_byte_tests;
