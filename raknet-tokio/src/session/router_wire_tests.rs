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

    /// Delivers the same frame once and returns the original peer's protocol output.
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
