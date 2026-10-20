//! A delayed output observer cannot move the original public close cutoff.

use super::*;
use std::time::Duration;

/// Holds this actor's next real Wait after its original closing wire output was admitted.
pub(super) struct WaitGate {
    pub(super) admitted: bool,
    entered: Option<oneshot::Sender<Duration>>,
    release: oneshot::Receiver<()>,
}

impl WaitGate {
    /// Delays observation only; the original output and its relative duration stay unchanged.
    pub(super) async fn before_wait(&mut self, after: Duration) {
        if self.admitted
            && let Some(entered) = self.entered.take()
        {
            let _ = entered.send(after);
            let _ = (&mut self.release).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_close_wait_retains_the_original_cutoff() {
    let now = Instant::now();
    let (mut local, mut peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now.into_std(),
        now.into_std(),
        RakSessionConfig {
            autoflush_interval_ms: constants::CLOSE_TIMEOUT,
            ..Default::default()
        },
    );
    let prefix = b"\xfeaccepted before the original public close";
    peer.send(
        Box::<[u8]>::from(prefix.as_slice()),
        RakReliability::ReliableOrdered,
        RakPriority::Immediate,
        now.into_std(),
    )
    .unwrap();
    while let Some(output) = peer.poll() {
        if let RakSessionOutput::Datagram(bytes, _) = output {
            local
                .handle(RakSessionInput::Datagram(bytes, now.into_std()))
                .unwrap();
        }
    }
    let address = local.addr();
    let id = local.id();
    let limit = local.max_message_len();
    let mtu = local.mtu();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let mut release = Some(release);
    let mut driver = Driver::established(local);
    driver.wait_gate = Some(WaitGate {
        admitted: false,
        entered: Some(entered),
        release: release_rx,
    });
    let (datagram_tx, mut datagrams) = wire::datagrams();
    let (disconnect_tx, mut disconnected) = unbounded_channel();
    let (mut session, input) =
        spawn::<Established>(driver, address, id, limit, mtu, datagram_tx, disconnect_tx);
    let mut actor = session.actor.take().unwrap();
    let lifetime = session.lifetime(None);
    let first_cutoff = Instant::now() + constants::CLOSE_TIMEOUT;
    let mut closing = match session.close() {
        Ok(closing) => closing,
        Err(error) => error.into_parts().0,
    };
    let mut held_wire = Vec::new();
    // This destruction bound does not extend the protocol deadline asserted below.
    let observed = tokio::time::timeout_at(first_cutoff + constants::CLOSE_TIMEOUT, async {
        let after = entered_rx.await.map_err(|error| error.to_string())?;
        closing
            .close_ack
            .take()
            .unwrap()
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        while let Ok(frame) = datagrams.try_recv() {
            held_wire.push(frame);
        }
        let closing_held = !lifetime.actor_retired() && !lifetime.released();
        let accepted = closing.recv::<Box<[u8]>>().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        release
            .take()
            .unwrap()
            .send(())
            .map_err(|_| "the original Wait gate disappeared".to_owned())?;
        (&mut actor).await.map_err(|error| error.to_string())?;
        let completed_at = Instant::now();
        let end = closing.recv::<Box<[u8]>>().await;
        let retired = lifetime.actor_retired();
        let metadata = closing
            .finish()
            .map_err(|_| "the original terminal was unavailable after join".to_owned())?;
        Ok::<_, String>((
            after,
            accepted,
            end,
            closing_held,
            retired,
            metadata,
            completed_at,
        ))
    })
    .await;
    if let Some(release) = release {
        let _ = release.send(());
    }
    drop(input);
    let joined_normally = actor.is_finished();
    if !actor.is_finished() {
        actor.abort();
        let _ = actor.await;
    }
    while let Ok(frame) = datagrams.try_recv() {
        held_wire.push(frame);
    }
    let route_retired = disconnected.try_recv() == Ok(id);
    let mut wire_identity = !held_wire.is_empty();
    let mut remote_closed = false;
    let mut peer_input_ok = true;
    for frame in held_wire {
        wire_identity &= frame.peer() == address && frame.id() == id && frame.belongs_to(&lifetime);
        let (bytes, _, _credit) = frame.into_parts();
        if peer.state() != RakSessionState::Disconnected {
            peer_input_ok &= peer
                .handle(RakSessionInput::Datagram(bytes, Instant::now().into_std()))
                .is_ok();
        }
        while let Some(output) = peer.poll() {
            if let RakSessionOutput::Disconnected(_, reason) = output {
                remote_closed |= reason == RakDisconnectReason::Remote;
            }
        }
    }
    let peer_finished = if peer.state() == RakSessionState::Disconnected {
        peer.finish().is_ok()
    } else {
        let mut closing = match peer.abort(Instant::now().into_std()) {
            Ok(closing) => closing,
            Err(error) => error.into_parts().0,
        };
        while closing.poll().is_some() {}
        closing.finish().is_ok()
    };
    drop(datagrams);
    drop(disconnected);
    let released = lifetime.released();
    let (after, accepted, end, closing_held, retired, metadata, completed_at) = observed
        .expect("the original actor reached its finite observation bound")
        .expect("the original actor completed its close and joined");
    assert!(
        joined_normally
            && route_retired
            && peer_input_ok
            && peer_finished
            && remote_closed
            && wire_identity
    );
    assert_eq!(after, constants::CLOSE_TIMEOUT);
    assert_eq!(accepted.unwrap().as_ref(), prefix);
    assert!(matches!(end, Err(RakSessionError::Closed)));
    assert!(closing_held && retired && released);
    assert_eq!(metadata.id(), id);
    assert_eq!(metadata.reason(), Some(RakDisconnectReason::Requested));
    assert!(
        completed_at <= first_cutoff,
        "delaying an already-produced Wait moved the first close cutoff"
    );
}
