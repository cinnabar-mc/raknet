//! Weak endpoint reservations follow actual typed retirement without retaining its owner.

use super::*;

#[tokio::test(start_paused = true)]
async fn weak_lifetime_remains_through_closing_and_releases_only_after_finish() {
    let now = Instant::now().into_std();
    let (local, _peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        Default::default(),
    );
    let (datagram_tx, datagram_rx) = wire::datagrams();
    let (disconnect_tx, _disconnect_rx) = unbounded_channel();
    let (mut session, input) = spawn_established(local, datagram_tx, disconnect_tx);
    let mut actor = session.actor.take().expect("the original actor is owned");
    let retirement = Arc::new(SessionRetirement::default());
    let lifetime = session.lifetime(Some(&retirement));
    let cutoff = Instant::now() + constants::CLOSE_TIMEOUT;
    let mut closing = match session.close() {
        Ok(closing) => closing,
        Err(error) => error.into_parts().0,
    };
    // The later observer bound grants no protocol grace; completion is checked against cutoff.
    let observed = tokio::time::timeout_at(cutoff + constants::CLOSE_TIMEOUT, async {
        // Observe the same actor's actual close admission before advancing its first cutoff.
        closing
            .close_ack
            .take()
            .unwrap()
            .await
            .map_err(|error| format!("original close receipt: {error}"))?
            .map_err(|error| format!("original close admission: {error}"))?;
        tokio::time::advance(cutoff.saturating_duration_since(Instant::now())).await;
        let terminal = closing.recv::<Box<[u8]>>().await;
        (&mut actor).await.map_err(|error| error.to_string())?;
        let completed_at = Instant::now();
        let retired = lifetime.actor_retired();
        let held = !lifetime.released();
        let finished = match closing.finish() {
            Ok(_) => true,
            Err(closing) => {
                drop(closing);
                false
            }
        };
        Ok::<_, String>((terminal, retired, held, finished, completed_at))
    })
    .await;
    drop(input);
    if !actor.is_finished() {
        actor.abort();
        let _ = actor.await;
    }
    let (terminal, retired, held, finished, completed_at) = observed
        .expect("original actor retirement reached its cutoff")
        .expect("the original actor joined normally");
    assert!(
        completed_at <= cutoff,
        "the original actor exceeded its first close cutoff"
    );
    assert!(matches!(terminal, Err(RakSessionError::Closed)));
    assert!(retired && held && finished);
    drop(datagram_rx);
    assert!(
        lifetime.released(),
        "weak metadata cannot retain a finished original lifetime"
    );
}

#[tokio::test(start_paused = true)]
async fn acknowledged_close_retains_weak_lifetime_until_finish() {
    let now = Instant::now().into_std();
    let (local, mut peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        Default::default(),
    );
    let (datagram_tx, mut datagram_rx) = wire::datagrams();
    let (disconnect_tx, _disconnect_rx) = unbounded_channel();
    let (mut session, input) = spawn_established(local, datagram_tx, disconnect_tx);
    let mut actor = session.actor.take().expect("the original actor is owned");
    let retirement = Arc::new(SessionRetirement::default());
    let lifetime = session.lifetime(Some(&retirement));
    let mut closing = match session.close() {
        Ok(closing) => closing,
        Err(error) => error.into_parts().0,
    };
    let observed = tokio::time::timeout(constants::CLOSE_TIMEOUT, async {
        let datagram = datagram_rx
            .recv()
            .await
            .ok_or_else(|| "the original wire closed before its disconnect".to_owned())?;
        let closing_held = !lifetime.actor_retired() && !lifetime.released();
        let (bytes, _, credit) = datagram.into_parts();
        drop(credit);
        peer.handle(RakSessionInput::Datagram(bytes, Instant::now().into_std()))
            .map_err(|error| format!("original peer disconnect: {error}"))?;
        let mut replies = 0;
        while let Some(output) = peer.poll() {
            if let RakSessionOutput::Datagram(bytes, _) = output {
                input
                    .send(SessionEvent::Io(RakSessionInput::Datagram(
                        bytes,
                        Instant::now().into_std(),
                    )))
                    .await
                    .map_err(|error| format!("original peer acknowledgment: {error}"))?;
                replies += 1;
            }
        }
        if peer.state() != RakSessionState::Disconnected || replies == 0 {
            return Err("the original peer did not acknowledge the disconnect".to_owned());
        }
        let terminal = closing.recv::<Box<[u8]>>().await;
        (&mut actor).await.map_err(|error| error.to_string())?;
        let retired = lifetime.actor_retired();
        let held = !lifetime.released();
        let finished = match closing.finish() {
            Ok(_) => true,
            Err(closing) => {
                drop(closing);
                false
            }
        };
        Ok::<_, String>((terminal, retired, held, finished, closing_held))
    })
    .await;
    drop(input);
    if !actor.is_finished() {
        actor.abort();
        let _ = actor.await;
    }
    let (terminal, retired, held, finished, closing_held) = observed
        .expect("the acknowledged close completes before its timeout")
        .expect("the original actor joined normally");
    assert!(
        closing_held,
        "the original lifetime remains until the peer ACK"
    );
    assert!(Instant::now().into_std().duration_since(now) < constants::CLOSE_TIMEOUT);
    assert!(matches!(terminal, Err(RakSessionError::Closed)));
    assert!(retired && held && finished);
    drop(datagram_rx);
    assert!(
        lifetime.released(),
        "weak metadata cannot retain a finished original lifetime"
    );
}
