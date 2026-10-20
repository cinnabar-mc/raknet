//! Weak endpoint reservations follow actual typed retirement without retaining its owner.

use super::*;

#[tokio::test]
async fn weak_lifetime_remains_through_closing_and_releases_only_after_finish() {
    let now = Instant::now().into_std();
    let (local, _peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        Default::default(),
    );
    let (datagram_tx, _datagram_rx) = unbounded_channel();
    let (disconnect_tx, _disconnect_rx) = unbounded_channel();
    let (mut session, input) = spawn_established(local, datagram_tx, disconnect_tx);
    let mut actor = session.actor.take().expect("the original actor is owned");
    let retirement = Arc::new(SessionRetirement::default());
    let lifetime = session.lifetime(&retirement);
    let mut closing = match session.close() {
        Ok(closing) => closing,
        Err(error) => error.into_parts().0,
    };
    let observed = tokio::time::timeout(constants::CLOSE_TIMEOUT, async {
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
        Ok::<_, String>((terminal, retired, held, finished))
    })
    .await;
    drop(input);
    if !actor.is_finished() {
        actor.abort();
        let _ = actor.await;
    }
    let (terminal, retired, held, finished) = observed
        .expect("original actor retirement reached its cutoff")
        .expect("the original actor joined normally");
    assert!(matches!(terminal, Err(RakSessionError::Closed)));
    assert!(retired && held && finished);
    assert!(
        lifetime.released(),
        "weak metadata cannot retain a finished original lifetime"
    );
}
