//! Real actor controls for caller-owned admission and a close behind committed work.

use super::*;
use std::future::{Future, poll_fn};
use std::task::Poll;
use std::time::Duration;

const BODY_BYTES: usize = 128;
const OUTBOUND_BYTES: i32 = (BODY_BYTES * 16) as i32;

struct ActorPair {
    session: RakSession<Established>,
    peer: raknet::prelude::RakSession<Established>,
    input: Sender<SessionEvent>,
    datagrams: tokio::sync::mpsc::Receiver<wire::Datagram>,
}

/// Starts the original actor only after both real core handshake directions complete.
fn actor() -> ActorPair {
    let now = Instant::now().into_std();
    let (local, peer) = crate::test_support::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        RakSessionConfig {
            max_outbound_bytes: OUTBOUND_BYTES,
            ..Default::default()
        },
    );
    let (output, receive) = wire::datagrams();
    let (disconnected, _) = unbounded_channel();
    let (session, input) = spawn_established(local, output, disconnected);
    ActorPair {
        session,
        peer,
        input,
        datagrams: receive,
    }
}

/// Fills the actual command FIFO with finite original read-only requests.
fn fill_controls(session: &RakSession<Established>) {
    while session.msg_tx.capacity() != 0 {
        let (reply, _) = oneshot::channel();
        session
            .msg_tx
            .try_send(RakSessionMsg::IsClosed(reply))
            .unwrap();
    }
}

/// Retires and joins the same actor before any behavioral verdict.
async fn cleanup<S: SessionPhase>(
    mut session: RakSession<S>,
    input: Sender<SessionEvent>,
) -> Result<(), String> {
    let mut actor = session
        .actor
        .take()
        .expect("the original actor is retained");
    drop(input);
    drop(session);
    match tokio::time::timeout(constants::CLOSE_TIMEOUT, &mut actor).await {
        Ok(result) => result.map_err(|error| format!("original actor join: {error}")),
        Err(_) => {
            actor.abort();
            let _ = actor.await;
            Err("the original session actor did not retire".to_owned())
        }
    }
}

/// Reads actual original wire output through the other negotiated core owner.
async fn received(
    peer: &mut raknet::prelude::RakSession<Established>,
    datagrams: &mut tokio::sync::mpsc::Receiver<wire::Datagram>,
) -> Result<Box<[u8]>, String> {
    loop {
        let datagram = datagrams
            .recv()
            .await
            .ok_or_else(|| "original actor output closed".to_owned())?;
        let (bytes, _, _credit) = datagram.into_parts();
        peer.handle(RakSessionInput::Datagram(bytes, Instant::now().into_std()))
            .map_err(|error| format!("original peer input: {error}"))?;
        while let Some(output) = peer.poll() {
            if let RakSessionOutput::Packet(bytes) = output {
                return Ok(bytes);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_before_command_commit_returns_bytes_and_oversize_does_not_retire() {
    let ActorPair {
        session,
        mut peer,
        input,
        mut datagrams,
    } = actor();
    let observed = tokio::time::timeout(constants::CLOSE_TIMEOUT, async {
        let oversized = session
            .send(
                vec![0xfe; OUTBOUND_BYTES as usize + 1].into_boxed_slice(),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await;
        fill_controls(&session);
        let mut canceled = Box::pin(session.send(
            vec![0xfe; OUTBOUND_BYTES as usize].into_boxed_slice(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
        ));
        let pending =
            poll_fn(|context| Poll::Ready(canceled.as_mut().poll(context).is_pending())).await;
        drop(canceled);
        let mut body = vec![1; BODY_BYTES];
        body[0] = 0xfe;
        let admitted = session
            .send(
                body.clone().into_boxed_slice(),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await;
        let delivered = received(&mut peer, &mut datagrams).await?;
        Ok::<_, String>((oversized, pending, admitted, delivered, body))
    })
    .await;
    let cleanup = cleanup(session, input).await;
    assert!(cleanup.is_ok(), "cleanup: {cleanup:?}");
    let (oversized, pending, admitted, delivered, body) = observed
        .expect("actual admission completes before the cutoff")
        .expect("the original wire peer remains healthy");
    assert!(
        matches!(oversized, Err(RakSessionError::MessageSizeExceeded(n)) if n == OUTBOUND_BYTES as usize + 1)
    );
    assert!(pending, "the full original FIFO must withhold commitment");
    assert!(
        admitted.is_ok(),
        "canceled reservations must return their byte credit"
    );
    assert_eq!(delivered.as_ref(), body);
}

#[tokio::test(flavor = "current_thread")]
async fn close_outside_full_fifo_preserves_committed_body_and_closing_snapshot() {
    let ActorPair {
        session,
        mut peer,
        input,
        mut datagrams,
    } = actor();
    let original_actor = session.task.upgrade().unwrap();
    let body = vec![0xfe; BODY_BYTES].into_boxed_slice();
    let mut send = Box::pin(session.send(
        body.clone(),
        RakReliability::ReliableOrdered,
        RakPriority::Immediate,
    ));
    let (pending, was_full) = poll_fn(|context| {
        let _held = original_actor.hold();
        let pending = send.as_mut().poll(context).is_pending();
        fill_controls(&session);
        Poll::Ready((pending, session.msg_tx.capacity() == 0))
    })
    .await;
    // The reply wait is canceled after original queue commitment.
    drop(send);
    let (close_admitted, closing) = match session.close() {
        Ok(closing) => (true, closing),
        Err(error) => (false, error.into_parts().0),
    };
    let observed = tokio::time::timeout(constants::CLOSE_TIMEOUT, async {
        let snapshot = closing.snapshot().await;
        let delivered = received(&mut peer, &mut datagrams).await?;
        let restored_closing = snapshot.map(|snapshot| {
            matches!(
                snapshot.restore(Instant::now().into_std(), Duration::ZERO),
                Ok(raknet::prelude::RestoredSession::Closing(_))
            )
        });
        Ok::<_, String>((restored_closing, delivered))
    })
    .await;
    let cleanup = cleanup(closing, input).await;
    assert!(cleanup.is_ok(), "cleanup: {cleanup:?}");
    let (restored_closing, delivered) = observed
        .expect("close and the committed prefix complete before the cutoff")
        .expect("the original wire peer remains healthy");
    assert!(pending && was_full);
    assert!(close_admitted, "close does not need a free command slot");
    assert!(
        matches!(restored_closing, Ok(true)),
        "a closing capture cannot reopen send permission"
    );
    assert_eq!(
        delivered, body,
        "close retains the earlier committed body after reply cancellation"
    );
}
