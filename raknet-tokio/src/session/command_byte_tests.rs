//! Actual established session commands retain bounded payload work before actor admission.

use super::*;
use crate::prelude::{RakClient, RakServer};
use std::future::{Future, poll_fn};
use std::net::Ipv4Addr;
use std::task::Poll;
use std::time::Duration;

const BODY_BYTES: usize = 128;
const OUTBOUND_BYTES: i32 = 2048;

type Echo = (Box<[u8]>, Box<[u8]>);

/// Echoes an actual reliable application body through both original established actors.
async fn echo(
    client: &mut RakSession<Established>,
    server: &mut RakSession<Established>,
    marker: &[u8],
) -> Result<Echo, RakSessionError> {
    client
        .send(
            marker.to_vec().into_boxed_slice(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
        )
        .await?;
    let request = server.recv::<Box<[u8]>>().await?;
    server
        .send(
            request.clone(),
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
        )
        .await?;
    Ok((request, client.recv::<Box<[u8]>>().await?))
}

#[tokio::test(flavor = "current_thread")]
async fn established_session_commands_bound_actual_queued_payload_before_actor_progress() {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut server = RakServer::new((Ipv4Addr::LOCALHOST, 0).into(), |config| {
        config.max_connections = 2;
        config.session.max_outbound_bytes = OUTBOUND_BYTES;
    })
    .start()
    .await
    .unwrap();
    let client = RakClient::new(|config| {
        config.session.max_outbound_bytes = OUTBOUND_BYTES;
    })
    .start()
    .await
    .unwrap();
    let neighbor = RakClient::new(|config| {
        config.session.max_outbound_bytes = OUTBOUND_BYTES;
    })
    .start()
    .await
    .unwrap();
    let mut sessions = Vec::new();
    let observed = async {
        let address = tokio::time::timeout_at(deadline, server.command_queue_probe())
            .await
            .map_err(|_| "bound socket probe timed out".to_owned())?
            .map_err(|_| "bound socket probe closed".to_owned())?
            .address
            .ok_or_else(|| "original bound socket has no address".to_owned())?;
        sessions.push(
            tokio::time::timeout_at(deadline, client.connect(address))
                .await
                .map_err(|_| "first public connect timed out".to_owned())?
                .map_err(|error| format!("first public connect: {error}"))?,
        );
        sessions.push(
            tokio::time::timeout_at(deadline, server.accept())
                .await
                .map_err(|_| "first public accept timed out".to_owned())?
                .map_err(|error| format!("first public accept: {error}"))?,
        );
        sessions.push(
            tokio::time::timeout_at(deadline, neighbor.connect(address))
                .await
                .map_err(|_| "neighbor public connect timed out".to_owned())?
                .map_err(|error| format!("neighbor public connect: {error}"))?,
        );
        sessions.push(
            tokio::time::timeout_at(deadline, server.accept())
                .await
                .map_err(|_| "neighbor public accept timed out".to_owned())?
                .map_err(|error| format!("neighbor public accept: {error}"))?,
        );
        if sessions
            .iter()
            .any(|session| session.max_message_len() < BODY_BYTES)
        {
            return Err(
                "original negotiated send limit is smaller than the fixture body".to_owned(),
            );
        }
        let before = b"\xfehealthy neighbor before original session queue pressure";
        let [_, _, healthy_client, healthy_server] = sessions.as_mut_slice() else {
            unreachable!("the four original handshake owners were retained")
        };
        let healthy_before =
            tokio::time::timeout_at(deadline, echo(healthy_client, healthy_server, before))
                .await
                .map_err(|_| "initial neighbor echo timed out".to_owned())?
                .map_err(|error| format!("initial neighbor echo: {error}"))?;

        // No other task can run before all these public futures have been polled.
        let (send, receive) = oneshot::channel();
        sessions[0]
            .msg_tx
            .send(RakSessionMsg::InspectCommands(send))
            .map_err(|_| "original established command owner closed".to_owned())?;
        let mut commands: Vec<_> = (0..=constants::MAX_RECEIVE_MESSAGES)
            .map(|_| {
                let mut body = vec![0; BODY_BYTES];
                body[0] = 0xfe;
                Box::pin(sessions[0].send(
                    body.into_boxed_slice(),
                    RakReliability::ReliableOrdered,
                    RakPriority::Immediate,
                ))
            })
            .collect();
        let mut completed_before_actor = false;
        tokio::task::unconstrained(poll_fn(|context| {
            for command in &mut commands {
                if Future::poll(command.as_mut(), context).is_ready() {
                    completed_before_actor = true;
                }
            }
            Poll::Ready(())
        }))
        .await;
        let queued = tokio::time::timeout_at(deadline, receive)
            .await
            .map_err(|_| "original FIFO front probe timed out".to_owned())?
            .map_err(|_| "original FIFO front probe closed".to_owned())?;
        // Dropping admitted reply waits never retracts their original queued messages.
        drop(commands);
        let bytes = queued
            .checked_mul(BODY_BYTES)
            .ok_or_else(|| "controlled queued body sum overflowed".to_owned())?;

        let after = b"\xfehealthy neighbor after canceled original send waits";
        let [_, _, healthy_client, healthy_server] = sessions.as_mut_slice() else {
            unreachable!("the original four handshake owners stay retained")
        };
        let healthy_after =
            tokio::time::timeout_at(deadline, echo(healthy_client, healthy_server, after))
                .await
                .map_err(|_| "post-pressure neighbor echo timed out".to_owned())?
                .map_err(|error| format!("post-pressure neighbor echo: {error}"))?;
        Ok::<_, String>((
            queued,
            bytes,
            completed_before_actor,
            healthy_before,
            healthy_after,
            before,
            after,
        ))
    }
    .await;

    let mut actors = Vec::new();
    for mut session in sessions {
        actors.push(session.actor.take().expect("the original actor is owned"));
        match session.close() {
            Ok(closing) => drop(closing),
            Err(error) => drop(error.into_parts().0),
        }
    }
    tokio::join!(client.stop(), neighbor.stop(), server.stop());
    let cleanup_deadline = Instant::now() + constants::CLOSE_TIMEOUT;
    let mut cleanup_errors = Vec::new();
    for mut actor in actors {
        match tokio::time::timeout_at(cleanup_deadline, &mut actor).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => cleanup_errors.push(format!("original actor join: {error}")),
            Err(_) => {
                actor.abort();
                let _ = actor.await;
                cleanup_errors.push("original actor needed abort after endpoint stop".to_owned());
            }
        }
    }

    assert!(
        cleanup_errors.is_empty(),
        "setup cleanup: {cleanup_errors:?}"
    );
    let (queued, bytes, completed_before_actor, healthy_before, healthy_after, before, after) =
        observed
            .expect("setup: actual handshake, FIFO observation and healthy neighbor must complete");
    assert!(
        !completed_before_actor,
        "an original send completed without actor progress"
    );
    assert_eq!(healthy_before.0.as_ref(), before);
    assert_eq!(healthy_before.1.as_ref(), before);
    assert_eq!(healthy_after.0.as_ref(), after);
    assert_eq!(healthy_after.1.as_ref(), after);
    assert!(
        queued <= constants::MAX_RECEIVE_MESSAGES && bytes <= OUTBOUND_BYTES as usize,
        "original session commands exceeded finite admission before actor progress: queued={queued}, bytes={bytes}"
    );
}
