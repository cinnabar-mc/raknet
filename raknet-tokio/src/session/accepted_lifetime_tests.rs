//! Actual accepted handles retain their configured slot through unread retirement.

use super::*;
use crate::prelude::{RakClient, RakServer, RunningClient, RunningServer};
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::task::JoinHandle;

const OUTBOUND_BYTES: i32 = 2048;

type Echo = (Box<[u8]>, Box<[u8]>);

/// Echoes through both original public established handles on the same socket owners.
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

/// Retains the same originally spawned task before any consuming close or drop.
fn retain_actor(session: &mut RakSession<Established>, actors: &mut Vec<JoinHandle<()>>) {
    actors.push(
        session
            .actor
            .take()
            .expect("the original actor remains owned"),
    );
}

/// Consumes send permission and drops the same locally closing owner.
fn close(session: RakSession<Established>) {
    match session.close() {
        Ok(closing) => drop(closing),
        Err(error) => drop(error.into_parts().0),
    }
}

/// Observes the real endpoint after actual actor retirement, never inferring it from elapsed time.
async fn routes(server: &RakServer<RunningServer>, expected: usize) -> Result<(), String> {
    for _ in 0..constants::MAX_RECEIVE_MESSAGES {
        let count = server
            .route_count_probe()
            .await
            .map_err(|_| "route probe closed".to_owned())?;
        if count == expected {
            return Ok(());
        }
    }
    Err("actual route transition exhausted its finite progress watchdog".to_owned())
}

#[tokio::test(flavor = "current_thread")]
async fn unaccepted_retired_session_keeps_its_slot_until_original_owner_drains() {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut server = RakServer::new((Ipv4Addr::LOCALHOST, 0).into(), |config| {
        config.max_connections = 2;
        config.session.max_outbound_bytes = OUTBOUND_BYTES;
    })
    .start()
    .await
    .unwrap();
    let mut clients: Vec<RakClient<RunningClient>> = Vec::new();
    for _ in 0..4 {
        clients.push(
            RakClient::new(|config| {
                config.session.max_outbound_bytes = OUTBOUND_BYTES;
            })
            .start()
            .await
            .unwrap(),
        );
    }
    let mut sessions = Vec::new();
    let mut actors = Vec::new();
    let mut stage = "bound socket observation";
    let observed = tokio::time::timeout_at(deadline, async {
        let address = server
            .command_queue_probe()
            .await
            .map_err(|_| "bound socket probe closed".to_owned())?
            .address
            .ok_or_else(|| "the original bound socket has no address".to_owned())?;
        stage = "healthy public handshake";
        let mut healthy = clients[0]
            .connect(address)
            .await
            .map_err(|error| format!("healthy connect: {error}"))?;
        retain_actor(&mut healthy, &mut actors);
        sessions.push(healthy);
        let mut accepted = server
            .accept()
            .await
            .map_err(|error| format!("healthy accept: {error}"))?;
        retain_actor(&mut accepted, &mut actors);
        sessions.push(accepted);
        let before = b"\xfehealthy while another handle is still unaccepted";
        let [healthy_client, healthy_server] = sessions.as_mut_slice() else {
            unreachable!()
        };
        let healthy_before = echo(healthy_client, healthy_server, before)
            .await
            .map_err(|error| format!("initial healthy echo: {error}"))?;

        stage = "unaccepted public handshake";
        let mut first = clients[1]
            .connect(address)
            .await
            .map_err(|error| format!("unaccepted connect: {error}"))?;
        retain_actor(&mut first, &mut actors);
        sessions.push(first);
        routes(&server, 2).await?;
        let retained = b"\xfeoriginal reliable prefix before unaccepted retirement";
        sessions
            .last()
            .expect("the original first client is retained")
            .send(
                retained.to_vec().into_boxed_slice(),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await
            .map_err(|error| format!("original prefix send: {error}"))?;
        let first = sessions
            .pop()
            .expect("the first client's actual handle is owned");
        stage = "original client graceful retirement";
        let mut closing = match first.close() {
            Ok(closing) => closing,
            Err(error) => return Err(format!("original client close: {}", error.into_parts().1)),
        };
        let mut unexpected_input = false;
        while closing.recv::<Box<[u8]>>().await.is_ok() {
            unexpected_input = true;
        }
        drop(closing);
        stage = "actual unaccepted server route removal";
        routes(&server, 1).await?;
        let queued_before = server.unaccepted_count();
        let [healthy_client, healthy_server] = sessions.as_mut_slice() else {
            unreachable!()
        };
        let healthy_during = echo(healthy_client, healthy_server, before)
            .await
            .map_err(|error| format!("healthy echo after actual route retirement: {error}"))?;

        stage = "admission while retired original handle is unread";
        let admitted_too_early = match clients[2].connect(address).await {
            Ok(mut premature) => {
                retain_actor(&mut premature, &mut actors);
                sessions.push(premature);
                routes(&server, 2).await?;
                true
            }
            Err(RakClientError::NoFreeIncomingConnections) => false,
            Err(error) => {
                return Err(format!(
                    "unexpected admission result instead of capacity refusal: {error}"
                ));
            }
        };
        stage = "original FIFO prefix and terminal drain";
        // This is still the oldest original handle; no earlier accept removed it.
        let mut original = server
            .accept()
            .await
            .map_err(|error| format!("retained original accept: {error}"))?;
        retain_actor(&mut original, &mut actors);
        let prefix = original
            .recv::<Box<[u8]>>()
            .await
            .map_err(|error| format!("retained original prefix: {error}"))?;
        let terminal = original.recv::<Box<[u8]>>().await;
        let finished = match original.finish() {
            Ok(_) => true,
            Err(original) => {
                drop(original);
                false
            }
        };
        if admitted_too_early {
            stage = "baseline unexpected peer retirement";
            let premature = sessions
                .pop()
                .expect("the unexpected real client is retained");
            close(premature);
            let mut premature = server
                .accept()
                .await
                .map_err(|error| format!("unexpected original server accept: {error}"))?;
            retain_actor(&mut premature, &mut actors);
            close(premature);
            routes(&server, 1).await?;
        }

        stage = "fresh public handshake after original owner drain";
        let mut fresh = clients[3]
            .connect(address)
            .await
            .map_err(|error| format!("fresh connect after drain: {error}"))?;
        retain_actor(&mut fresh, &mut actors);
        sessions.push(fresh);
        let mut accepted = server
            .accept()
            .await
            .map_err(|error| format!("fresh accept after drain: {error}"))?;
        retain_actor(&mut accepted, &mut actors);
        sessions.push(accepted);
        let after = b"\xfefresh admission after original owner drains";
        let [healthy_client, healthy_server, fresh_client, fresh_server] = sessions.as_mut_slice()
        else {
            unreachable!()
        };
        stage = "fresh and healthy final markers";
        let fresh_echo = echo(fresh_client, fresh_server, after)
            .await
            .map_err(|error| format!("fresh original echo: {error}"))?;
        let healthy_after = echo(healthy_client, healthy_server, after)
            .await
            .map_err(|error| format!("final healthy echo: {error}"))?;
        Ok::<_, String>((
            admitted_too_early,
            queued_before,
            prefix,
            terminal,
            finished,
            unexpected_input,
            healthy_before,
            healthy_during,
            healthy_after,
            fresh_echo,
            before,
            after,
            retained,
        ))
    })
    .await;

    for session in sessions {
        close(session);
    }
    // Any baseline-unexpected accepted handle is still drained and its actual task retained.
    while server.unaccepted_count() != 0 {
        let mut session = server
            .accept()
            .await
            .expect("the original queue reported a retained handle");
        retain_actor(&mut session, &mut actors);
        close(session);
    }
    let Ok([healthy, original, premature, fresh]) =
        <[RakClient<RunningClient>; 4]>::try_from(clients)
    else {
        unreachable!("the four original client socket owners were retained")
    };
    tokio::join!(
        healthy.stop(),
        original.stop(),
        premature.stop(),
        fresh.stop(),
        server.stop()
    );
    let actor_count = actors.len();
    let cleanup_deadline = Instant::now() + constants::CLOSE_TIMEOUT;
    let mut cleanup_errors = Vec::new();
    for mut actor in actors {
        match tokio::time::timeout_at(cleanup_deadline, &mut actor).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => cleanup_errors.push(format!("original actor join: {error}")),
            Err(_) => {
                actor.abort();
                let _ = actor.await;
                cleanup_errors.push("original actor needed abort after endpoint stops".to_owned());
            }
        }
    }
    assert!(
        cleanup_errors.is_empty(),
        "setup cleanup: {cleanup_errors:?}"
    );
    let (
        admitted_too_early,
        queued_before,
        prefix,
        terminal,
        finished,
        unexpected_input,
        healthy_before,
        healthy_during,
        healthy_after,
        fresh_echo,
        before,
        after,
        retained,
    ) = observed
        .unwrap_or_else(|_| panic!("setup: original cutoff expired during {stage}"))
        .expect(
            "setup: actual route retirement, retained prefix and fresh/healthy peers must complete",
        );
    assert_eq!(
        actor_count,
        if admitted_too_early { 8 } else { 6 },
        "both original actors from every actual completed handshake were retained and joined"
    );
    assert_eq!(
        queued_before, 1,
        "the original retired handle must remain in its original FIFO"
    );
    assert_eq!(prefix.as_ref(), retained);
    assert!(matches!(terminal, Err(RakSessionError::Closed)) && finished && !unexpected_input);
    for echo in [healthy_before, healthy_during] {
        assert_eq!(echo.0.as_ref(), before);
        assert_eq!(echo.1.as_ref(), before);
    }
    for echo in [healthy_after, fresh_echo] {
        assert_eq!(echo.0.as_ref(), after);
        assert_eq!(echo.1.as_ref(), after);
    }
    assert!(
        !admitted_too_early,
        "actual route retirement released capacity while the original accepted handle still retained unread reliable data and terminal metadata"
    );
}
