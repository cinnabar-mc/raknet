//! Accepted extreme timer policy stays driveable through the public session owner.

use raknet::prelude::*;
#[path = "support/negotiated.rs"]
mod negotiated;
use std::time::{Duration, Instant};

const UPDATE_LIMIT: usize = 4096;

/// Obtains both real session owners through the existing bounded handshake fixture.
fn established(config: RakSessionConfig, now: Instant) -> RakSession<Established> {
    config.validate().unwrap();
    let (session, _peer) = negotiated::negotiated_pair(
        "127.0.0.1:54321".parse().unwrap(),
        "127.0.0.1:19132".parse().unwrap(),
        now,
        now,
        config,
    );
    session
}

/// Consumes actual waits until the silent peer retires, preserving terminal output order.
fn drive_until_terminal<S: SessionPhase>(
    mut session: RakSession<S>,
    mut now: Instant,
    wait_bound: Option<Duration>,
    expected: RakDisconnectReason,
) {
    let id = session.id();
    let peer = session.addr();
    let mut datagrams = 0;
    for _ in 0..UPDATE_LIMIT {
        let mut wait = None;
        let mut terminal = None;
        while let Some(output) = session.poll() {
            assert!(terminal.is_none(), "terminal output must be last");
            match output {
                RakSessionOutput::Datagram(bytes, destination) => {
                    assert!(!bytes.is_empty());
                    assert_eq!(destination, peer);
                    datagrams += 1;
                }
                RakSessionOutput::Wait(after) => wait = Some(after),
                RakSessionOutput::Disconnected(terminal_id, reason) => {
                    assert_eq!(terminal_id, id);
                    terminal = Some(reason);
                }
                RakSessionOutput::Packet(_) => panic!("the silent peer sent no application data"),
            }
        }
        if let Some(reason) = terminal {
            assert_eq!(reason, expected);
            assert!(datagrams > 0, "protocol output precedes retirement");
            let closed = session.finish().unwrap();
            assert_eq!(closed.id(), id);
            assert_eq!(closed.addr(), peer);
            assert_eq!(closed.reason(), Some(expected));
            return;
        }
        let after = wait.expect("a live owner must schedule its next update");
        assert_ne!(after, Duration::MAX, "existing liveness must cap a wait");
        assert!(!after.is_zero(), "a live scheduled update must progress");
        if let Some(bound) = wait_bound {
            assert!(
                after <= bound,
                "a wait must respect the finite policy bound"
            );
        }
        now = now
            .checked_add(after)
            .expect("the emitted wait must be usable");
        session.handle(RakSessionInput::Update(now)).unwrap();
    }
    panic!("the silent peer did not retire within the controlled work bound");
}

/// Queues real reliable work, then drives the original or explicitly restored Closing owner.
fn pending_close(config: RakSessionConfig, restore: bool, expected: RakDisconnectReason) {
    let wait_bound = [config.autoflush_interval_ms, config.close_timeout]
        .into_iter()
        .filter(|duration| *duration != Duration::MAX)
        .min();
    let mut now = Instant::now();
    let mut session = established(config.clone(), now);
    session
        .send(
            Box::from(&b"original reliable application bytes"[..]),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
            now,
        )
        .unwrap();
    let mut closing = session.close(now).unwrap();
    assert_eq!(closing.state(), RakSessionState::Closing);
    assert!(closing.outbound_bytes() > 0);
    if restore {
        // Capture drains the original output; this case exercises the restored owner.
        while closing.poll().is_some() {}
        let id = closing.id();
        let peer = closing.addr();
        let retained = closing.outbound_bytes();
        let snapshot = closing.snapshot(now).unwrap();
        assert_eq!(snapshot.config().autoflush, config.autoflush);
        assert_eq!(
            snapshot.config().autoflush_interval_ms,
            config.autoflush_interval_ms
        );
        assert_eq!(snapshot.config().close_timeout, config.close_timeout);
        drop(closing);
        let elapsed = RakSessionConfig::default().autoflush_interval_ms;
        now = now.checked_add(elapsed).unwrap();
        closing = match snapshot.restore(now, elapsed).unwrap() {
            RestoredSession::Closing(closing) => closing,
            other => panic!("closing restore must retain the saved capability: {other:?}"),
        };
        assert_eq!(closing.id(), id);
        assert_eq!(closing.addr(), peer);
        assert_eq!(closing.state(), RakSessionState::Closing);
        assert_eq!(closing.outbound_bytes(), retained);
        closing.handle(RakSessionInput::Update(now)).unwrap();
    }
    drive_until_terminal(closing, now, wait_bound, expected);
}

#[test]
fn maximum_automatic_flush_cadence_keeps_public_session_updates_bounded() {
    let now = Instant::now();
    let mut session = established(
        RakSessionConfig {
            autoflush_interval_ms: Duration::MAX,
            ..Default::default()
        },
        now,
    );
    session.handle(RakSessionInput::Update(now)).unwrap();
    assert_eq!(session.state(), RakSessionState::Connected);
    drive_until_terminal(session, now, None, RakDisconnectReason::Timeout);
}

#[test]
fn maximum_flush_cadence_keeps_pending_close_bounded() {
    pending_close(
        RakSessionConfig {
            autoflush: false,
            autoflush_interval_ms: Duration::MAX,
            ..Default::default()
        },
        false,
        RakDisconnectReason::Requested,
    );
}

#[test]
fn maximum_flush_cadence_keeps_restored_pending_close_bounded() {
    pending_close(
        RakSessionConfig {
            autoflush: false,
            autoflush_interval_ms: Duration::MAX,
            ..Default::default()
        },
        true,
        RakDisconnectReason::Requested,
    );
}

#[test]
fn maximum_close_timeout_keeps_pending_close_bounded() {
    pending_close(
        RakSessionConfig {
            autoflush: false,
            close_timeout: Duration::MAX,
            ..Default::default()
        },
        false,
        RakDisconnectReason::Timeout,
    );
}

#[test]
fn maximum_close_timeout_keeps_restored_pending_close_bounded() {
    pending_close(
        RakSessionConfig {
            autoflush: false,
            close_timeout: Duration::MAX,
            ..Default::default()
        },
        true,
        RakDisconnectReason::Timeout,
    );
}

#[test]
fn maximum_flush_and_close_durations_leave_liveness_in_control() {
    pending_close(
        RakSessionConfig {
            autoflush: false,
            autoflush_interval_ms: Duration::MAX,
            close_timeout: Duration::MAX,
            ..Default::default()
        },
        false,
        RakDisconnectReason::Timeout,
    );
}

#[test]
fn maximum_flush_and_close_durations_leave_restored_liveness_in_control() {
    pending_close(
        RakSessionConfig {
            autoflush: false,
            autoflush_interval_ms: Duration::MAX,
            close_timeout: Duration::MAX,
            ..Default::default()
        },
        true,
        RakDisconnectReason::Timeout,
    );
}

#[test]
fn zero_close_timeout_preserves_close_then_update_retirement() {
    let now = Instant::now();
    let mut session = established(
        RakSessionConfig {
            autoflush: false,
            close_timeout: Duration::ZERO,
            ..Default::default()
        },
        now,
    );
    session
        .send(
            Box::from(&b"pending before immediate expiry"[..]),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
            now,
        )
        .unwrap();
    let mut closing = session.close(now).unwrap();
    assert_eq!(closing.state(), RakSessionState::Closing);
    assert!(closing.outbound_bytes() > 0);
    closing.handle(RakSessionInput::Update(now)).unwrap();
    assert_eq!(closing.state(), RakSessionState::Disconnected);
    drive_until_terminal(closing, now, None, RakDisconnectReason::Requested);
}

#[test]
fn zero_close_timeout_is_not_due_before_its_start() {
    let now = Instant::now();
    let mut session = established(
        RakSessionConfig {
            autoflush: false,
            close_timeout: Duration::ZERO,
            ..Default::default()
        },
        now,
    );
    session
        .send(
            Box::from(&b"pending across an earlier caller time"[..]),
            RakReliability::ReliableOrdered,
            RakPriority::Normal,
            now,
        )
        .unwrap();
    let close_at = now
        .checked_add(RakSessionConfig::default().autoflush_interval_ms)
        .unwrap();
    let mut closing = session.close(close_at).unwrap();
    closing.handle(RakSessionInput::Update(now)).unwrap();
    assert_eq!(closing.state(), RakSessionState::Closing);
    assert!(closing.outbound_bytes() > 0);
    while closing.poll().is_some() {}
    closing.handle(RakSessionInput::Update(close_at)).unwrap();
    assert_eq!(closing.state(), RakSessionState::Disconnected);
    drive_until_terminal(closing, close_at, None, RakDisconnectReason::Requested);
}
