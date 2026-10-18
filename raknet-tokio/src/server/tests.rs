use super::*;

/// Exercises the public adapter against the original genuinely negotiated snapshot policy.
async fn reject_manual_flush_snapshot(snapshot: RakSessionSnapshot) {
    let mut server = RakServer::new("127.0.0.1:0".parse().unwrap(), |_| {})
        .start()
        .await
        .unwrap();
    let result = server.adopt(snapshot, Duration::ZERO).await;
    let rejected = result.is_err();
    drop(result);
    server.stop().await;
    assert!(
        rejected,
        "automatic server adopted manual flushing without a tick capability"
    );
}

#[tokio::test]
async fn automatic_server_rejects_established_manual_flush_snapshot() {
    let (_output, established, _) = crate::test_support::manual_flush_snapshots();
    reject_manual_flush_snapshot(established).await;
}

#[tokio::test]
async fn automatic_server_rejects_closing_manual_flush_snapshot() {
    let (_output, _, closing) = crate::test_support::manual_flush_snapshots();
    reject_manual_flush_snapshot(closing).await;
}

#[tokio::test]
async fn rejected_snapshot_retains_original_policy_before_time_restoration() {
    let (_output, established, closing) = crate::test_support::manual_flush_snapshots();
    let mut server = RakServer::new("127.0.0.1:0".parse().unwrap(), |_| {})
        .start()
        .await
        .unwrap();
    for original in [established, closing] {
        let uptime = original.captured_uptime_ms();
        let Err(ServerAdoptError::UnsupportedConfiguration { snapshot, cause }) =
            server.adopt(original, Duration::MAX).await
        else {
            panic!("manual policy must return its capture before invalid time restoration")
        };
        assert_eq!(cause.kind(), std::io::ErrorKind::InvalidInput);
        assert!(!snapshot.config().autoflush);
        assert_eq!(snapshot.captured_uptime_ms(), uptime);
        assert!(
            snapshot
                .restore(Instant::now().into_std(), Duration::ZERO)
                .is_ok()
        );
    }
    server.stop().await;
}

#[tokio::test]
#[ignore]
async fn rak_server() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_test_writer()
        .compact()
        .try_init();

    let server = RakServer::new("127.0.0.1:19132".parse().unwrap(), |config| {
        config.guid = 123456789;
        config.message = Box::new(*b"MCPE;Chorus;0;1.0.0;0;-1;123456789;Chorus;Survival");
    });

    let mut server = server.start().await.unwrap();

    let mut sessions = Vec::new();

    loop {
        tokio::select! {
            Ok(session) = server.accept() => {
                sessions.push(session);
                debug!("received session")
            }
        }
    }
}
