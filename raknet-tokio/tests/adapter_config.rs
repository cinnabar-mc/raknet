//! Public startup rejects session policies that the automatic actor cannot drive.

use raknet_tokio::prelude::{RakClient, RakServer};

#[tokio::test]
async fn automatic_client_rejects_manual_flush_and_retains_configuration() {
    let configured = RakClient::new(|config| config.session.autoflush = false);
    let error = match configured.start().await {
        Err(error) => error,
        Ok(running) => {
            running.stop().await;
            panic!("automatic client accepted manual flushing without a tick capability")
        }
    };
    let (configured, _) = error.into_parts();
    assert!(configured.start().await.is_err());
}

#[tokio::test]
async fn automatic_server_rejects_manual_flush_and_retains_configuration() {
    let configured = RakServer::new("127.0.0.1:0".parse().unwrap(), |config| {
        config.session.autoflush = false;
    });
    let error = match configured.start().await {
        Err(error) => error,
        Ok(running) => {
            running.stop().await;
            panic!("automatic server accepted manual flushing without a tick capability")
        }
    };
    let (configured, _) = error.into_parts();
    assert!(configured.start().await.is_err());
}
