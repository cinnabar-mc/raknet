//! Public construction rejects policies that the frame-driven adapter cannot flush.

use bevy_raknet::prelude::{RakClient, RakServer};

#[test]
fn automatic_client_rejects_manual_flush() {
    assert!(
        RakClient::new(|config| config.session.autoflush = false).is_err(),
        "automatic client accepted manual flushing without a tick capability"
    );
}

#[test]
fn automatic_server_rejects_manual_flush() {
    assert!(
        RakServer::new("127.0.0.1:0".parse().unwrap(), |config| {
            config.session.autoflush = false;
        })
        .is_err(),
        "automatic server accepted manual flushing without a tick capability"
    );
}
