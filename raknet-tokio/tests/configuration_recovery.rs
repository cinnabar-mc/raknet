//! Unsupported automatic-adapter policy is diagnosed before a real socket bind.

use raknet_tokio::prelude::{RakClient, RakServer, RakServerError};
use std::{io::ErrorKind, net::UdpSocket};

#[tokio::test]
async fn client_policy_rejection_precedes_occupied_socket_binding() {
    let occupied = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port();
    let configured = RakClient::new(|config| {
        config.local_port = port;
        config.session.autoflush = false;
    });
    let Err(failure) = configured.start().await else {
        panic!("unsupported policy must be rejected before socket binding")
    };
    let (_configured, cause) = failure.into_parts();
    assert!(
        matches!(cause, RakServerError::IOError(error) if error.kind() == ErrorKind::InvalidInput)
    );
}

#[tokio::test]
async fn server_policy_rejection_precedes_occupied_socket_binding() {
    let occupied = UdpSocket::bind("127.0.0.1:0").unwrap();
    let configured = RakServer::new(occupied.local_addr().unwrap(), |config| {
        config.session.autoflush = false;
    });
    let Err(failure) = configured.start().await else {
        panic!("unsupported policy must be rejected before socket binding")
    };
    let (_configured, cause) = failure.into_parts();
    assert!(
        matches!(cause, RakServerError::IOError(error) if error.kind() == ErrorKind::InvalidInput)
    );
}
