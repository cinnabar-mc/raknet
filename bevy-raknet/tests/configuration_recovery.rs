//! Original configurations survive both policy rejection and actual socket setup failure.

use bevy_raknet::prelude::{RakClient, RakServer};
use std::{io::ErrorKind, net::UdpSocket};

#[test]
fn client_retains_configuration_before_binding_and_through_explicit_retry() {
    let occupied = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port();
    let Err(failure) = RakClient::new(|config| {
        config.local_port = port;
        config.session.autoflush = false;
    }) else {
        panic!("unsupported policy must be rejected before socket binding")
    };
    let (mut config, cause) = failure.into_parts();
    assert_eq!(cause.kind(), ErrorKind::InvalidInput);
    assert_eq!(config.local_port, port);
    assert!(!config.session.autoflush);
    config.session.autoflush = true;
    let Err(failure) = RakClient::new(|next| *next = config) else {
        panic!("the retained occupied port must still be used")
    };
    let (config, cause) = failure.into_parts();
    assert_eq!(cause.kind(), ErrorKind::AddrInUse);
    assert_eq!(config.local_port, port);
    drop(occupied);
    drop(RakClient::new(|next| *next = config).unwrap());
}

#[test]
fn server_retains_address_and_configuration_through_explicit_retry() {
    let occupied = UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let advertisement = b"original configured advertisement";
    let Err(failure) = RakServer::new(address, |config| {
        config.session.autoflush = false;
        config.message = Box::from(advertisement.as_slice());
    }) else {
        panic!("unsupported policy must be rejected before socket binding")
    };
    let (retained_address, mut config, cause) = failure.into_parts();
    assert_eq!(cause.kind(), ErrorKind::InvalidInput);
    assert_eq!(retained_address, address);
    assert_eq!(config.message.as_ref(), advertisement);
    assert!(!config.session.autoflush);
    config.session.autoflush = true;
    let Err(failure) = RakServer::new(retained_address, |next| *next = config) else {
        panic!("the retained occupied address must still be used")
    };
    let (retained_address, config, cause) = failure.into_parts();
    assert_eq!(cause.kind(), ErrorKind::AddrInUse);
    assert_eq!(retained_address, address);
    assert_eq!(config.message.as_ref(), advertisement);
    drop(occupied);
    drop(RakServer::new(retained_address, |next| *next = config).unwrap());
}
