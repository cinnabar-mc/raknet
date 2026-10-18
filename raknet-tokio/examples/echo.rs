//! Advertise and echo one connected RakNet peer; this is not a Minecraft server.

use raknet_tokio::prelude::{RakPriority, RakReliability, RakServer, RakSessionError};
use std::error::Error;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:19132".to_owned())
        .parse()?;
    let mut server = RakServer::new(address, |config| {
        config.max_connections = 1;
        config.message = b"RakNet echo example".to_vec().into_boxed_slice();
    });
    server.start().await?;
    println!("Listening on {address}; press Ctrl-C to stop.");

    let result = tokio::select! {
        result = echo_next_peer(&mut server) => result,
        result = tokio::signal::ctrl_c() => result.map_err(Into::into),
    };
    server.stop().await;
    result
}

/// Echoes complete messages from the next peer until it closes or a send fails.
async fn echo_next_peer(server: &mut RakServer) -> Result<(), Box<dyn Error>> {
    let mut session = server.accept().await?;
    loop {
        let packet = match session.recv::<Box<[u8]>>().await {
            Ok(packet) => packet,
            Err(RakSessionError::Closed) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        session
            .send(packet, RakReliability::ReliableOrdered, RakPriority::Normal)
            .await?;
    }
}
