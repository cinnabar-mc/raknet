//! Query a numeric RakNet endpoint's advertisement without opening a session.

use raknet_tokio::prelude::RakClient;
use std::error::Error;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:19132".to_owned())
        .parse()?;
    let mut client = RakClient::new(|_| {});
    client.start().await?;

    let reply = client.ping(address).await;
    client.stop().await;

    let (advertisement, latency) = reply?;
    println!(
        "{address}: {:?}, advertisement: {}",
        latency,
        String::from_utf8_lossy(&advertisement)
    );
    Ok(())
}
