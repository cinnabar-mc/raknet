# raknet

The RakNet protocol without socket I/O, background tasks or a required runtime.
Use this crate when your application already has an event loop, or needs to
schedule many sessions on shared UDP sockets.

```toml
[dependencies]
raknet = { git = "https://github.com/cinnabar-mc/raknet" }
```

A client starts with configuration and a clock. Starting an attempt produces work
for your event loop; it does not open a socket or send anything itself.

```rust
use raknet::prelude::*;
use std::{net::SocketAddr, time::Instant};

fn start_attempt(
    remote: SocketAddr,
    local: SocketAddr,
    clock: RakClock,
    now: Instant,
) -> Result<RakClient<Active>, Box<dyn std::error::Error>> {
    let client = RakClient::new(RakClientConfig::default(), clock)?;
    Ok(client.connect(remote, local, ClientGuid::generate()?, now)?)
}
```

Create one `RakClock` for the endpoint and pass your socket's actual local
address to `start_attempt`. Feed incoming UDP
datagrams and timer updates through `Sans::handle`, then drain `poll`. When
`SessionConnected` returns a session, use its `send` method and receive complete
messages through `RakSessionOutput::Packet`.

The [sans-I/O guide](../docs/integration.md) covers the full loop: MTU write
feedback, server routing, time, bounded I/O, close and snapshots. For a controlled
shared-socket implementation, see [shared_udp.rs](tests/shared_udp.rs).
If you prefer an existing integration, use [Tokio](../raknet-tokio/README.md) or
[Bevy](../bevy-raknet/README.md).

[Apache-2.0](../LICENSE); see [NOTICE](../NOTICE) for attribution.
