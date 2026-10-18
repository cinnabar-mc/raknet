<br />
<div align="center">

# RakNet

A RakNet transport library written in Rust.

[![rust][rust_badge_url]][rust_url]
[![transport][transport_badge_url]][transport_url]
[![license][license_badge_url]][license_url]

</div>

<!-- BADGES -->
[transport_badge_url]: https://img.shields.io/badge/transport-raknet-black?style=flat-square
[transport_url]: https://github.com/facebookarchive/RakNet

[rust_badge_url]: https://img.shields.io/badge/rust-2024-%23D34516?style=flat-square&logo=rust&logoColor=%23D34516&labelColor=white
[rust_url]: https://rust-lang.org/

[license_badge_url]: https://img.shields.io/github/license/bedrock-crustaceans/raknet?style=flat-square
[license_url]: LICENSE
<!-- BADGES -->

### Crates

- **[`raknet`](raknet)** - a pure sans-io implementation (client, server, and session).
- **[`raknet-tokio`](raknet-tokio)** - an async tokio wrapper over the sans-io crate.
- **[`bevy-raknet`](bevy-raknet)** - a Bevy plugin wrapping the sans-io crate directly.

### Usage

- tokio server:
    
    ```rust
    let mut server = RakServer::new(addr, |config| {
        config.guid = 123456789;
        config.message = Box::new(*b"my server message");
    });
    server.start().await?;
    
    loop {
        let mut session = server.accept().await?;
        tokio::spawn(async move {
            while let Ok(packet) = session.recv::<Box<[u8]>>().await {
                let _ = session
                    .send(packet, RakReliability::ReliableOrdered, RakPriority::Immediate)
                    .await;
            }
        });
    }
    ```

- bevy server:

    ```rust
    app.add_plugins(RakServerPlugin)
        .insert_resource(RakServer::new(addr, |_| {})?)
        .add_systems(Update, (on_connect, echo).after(RakServerSet));
    
    fn on_connect(mut events: MessageReader<RakServerEvent>, mut commands: Commands) {
        for event in events.read() {
            if let RakServerEvent::SessionConnected { id, .. } = event {
                commands.spawn(Player(*id));
            }
        }
    }
    
    fn echo(mut server: ResMut<RakServer>) {
        while let Some((id, packet)) = server.recv() {
            let _ = server.send(id, packet, RakReliability::ReliableOrdered, RakPriority::Immediate);
        }
    }
    ```

### Sans-I/O connection drivers

The core receives caller-supplied `SystemTime` values. Pass the same clock to
`RakSession::new` and `RakSession::restore` that drives subsequent session inputs.

A `RakClientOutput::MtuProbe` transfers one probe write to the I/O adapter. Measure
only the socket write, then submit its opaque ID, completion time, elapsed write
duration, and `is_mtu_probe_size_error` result through
`RakClientInput::MtuProbeResult`. Submit a completion for failed writes too. The
core keeps preparation time out of slow-write detection, continues MTU discovery
while Request 2 retries, and ignores stale completions after cancellation or
reconnection. Validate `RakClientConfig` before opening a socket.
