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

The core receives caller-supplied `std::time::Instant` values. Create a `RakClock`
from the endpoint start time and reuse it across connection attempts; wire
counters are milliseconds of monotonic uptime. Session construction takes an
owned `RakSessionConfig`. Authentication UTC is independent of transport time.

`snapshot(now)` captures portable millisecond offsets without serializing an
`Instant`. To move a session between processes, call `restore(snapshot, now,
elapsed)` with the destination clock and time since capture. SDK `adopt` takes
the same elapsed duration. Invalid address or clock ranges return a typed error.
Drain pending output before capture; a restored closing session receives a fresh
close timeout. Use matching library revisions at both ends of a snapshot transfer.

Congestion counts encapsulated bytes separately from retained datagram wire bytes.
Each admitted ACK grows a busy window once; NACK recovery keeps its wrapping
datagram-sequence boundary through retransmission and snapshots. Feedback flags
use the current slow-start and send-burst state, which also survive transfer.

A `RakClientOutput::MtuProbe` transfers one probe write to the I/O adapter. Measure
only the socket write, then submit its opaque ID, completion time, elapsed write
duration, and `is_mtu_probe_size_error` result through
`RakClientInput::MtuProbeResult`. Submit a completion for failed writes too. The
core keeps preparation time out of slow-write detection, continues MTU discovery
while Request 2 retries, and ignores stale completions after cancellation or
reconnection. Validate `RakClientConfig` before opening a socket.

### Sharing a caller-owned UDP socket

The pure core can drive multiple sessions from one unconnected UDP socket per
address family. Keep each `SessionConnected` handoff in a table keyed by its
canonical remote `SocketAddr`; client-local session IDs are not globally unique.
Reject a second live route for the same tuple. RakNet connected datagrams have no
extra connection ID to distinguish two sessions using the same local socket and
remote endpoint. Use separate family sockets rather than assuming IPv6 dual stack.

A frame or existing timer-wheel owner can use this concrete loop:

1. Retain an established-session datagram until `send_to` accepts it. `WouldBlock`
   or interruption does not transfer ownership or reset its transport deadline.
2. Read a bounded number of complete UDP datagrams, canonicalize each sender and
   feed only the matching session with `RakSessionInput::Datagram(bytes, now)`.
3. Feed due sessions `Update(now)`, using one caller-supplied monotonic time.
   Retain their `Wait` deadlines and select the earliest for the next wake. Do not
   restart a peer's timeout when another route wakes.
4. Drain each session's outputs in order. Route `Datagram` through the same socket,
   retain application packets under the consumer's bounds, and retire only that
   peer after its preceding outputs and `Disconnected`. Dropping a session sends
   nothing; drive explicit disconnect when protocol notification is required.

[The controlled shared-UDP fixture](raknet/tests/shared_udp.rs) composes these
public actions with four already established sessions, one application socket,
explicit caller clocks, bounded one-message outbound admission, retained blocked
writes and peer-specific cancellation/timeouts. Its remote sockets represent
different servers. It also checks canonical duplicate/family rejection and has a
separate IPv6 case that reports a missing loopback fixture when unavailable.
The fixture sink is deliberately small and drained every round; it is not a
general application receive-backpressure implementation.

Negotiation still needs one `RakClient` attempt owner per remote and correct
`MtuProbeResult` feedback for each write attempt, including failures, before
handoff. Reconnect through that owner; merely inserting a new session at a reused
tuple cannot distinguish old in-flight UDP from a new generation. This fixture
does not qualify concurrent negotiation, large-scale scheduling, a live server
or a Bevy application cutover.
The existing Bevy client resource remains one client/socket; the optional Tokio
edge keeps its own runtime policy. Sharing pure-core sessions requires neither
adapter nor a new protocol engine, runtime, per-session task or timer abstraction.
