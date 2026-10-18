# RakNet

Rust RakNet transport for clients and servers, with a sans-I/O core and separate
Tokio and Bevy adapters. The core owns negotiation, reliability, fragmentation,
ordering and congestion control; your application can own the sockets, clock and
scheduler.

This is the [Cinnabar](https://github.com/cinnabar-mc) fork of
[bedrock-crustaceans/raknet](https://github.com/bedrock-crustaceans/raknet).
It transports application messages. Minecraft login, authentication, packet
codecs and resource packs belong to the layer above it, such as
[bedrock-net](https://github.com/cinnabar-mc/bedrock-net).

## Choose an integration

| Crate | Use it when | I/O and scheduling |
| --- | --- | --- |
| [`raknet`](raknet) | You have an event loop, frame scheduler or many sessions to multiplex. | Caller-owned sockets and time; no Tokio dependency or background tasks. |
| [`raknet-tokio`](raknet-tokio) | Your application already runs Tokio. | Async client, server and session handles; adapter-owned sockets and tasks. |
| [`bevy-raknet`](bevy-raknet) | You want a Bevy resource and connection messages. | Nonblocking UDP driven in `PreUpdate`, directly over the core; no Tokio dependency. |

The adapters are separate crates, rather than features of `raknet`. Their
`RakClient`, `RakServer` and `RakSession` names refer to different integration
types. Import from the chosen crate's `prelude`.

## Add a dependency

Use the Cinnabar Git repository to select this implementation:

```toml
[dependencies]
raknet = { git = "https://github.com/cinnabar-mc/raknet" }
```

For Tokio, use `raknet-tokio` instead:

```toml
[dependencies]
raknet-tokio = { git = "https://github.com/cinnabar-mc/raknet" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

For Bevy, use `bevy-raknet` from the same repository and match the Bevy version
in its [manifest](bevy-raknet/Cargo.toml). Commit your application's `Cargo.lock`;
if you use several crates from this repository, keep them on the same Git
revision. The workspace uses Rust's 2024 edition. Its
[CI configuration](.github/workflows/ci.yml) shows the toolchain and platforms
currently checked.

## Start with the examples

The examples are Cargo example targets using existing crate dependencies. Run
these commands from this checkout:

```sh
# Advertise a message and echo one connected peer until it closes or you press Ctrl-C.
cargo run --locked -p raknet-tokio --example echo -- 127.0.0.1:19132

# From another terminal, query that server's advertisement without connecting.
cargo run --locked -p raknet-tokio --example ping -- 127.0.0.1:19132
```

- [`ping.rs`](raknet-tokio/examples/ping.rs) creates a client, starts its socket
  owner, awaits an advertisement and latency, then stops the owner on either
  success or failure. It also accepts a numeric IPv6 endpoint such as
  `[::1]:19132` when IPv6 is available.
- [`echo.rs`](raknet-tokio/examples/echo.rs) accepts one peer and echoes complete
  application messages using `ReliableOrdered` and `Normal` priority. It is a
  transport example, not a Minecraft server.
- [`shared_udp.rs`](raknet/tests/shared_udp.rs) demonstrates a controlled caller
  loop for already established core sessions sharing one application socket.
  See the [integration guide](docs/integration.md#many-sessions-on-shared-sockets)
  for its routing and ownership constraints.

For a connected Tokio client, the sequence is `RakClient::new`, `start().await`,
`connect(remote).await`, then the returned session's `send` and `recv`. Keep the
client owner running while using its session. `recv::<Box<[u8]>>()` returns one
complete application message. A successful `send` is not a remote delivery
receipt. Request graceful session shutdown with `close().await` and finish the
socket owner's lifetime with `client.stop().await` or `server.stop().await`.

## Drive the core yourself

All three core engines implement `Sans`: feed typed input with `handle`, then
drain typed output with `poll`. None opens a socket or reads the clock to advance
the protocol.

| Engine | Feed | Handle its output |
| --- | --- | --- |
| `RakClient` | Ping/connect requests, datagrams with their source address, timer updates and MTU write completions. | Send probes/datagrams, schedule a wake, receive a pong or take ownership of a negotiated session. |
| `RakServer` | Received datagrams, periodic updates and endpoint configuration. | Send replies, route connected datagrams by session ID or take ownership of a new session. |
| `RakSession` | Routed datagrams, application sends, timer updates and close requests. | Send datagrams, deliver complete application packets, schedule a wake or retire a disconnected session. |

Create one `RakClock` from your endpoint's start time, and supply monotonic
`Instant` values from that clock domain to subsequent inputs. A `Wait` is a
relative deadline anchored to the input that generated it. The engine makes
progress only when you service its outputs, socket input and timers.

Client negotiation also requires feedback for **every MTU probe socket write**,
including failed writes. A `SessionConnected` output transfers owned protocol
state; immediately drain its existing outputs before processing newer input.
These details are essential when building a frame-driven or shared-socket
adapter. The [integration guide](docs/integration.md) covers negotiation,
demultiplexing, deadlines, shutdown and snapshots.

## Delivery and capacity

Use `ReliableOrdered` for ordered reliable application traffic. Prefer queued
priorities (`High`, `Normal`, `Low`) for sustained sends: `Immediate` bypasses
congestion and receive-window scheduling.

The core exposes `RakSessionConfig`, `max_message_len()` and `outbound_bytes()`
for configuring and observing its bounds. Oversized messages return
`RakSessionError::MessageSizeExceeded`; exceeding the outbound byte budget
disconnects the session with `OutboundBytesExceeded`. Always inspect terminal
outputs as well as the return value of `handle`.

Drain application packets promptly and bound your own queues. Protocol retention
limits do not bound every adapter or application queue. The Tokio adapter bounds
session input and received-message channels and closes an overloaded session;
its command and datagram channels are unbounded. The Bevy adapter's received
message queues must be drained by application systems.

## Bevy integration

Add `RakClientPlugin` or `RakServerPlugin`, and insert the corresponding
`RakClient::new(...)` or `RakServer::new(address, ...)` resource. These
constructors bind nonblocking sockets and return `std::io::Result`.

The plugins update their resources in `PreUpdate` and emit `RakClientEvent` or
`RakServerEvent` messages. Order same-schedule consumers after `RakClientSet` or
`RakServerSet`; drain packets through the resource's `recv` method. A disconnect
request needs continued updates until the disconnected event arrives.

The current Bevy client resource owns one IPv4 socket and one connection. For
many clients, explicit IPv6 policy or stricter I/O backpressure, use the core
with your own socket and scheduling owner rather than creating one plugin
resource per session.

## Compatibility and development

This fork targets the RakNet transport used by Bedrock and incorporates fixes
based on the public Lunar go-raknet implementation. The default protocol and
transport limits are defined in [`constants`](raknet/src/util/constants.rs).
This does not establish complete compatibility with every RakNet peer, network
path or game version. Validate the servers and network conditions your
application supports.

From the repository root:

```sh
cargo fmt --all --check
cargo test --locked -p raknet
cargo check --locked -p raknet-tokio --examples
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo doc --locked --workspace --no-deps
```

Use the focused core command when working only on the protocol engine; workspace
commands also build the Tokio and Bevy adapters.

## License and attribution

The original Rust implementation and this fork use the
[Apache-2.0 license](LICENSE). Public Lunar go-raknet behavior references retain
their [MIT notice](LICENSE-MIT-go-raknet). See [NOTICE](NOTICE) for the Cinnabar
modifications and reference attribution.
