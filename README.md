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

Application writes emit available datagrams immediately at every priority.
Congestion and ordering windows can retain a write until capacity opens; an ACK
drives those queued writes immediately. Priority selects their scheduling order.
The automatic flush interval controls
maintenance such as acknowledgements and retransmissions, not application delay.

Tokio and Bevy require `config.session.autoflush = true`, the default. They
reject manual flushing before opening a socket because their handles expose no
caller `tick`. Tokio also rejects manual-flush snapshots before restoration or
actor dispatch, returning the original capture in its adoption error.

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

For a connected Tokio client, `RakClient::new(...).start().await` consumes the
configuration and returns `RakClient<RunningClient>`. Call `connect(remote).await`
on that owner, then use the returned `RakSession<Established>`'s `send` and `recv`. Keep the
client owner running while using its session. `recv::<Box<[u8]>>()` returns one
complete application message. A successful `send` is not a remote delivery
receipt. `session.close()` synchronously consumes send permission and returns a
`RakSession<Closing>` that can drain the received prefix. Keep the socket owner
running until terminal completion; `client.stop().await` or `server.stop().await`
consumes that running owner and awaits its shutdown.

Peer silence starts graceful close after five seconds plus twice the measured
round trip, using 50ms before the first measurement. The first close retains its
configured drain interval (five seconds by default) and retries its final notice
until acknowledged or that interval ends. Deferred reliable delivery and receive
ordering gaps retain their fifteen-second progress window; expiry fails the
session rather than silently discarding acknowledged bytes. Accepted reliable
fragments remain within the original byte and fragment limits until delivery or
session retirement. Unreliable reassembly slots can be reclaimed after ten
seconds without a new fragment.

## Drive the core yourself

Started core drivers implement `Sans`: feed typed I/O with `handle`, then drain
output with `poll`. Setup, start and close use consuming methods. None opens a
socket or reads the clock to advance the protocol.

| Engine | Feed | Handle its output |
| --- | --- | --- |
| `RakClient<Active>` | Datagrams with their source address, timer updates and MTU write completions. | Send probes/datagrams, schedule a wake or take ownership of a negotiated session. |
| `RakServer` | Received datagrams, periodic updates and endpoint configuration. | Send replies, route connected datagrams by session ID or take ownership of a new session. |
| `RakSession<Established>` / `RakSession<Closing>` | Routed datagrams and timer updates. Only Established has `send`; consuming `close` returns Closing. | Send datagrams, deliver complete application packets, schedule a wake or retire a disconnected session. |

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

The core starts with `RakClient<Configured>`. Its consuming `connect(...)` returns
`RakClient<Active>`; only a genuine `SessionConnected` handoff grants an
established session. Live owners cannot be cloned or built from raw session
metadata. Peer disconnection remains a runtime result. See the
[integration guide](docs/integration.md) for the complete lifecycle.

## Delivery and capacity

Use `ReliableOrdered` for ordered reliable application traffic. Prefer queued
priorities (`High`, `Normal`, `Low`) for sustained sends: `Immediate` bypasses
congestion and receive-window scheduling.

Set local limits through the client or server configuration's `session` field.
`RakSessionConfig`, `max_message_len()` and `outbound_bytes()` configure and
observe those bounds. Oversized messages return
`RakSessionError::MessageSizeExceeded`; exceeding the outbound byte budget
disconnects the session with `OutboundBytesExceeded`. Always inspect terminal
outputs as well as the return value of `handle`.

Drain application packets promptly and bound your own queues. Protocol retention
limits do not bound every adapter or application queue. The Tokio adapter bounds
session input and received-message channels and closes an overloaded session.
Its endpoint and session command FIFOs retain at most `MAX_RECEIVE_MESSAGES`
requests each. Session send commands also reserve their actual body bytes from
that session's `max_outbound_bytes` before commitment. Waiting bodies remain
caller-owned; canceling before commitment returns the reservations, while
canceling an admitted reply wait does not retract its send. A body larger than
that entire byte budget returns `MessageSizeExceeded` before commitment, even
when the negotiated message limit is larger. Close bypasses a full command FIFO,
seals new sends, and retains earlier committed work under the first close cutoff.
A server's original `max_connections` reservation follows its accepted handle,
including unread retired data and terminal metadata, until final `finish` or
whole-handle drop. Lowering the setting or setting it to zero changes only future
admission. Network retirement removes the dead route promptly; its weak reservation
does not retain the handle or its payload. Datagram channels need their own
bounds. These are per-stage storage bounds, not an aggregate byte or RSS limit.

The Tokio final receive FIFO also uses the session's positive `max_queued_bytes`
as a new aggregate complete-body budget. One already-valid body larger than
that budget may enter an empty FIFO alone; no successor enters behind it.
The first count or byte refusal permanently seals final delivery and takes the
existing immediate-close path. Its accepted prefix remains readable; popping
older bodies cannot admit a successor across that gap. Earlier protocol terminal
causes are preserved. Credit ends when `recv` pops the original body or the
receiver drops it, so caller-owned returned bodies are outside this FIFO bound.
This does not reduce the core's supported single-message size or impose an MTU
floor, and it does not bound consumer-retained memory or total RSS.

Running Tokio server setters return `Result` with `TrySendError::Full(input)` or
`Closed(input)` when the original FIFO cannot admit a control. Rejected
`set_message` input is returned without conversion or copying. Configured
setters remain infallible setup. Async endpoint requests wait for real capacity;
canceling before admission queues nothing, and canceling after admission does
not retract the queued request. Ping's existing timeout includes that capacity
wait. Snapshot adoption restores the original clock before waiting.

Endpoint stop requests bypass a full command FIFO and retain their first close
cutoff. Fully await consuming `stop` to join its original socket owner; dropping
that future or the running resource does not promise a completed join. Each
endpoint retains one physical send, including its original route credit, while
socket readiness is pending. Stop, route retirement and the first cutoff remain
selectable; final draining uses the same remaining allowance. Unsent output is
dropped at the cutoff. Ordinary input, commands and core updates pause behind
that head, so this does not establish shared-socket fairness during a stalled
write or promise remote delivery.

Bevy's original final receive queues retain at most `MAX_RECEIVE_MESSAGES` per
session and that session's `max_queued_bytes` of complete bodies. The first
rejected body retires that peer with `ReceiveBufferExceeded`; its accepted
prefix remains readable, and later bodies cannot cross the rejected gap after
capacity is released. Existing protocol failures retain their original reason.
These are local storage bounds, not remote-delivery receipts or an RSS limit.

## Bevy integration

Add `RakClientPlugin` or `RakServerPlugin`, and insert the corresponding
`RakClient::new(...)` or `RakServer::new(address, ...)` resource. These
constructors bind nonblocking sockets and return `ClientStartError` or
`ServerStartError` on failure. Their `into_parts()` methods recover the original
configuration, bind address for servers, and I/O cause for an explicit retry.

The plugins update their resources in `PreUpdate` and emit `RakClientEvent` or
`RakServerEvent` messages. Order same-schedule consumers after `RakClientSet` or
`RakServerSet`; drain packets through the resource's `recv` method.

Start a client attempt through `client.idle()`'s consuming `connect(peer)`
capability. Application sends require an exclusive established capability from
`client.connected()` or `server.connected(id)`. Calling its consuming `close()`
returns a closing capability without `send`; continue plugin updates until the
disconnected event arrives. Neither resource exposes unrestricted application
sends or mutable protocol state.

Keep draining both `recv` and lifecycle events after disconnection. A client
cannot start another connection while its original data or notifications remain
unread. A server retains that admission slot until both drain, including when
`max_connections` is lowered; zero refuses new admissions. Closing removes live
send permission without discarding the final receive prefix.

Discovery pongs have the same count and configured byte bounds. Excess replies
are counted and discarded without retiring a game peer. Public `ping` drains
its original nonblocking outgoing discovery work immediately, so calls before a
frame cannot build a separate retained queue.

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

The `test-support` features enable golden, interop and benchmark comparisons
against go-raknet; see [tools/go-raknet](tools/go-raknet/README.md).

## License and attribution

The original Rust implementation and this fork use the
[Apache-2.0 license](LICENSE). Public Lunar go-raknet behavior references retain
their [MIT notice](LICENSE-MIT-go-raknet). See [NOTICE](NOTICE) for the Cinnabar
modifications and reference attribution.
