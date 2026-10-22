# Driving the sans-I/O core

Use `raknet` when you want to control sockets, time and scheduling. The core
handles the protocol; it returns work for your event loop instead of performing
I/O. Import the public types and the `Sans` trait from `raknet::prelude`.

For ready-made socket integrations, see [Tokio](tokio.md) or [Bevy](bevy.md).

## The event loop

Each active client, server or session accepts typed input with `handle` and
returns output with `poll`. After every input:

1. Save its result and drain the outputs in order, even if the input failed.
2. Write outgoing datagrams, deliver application messages and update timers.
3. Keep any output your socket or consumer cannot yet accept.
4. Feed the next datagram or due timer update, then repeat.

A successful input can still produce `Disconnected`, for example when outbound
capacity is exhausted. An error can also leave earlier output to send or deliver.
Do not discard that output merely because `handle` returned an error.

Construct one `RakClock::new(origin)` for the endpoint. Supply monotonic
`Instant` values from the same clock domain, at or after that origin. The wire
timestamp counts milliseconds of endpoint uptime, not UTC.

`Wait(duration)` is relative to the input time that produced it. Schedule from
that time, not from when you eventually poll the output. Otherwise blocked I/O
can silently extend deadlines. Retain the previous timer until its replacement
or terminal output has been processed.

## Connect a client

Create an attempt using the local address of your socket and a fresh GUID:

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

`RakClient<Configured>` becomes `RakClient<Active>` when `connect` consumes it.
This starts the attempt without opening a socket. A start error's `into_parts()`
returns the configured client and cause. Invalid configuration fails before I/O.

Choose a socket with the same address family as the peer; use its route-selected
local address when available. IPv4-mapped IPv6 endpoints are canonicalized.
Generate a new GUID for each new attempt and propagate entropy failures. The
core retains that GUID across its own retries.

Drain the client's outputs and perform each action:

| Output | Action |
| --- | --- |
| `MtuProbe { id, datagram, peer }` | Write the probe and report the completion below. |
| `SocketDatagram(bytes, peer)` | Send this complete UDP datagram. |
| `Wait(after)` | Schedule `RakClientInput::Update(now)` at the due time. |
| `SessionConnected(session)` | Keep the established session and immediately drain its queued outputs. |
| `SessionDatagram(bytes)` | Feed `RakSessionInput::Datagram(bytes, now)` to that session. |
| `SessionClosing(session)` | Keep the restored closing session and process its remaining output. |
| `Pong { .. }` | Deliver discovery data and latency to your application. |

Feed received datagrams as `RakClientInput::Datagram(bytes, source, now)`.
After connection, you may route them directly to the session instead. If they
still pass through the client, forward its `SessionDatagram` outputs.

A newly connected session can already contain application messages or a terminal
notification from the last handshake datagram. Drain those before newer input.
When retiring the session, process its terminal output and use the active
client's consuming `reset()` to prepare another attempt. Reset does not close
the separately stored session, and it preserves the probe-ID counter.

### Report MTU probe writes

Every `MtuProbe` needs exactly one completion, including failed writes:

1. Start measuring immediately before the socket write, after waiting in your
   own output queue.
2. Record the write's completion time and elapsed duration.
3. Classify size errors with `is_mtu_probe_size_error(&error)`.
4. Feed `RakClientInput::MtuProbeResult { id, now, elapsed, too_large }` using
   the probe's original ID.

Do not label all I/O errors as size errors or include unrelated queue preparation
in the elapsed write time. Late results from canceled attempts are ignored by
ID. `is_transient_udp_read_error` supports a bounded read-retry policy during
initial offline negotiation; it is not a general connected-session retry rule.

## Accept on a server

Bind your socket first, then construct
`RakServer::new(config, bound_address, clock, now)?`. Feed
`RakServerInput::Datagram(bytes, source, now)` and periodic `Update(now)` inputs.
The server does not emit `Wait`, so choose an update cadence for pending
handshakes and housekeeping.

| Output | Action |
| --- | --- |
| `SocketDatagram(bytes, peer)` | Send the handshake or discovery reply. |
| `SessionConnected(session)` | Store the session by its server-assigned ID and drain it. |
| `SessionDatagram(bytes, id)` | Pass the datagram to the matching stored session. |
| `SessionClosing(session)` | Process the restored closing session without allowing new sends. |

After processing a session's preceding outputs and `Disconnected`, feed
`RakServerInput::RemoveSession(id, now)` and remove its route from your table.
This releases the server's connection accounting too.

## Send and receive messages

Only completed negotiation or trusted snapshot restoration produces a session.
`RakSession<Established>` can send; `RakSession<Closing>` cannot. A remote peer
can fail in either phase, so still handle runtime errors and terminal output.

```rust
use raknet::prelude::*;
use std::time::Instant;

fn send_greeting(
    session: &mut RakSession<Established>,
    now: Instant,
) -> Result<(), RakSessionError> {
    session.send(
        b"hello".to_vec().into_boxed_slice(),
        RakReliability::ReliableOrdered,
        RakPriority::Normal,
        now,
    )
}
```

`send` queues one complete message and may immediately produce datagrams.
Drain `poll()` after it just as you would after an input. Success is local
admission, not evidence that the remote application read the message. Priorities
select scheduling order; all remain subject to congestion and receive-window
limits.

Feed routed datagrams through `RakSessionInput::Datagram(bytes, now)` and due
timers through `Update(now)`. Process session outputs as follows:

| Output | Action |
| --- | --- |
| `Datagram(bytes, peer)` | Retain until your socket accepts a complete write. |
| `Packet(bytes)` | Deliver one complete application message. |
| `Wait(after)` | Update the timer using the generating input's time. |
| `Disconnected(id, reason)` | Retire the route after the preceding output has drained. |

With the default `session.autoflush = true`, updates drive ACKs, retransmissions
and queued work. If you choose manual flushing, call `session.tick(now)` too.
Endpoint updates still advance pending handshakes in manual mode, but do not
tick sessions after transferring them to you.

### Bound your queues

Check `max_message_len()` and use `outbound_bytes()` to control application
admission. An oversized message returns `MessageSizeExceeded`. Exceeding
`config.session.max_outbound_bytes` requests disconnection; it is not a
retryable `WouldBlock` result.

The core bounds retained protocol state, including split and ordered receive
queues. You must also bound unprocessed output, pending socket writes and
messages already delivered to your application.

For a connected datagram write, retain the exact bytes and destination after
`WouldBlock` or `Interrupted`; neither result transfers the datagram or extends
its protocol deadline. Pause new work behind blocked output and keep timers and
other peers serviceable according to your scheduler's budget.

If application delivery is full, `handle_backpressured` can process incoming
control traffic while deferring new application payloads. New reliable frames
are refused before acknowledgement so the sender can retry; already accepted
messages and fragments stay retained. Unreliable traffic has no retry guarantee.
`poll_control()` takes wire/timer/terminal output while leaving application
packets queued for ordinary `poll()`.

When using these methods, preserve the queued message prefix before reporting
its terminal result. Ordered controls cannot skip earlier application messages.
Use `expiry_deadline(now)` to service the original liveness or close cutoff
while output is blocked; it does not itself flush or advance the protocol.

## Close a session

Consume an established session with `session.close(now)` and keep processing
input, output and updates on the returned closing session. If close fails,
`error.into_parts()` returns that closing session and cause. It still contains
its queued output and cannot resume sending.

Graceful close finishes after pending work is acknowledged or the configured
close timeout expires. Process the final datagram before removing the route.
`abort(now)` requests immediate retirement while preserving existing output.
Dropping a core session performs no I/O.

After terminal output and all queued messages have drained, `finish()` returns
`ClosedSession` metadata. If the session is not finished, it returns the session
unchanged. Neither a successful send nor completed close proves remote
application processing.

## Many sessions on shared sockets

Core sessions need no dedicated task or socket. You can keep them in a route
table and wake for the earliest deadline in your existing scheduler.

Use an explicit socket per address family instead of assuming dual-stack
behavior. Within a socket, key routes by `canonical_socket_addr(remote)`.
Client-local session IDs are not globally unique. Only one live route can use a
local-socket/remote-endpoint tuple: connected RakNet datagrams have no extra
connection ID to distinguish another session at that same tuple.

Retry retained writes, read a bounded number of datagrams, service due timers
and drain output under your consumer's capacity. Activity on another route must
not restart a peer's timeout. Keep a separate negotiation attempt per remote
and report every MTU probe write through that attempt.

[shared_udp.rs](../raknet/tests/shared_udp.rs) demonstrates these steps for four
already established sessions, including blocked writes, cancellation, timeouts
and address families. Its small receive sink drains every round; an application
still needs its own receive-pressure policy. It is not a concurrent-negotiation
example or a performance guarantee. Replacing a route at the same tuple also
cannot distinguish delayed packets from the preceding connection.

## Snapshots and adoption

`session.snapshot(now)` captures trusted application state, including retained
protocol messages and congestion state. It rejects a nonempty output queue with
`PendingOutput` because that output is not serialized. Drain it first. Also
coordinate pending external writes: taking a snapshot does not stop the old
session or transfer I/O.

Restore with `snapshot.restore(destination_now, elapsed)`, where `elapsed` is the
time since capture. Saved millisecond offsets are rebased to the destination's
monotonic clock. Invalid addresses, unrepresentable times or incomplete
handshakes return `RakSessionRestoreError`.

The result is `RestoredSession::Established`, `Closing` or `Closed`. A restored
closing session cannot send. Restoration of a closing snapshot uses its saved
policy with a fresh close timeout. Snapshots have read-only fields and are not a
stable wire format or authentication proof; use matching library revisions.

Core `adopt` consumes an established or closing session; server adoption assigns
a fresh local ID. Tokio adoption takes a snapshot and returns the corresponding
session or terminal metadata. It also rejects queued application messages during
capture and rejects all manual-flush snapshots before restoring them. An
`UnsupportedConfiguration` adoption error returns the original boxed snapshot
and I/O cause; other restore or dispatch errors consume it.

Adoption does not redo the handshake or move the network route. Arrange routing
and stop the old session so only one copy advances the protocol. Borrow the
original policy through `snapshot.config()` when choosing an integration.
