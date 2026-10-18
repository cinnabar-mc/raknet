# Integrating the sans-I/O core

This guide describes the public `raknet::prelude` API. Use it when the application
owns I/O and scheduling. The Tokio and Bevy crates wrap the same engines, but
choose their own socket, task and queue policies.

## Ownership and time

Import `Sans` to call `handle` and `poll`. An input consumes its owned byte
buffer. Outputs transfer owned buffers back to the caller; retain them until
the socket or application consumer accepts them.

Construct `RakClock::new(origin)` once for an endpoint. Reuse it across client
attempts and their sessions. All supplied `Instant` values must belong to the
same monotonic clock domain and be at or after the origin. The wire counter is
milliseconds of endpoint uptime, not UTC. Game authentication time is a separate
concern.

After each input, inspect its `Result` and drain outputs in order. A successful
`handle` can still produce a `Disconnected` output, for example on outbound
capacity exhaustion. An output produced before an error can still require
delivery or cleanup; do not discard the queue solely because `handle` failed.

`Wait(duration)` is relative to the **input time that generated it**, not the
later time at which `poll()` returns it. Keep that input time with the output
batch. If a blocked socket delays a drain, anchoring the deadline to the eventual
poll time silently extends timeouts. Keep the previous timer obligation visible
until the replacement `Wait` or terminal output has been drained.

## Client negotiation

1. Build `RakClientConfig` and call `validate()` before opening a socket.
2. Bind a socket for the intended address family. Construct
   `RakClient::new(config, clock)`.
3. Generate a fresh `ClientGuid` for the attempt at the I/O boundary. Feed
   `RakClientInput::Connect { remote, local, guid, now }`. Supply the actual bound
   local endpoint; use a route-selected local address when available.
4. Drain outputs, receive complete UDP datagrams, and feed
   `RakClientInput::Datagram(bytes, source, now)`. Feed `Update(now)` when the
   scheduled deadline becomes due.
5. On `SessionConnected(session)`, take ownership of the session and drain its
   existing outputs immediately. It can already contain application packets or
   a terminal notification from the handshake's final datagram.

`Connect` canonicalizes IPv4-mapped IPv6 endpoints and rejects mismatched local
and remote address families. `ClientGuid::generate()` can fail if OS entropy is
unavailable; propagate that failure instead of reusing a previous attempt's
identity. The core retains one attempt's identity across its own retries.

| `RakClientOutput` | Caller action |
| --- | --- |
| `MtuProbe { id, datagram, peer }` | Perform one probe write and report its completion as described below. |
| `SocketDatagram(bytes, peer)` | Send a complete UDP datagram to that endpoint. |
| `SessionConnected(session)` | Store the owned session and service its pending outputs. |
| `SessionDatagram(bytes)` | Feed the handed-off session with `RakSessionInput::Datagram(bytes, now)`. |
| `Wait(duration)` | Replace the attempt's deadline using the generating input time. |
| `Pong { addr, message, echoed_timestamp, latency }` | Deliver the advertisement and measured latency to the ping owner. |

After handoff, you can route datagrams directly to the session. If inbound
traffic still passes through the `RakClient`, forward its `SessionDatagram`
outputs to that session. When retiring a session managed through that client,
drain the session's terminal output and feed `RakClientInput::Disconnect` to
release the client's route. That input cancels/resets the attempt owner; it does
not replace closing the handed-off session. Reconnect requires a fresh attempt,
not just a replacement session in the routing table.

### MTU write completion

For each `MtuProbe`:

1. Start measuring immediately before the socket write, after any preparation or
   waiting in your own queue.
2. Perform the write and record its completion time and elapsed write duration.
3. Classify an oversized-write error with `is_mtu_probe_size_error(&error)`.
4. Feed `RakClientInput::MtuProbeResult { id, now, elapsed, too_large }`, using
   the exact opaque ID supplied with the probe. Report failures too.

Probe completion lets the core distinguish slow writes and MTU size errors while
continuing its bounded discovery/retry state machine. Do not turn every failed
write into `too_large`, omit failed completions, or include unrelated queue
preparation in the measured duration. Late completions from a cancelled or
replaced attempt are ignored by ID.

`is_transient_udp_read_error` classifies OS errors that an adapter may retry
**during initial offline negotiation**, with a bounded retry policy. It is not
a blanket recovery policy for connected sessions or other socket operations.

## Server negotiation and routing

Construct `RakServer::new(config, bound_address, clock, now)`. Feed received
datagrams to `RakServerInput::Datagram(bytes, source, now)` and service periodic
`Update(now)` inputs for pending handshakes and endpoint housekeeping. The server
does not emit `Wait`; its integration chooses that update cadence.

| `RakServerOutput` | Caller action |
| --- | --- |
| `SocketDatagram(bytes, peer)` | Send the endpoint's handshake or discovery reply. |
| `SessionConnected(session)` | Store the session by its server-assigned `RakSessionId`, then drain it. |
| `SessionDatagram(bytes, id)` | Feed the matching session with `Datagram(bytes, now)`. |

After a session's preceding outputs and `Disconnected` have been processed, feed
`RakServerInput::RemoveSession(id, now)` and remove the application route. This
releases the server's connection accounting as well as your owned session.

## Established sessions

Use sessions obtained from negotiation or explicit snapshot adoption. Calling
`RakSession::new` constructs connected protocol state; it does not perform a
handshake or validate an arbitrary peer's identity.

| `RakSessionInput` | Meaning |
| --- | --- |
| `Datagram(bytes, now)` | One complete datagram already routed to this peer. |
| `Send(bytes, reliability, priority, now)` | Submit one complete application message. |
| `Update(now)` | Advance ACKs, retransmission, liveness and close processing. |
| `Disconnect(now)` | Start graceful close; refuse new sends and drain retained outbound data within the close deadline. |
| `DisconnectNow(now)` | Request immediate retirement and emit its final transport outputs. |

| `RakSessionOutput` | Caller action |
| --- | --- |
| `Datagram(bytes, peer)` | Retain the exact datagram until the socket accepts a complete write. |
| `Packet(bytes)` | Deliver one complete application message under your consumer's bounds. |
| `Wait(duration)` | Schedule the next update, anchored to its generating input. |
| `Disconnected(id, reason)` | Retire this route after all preceding outputs. |

`Packet` carries application framing, not an arbitrary stream segment.
Reliability is chosen per send. For sustained reliable ordered messages, use
`RakReliability::ReliableOrdered` with a queued priority such as
`RakPriority::Normal`. `Immediate` bypasses congestion and receive-window
scheduling and is unsuitable as the default for bulk reliable traffic.

### Capacity and I/O backpressure

Check `max_message_len()` before moving a large message into `Send`. A message
over that limit returns `MessageSizeExceeded`. Observe `outbound_bytes()` and
apply bounded application admission before submitting more work; exceeding
`RakSessionConfig::max_outbound_bytes` requests disconnection, rather than
returning a retryable `WouldBlock` error.

The core bounds retained protocol state, including split and ordered receive
queues. It does not bound all output you leave undrained, or data your application
retains after polling. Set limits for pending application sends, blocked socket
writes and delivered application messages in the I/O owner.

For established-session datagrams, `WouldBlock` or `Interrupted` does not transfer
ownership to the socket. Retain the exact bytes and destination for retry;
neither error restarts the peer's protocol deadline. UDP writes must be complete.
Avoid admitting unlimited new protocol work behind a blocked output queue, and
keep timers and unrelated peers serviceable under your scheduler's I/O budget.

### Shutdown

To close gracefully, feed `Disconnect(now)` and continue servicing socket input,
outputs and updates until `Disconnected`. Closing may finish after retained data
is acknowledged or the configured close timeout expires; it is not a guarantee
that the peer application processed that data. Send any preceding terminal
datagram before removing the route. Dropping a pure-core session performs no I/O.

The Tokio session's `close().await` acknowledges its close request, not completion
of the entire wire shutdown. Keep the socket owner running; finish its lifetime
with `RakClient::stop().await` or `RakServer::stop().await`. Bevy users must keep
updating until the corresponding disconnected event arrives.

## Many sessions on shared sockets

Core sessions need no dedicated task, socket or runtime. An application can keep
many sessions on an unconnected UDP socket and schedule their earliest deadline
through an existing event loop or timer wheel.

Use one explicit socket per address family rather than assuming IPv6 dual-stack
behavior. Within each socket, key routes by `canonical_socket_addr(remote)`.
Client-local `RakSessionId` values are not globally unique. Reject a second live
route for the same local-socket/remote-endpoint tuple: connected RakNet datagrams
carry no additional connection ID to demultiplex those sessions.

A practical caller loop:

1. Retry retained writes without replacing their bytes or extending deadlines.
2. Read a bounded number of complete datagrams; canonicalize the source and route
   only to its matching attempt or session.
3. Feed due session updates with the current caller-supplied monotonic time.
4. Drain outputs in order under bounded consumer capacity, retain blocked writes
   and retire only the peer whose terminal output was reached.
5. Wake for the earliest remaining peer deadline or socket readiness. Activity on
   another route must not restart a peer's timeout.

[`shared_udp.rs`](../raknet/tests/shared_udp.rs) implements these actions for four
already established sessions, including blocked writes, per-peer cancellation,
timeouts and family checks. It uses a deliberately small receive sink drained
every round; it is not a complete application receive-backpressure policy.

Negotiation still needs a separate attempt owner per remote and correct probe
completion feedback. Reconnect through that owner. Replacing a table entry at
the same tuple cannot distinguish delayed UDP from the old generation. The
fixture does not establish concurrent negotiation or large-scale performance.

## Snapshots and adoption

`session.snapshot(now)` captures serializable protocol state, including retained
messages and congestion state. Drain pending outputs before capture: those
outputs are not included. Snapshots are application state, not a stable wire
format; use matching library revisions at both ends.

Restore with `RakSession::restore(snapshot, destination_now, elapsed)`, where
`elapsed` is the time since capture supplied by the transfer owner. Saved times
are portable millisecond offsets; restore rebases them into the destination's
monotonic clock domain. Invalid addresses and unrepresentable times return
`RakSessionRestoreError`. Restoring a closing session gives it a fresh close
timeout.

Core `RakClient::adopt` and `RakServer::adopt` take an owned session and emit it
through the normal `SessionConnected` path. Tokio `adopt` methods take a snapshot
and transfer elapsed duration. Server adoption assigns a fresh local session ID.
Adoption neither re-handshakes the peer nor moves its network route. Stop the
old owner and arrange socket/peer routing so only the new owner advances the
session; taking a snapshot alone leaves the old owner running.
