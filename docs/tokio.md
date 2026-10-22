# Using RakNet with Tokio

Start with the [echo server and client](../README.md#send-a-message).
Both use `raknet_tokio::prelude::*`; the similarly named types in `raknet` and
`bevy-raknet` have different APIs.

## Connect or accept

`RakClient::new(configure)` and `RakServer::new(address, configure)` create
configuration. Their consuming `start().await` methods open sockets and start
network tasks. A running client connects to one peer at a time; a running server
accepts sessions through `accept().await`.

Configure local limits before starting:

```rust,no_run
use raknet_tokio::prelude::*;

async fn start_server() -> Result<RakServer<RunningServer>, ServerStartError> {
    RakServer::new(
        "127.0.0.1:19132".parse().expect("literal address"),
        |config| {
            config.max_connections = 32;
            config.message = b"My RakNet server".to_vec().into_boxed_slice();
        },
    )
    .start()
    .await
}
```

A start failure's `into_parts()` returns the configured endpoint and the cause,
so you can report the error or change configuration before retrying. Tokio
requires `config.session.autoflush = true`, the default, because its sessions
have no manual `tick` method. Unsupported settings are rejected before binding.

Keep the running endpoint alive while using a session. Stopping it also ends its
sessions. `set_message`, `set_max_connections`, `block` and `unblock` on a running
server return `TrySendError::Full(input)` or `Closed(input)` if the control queue
cannot accept the change; inspect the result instead of assuming it took effect.

## Send and receive

An established session sends complete `Box<[u8]>` messages. `recv::<Box<[u8]>>()`
returns one complete message. Both operations also accept the conversions shown
in their signatures.

Use `RakReliability::ReliableOrdered` for reliable, ordered traffic and
`RakPriority::Normal` as a starting point. Other priorities change scheduling
order; even `Immediate` remains subject to congestion and receive-window limits.
A write can emit available datagrams immediately. It does not have to wait for
the automatic ACK/retransmission timer.

`send().await` confirms local protocol admission, not physical transmission or
remote application processing. It waits for bounded command and byte capacity;
a message larger than `max_message_len()` or the entire configured outbound
budget returns `MessageSizeExceeded`. Dropping a send future before commitment
releases its reservations. Dropping it after commitment does not retract the
message, so retrying blindly can duplicate application data.

### Send from another task

`session.sender()` returns a cloneable `SessionSender` without sharing receive
or close access. Reserve capacity before doing work that must happen immediately
before admission:

```rust,no_run
use raknet_tokio::prelude::*;

async fn send_reserved(
    session: &RakSession<Established>,
    body: Box<[u8]>,
) -> Result<(), RakSessionError> {
    let sender = session.sender();
    let permit = sender.reserve_send(body.len()).await?;
    let mut receipt = permit.commit(body, RakReliability::ReliableOrdered, RakPriority::Normal)?;
    receipt.admitted().await
}
```

`reserve_send` holds one command slot and a byte allowance without accepting a
message. Dropping the permit releases them. `commit` is synchronous and fails if
close revoked the reservation or the body exceeds the reserved size. The receipt
can be retained across cancellation of `admitted()`; it reports the same local
result once. Do not commit a replacement because that wait was canceled.

### Keep receive queues moving

Read promptly and bound any queue you add after `recv`. The adapter's receive
FIFO has a count limit and uses the session's `max_queued_bytes` as its complete
message byte budget. One valid message larger than that budget may occupy an
empty Tokio FIFO alone. No successor fits behind it.

The first message that cannot enter the final FIFO closes that session. Earlier
messages remain readable, and later messages cannot appear across the resulting
gap. The protocol reason is available through `finish()`. Calling `recv` releases
the adapter's byte charge; messages you keep afterwards use your application's
memory. These per-stage bounds are not a total process-memory limit.

## Discovery

`client.ping(peer).await` returns an advertisement and latency without connecting.
See [ping.rs](../raknet-tokio/examples/ping.rs). Its timeout includes waiting for
endpoint command capacity. Discovery metadata is bounded; excess simultaneous
pings return `PendingPingCapacityExceeded`.

Use a numeric `SocketAddr`, including `[::1]:19132` for IPv6. The client manages
separate IPv4 and IPv6 sockets. A server binds the address you supply; do not
assume an IPv6 wildcard bind also accepts IPv4 on every platform.

## Shutdown

`session.close()` consumes an established session immediately and returns a
`RakSession<Closing>`. It cannot send new application messages, but can still
receive messages already accepted. An error also contains that closing session:
recover it with `into_parts()` rather than losing unread messages.

```rust,no_run
use raknet_tokio::prelude::*;

async fn close_and_drain(
    session: RakSession<Established>,
) -> Result<ClosedSession, RakSession<Closing>> {
    let mut closing = match session.close() {
        Ok(closing) => closing,
        Err(error) => {
            let (closing, cause) = error.into_parts();
            eprintln!("Close request failed: {cause}");
            closing
        }
    };
    while let Ok(message) = closing.recv::<Box<[u8]>>().await {
        println!("Final message: {} bytes", message.len());
    }
    closing.finish()
}
```

A terminal `recv` error follows the accepted message prefix. `finish()` consumes
a fully retired, drained session and returns its disconnection metadata. If
retirement is not complete, it returns the session unchanged; retain it and
continue draining/observing completion. The first close cutoff is retained
across cancellation and repeated waits. Closing does not prove that the remote
application processed every send.

Keep the client/server running during this sequence. To stop the entire
endpoint, consume it with `client.stop().await` or `server.stop().await`.
Await this future to join the endpoint task. Dropping the future or resource is
not proof that shutdown completed. Stop bypasses a full command queue, but
unsent datagrams are discarded when its first close cutoff expires.

An accepted server session keeps its connection-limit slot until `finish()` or
whole-session drop, including unread messages after network disconnection.
Lowering `max_connections`, or setting it to zero, affects new admissions only.

## Snapshots and socket pressure

`snapshot().await` rejects undrained protocol output or already queued application
messages. A snapshot does not stop the original session or move its socket.
See [snapshots and adoption](integration.md#snapshots-and-adoption) before using
`client.adopt(snapshot, elapsed)` or `server.adopt(snapshot, elapsed)`.
Manual-flush snapshots are returned unchanged as `UnsupportedConfiguration`.

A blocked physical UDP write retains its datagram and destination until socket
readiness, route retirement or the original stop cutoff. Ordinary endpoint work
pauses behind that write. Use the [sans-I/O core](integration.md) if your
application needs its own shared-socket fairness or I/O backpressure policy.
