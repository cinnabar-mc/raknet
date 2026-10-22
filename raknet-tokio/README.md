# raknet-tokio

Async RakNet clients, servers and message sessions for Tokio. The adapter manages
UDP sockets, protocol updates and receive queues. Your application supplies
message bytes and handles received messages.

```toml
[dependencies]
raknet-tokio = { git = "https://github.com/cinnabar-mc/raknet" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

See the [echo server and client](../README.md#send-a-message) for a complete first
exchange. The usual call sequence is:

- Client: `RakClient::new(configure).start().await?`, then `connect(peer).await?`.
- Server: `RakServer::new(address, configure).start().await?`, then
  `accept().await?`.
- Session: `send(message, reliability, priority).await?` and
  `recv::<Box<[u8]>>().await?`.

`send` completes local protocol admission, not remote delivery. Keep the running
endpoint alive for its sessions, and keep reading: a full receive queue closes
that session while preserving messages already queued.

The [Tokio guide](../docs/tokio.md) explains discovery, send-only handles,
capacity reservations, cancellation, closing one session and stopping the
endpoint. Use [the core](../raknet/README.md) if you need to supply your own I/O
or scheduling instead.

[Apache-2.0](../LICENSE); see [NOTICE](../NOTICE) for attribution.
