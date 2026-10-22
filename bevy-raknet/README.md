# bevy-raknet

RakNet clients and servers driven by Bevy systems. The plugins use nonblocking
UDP sockets and update in `PreUpdate`; they do not require Tokio.

```toml
[dependencies]
bevy-raknet = { git = "https://github.com/cinnabar-mc/raknet" }
```

Match your application's Bevy dependencies to the versions in
[this crate's manifest](Cargo.toml). Add `RakClientPlugin` and insert a
`RakClient`, or use `RakServerPlugin` with a `RakServer`. The plugin handles
network updates; your systems read connection messages and call the resource's
`recv` method.

Use `client.idle()` to start a connection. Once connected,
`client.connected()` or `server.connected(id)` provides the session's `send`
and `close` methods. Closing disables sends while the plugin finishes processing
the session.

The [Bevy guide](../docs/bevy.md) includes client and server systems, message
handling, scheduling and shutdown. The current client resource supports one
connection on an IPv4 socket. Use [the core](../raknet/README.md) for a custom
shared-socket or address-family policy.

See [the repository license](../LICENSE) and [NOTICE](../NOTICE).
