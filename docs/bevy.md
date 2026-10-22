# Using RakNet with Bevy

The plugins run nonblocking UDP I/O in `PreUpdate`, with no Tokio runtime.
Match your Bevy version to [the manifest](../bevy-raknet/Cargo.toml).
Import networking types from `bevy_raknet::prelude`.

## Connect and send

Create a client, start an attempt and insert it into your app. The plugin
advances the handshake on subsequent updates. This example sends one message
when the connection event arrives and prints incoming replies.

```rust,no_run
use bevy_app::{App, PreUpdate, ScheduleRunnerPlugin};
use bevy_ecs::prelude::*;
use bevy_raknet::prelude::*;
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let mut client = RakClient::new(|_| {})?;
    client
        .idle()
        .expect("new client is idle")
        .connect("127.0.0.1:19132".parse()?)?;

    let mut app = App::new();
    app.add_plugins((
        ScheduleRunnerPlugin::run_loop(Duration::from_millis(16)),
        RakClientPlugin,
    ))
    .insert_resource(client)
    .add_systems(PreUpdate, exchange.after(RakClientSet));
    app.run();
    Ok(())
}

fn exchange(mut client: ResMut<RakClient>, mut events: MessageReader<RakClientEvent>) {
    for event in events.read() {
        match event {
            RakClientEvent::Connected => {
                if let Some(mut session) = client.connected() {
                    if let Err(error) = session.send(
                        b"hello".to_vec().into_boxed_slice(),
                        RakReliability::ReliableOrdered,
                        RakPriority::Normal,
                    ) {
                        eprintln!("Send failed: {error}");
                    }
                }
            }
            RakClientEvent::Disconnected { reason } => println!("Disconnected: {reason:?}"),
        }
    }
    while let Some(message) = client.recv() {
        println!("{}", String::from_utf8_lossy(&message));
    }
}
```

The example's schedule runner keeps networking ticking. In an existing Bevy
app, add the networking plugin, resource and systems to that app and keep its
usual runner. Systems in `PreUpdate` should run after `RakClientSet` or
`RakServerSet` to see the latest messages. Systems in a later schedule already
run after `PreUpdate`.

The current client uses one IPv4 socket and one connection. The
[sans-I/O core](integration.md) is available for custom address-family or
shared-socket scheduling.

## Accept and reply

Insert a `RakServer` and add `RakServerPlugin`:

```rust,no_run
use bevy_app::{App, PreUpdate, ScheduleRunnerPlugin};
use bevy_ecs::prelude::*;
use bevy_raknet::prelude::*;
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let server = RakServer::new("127.0.0.1:19132".parse()?, |config| {
        config.max_connections = 32;
    })?;
    let mut app = App::new();
    app.add_plugins((
        ScheduleRunnerPlugin::run_loop(Duration::from_millis(16)),
        RakServerPlugin,
    ))
    .insert_resource(server)
    .add_systems(PreUpdate, echo.after(RakServerSet));
    app.run();
    Ok(())
}

fn echo(mut server: ResMut<RakServer>) {
    while let Some((id, message)) = server.recv() {
        if let Some(mut session) = server.connected(id) {
            if let Err(error) = session.send(
                message,
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
            ) {
                eprintln!("Send failed: {error}");
            }
        }
    }
}
```

`RakServerEvent::SessionConnected` supplies the ID, address and GUID;
`SessionDisconnected` supplies the ID and reason. Read these through a
`MessageReader<RakServerEvent>`. `server.recv()` returns the oldest queued body
with its ID, including messages queued before a peer disconnected. When you
already have an ID, the borrowed session's `recv()` reads only that peer.

## Close a connection

Obtain `client.connected()` or `server.connected(id)` and call its consuming
`close()`. The returned closing session has receive access but no `send` method.
If close fails, `into_parts()` recovers the closing session and cause.

Release that borrow and keep the plugin updating until the disconnected event
arrives. Continue draining received messages too; closing preserves the accepted
prefix. Removing the resource immediately only performs best-effort cleanup.
There is no asynchronous endpoint `stop` method in this adapter.

The client can begin another attempt through `idle()` only after the previous
connection's data and notifications have drained. A server keeps the admission
slot until its queued data and notifications drain. The plugin transfers the
resource's notifications into Bevy messages each update; when driving the
resource manually, drain `next_event()` as well as `recv()`. Lowering
`set_max_connections`, including setting it to zero, affects new admissions.

## Configuration and capacity

The constructors bind their sockets immediately and can fail. Use a start
error's `into_parts()` to recover configuration and the I/O cause; a server start
error also returns the bind address. Both adapters require
`config.session.autoflush = true`, the default, and reject manual flushing before
binding. Only use `update()` yourself if you are deliberately driving the
resource without its plugin; do not update it twice each frame.

Keep receive queues moving. Per-session delivery uses the configured
`max_queued_bytes` and the core's `MAX_RECEIVE_MESSAGES` count limit. A message
that cannot fit closes that peer with `ReceiveBufferExceeded`; earlier messages
remain readable, and later messages cannot cross that gap. A pre-existing
protocol failure keeps its reason. These limits do not cover data retained by
your application after `recv()`.

`ping` sends a discovery request without connecting. Drain `recv_pong()` for
advertisements and latency. Discovery replies use the same count and configured
byte ceilings; excess replies are discarded without ending a game session.
The nonblocking socket edge is best effort. For custom blocked-write retention
or scheduling across many sessions, use the core directly.
