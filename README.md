# RakNet

RakNet provides message-based networking over UDP for Rust clients and servers.
It handles connection setup, reliable delivery, ordering, fragmentation and
congestion control. You send and receive complete messages.

## Installation

For the examples below, add:

```toml
[dependencies]
raknet-tokio = { git = "https://github.com/cinnabar-mc/raknet" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Send a message

Save this as `src/bin/server.rs`. It accepts one connection, echoes one message,
then stops.

```rust,no_run
use raknet_tokio::prelude::*;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut server = RakServer::new("127.0.0.1:19132".parse()?, |_| {})
        .start()
        .await?;

    let mut session = server.accept().await?;
    let exchange = async {
        let message = session.recv::<Box<[u8]>>().await?;
        session
            .send(
                message,
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
            )
            .await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;

    server.stop().await;
    exchange
}
```

Save this as `src/bin/client.rs`. It connects and sends a message:

```rust,no_run
use raknet_tokio::prelude::*;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = RakClient::new(|_| {}).start().await?;
    let mut session = client.connect("127.0.0.1:19132".parse()?).await?;
    let exchange = async {
        session
            .send(
                b"hello".to_vec().into_boxed_slice(),
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
            )
            .await?;
        let reply = session.recv::<Box<[u8]>>().await?;
        println!("{}", String::from_utf8_lossy(&reply));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;

    client.stop().await;
    exchange
}
```

Run `cargo run --bin server`, then `cargo run --bin client` in another terminal.

`start` opens the sockets; `connect` and `accept` return established sessions.
Keep the running client or server alive while using its sessions. `send` accepts
one message locally; it does not confirm that the remote application read it.
`recv` returns one message, or an error when the session ends.

These examples stop the whole endpoint after the exchange. To close one session
while keeping the endpoint running, see [Tokio shutdown](docs/tokio.md#shutdown).

## Other integrations

Use `raknet` or `bevy-raknet` from the same Git repository for the other
integrations. If you depend on multiple crates here, select the same revision
for all of them and commit your `Cargo.lock`. The crates use Rust 2024.

Choose the integration that fits your application:

| Crate | Best fit |
| --- | --- |
| [`raknet`](raknet/README.md) | Your own sockets, event loop or shared-socket scheduler. No runtime required. |
| [`raknet-tokio`](raknet-tokio/README.md) | Async clients and servers running on Tokio. |
| [`bevy-raknet`](bevy-raknet/README.md) | Networking driven by Bevy systems, without Tokio. |

## Guides and examples

- [Tokio](docs/tokio.md): discovery, send/receive, queue capacity and shutdown.
- [Sans-I/O](docs/integration.md): supply UDP input and time, handle MTU probes,
  and schedule multiple sessions on shared sockets.
- [Bevy](docs/bevy.md): add the plugins, handle connection events and exchange
  messages from systems.

The checkout also includes runnable Tokio examples:

```sh
cargo run --locked -p raknet-tokio --example echo -- 127.0.0.1:19132
cargo run --locked -p raknet-tokio --example ping -- 127.0.0.1:19132
```

The [echo server](raknet-tokio/examples/echo.rs) keeps echoing one peer until it
closes or you press Ctrl-C. [Ping](raknet-tokio/examples/ping.rs) reads its
advertisement without connecting. These examples exchange RakNet traffic; they
do not implement a Minecraft server.

## Development

From this checkout:

```sh
cargo test --locked --workspace --all-features
cargo doc --locked --workspace --no-deps
```

See [the CI workflow](.github/workflows/ci.yml) for formatting and lint checks,
and [the Go helper guide](tools/go-raknet/README.md) for interop tests and
benchmarks. Defaults and protocol limits are defined in
[`constants`](raknet/src/util/constants.rs).

## License

This is the [Cinnabar](https://github.com/cinnabar-mc) fork of
[bedrock-crustaceans/raknet](https://github.com/bedrock-crustaceans/raknet).
For Minecraft Login, packet encoding and resource packs, use a higher layer such
as [bedrock-net](https://github.com/cinnabar-mc/bedrock-net).

The original implementation and this fork use [Apache-2.0](LICENSE).
See [NOTICE](NOTICE) and the retained [MIT notice](LICENSE-MIT-go-raknet) for
Lunar go-raknet reference attribution.
