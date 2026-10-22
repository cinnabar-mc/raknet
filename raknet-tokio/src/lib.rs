//! Async RakNet clients and servers for Tokio.
//!
//! Start a [`prelude::RakClient`] or [`prelude::RakServer`], connect or accept,
//! then exchange complete messages through [`prelude::RakSession`]. Keep the
//! running endpoint alive for its sessions and await `stop` when shutting down.
//!
//! ```no_run
//! use raknet_tokio::prelude::*;
//! # async fn exchange() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RakClient::new(|_| {}).start().await?;
//! let mut session = client.connect("127.0.0.1:19132".parse()?).await?;
//! let result = async {
//!     session.send(
//!         b"hello".to_vec().into_boxed_slice(),
//!         RakReliability::ReliableOrdered,
//!         RakPriority::Normal,
//!     ).await?;
//!     let reply = session.recv::<Box<[u8]>>().await?;
//!     println!("{}", String::from_utf8_lossy(&reply));
//!     Ok::<_, Box<dyn std::error::Error>>(())
//! }.await;
//! client.stop().await;
//! result
//! # }
//! ```
//!
//! See the [echo server and client](https://github.com/cinnabar-mc/raknet#send-a-message)
//! for a complete pair, and the [Tokio guide](https://github.com/cinnabar-mc/raknet/blob/main/docs/tokio.md)
//! for discovery, capacity, cancellation and shutdown. A successful send confirms
//! local protocol admission, not remote application processing.

mod client;
mod configuration;
#[cfg(feature = "test-support")]
pub mod impairment;
mod inline;
mod server;
mod session;
mod stop;

/// Async endpoint, session, configuration and delivery types.
pub mod prelude {
    pub use super::client::{
        ClientAdoptError, ClientStartError, ConfiguredClient, RakClient, RunningClient,
    };
    pub use super::server::{
        ConfiguredServer, RakServer, RunningServer, ServerAdoptError, ServerStartError,
        error::RakServerError,
    };
    pub use super::session::{
        RakSession, RestoredSession, SendPermit, SendReceipt, SessionCloseError, SessionSender,
        SnapshotError,
    };
    pub use raknet::prelude::{
        ClosedSession, Closing, Established, RakClientConfig, RakClientError, RakPriority,
        RakReliability, RakServerConfig, RakSessionConfig, RakSessionError, RakSessionSnapshot,
        SessionPhase,
    };
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod endpoint_command_tests;
