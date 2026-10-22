//! RakNet clients and servers driven by Bevy's `PreUpdate` schedule.
//!
//! Add [`prelude::RakClientPlugin`] with a [`prelude::RakClient`] resource, or
//! [`prelude::RakServerPlugin`] with a [`prelude::RakServer`]. The plugins drive
//! nonblocking UDP directly; no Tokio runtime is required.
//!
//! ```no_run
//! use bevy_app::App;
//! use bevy_raknet::prelude::*;
//!
//! let mut client = RakClient::new(|_| {})?;
//! client.idle().expect("new client is idle")
//!     .connect("127.0.0.1:19132".parse()?)?;
//! let mut app = App::new();
//! app.add_plugins(RakClientPlugin).insert_resource(client);
//! // Keep your app's runner updating. Read RakClientEvent, send through
//! // client.connected(), and drain client.recv() in your systems.
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The [Bevy guide](https://github.com/cinnabar-mc/raknet/blob/main/docs/bevy.md)
//! has client/server systems, scheduling and shutdown examples. Order `PreUpdate`
//! consumers after [`prelude::RakClientSet`] or [`prelude::RakServerSet`].

pub mod client;
mod configuration;
mod delivery;
pub mod server;
mod session;

/// Plugins, resources, connection messages and session types for Bevy systems.
pub mod prelude {
    pub use crate::client::{
        ClientCloseError, ClientStartError, RakClient, RakClientEvent, RakClientIdle,
        RakClientPlugin, RakClientSession, RakClientSet,
    };
    pub use crate::server::{
        RakServer, RakServerEvent, RakServerPlugin, RakServerSession, RakServerSet,
        ServerCloseError, ServerStartError,
    };
    pub use raknet::prelude::{
        Closing, Established, RakClientConfig, RakDisconnectReason, RakPriority, RakReliability,
        RakServerConfig, RakSessionError, RakSessionId,
    };
}
