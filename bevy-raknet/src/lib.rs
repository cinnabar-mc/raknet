pub mod client;
pub mod server;
mod session;

pub mod prelude {
    pub use crate::client::{
        ClientCloseError, RakClient, RakClientEvent, RakClientIdle, RakClientPlugin,
        RakClientSession, RakClientSet,
    };
    pub use crate::server::{
        RakServer, RakServerEvent, RakServerPlugin, RakServerSession, RakServerSet,
        ServerCloseError,
    };
    pub use raknet::prelude::{
        Closing, Established, RakClientConfig, RakDisconnectReason, RakPriority, RakReliability,
        RakServerConfig, RakSessionError, RakSessionId,
    };
}
