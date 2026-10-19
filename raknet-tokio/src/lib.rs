mod client;
mod configuration;
mod server;
mod session;

pub mod prelude {
    pub use super::client::{
        ClientAdoptError, ClientStartError, ConfiguredClient, RakClient, RunningClient,
    };
    pub use super::server::{
        ConfiguredServer, RakServer, RunningServer, ServerAdoptError, ServerStartError,
        error::RakServerError,
    };
    pub use super::session::{RakSession, RestoredSession, SessionCloseError, SnapshotError};
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
