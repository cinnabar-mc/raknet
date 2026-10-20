mod client;
mod configuration;
#[cfg(feature = "test-support")]
pub mod impairment;
mod server;
mod session;
mod stop;

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
