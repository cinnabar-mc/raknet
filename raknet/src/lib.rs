// Modified by Cinnabar contributors for caller-driven connection negotiation.
mod client;
mod protocol;
mod sans;
mod server;
mod session;
mod types;
mod util;

pub mod prelude {
    pub use crate::client::{
        MtuProbeId, RakClient, config::RakClientConfig, error::RakClientError,
        input::RakClientInput, output::RakClientOutput,
    };
    pub use crate::sans::Sans;
    pub use crate::server::{
        RakServer, config::RakServerConfig, error::RakServerError, input::RakServerInput,
        output::RakServerOutput,
    };
    pub use crate::session::snapshot::RakSessionSnapshot;
    pub use crate::session::{
        RakSession, RakSessionId,
        config::RakSessionConfig,
        error::RakSessionError,
        input::RakSessionInput,
        output::{RakDisconnectReason, RakSessionOutput},
        state::RakSessionState,
    };
    pub use crate::types::*;
    pub use crate::util::constants;
    pub use crate::util::io_error::{is_mtu_probe_size_error, is_transient_udp_read_error};
    pub use crate::util::socket_addr::canonical_socket_addr;
}
