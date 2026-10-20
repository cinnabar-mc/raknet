// Modified by Cinnabar contributors for caller-driven connection negotiation.
mod client;
mod clock;
mod lifecycle;
#[cfg(not(feature = "test-support"))]
mod protocol;
/// Wire codecs, reachable only for benchmarks and cross-implementation tests.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod protocol;
mod sans;
mod server;
mod session;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support;
mod types;
mod util;

pub mod prelude {
    pub use crate::client::lifecycle::{
        ClientAdoptError, ClientStartError, RakClient, RakClientInput, RakClientOutput,
    };
    pub use crate::client::{
        ClientGuid, MtuProbeId, config::RakClientConfig, error::RakClientError,
    };
    pub use crate::clock::RakClock;
    pub use crate::lifecycle::{Active, Closing, Configured, Established, SessionPhase};
    pub use crate::sans::Sans;
    pub use crate::server::lifecycle::{RakServer, RakServerOutput, ServerAdoptError};
    pub use crate::server::{
        config::RakServerConfig, error::RakServerError, input::RakServerInput,
    };
    pub use crate::session::lifecycle::{
        ClosedSession, RakSession, RakSessionCaptureError, RakSessionInput, RakSessionSnapshot,
        RestoredSession, SessionCloseError,
    };
    pub use crate::session::snapshot::RakSessionRestoreError;
    pub use crate::session::{
        RakSessionId,
        config::RakSessionConfig,
        error::RakSessionError,
        output::{RakDisconnectReason, RakSessionOutput},
        state::RakSessionState,
    };
    pub use crate::types::*;
    pub use crate::util::constants;
    pub use crate::util::io_error::{is_mtu_probe_size_error, is_transient_udp_read_error};
    pub use crate::util::socket_addr::canonical_socket_addr;
}

#[cfg(test)]
mod manual_handshake_tests;
