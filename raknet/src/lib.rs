//! RakNet messages over UDP, with sockets and scheduling supplied by your application.
//!
//! Use [`prelude::RakClient`] to connect or [`prelude::RakServer`] to accept peers.
//! Feed received datagrams and timer updates through [`prelude::Sans::handle`],
//! then drain [`prelude::Sans::poll`] for writes, messages and deadlines.
//!
//! The [sans-I/O guide](https://github.com/cinnabar-mc/raknet/blob/main/docs/integration.md)
//! covers MTU write feedback, routing, shutdown and shared sockets. For an existing
//! runtime integration, see the `raknet-tokio` and `bevy-raknet` crates.
//!
//! # Start a client attempt
//!
//! ```
//! use raknet::prelude::*;
//! use std::time::Instant;
//!
//! let now = Instant::now();
//! let client = RakClient::new(RakClientConfig::default(), RakClock::new(now))?;
//! let mut attempt = client.connect(
//!     "127.0.0.1:19132".parse()?,
//!     "0.0.0.0:0".parse()?,
//!     ClientGuid::generate()?,
//!     now,
//! )?;
//! // Use your bound socket's local address above. Drain attempt.poll(), perform
//! // its writes, then feed received datagrams, write results and timer updates.
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Connection setup returns an established session through `SessionConnected`.
//! Only established sessions can send. After `close`, keep draining the closing
//! session's messages, datagrams and terminal output.

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

/// Common client, server, session and I/O types for a caller-driven integration.
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
