//! Started attempt capabilities over the private network state machine.

use super::config::RakClientConfig;
use super::error::RakClientError;
use super::input::ClientInput;
use super::output::ClientOutput;
use super::{ClientEngine, ClientGuid, MtuProbeId};
use crate::clock::RakClock;
use crate::lifecycle::{Active, Closing, Configured, Established, SessionPhase};
use crate::sans::Sans;
use crate::session::lifecycle::RakSession;
use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// I/O feedback for a started attempt or its handed-off route.
#[derive(Debug)]
pub enum RakClientInput {
    MtuProbeResult {
        id: MtuProbeId,
        now: Instant,
        elapsed: Duration,
        too_large: bool,
    },
    Datagram(Box<[u8]>, SocketAddr, Instant),
    Update(Instant),
}

/// Ordered work or an owned established/closing session from the client driver.
#[derive(Debug)]
pub enum RakClientOutput {
    MtuProbe {
        id: MtuProbeId,
        datagram: Box<[u8]>,
        peer: SocketAddr,
    },
    SocketDatagram(Box<[u8]>, SocketAddr),
    SessionDatagram(Box<[u8]>),
    SessionConnected(RakSession<Established>),
    SessionClosing(RakSession<Closing>),
    Wait(Duration),
    Pong {
        addr: SocketAddr,
        message: Box<[u8]>,
        echoed_timestamp: u64,
        latency: Duration,
    },
}

/// A configured endpoint or a started attempt/route with exclusive ownership.
///
/// The network handshake remains internally enum-driven. Only its completed
/// handoff can grant an established session's application-send capability.
///
/// ```
/// use raknet::prelude::*;
/// let now = std::time::Instant::now();
/// let client = RakClient::new(RakClientConfig::default(), RakClock::new(now))?;
/// let active = client.connect("127.0.0.1:19132".parse()?, "0.0.0.0:0".parse()?,
///     ClientGuid::from_raw(7), now)?;
/// assert!(active.is_connecting());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ```compile_fail
/// use raknet::prelude::*;
/// fn start_twice(client: RakClient<Active>, peer: std::net::SocketAddr,
///     local: std::net::SocketAddr, guid: ClientGuid, now: std::time::Instant) {
///     client.connect(peer, local, guid, now);
/// }
/// ```
#[must_use = "the client owner must be driven or explicitly cancelled"]
pub struct RakClient<S> {
    engine: Box<ClientEngine>,
    adopted_closing: bool,
    phase: PhantomData<S>,
}

impl<S> fmt::Debug for RakClient<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RakClient")
            .field("connecting", &self.engine.is_connecting())
            .finish_non_exhaustive()
    }
}

impl RakClient<Configured> {
    /// Validates the endpoint configuration without performing I/O.
    pub fn new(config: RakClientConfig, clock: RakClock) -> Result<Self, RakClientError> {
        config.validate()?;
        Ok(Self {
            engine: Box::new(ClientEngine::new(config, clock)),
            adopted_closing: false,
            phase: PhantomData,
        })
    }

    /// Starts one attempt and consumes the right to start another until reset.
    pub fn connect(
        mut self,
        remote: SocketAddr,
        local: SocketAddr,
        guid: ClientGuid,
        now: Instant,
    ) -> Result<RakClient<Active>, ClientStartError> {
        if let Err(cause) = self.engine.handle(ClientInput::Connect {
            remote,
            local,
            guid,
            now,
        }) {
            return Err(ClientStartError {
                client: self,
                cause,
            });
        }
        Ok(RakClient {
            engine: self.engine,
            adopted_closing: false,
            phase: PhantomData,
        })
    }

    /// Queues an idle discovery ping with the caller's fresh identity and clock.
    pub fn ping(
        &mut self,
        remote: SocketAddr,
        guid: ClientGuid,
        now: Instant,
    ) -> Result<(), RakClientError> {
        self.engine.handle(ClientInput::Ping { remote, guid, now })
    }

    /// Adopts an owned phase without changing its local send capability.
    pub fn adopt<S: SessionPhase>(
        mut self,
        session: RakSession<S>,
    ) -> Result<RakClient<Active>, ClientAdoptError<S>> {
        if session.terminal_drained() {
            return Err(ClientAdoptError {
                client: self,
                session,
                cause: RakClientError::Closed,
            });
        }
        let closing = S::CLOSING;
        let (engine, terminal) = session.into_engine();
        match self.engine.adopt_owned(*engine) {
            Ok(()) => Ok(RakClient {
                engine: self.engine,
                adopted_closing: closing,
                phase: PhantomData,
            }),
            Err((engine, cause)) => Err(ClientAdoptError {
                client: self,
                session: RakSession::recover(engine, terminal),
                cause,
            }),
        }
    }
}

impl RakClient<Active> {
    /// Observes the offline phase for a bounded socket-edge read retry policy.
    pub fn is_offline_negotiating(&self) -> bool {
        self.engine.is_offline_negotiating()
    }
    /// Observes whether the peer's establishment is still pending.
    pub fn is_connecting(&self) -> bool {
        self.engine.is_connecting()
    }

    /// Cancels the attempt/route; a handed-off session must be closed separately.
    pub fn reset(mut self) -> RakClient<Configured> {
        // This private input is an infallible local reset and preserves the probe counter.
        let _ = self.engine.handle(ClientInput::Disconnect);
        RakClient {
            engine: self.engine,
            adopted_closing: false,
            phase: PhantomData,
        }
    }
}

impl<S> RakClient<S> {
    /// Takes the next ordered I/O obligation or owned phase handoff.
    pub fn poll(&mut self) -> Option<RakClientOutput> {
        Some(match self.engine.poll()? {
            ClientOutput::MtuProbe { id, datagram, peer } => {
                RakClientOutput::MtuProbe { id, datagram, peer }
            }
            ClientOutput::SocketDatagram(bytes, peer) => {
                RakClientOutput::SocketDatagram(bytes, peer)
            }
            ClientOutput::SessionDatagram(bytes) => RakClientOutput::SessionDatagram(bytes),
            ClientOutput::SessionConnected(engine) if self.adopted_closing => {
                RakClientOutput::SessionClosing(RakSession::from_engine(engine))
            }
            ClientOutput::SessionConnected(engine) => {
                RakClientOutput::SessionConnected(RakSession::from_engine(engine))
            }
            ClientOutput::Wait(after) => RakClientOutput::Wait(after),
            ClientOutput::Pong {
                addr,
                message,
                echoed_timestamp,
                latency,
            } => RakClientOutput::Pong {
                addr,
                message,
                echoed_timestamp,
                latency,
            },
        })
    }

    /// Applies socket feedback without exposing start, send or phase-cast inputs.
    fn io(&mut self, input: RakClientInput) -> Result<(), RakClientError> {
        self.engine.handle(match input {
            RakClientInput::MtuProbeResult {
                id,
                now,
                elapsed,
                too_large,
            } => ClientInput::MtuProbeResult {
                id,
                now,
                elapsed,
                too_large,
            },
            RakClientInput::Datagram(bytes, peer, now) => ClientInput::Datagram(bytes, peer, now),
            RakClientInput::Update(now) => ClientInput::Update(now),
        })
    }
}

impl Sans for RakClient<Active> {
    type Input = RakClientInput;
    type Output = RakClientOutput;
    type Error = RakClientError;
    fn handle(&mut self, input: Self::Input) -> Result<(), Self::Error> {
        self.io(input)
    }
    fn poll(&mut self) -> Option<Self::Output> {
        RakClient::poll(self)
    }
}

impl RakClient<Configured> {
    /// Advances idle endpoint timers without granting a connection-start capability.
    pub fn update(&mut self, now: Instant) -> Result<(), RakClientError> {
        self.io(RakClientInput::Update(now))
    }

    /// Routes idle discovery replies without starting a connection attempt.
    pub fn receive(
        &mut self,
        bytes: Box<[u8]>,
        peer: SocketAddr,
        now: Instant,
    ) -> Result<(), RakClientError> {
        self.io(RakClientInput::Datagram(bytes, peer, now))
    }
}

/// A failed start retains its configured owner and every pending output.
#[derive(Debug, thiserror::Error)]
#[error("client start failed: {cause}")]
pub struct ClientStartError {
    client: RakClient<Configured>,
    #[source]
    cause: RakClientError,
}

impl ClientStartError {
    /// Recovers the configured owner and original start error.
    pub fn into_parts(self) -> (RakClient<Configured>, RakClientError) {
        (self.client, self.cause)
    }
}

/// Rejected adoption retains both the configured endpoint and session phase.
#[derive(Debug, thiserror::Error)]
#[error("client adoption failed: {cause}")]
pub struct ClientAdoptError<S: SessionPhase> {
    client: RakClient<Configured>,
    session: RakSession<S>,
    #[source]
    cause: RakClientError,
}

impl<S: SessionPhase> ClientAdoptError<S> {
    /// Recovers both owners and the original adoption failure.
    pub fn into_parts(self) -> (RakClient<Configured>, RakSession<S>, RakClientError) {
        (self.client, self.session, self.cause)
    }
}
