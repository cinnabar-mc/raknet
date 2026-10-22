//! Public server handoffs grant only the phase the endpoint actually owns.

use super::ServerEngine;
use super::config::RakServerConfig;
use super::error::RakServerError;
use super::input::RakServerInput;
use super::output::ServerOutput;
use crate::clock::RakClock;
use crate::lifecycle::{Closing, Established, SessionPhase};
use crate::sans::Sans;
use crate::session::RakSessionId;
use crate::session::lifecycle::RakSession;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Instant;

/// An ordered server I/O obligation or genuine owned session handoff.
#[derive(Debug)]
pub enum RakServerOutput {
    SocketDatagram(Box<[u8]>, SocketAddr),
    SessionDatagram(Box<[u8]>, RakSessionId),
    SessionConnected(RakSession<Established>),
    SessionClosing(RakSession<Closing>),
}

/// A RakNet server driven by your socket I/O and periodic updates.
///
/// Feed `RakServerInput` through `Sans::handle` and drain `Sans::poll` for
/// replies, connected sessions and datagrams to route to those sessions.
pub struct RakServer {
    engine: ServerEngine,
    closing_adoptions: HashSet<RakSessionId>,
}

impl RakServer {
    /// Constructs the endpoint with the caller's bound address and monotonic time.
    pub fn new(
        config: RakServerConfig,
        addr: SocketAddr,
        clock: RakClock,
        now: Instant,
    ) -> Result<Self, RakServerError> {
        config.session.validate()?;
        Ok(Self {
            engine: ServerEngine::new(config, addr, clock, now),
            closing_adoptions: HashSet::new(),
        })
    }

    /// Consumes an already established or closing phase and assigns a local ID.
    /// Capacity and duplicate-route refusals return the unchanged original owner.
    pub fn adopt<S: SessionPhase>(
        &mut self,
        session: RakSession<S>,
    ) -> Result<RakSessionId, ServerAdoptError<S>> {
        if session.terminal_drained() {
            return Err(ServerAdoptError {
                session,
                cause: crate::session::error::RakSessionError::Closed.into(),
            });
        }
        if self.engine.session_map.len() >= self.engine.config.max_connections {
            return Err(ServerAdoptError {
                session,
                cause: RakServerError::RefusingConnection("maximum connections reached".into()),
            });
        }
        if self.engine.session_map.contains_key(&session.addr()) {
            return Err(ServerAdoptError {
                session,
                cause: RakServerError::RefusingConnection("the remote route already exists".into()),
            });
        }
        let (engine, _) = session.into_engine();
        let id = self.engine.adopt(*engine);
        if S::CLOSING {
            self.closing_adoptions.insert(id);
        }
        Ok(id)
    }
}

/// Rejected adoption keeps the session and its saved local phase owned.
#[derive(Debug, thiserror::Error)]
#[error("server adoption failed: {cause}")]
pub struct ServerAdoptError<S: SessionPhase> {
    session: RakSession<S>,
    #[source]
    cause: RakServerError,
}

impl<S: SessionPhase> ServerAdoptError<S> {
    /// Recovers the unchanged owner and original adoption error.
    pub fn into_parts(self) -> (RakSession<S>, RakServerError) {
        (self.session, self.cause)
    }
}

impl Sans for RakServer {
    type Input = RakServerInput;
    type Output = RakServerOutput;
    type Error = RakServerError;

    fn handle(&mut self, input: Self::Input) -> Result<(), Self::Error> {
        self.engine.handle(input)
    }

    fn poll(&mut self) -> Option<Self::Output> {
        Some(match self.engine.poll()? {
            ServerOutput::SocketDatagram(bytes, peer) => {
                RakServerOutput::SocketDatagram(bytes, peer)
            }
            ServerOutput::SessionDatagram(bytes, id) => RakServerOutput::SessionDatagram(bytes, id),
            ServerOutput::SessionConnected(engine) if self.closing_adoptions.remove(&engine.id) => {
                RakServerOutput::SessionClosing(RakSession::from_engine(engine))
            }
            ServerOutput::SessionConnected(engine) => {
                RakServerOutput::SessionConnected(RakSession::from_engine(engine))
            }
        })
    }
}
