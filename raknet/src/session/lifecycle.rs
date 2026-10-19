//! Affine session ownership over the private protocol engine.

use super::RakSessionId;
use super::SessionEngine;
use super::error::RakSessionError;
use super::input::SessionInput;
use super::output::{RakDisconnectReason, RakSessionOutput};
use super::snapshot::{RakSessionRestoreError, SessionSnapshot};
use super::state::RakSessionState;
use crate::lifecycle::{Closing, Established, SessionPhase};
use crate::sans::Sans;
use crate::types::{RakPriority, RakReliability};
use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// I/O events accepted by established and closing sessions.
#[derive(Debug)]
pub enum RakSessionInput {
    Datagram(Box<[u8]>, Instant),
    Update(Instant),
}

/// A negotiated or explicitly restored session with local phase capabilities.
///
/// Live sessions cannot be cloned or constructed from raw endpoint metadata.
/// Peer failure remains a runtime result, even before local close.
///
/// ```compile_fail
/// use raknet::prelude::*;
/// fn duplicate_owner(session: RakSession<Established>) {
///     session.clone();
/// }
/// ```
///
/// ```compile_fail
/// use raknet::prelude::*;
/// fn send_after_close(mut session: RakSession<Closing>, now: std::time::Instant) {
///     session.send(Box::from([1_u8]), RakReliability::ReliableOrdered,
///         RakPriority::Normal, now);
/// }
/// ```
#[must_use = "sessions must be driven or explicitly retired"]
pub struct RakSession<S: SessionPhase> {
    pub(crate) engine: Box<SessionEngine>,
    terminal: Option<RakDisconnectReason>,
    phase: PhantomData<S>,
}

impl<S: SessionPhase> fmt::Debug for RakSession<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RakSession")
            .field("id", &self.id())
            .field("peer", &self.addr())
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl<S: SessionPhase> RakSession<S> {
    /// Wraps a state whose establishment or adoption was verified by its owner.
    pub(crate) fn from_engine(engine: Box<SessionEngine>) -> Self {
        Self {
            engine,
            terminal: None,
            phase: PhantomData,
        }
    }

    /// Moves the private engine together with already-observed terminal metadata.
    pub(crate) fn into_engine(self) -> (Box<SessionEngine>, Option<RakDisconnectReason>) {
        (self.engine, self.terminal)
    }

    /// Recovers an owner rejected by an internal adoption transition.
    pub(crate) fn recover(
        engine: Box<SessionEngine>,
        terminal: Option<RakDisconnectReason>,
    ) -> Self {
        Self {
            engine,
            terminal,
            phase: PhantomData,
        }
    }

    /// Detects a terminal owner that has no remaining output to transfer.
    pub(crate) fn terminal_drained(&self) -> bool {
        self.engine.state == RakSessionState::Disconnected && self.engine.output.is_empty()
    }

    /// Returns the endpoint-local routing identifier.
    pub fn id(&self) -> RakSessionId {
        self.engine.id
    }
    /// Returns the peer endpoint without allowing route mutation.
    pub fn addr(&self) -> SocketAddr {
        self.engine.addr
    }
    /// Returns the peer's negotiated GUID.
    pub fn guid(&self) -> u64 {
        self.engine.guid
    }
    /// Returns the engine's negotiated payload MTU.
    pub fn mtu(&self) -> u16 {
        self.engine.mtu
    }
    /// Observes current peer/liveness state without changing local capabilities.
    pub fn state(&self) -> RakSessionState {
        self.engine.get_state()
    }
    /// Returns the congestion controller's current round-trip estimate.
    pub fn rtt(&self) -> Duration {
        self.engine.rtt()
    }
    /// Returns bytes retained for outbound protocol delivery.
    pub fn outbound_bytes(&self) -> usize {
        self.engine.outbound_bytes()
    }
    /// Returns the largest complete application message this session can admit.
    pub fn max_message_len(&self) -> usize {
        self.engine.max_message_len()
    }

    /// Borrows the original immutable session policy for a caller-owned delivery edge.
    pub fn config(&self) -> &super::config::RakSessionConfig {
        &self.engine.config
    }

    /// Explicitly flushes protocol work when the caller owns the flush cadence.
    pub fn tick(&mut self, now: Instant) -> Result<(), RakSessionError> {
        self.engine.tick(now)
    }

    /// Captures trusted application state without omitting already-produced output.
    ///
    /// # Errors
    /// Returns `PendingOutput` until the output queue has been drained.
    pub fn snapshot(&self, now: Instant) -> Result<RakSessionSnapshot, RakSessionCaptureError> {
        if !self.engine.output.is_empty() {
            return Err(RakSessionCaptureError::PendingOutput);
        }
        Ok(RakSessionSnapshot {
            inner: self.engine.snapshot(now),
            local_closing: S::CLOSING,
        })
    }

    /// Transfers terminal metadata only after every queued output has been drained.
    pub fn finish(self) -> Result<ClosedSession, Self> {
        if self.engine.state != RakSessionState::Disconnected || !self.engine.output.is_empty() {
            return Err(self);
        }
        Ok(ClosedSession {
            id: self.id(),
            peer: self.addr(),
            reason: self.terminal,
        })
    }

    /// Consumes local permission and requests immediate retirement, retaining output.
    pub fn abort(self, now: Instant) -> Result<RakSession<Closing>, SessionCloseError> {
        self.begin_close(now, true)
    }

    /// Changes the local capability before invoking a fallible protocol transition.
    fn begin_close(
        self,
        now: Instant,
        immediate: bool,
    ) -> Result<RakSession<Closing>, SessionCloseError> {
        let mut closing = RakSession {
            engine: self.engine,
            terminal: self.terminal,
            phase: PhantomData,
        };
        let input = if immediate {
            SessionInput::DisconnectNow(now)
        } else {
            SessionInput::Disconnect(now)
        };
        match closing.engine.handle(input) {
            Ok(()) => Ok(closing),
            Err(cause) => Err(SessionCloseError {
                session: closing,
                cause,
            }),
        }
    }
}

#[cfg(test)]
mod tests;

impl RakSession<Established> {
    /// Submits one complete message; success does not acknowledge peer delivery.
    ///
    /// # Errors
    /// Returns closed, message-size or protocol errors. Inspect terminal outputs too.
    pub fn send(
        &mut self,
        bytes: Box<[u8]>,
        reliability: RakReliability,
        priority: RakPriority,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        self.engine
            .handle(SessionInput::Send(bytes, reliability, priority, now))
    }

    /// Consumes send permission and starts graceful close without dropping output.
    pub fn close(self, now: Instant) -> Result<RakSession<Closing>, SessionCloseError> {
        self.begin_close(now, false)
    }
}

impl<S: SessionPhase> Sans for RakSession<S> {
    type Input = RakSessionInput;
    type Output = RakSessionOutput;
    type Error = RakSessionError;

    fn handle(&mut self, input: Self::Input) -> Result<(), Self::Error> {
        self.engine.handle(match input {
            RakSessionInput::Datagram(bytes, now) => SessionInput::Datagram(bytes, now),
            RakSessionInput::Update(now) => SessionInput::Update(now),
        })
    }

    fn poll(&mut self) -> Option<Self::Output> {
        let output = self.engine.poll()?;
        if let RakSessionOutput::Disconnected(_, reason) = &output {
            self.terminal = Some(*reason);
        }
        Some(output)
    }
}

/// A failed close still owns the closing session and all preceding output.
#[derive(Debug, thiserror::Error)]
#[error("session close failed: {cause}")]
pub struct SessionCloseError {
    session: RakSession<Closing>,
    #[source]
    cause: RakSessionError,
}

impl SessionCloseError {
    /// Recovers the drainable closing owner and original protocol error.
    pub fn into_parts(self) -> (RakSession<Closing>, RakSessionError) {
        (self.session, self.cause)
    }
}

/// Terminal metadata with no protocol or application-send capability.
#[derive(Debug)]
pub struct ClosedSession {
    id: RakSessionId,
    peer: SocketAddr,
    reason: Option<RakDisconnectReason>,
}

impl ClosedSession {
    /// Returns the retired route's local identifier.
    pub fn id(&self) -> RakSessionId {
        self.id
    }
    /// Returns the retired peer endpoint.
    pub fn addr(&self) -> SocketAddr {
        self.peer
    }
    /// Returns the observed reason, or None for a previously drained snapshot.
    pub fn reason(&self) -> Option<RakDisconnectReason> {
        self.reason
    }
}

/// Captured application state, restored only through an explicit phase check.
///
/// Snapshots are trusted application state, not peer authentication evidence or
/// an untrusted-input format. Use matching library revisions across transfer.
#[derive(Clone, facet::Facet)]
pub struct RakSessionSnapshot {
    inner: SessionSnapshot,
    local_closing: bool,
}

/// Capture never discards protocol output that has already been produced.
#[derive(Debug, thiserror::Error)]
pub enum RakSessionCaptureError {
    #[error("session output must be drained before capture")]
    PendingOutput,
}

impl fmt::Debug for RakSessionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RakSessionSnapshot")
            .field("id", &self.inner.id)
            .field("state", &self.inner.state)
            .finish_non_exhaustive()
    }
}

/// A trusted restore preserves its saved local phase rather than reopening it.
#[derive(Debug)]
pub enum RestoredSession {
    Established(RakSession<Established>),
    Closing(RakSession<Closing>),
    Closed(ClosedSession),
}

impl RakSessionSnapshot {
    /// Borrows the saved local policy without restoring or changing this capture.
    pub fn config(&self) -> &super::config::RakSessionConfig {
        &self.inner.config
    }
    /// Returns the captured endpoint uptime without exposing mutable saved state.
    pub fn captured_uptime_ms(&self) -> u64 {
        self.inner.captured_uptime_ms
    }
    /// Rebases trusted saved state onto the destination clock and preserves phase.
    ///
    /// # Errors
    /// Rejects incomplete handshakes, invalid addresses and unrepresentable times.
    pub fn restore(
        self,
        now: Instant,
        elapsed: Duration,
    ) -> Result<RestoredSession, RakSessionRestoreError> {
        if self.inner.state == RakSessionState::Connecting {
            return Err(RakSessionRestoreError::IncompleteHandshake);
        }
        let local_closing = self.local_closing;
        let engine = Box::new(SessionEngine::restore(self.inner, now, elapsed)?);
        Ok(match engine.state {
            RakSessionState::Connected if local_closing => {
                RestoredSession::Closing(RakSession::from_engine(engine))
            }
            RakSessionState::Connected => {
                RestoredSession::Established(RakSession::from_engine(engine))
            }
            RakSessionState::Closing => RestoredSession::Closing(RakSession::from_engine(engine)),
            RakSessionState::Disconnected => RestoredSession::Closed(ClosedSession {
                id: engine.id,
                peer: engine.addr,
                reason: None,
            }),
            RakSessionState::Connecting => unreachable!("incomplete snapshots were rejected"),
        })
    }
}
