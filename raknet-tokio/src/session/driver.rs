//! Actor-owned phases over the typed core session.

use raknet::prelude::*;
use std::time::Instant;

pub(super) enum Phase {
    Established(RakSession<Established>),
    Closing(RakSession<Closing>),
}

pub(super) struct Driver {
    phase: Option<Phase>,
}

impl Driver {
    /// Takes a genuinely negotiated send-capable session.
    pub(super) fn established(session: RakSession<Established>) -> Self {
        Self {
            phase: Some(Phase::Established(session)),
        }
    }
    /// Takes a restored locally closing session without reopening it.
    pub(super) fn closing(session: RakSession<Closing>) -> Self {
        Self {
            phase: Some(Phase::Closing(session)),
        }
    }
    /// Borrows the same validated session policy before starting the actor.
    pub(super) fn config(&self) -> &RakSessionConfig {
        match self
            .phase
            .as_ref()
            .expect("the actor owns its session phase")
        {
            Phase::Established(session) => session.config(),
            Phase::Closing(session) => session.config(),
        }
    }
    /// Applies protocol I/O in either live phase.
    pub(super) fn handle(&mut self, input: RakSessionInput) -> Result<(), RakSessionError> {
        match self.phase.as_mut().ok_or(RakSessionError::Closed)? {
            Phase::Established(session) => session.handle(input),
            Phase::Closing(session) => session.handle(input),
        }
    }
    /// Sends only while the actor still owns established permission.
    pub(super) fn send(
        &mut self,
        bytes: Box<[u8]>,
        reliability: RakReliability,
        priority: RakPriority,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        match self.phase.as_mut().ok_or(RakSessionError::Closed)? {
            Phase::Established(session) => session.send(bytes, reliability, priority, now),
            Phase::Closing(_) => Err(RakSessionError::Closed),
        }
    }
    /// Consumes local send permission even if a protocol transition fails.
    pub(super) fn close(&mut self, now: Instant, immediate: bool) -> Result<(), RakSessionError> {
        let phase = self.phase.take().ok_or(RakSessionError::Closed)?;
        let result = match phase {
            Phase::Established(session) if immediate => session.abort(now),
            Phase::Established(session) => session.close(now),
            Phase::Closing(session) if immediate => session.abort(now),
            Phase::Closing(session) => {
                self.phase = Some(Phase::Closing(session));
                return Ok(());
            }
        };
        match result {
            Ok(session) => {
                self.phase = Some(Phase::Closing(session));
                Ok(())
            }
            Err(error) => {
                let (session, cause) = error.into_parts();
                self.phase = Some(Phase::Closing(session));
                Err(cause)
            }
        }
    }
    /// Takes the next output without dropping its packet/terminal prefix.
    pub(super) fn poll(&mut self) -> Option<RakSessionOutput> {
        match self.phase.as_mut()? {
            Phase::Established(session) => session.poll(),
            Phase::Closing(session) => session.poll(),
        }
    }
    /// Observes network state without granting a local capability.
    pub(super) fn state(&self) -> RakSessionState {
        match self.phase.as_ref() {
            Some(Phase::Established(session)) => session.state(),
            Some(Phase::Closing(session)) => session.state(),
            None => RakSessionState::Disconnected,
        }
    }
    /// Captures the actor's actual saved phase.
    pub(super) fn snapshot(
        &self,
        now: Instant,
    ) -> Result<RakSessionSnapshot, RakSessionCaptureError> {
        match self
            .phase
            .as_ref()
            .expect("the actor owns its session phase")
        {
            Phase::Established(session) => session.snapshot(now),
            Phase::Closing(session) => session.snapshot(now),
        }
    }
    /// Produces terminal metadata after the actor has drained its terminal output.
    pub(super) fn finish(mut self) -> Option<ClosedSession> {
        match self.phase.take()? {
            Phase::Established(session) => session.finish().ok(),
            Phase::Closing(session) => session.finish().ok(),
        }
    }
}
