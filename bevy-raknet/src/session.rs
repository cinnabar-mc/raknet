//! Plugin-owned session phases, never exposed as an unrestricted driver.

use raknet::prelude::*;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub(crate) enum SessionSlot {
    Established(RakSession<Established>),
    Closing(RakSession<Closing>),
}

impl SessionSlot {
    /// Captures final-delivery accounting from the same actually negotiated policy.
    pub(crate) fn receive_budget(&self) -> crate::delivery::ReceiveBudget {
        let policy = match self {
            Self::Established(session) => session.config(),
            Self::Closing(session) => session.config(),
        };
        crate::delivery::ReceiveBudget::new(policy.max_queued_bytes)
    }

    /// Returns whether this slot still owns local application-send permission.
    pub(crate) fn established(&self) -> bool {
        matches!(self, Self::Established(_))
    }
    /// Returns the routing identifier without exposing mutable core metadata.
    pub(crate) fn id(&self) -> RakSessionId {
        match self {
            Self::Established(s) => s.id(),
            Self::Closing(s) => s.id(),
        }
    }
    /// Returns the peer route.
    pub(crate) fn addr(&self) -> SocketAddr {
        match self {
            Self::Established(s) => s.addr(),
            Self::Closing(s) => s.addr(),
        }
    }
    /// Returns the peer GUID.
    pub(crate) fn guid(&self) -> u64 {
        match self {
            Self::Established(s) => s.guid(),
            Self::Closing(s) => s.guid(),
        }
    }
    /// Observes the current round-trip estimate in either phase.
    pub(crate) fn rtt(&self) -> Duration {
        match self {
            Self::Established(s) => s.rtt(),
            Self::Closing(s) => s.rtt(),
        }
    }
    /// Applies I/O to the actual typed owner.
    pub(crate) fn handle(&mut self, input: RakSessionInput) -> Result<(), RakSessionError> {
        match self {
            Self::Established(s) => s.handle(input),
            Self::Closing(s) => s.handle(input),
        }
    }
    /// Takes the next FIFO output from the actual typed owner.
    pub(crate) fn poll(&mut self) -> Option<RakSessionOutput> {
        match self {
            Self::Established(s) => s.poll(),
            Self::Closing(s) => s.poll(),
        }
    }
    /// Consumes local permission while retaining a closing owner on every result.
    pub(crate) fn close(
        self,
        now: Instant,
        immediate: bool,
    ) -> (Self, Result<(), RakSessionError>) {
        let result = match self {
            Self::Established(s) if immediate => s.abort(now),
            Self::Established(s) => s.close(now),
            Self::Closing(s) if immediate => s.abort(now),
            Self::Closing(s) => return (Self::Closing(s), Ok(())),
        };
        match result {
            Ok(s) => (Self::Closing(s), Ok(())),
            Err(error) => {
                let (s, cause) = error.into_parts();
                (Self::Closing(s), Err(cause))
            }
        }
    }
    /// Internal routing admission; public applications must hold a connected capability.
    pub(crate) fn send(
        &mut self,
        bytes: Box<[u8]>,
        reliability: RakReliability,
        priority: RakPriority,
        now: Instant,
    ) -> Result<(), RakSessionError> {
        match self {
            Self::Established(s) => s.send(bytes, reliability, priority, now),
            Self::Closing(_) => Err(RakSessionError::Closed),
        }
    }
}
