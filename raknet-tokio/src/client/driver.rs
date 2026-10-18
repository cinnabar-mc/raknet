//! The socket actor's exclusive configured or started core owner.

use raknet::prelude::*;
use std::net::SocketAddr;
use std::time::Instant;

enum Phase {
    Configured(RakClient<Configured>),
    Active(RakClient<Active>),
}

pub(super) struct Driver {
    phase: Option<Phase>,
}

impl Driver {
    /// Validates and takes the passive core owner before the socket task starts.
    pub(super) fn new(config: RakClientConfig, clock: RakClock) -> Result<Self, RakClientError> {
        Ok(Self {
            phase: Some(Phase::Configured(RakClient::new(config, clock)?)),
        })
    }
    /// Performs one owned start transition, restoring the configured owner on failure.
    pub(super) fn connect(
        &mut self,
        remote: SocketAddr,
        local: SocketAddr,
        guid: ClientGuid,
        now: Instant,
    ) -> Result<(), RakClientError> {
        match self.phase.take().expect("the actor owns its phase") {
            Phase::Configured(client) => match client.connect(remote, local, guid, now) {
                Ok(client) => {
                    self.phase = Some(Phase::Active(client));
                    Ok(())
                }
                Err(error) => {
                    let (client, cause) = error.into_parts();
                    self.phase = Some(Phase::Configured(client));
                    Err(cause)
                }
            },
            Phase::Active(client) => {
                self.phase = Some(Phase::Active(client));
                Err(RakClientError::AlreadyConnected)
            }
        }
    }
    /// Applies I/O without granting another connection-start capability.
    pub(super) fn handle(&mut self, input: RakClientInput) -> Result<(), RakClientError> {
        match self.phase.as_mut().expect("the actor owns its phase") {
            Phase::Active(client) => client.handle(input),
            Phase::Configured(client) => match input {
                RakClientInput::Datagram(bytes, peer, now) => client.receive(bytes, peer, now),
                RakClientInput::Update(now) => client.update(now),
                RakClientInput::MtuProbeResult { .. } => Ok(()),
            },
        }
    }
    /// Queues discovery only while the endpoint is idle, as in the core policy.
    pub(super) fn ping(
        &mut self,
        remote: SocketAddr,
        guid: ClientGuid,
        now: Instant,
    ) -> Result<(), RakClientError> {
        match self.phase.as_mut().expect("the actor owns its phase") {
            Phase::Configured(client) => client.ping(remote, guid, now),
            Phase::Active(_) => Ok(()),
        }
    }
    /// Takes the next ordered output from the phase's actual owner.
    pub(super) fn poll(&mut self) -> Option<RakClientOutput> {
        match self.phase.as_mut()? {
            Phase::Configured(client) => client.poll(),
            Phase::Active(client) => client.poll(),
        }
    }
    /// Releases an attempt/route while preserving the endpoint's probe-ID counter.
    pub(super) fn reset(&mut self) {
        self.phase = self.phase.take().map(|phase| match phase {
            Phase::Active(client) => Phase::Configured(client.reset()),
            Phase::Configured(client) => Phase::Configured(client),
        });
    }
    /// Observes the internal network phase to preserve existing failure reporting.
    pub(super) fn is_connecting(&self) -> bool {
        matches!(self.phase.as_ref(), Some(Phase::Active(client)) if client.is_connecting())
    }
    /// Adopts an established session without minting a new session capability.
    pub(super) fn adopt<S: SessionPhase>(
        &mut self,
        session: RakSession<S>,
    ) -> Result<(), RakClientError> {
        match self.phase.take().expect("the actor owns its phase") {
            Phase::Configured(client) => match client.adopt(session) {
                Ok(client) => {
                    self.phase = Some(Phase::Active(client));
                    Ok(())
                }
                Err(error) => {
                    let (client, _session, cause) = error.into_parts();
                    self.phase = Some(Phase::Configured(client));
                    Err(cause)
                }
            },
            Phase::Active(client) => {
                self.phase = Some(Phase::Active(client));
                Err(RakClientError::AlreadyConnected)
            }
        }
    }
}
