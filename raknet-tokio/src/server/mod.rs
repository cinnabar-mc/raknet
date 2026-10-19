// Modified by Cinnabar contributors for caller-driven connection negotiation.
pub mod error;
mod msg;
mod phases;
pub use phases::{ConfiguredServer, RunningServer};

use crate::server::error::RakServerError;
use crate::server::msg::RakServerMsg;
use crate::session::{RakSession, RestoredSession, SessionEvent, spawn_closing, spawn_established};
use crate::stop::StopState;
use raknet::prelude::{
    Established, RakClock, RakServer as RakServerIntl, RakServerConfig, RakServerInput,
    RakServerOutput, RakSessionId, RakSessionInput, RakSessionSnapshot,
    RestoredSession as CoreRestoredSession, Sans, constants,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Sender, channel, unbounded_channel};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::debug;

/// A configured or running server; only the running owner can accept sessions.
///
/// ```compile_fail
/// use raknet_tokio::prelude::*;
/// fn before_start(mut server: RakServer<ConfiguredServer>) {
///     server.accept();
/// }
/// ```
pub struct RakServer<S> {
    state: S,
}

impl RakServer<ConfiguredServer> {
    pub fn new<T>(addr: SocketAddr, conf: T) -> Self
    where
        T: FnOnce(&mut RakServerConfig),
    {
        let mut config = RakServerConfig::default();
        conf(&mut config);

        RakServer {
            state: ConfiguredServer { addr, config },
        }
    }

    /// Updates the advertisement before socket binding.
    pub fn set_message<T: Into<Box<[u8]>>>(&mut self, value: T) {
        self.state.config.message = value.into();
    }
    /// Updates configured connection admission before socket binding.
    pub fn set_max_connections(&mut self, value: usize) {
        self.state.config.max_connections = value;
    }

    /// Binds an automatic socket owner, retaining configuration on validation or bind failure.
    /// Manual session flushing is unsupported because this handle exposes no caller tick.
    pub async fn start(self) -> Result<RakServer<RunningServer>, ServerStartError> {
        if let Err(error) = self.state.config.session.validate() {
            return Err(ServerStartError {
                server: Box::new(self),
                cause: raknet::prelude::RakServerError::from(error).into(),
            });
        }
        if let Err(cause) =
            crate::configuration::require_automatic_flush(&self.state.config.session)
        {
            return Err(ServerStartError {
                server: Box::new(self),
                cause: cause.into(),
            });
        }
        let socket = match UdpSocket::bind(self.state.addr).await {
            Ok(socket) => socket,
            Err(error) => {
                return Err(ServerStartError {
                    server: Box::new(self),
                    cause: error.into(),
                });
            }
        };
        let config = self.state.config;
        let addr = self.state.addr;
        let (session_tx, session_rx) = unbounded_channel();
        let (msg_tx, msg_rx) = channel(constants::MAX_RECEIVE_MESSAGES);
        let stop = Arc::new(StopState::default());

        let handle = tokio::spawn({
            let stop = stop.clone();
            let socket = socket;
            let mut msg_rx = msg_rx;

            async move {
                let mut sessions: HashMap<RakSessionId, Sender<SessionEvent>> = HashMap::new();

                let mut buf = vec![0u8; config.max_mtu_size as usize].into_boxed_slice();
                let mut server = RakServerIntl::new(
                    config,
                    addr,
                    RakClock::new(Instant::now().into_std()),
                    Instant::now().into_std(),
                )
                .expect("session configuration was validated before binding");

                let (dgram_tx, mut dgram_rx) = unbounded_channel::<(Box<[u8]>, SocketAddr)>();
                let (disconnect_tx, mut disconnect_rx) = unbounded_channel::<RakSessionId>();

                let mut stop_deadline: Option<Instant> = None;
                let mut adoptions: HashMap<
                    RakSessionId,
                    tokio::sync::oneshot::Sender<Result<RestoredSession, RakServerError>>,
                > = HashMap::new();

                let mut update_interval = interval(Duration::from_millis(
                    constants::AUTOFLUSH_INTERVAL_MS as u64,
                ));
                update_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    if stop_deadline.is_none()
                        && let Some(deadline) = stop.deadline()
                    {
                        msg_rx.close();
                        let now = Instant::now().into_std();
                        for session in sessions.values() {
                            let _ = session.try_send(SessionEvent::Close(now));
                        }
                        stop_deadline = Some(deadline);
                    }
                    if let Some(deadline) = stop_deadline
                        && ((sessions.is_empty() && msg_rx.is_empty())
                            || Instant::now() >= deadline)
                    {
                        break;
                    }
                    tokio::select! {
                        _ = stop.wait(), if stop_deadline.is_none() => continue,
                        _ = update_interval.tick() => {
                            let now = Instant::now().into_std();

                            let _ = server.handle(RakServerInput::Update(now));
                        }
                        Ok((len, addr)) = socket.recv_from(&mut buf) => {
                            let now = Instant::now().into_std();

                            match server.handle(RakServerInput::Datagram(buf[..len].into(), addr, now)) {
                                Ok(_) => {},
                                Err(e) => debug!("server failed to handle inbound datagram: {e}")
                            }
                        }
                        Some((buf, addr)) = dgram_rx.recv() => {
                            let _ = socket.send_to(buf.as_ref(), addr).await;
                        }
                        Some(id) = disconnect_rx.recv() => {
                            let now = Instant::now().into_std();

                            let _ = server.handle(RakServerInput::RemoveSession(id, now));
                            sessions.remove(&id);
                        }
                        Some(msg) = msg_rx.recv() => {
                            match msg {
                                #[cfg(test)]
                                RakServerMsg::InspectCommands(reply) => {
                                    let _ = reply.send(crate::endpoint_command_tests::QueueState {
                                        queued: msg_rx.len(),
                                        address: socket.local_addr().ok(),
                                    });
                                }
                                RakServerMsg::SetMessage(msg) => {
                                    let _ = server.handle(RakServerInput::SetMessage(msg));
                                },
                                RakServerMsg::SetMaxConnections(n) => {
                                    let _ = server.handle(RakServerInput::SetMaxConnections(n));
                                }
                                RakServerMsg::Block(ip, duration) => {
                                    let _ = server.handle(RakServerInput::Block(ip, duration, Instant::now().into_std()));
                                }
                                RakServerMsg::Unblock(ip) => {
                                    let _ = server.handle(RakServerInput::Unblock(ip));
                                }
                                RakServerMsg::Adopt(session, reply) => {
                                    let id = match session {
                                        CoreRestoredSession::Established(session) => server.adopt(session).map_err(|error| error.into_parts().1),
                                        CoreRestoredSession::Closing(session) => server.adopt(session).map_err(|error| error.into_parts().1),
                                        CoreRestoredSession::Closed(terminal) => {
                                            let _ = reply.send(Ok(RestoredSession::Closed(terminal)));
                                            continue;
                                        }
                                    };
                                    match id {
                                        Ok(id) => { adoptions.insert(id, reply); }
                                        Err(cause) => { let _ = reply.send(Err(cause.into())); }
                                    }
                                }
                            }
                        }
                    }

                    while let Some(msg) = server.poll() {
                        match msg {
                            RakServerOutput::SocketDatagram(buf, addr) => {
                                let _ = socket.send_to(&buf, addr).await;
                            }
                            RakServerOutput::SessionDatagram(buf, id) => {
                                if let Some(session) = sessions.get_mut(&id) {
                                    let now = Instant::now().into_std();

                                    let input = RakSessionInput::Datagram(buf, now);
                                    if let Err(TrySendError::Full(_)) =
                                        session.try_send(SessionEvent::Io(input))
                                    {
                                        debug!("closing session {id:?}, its input queue is full");
                                        sessions.remove(&id);
                                    }
                                } else {
                                    debug!("no session found with id {id:?}");
                                }
                            }
                            RakServerOutput::SessionClosing(session) => {
                                let id = session.id();
                                let (session, tx) =
                                    spawn_closing(session, dgram_tx.clone(), disconnect_tx.clone());
                                sessions.insert(id, tx);
                                if let Some(reply) = adoptions.remove(&id) {
                                    let _ = reply.send(Ok(RestoredSession::Closing(session)));
                                }
                            }
                            RakServerOutput::SessionConnected(session) => {
                                let id = session.id();

                                debug!("session {:?} connected", id);

                                let (session, tx) = spawn_established(
                                    session,
                                    dgram_tx.clone(),
                                    disconnect_tx.clone(),
                                );

                                if stop_deadline.is_some() || stop.requested() {
                                    let _ =
                                        tx.try_send(SessionEvent::Close(Instant::now().into_std()));
                                    sessions.insert(id, tx);
                                    continue;
                                }

                                sessions.insert(id, tx);

                                if let Some(reply) = adoptions.remove(&id) {
                                    let _ = reply.send(Ok(RestoredSession::Established(session)));
                                } else {
                                    let _ = session_tx.send(session);
                                }
                            }
                        }
                    }
                }

                while let Ok((buf, addr)) = dgram_rx.try_recv() {
                    let _ = socket.send_to(buf.as_ref(), addr).await;
                }
            }
        });

        Ok(RakServer {
            state: RunningServer {
                handle: Some(handle),
                session_rx,
                msg_tx,
                stop,
            },
        })
    }
}

impl RakServer<RunningServer> {
    /// Reads only the original queue at this probe's FIFO position and the same bound socket.
    #[cfg(test)]
    pub(crate) fn command_queue_probe(
        &self,
    ) -> tokio::sync::oneshot::Receiver<crate::endpoint_command_tests::QueueState> {
        let (send, receive) = tokio::sync::oneshot::channel();
        let _ = self
            .state
            .msg_tx
            .try_send(RakServerMsg::InspectCommands(send));
        receive
    }

    /// Reserves before converting input, returning the original value on Full or Closed.
    fn try_command<T>(
        &self,
        value: T,
        command: impl FnOnce(T) -> RakServerMsg,
    ) -> Result<(), TrySendError<T>> {
        if self.state.stop.requested() {
            return Err(TrySendError::Closed(value));
        }
        match self.state.msg_tx.try_reserve() {
            Ok(permit) => {
                if self.state.stop.requested() {
                    return Err(TrySendError::Closed(value));
                }
                permit.send(command(value));
                Ok(())
            }
            Err(TrySendError::Full(())) => Err(TrySendError::Full(value)),
            Err(TrySendError::Closed(())) => Err(TrySendError::Closed(value)),
        }
    }

    /// Reserves the original FIFO while retaining the caller's already-restored session clock.
    async fn command(&self, message: RakServerMsg) -> Result<(), RakServerError> {
        tokio::select! {
            _ = self.state.stop.wait() => Err(RakServerError::Closed),
            permit = self.state.msg_tx.reserve() => {
                let permit = permit.map_err(|_| RakServerError::Closed)?;
                if self.state.stop.requested() {
                    return Err(RakServerError::Closed);
                }
                permit.send(message);
                Ok(())
            }
        }
    }

    /// Tries to update the advertisement, retaining unaccepted input on Full or Closed.
    pub fn set_message<T: Into<Box<[u8]>>>(&mut self, value: T) -> Result<(), TrySendError<T>> {
        self.try_command(value, |value| RakServerMsg::SetMessage(value.into()))
    }
    /// Tries to update future admission without reordering earlier accepted controls.
    pub fn set_max_connections(&mut self, value: usize) -> Result<(), TrySendError<usize>> {
        self.try_command(value, RakServerMsg::SetMaxConnections)
    }
    /// Tries to enqueue one ordered explicit IP block.
    pub fn block(
        &mut self,
        ip: IpAddr,
        duration: Duration,
    ) -> Result<(), TrySendError<(IpAddr, Duration)>> {
        self.try_command((ip, duration), |(ip, duration)| {
            RakServerMsg::Block(ip, duration)
        })
    }
    /// Tries to enqueue one ordered removal of an explicit IP block.
    pub fn unblock(&mut self, ip: IpAddr) -> Result<(), TrySendError<IpAddr>> {
        self.try_command(ip, RakServerMsg::Unblock)
    }
    /// Consumes the running capability and awaits its graceful stop.
    pub async fn stop(mut self) {
        self.state.stop.request();
        if let Some(handle) = self.state.handle.take() {
            let _ = handle.await;
        }
    }

    /// Resumes a session captured with [`crate::session::RakSession::snapshot`] on
    /// another server, returning its handle here. `elapsed` is the time since capture.
    /// Manual-flush captures are returned unchanged before restoration or dispatch;
    /// other restore/actor errors consume the capture.
    pub async fn adopt(
        &mut self,
        snapshot: RakSessionSnapshot,
        elapsed: Duration,
    ) -> Result<RestoredSession, ServerAdoptError> {
        if let Err(cause) = crate::configuration::require_automatic_flush(snapshot.config()) {
            return Err(ServerAdoptError::UnsupportedConfiguration {
                snapshot: Box::new(snapshot),
                cause,
            });
        }
        let session = snapshot
            .restore(Instant::now().into_std(), elapsed)
            .map_err(RakServerError::from)?;
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.command(RakServerMsg::Adopt(session, tx)).await?;

        Ok(rx.await.map_err(|_| RakServerError::Closed)??)
    }

    pub async fn accept(&mut self) -> Result<RakSession<Established>, RakServerError> {
        self.state
            .session_rx
            .recv()
            .await
            .ok_or(RakServerError::Closed)
    }
}

/// An adapter-policy rejection retains the original capture before any restore or actor dispatch.
#[derive(Debug, thiserror::Error)]
pub enum ServerAdoptError {
    /// This automatic endpoint cannot drive the saved manual-flush policy.
    #[error("server snapshot configuration is unsupported: {cause}")]
    UnsupportedConfiguration {
        /// The exact unconsumed application capture, without a rebased clock.
        snapshot: Box<RakSessionSnapshot>,
        /// Adapter-owned local validation failure.
        #[source]
        cause: std::io::Error,
    },
    /// Restoration or actor admission consumed the capture and failed.
    #[error(transparent)]
    Session(#[from] RakServerError),
}

/// A failed bind retains the original server configuration.
#[derive(thiserror::Error)]
#[error("server start failed: {cause}")]
pub struct ServerStartError {
    server: Box<RakServer<ConfiguredServer>>,
    #[source]
    cause: RakServerError,
}

impl std::fmt::Debug for ServerStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerStartError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl ServerStartError {
    /// Recovers the unstarted owner and original socket failure.
    pub fn into_parts(self) -> (RakServer<ConfiguredServer>, RakServerError) {
        (*self.server, self.cause)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod command_tests;
