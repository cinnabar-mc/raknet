// Modified by Cinnabar contributors for caller-driven connection negotiation.
pub mod error;
mod msg;
mod phases;
mod retention;
pub use phases::{ConfiguredServer, RunningServer};

use crate::server::error::RakServerError;
use crate::server::msg::RakServerMsg;
use crate::session::wire;
use crate::session::{
    RakSession, RestoredSession, SessionEvent, SessionRetirement, spawn_closing, spawn_established,
};
use crate::stop::StopState;
use raknet::prelude::{
    Established, RakClock, RakServer as RakServerIntl, RakServerConfig, RakServerInput,
    RakServerOutput, RakSessionId, RakSessionInput, RakSessionSnapshot,
    RestoredSession as CoreRestoredSession, Sans, constants,
};
use retention::Route;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::debug;

/// A configured or running Tokio server.
///
/// Call `start` to bind the socket, then `accept` to receive established message
/// sessions. Keep the server running while using them and await `stop` at shutdown.
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
    /// Creates server configuration for the bind address; no socket is opened yet.
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

    /// Validates settings, binds the UDP socket and starts accepting connections.
    ///
    /// # Errors
    /// Returns the configured server and cause through `ServerStartError::into_parts`.
    /// Manual flushing is rejected before binding; Tokio sessions require autoflush.
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
        #[cfg(test)]
        let send_gate = crate::session::physical_send_tests::capture(&stop);

        let socket = Arc::new(socket);
        let (handle, _) = crate::inline::Task::spawn(|endpoint| {
            let stop = stop.clone();
            let socket = socket;
            let mut msg_rx = msg_rx;

            async move {
                let mut sessions: HashMap<RakSessionId, Route> = HashMap::new();
                let mut retained = HashMap::new();
                let retirement = Arc::new(SessionRetirement::default());
                let mut max_connections = config.max_connections;

                let mut buf = vec![0u8; config.max_mtu_size as usize].into_boxed_slice();
                let mut server = RakServerIntl::new(
                    config,
                    addr,
                    RakClock::new(Instant::now().into_std()),
                    Instant::now().into_std(),
                )
                .expect("session configuration was validated before binding");

                let weak_socket = Arc::downgrade(&socket);
                let writer = wire::SocketWriter::new(
                    Arc::clone(&stop),
                    move |bytes, peer| {
                        weak_socket
                            .upgrade()
                            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotConnected))?
                            .try_send_to(bytes, peer)
                    },
                    #[cfg(test)]
                    send_gate.clone(),
                );
                let (dgram_tx, mut dgram_rx) =
                    wire::datagrams_on(endpoint, Some(Arc::clone(&writer)));
                let (disconnect_tx, mut disconnect_rx) = unbounded_channel::<RakSessionId>();

                let mut stop_deadline: Option<Instant> = None;
                let mut pending: Option<wire::SendDatagram> = None;
                let mut adoptions: HashMap<
                    RakSessionId,
                    tokio::sync::oneshot::Sender<Result<RestoredSession, RakServerError>>,
                > = HashMap::new();

                let mut update_interval = interval(Duration::from_millis(
                    constants::AUTOFLUSH_INTERVAL_MS as u64,
                ));
                update_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    if retirement.take() {
                        retention::reap(
                            &mut sessions,
                            &mut retained,
                            &mut server,
                            max_connections,
                            Instant::now().into_std(),
                        );
                    }
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
                    if stop_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        break;
                    }
                    // Retained session output drains under the same first stop cutoff.
                    let draining =
                        stop_deadline.is_some() && sessions.is_empty() && msg_rx.is_empty();
                    if draining && pending.is_none() {
                        match dgram_rx.try_recv() {
                            Ok(datagram) => {
                                if retention::owns_wire(&datagram, &sessions, &retained) {
                                    pending = Some(wire::SendDatagram::Session(datagram));
                                } else {
                                    continue;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    writer.set_pending(pending.is_some());
                    tokio::select! {
                        _ = stop.wait(), if stop_deadline.is_none() => continue,
                        _ = async {
                            match stop_deadline {
                                Some(deadline) => tokio::time::sleep_until(deadline).await,
                                None => std::future::pending().await,
                            }
                        } => {}
                        _ = async {
                            let head = pending.as_ref().expect("one retained physical send");
                            #[cfg(test)]
                            crate::session::physical_send_tests::before_send(&send_gate).await;
                            socket.send_to(head.bytes(), head.peer()).await
                        }, if pending.is_some() => {
                            pending = None;
                        }
                        _ = retirement.wait() => continue,
                        _ = update_interval.tick(), if pending.is_none() && !draining => {
                            let now = Instant::now().into_std();

                            let _ = server.handle(RakServerInput::Update(now));
                        }
                        received = socket.recv_from(&mut buf), if pending.is_none() && !draining => {
                            // An error must not disable receiving until another branch wakes.
                            let Ok((len, addr)) = received else { continue };
                            let now = Instant::now().into_std();

                            if retirement.take() {
                                retention::reap(&mut sessions, &mut retained, &mut server, max_connections, now);
                            }
                            match server.handle(RakServerInput::Datagram(buf[..len].into(), addr, now)) {
                                Ok(_) => {},
                                Err(e) => debug!("server failed to handle inbound datagram: {e}")
                            }
                        }
                        Some(datagram) = dgram_rx.recv(), if pending.is_none() && !draining => {
                            if retention::owns_wire(&datagram, &sessions, &retained) {
                                pending = Some(wire::SendDatagram::Session(datagram));
                            }
                        }
                        Some(id) = disconnect_rx.recv() => {
                            let now = Instant::now().into_std();

                            retention::retire(id, &mut sessions, &mut retained, &mut server, now);
                            retention::reap(&mut sessions, &mut retained, &mut server, max_connections, now);
                        }
                        Some(msg) = msg_rx.recv(), if pending.is_none() && !draining => {
                            match msg {
                                #[cfg(test)]
                                RakServerMsg::InspectCommands(reply) => {
                                    let _ = reply.send(crate::endpoint_command_tests::QueueState {
                                        queued: msg_rx.len(),
                                        address: socket.local_addr().ok(),
                                    });
                                }
                                #[cfg(test)]
                                RakServerMsg::InspectRoutes(reply) => {
                                    let _ = reply.send(sessions.len());
                                }
                                RakServerMsg::SetMessage(msg) => {
                                    let _ = server.handle(RakServerInput::SetMessage(msg));
                                },
                                RakServerMsg::SetMaxConnections(n) => {
                                    max_connections = n;
                                    retention::reap(&mut sessions, &mut retained, &mut server, max_connections, Instant::now().into_std());
                                }
                                RakServerMsg::Block(ip, duration) => {
                                    let _ = server.handle(RakServerInput::Block(ip, duration, Instant::now().into_std()));
                                }
                                RakServerMsg::Unblock(ip) => {
                                    let _ = server.handle(RakServerInput::Unblock(ip));
                                }
                                RakServerMsg::Adopt(session, reply) => {
                                    retention::reap(&mut sessions, &mut retained, &mut server, max_connections, Instant::now().into_std());
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

                    while pending.is_none()
                        && !draining
                        && stop_deadline.is_none_or(|deadline| Instant::now() < deadline)
                        && let Some(msg) = server.poll()
                    {
                        match msg {
                            RakServerOutput::SocketDatagram(buf, addr) => {
                                pending = Some(wire::SendDatagram::Socket(buf, addr));
                            }
                            RakServerOutput::SessionDatagram(buf, id) => {
                                if let Some(session) = sessions.get_mut(&id) {
                                    let now = Instant::now().into_std();

                                    let input = RakSessionInput::Datagram(buf, now);
                                    if let Err(TrySendError::Full(_)) =
                                        session.try_send(SessionEvent::Io(input))
                                    {
                                        debug!("closing session {id:?}, its input queue is full");
                                        retention::retire(
                                            id,
                                            &mut sessions,
                                            &mut retained,
                                            &mut server,
                                            now,
                                        );
                                        retention::reap(
                                            &mut sessions,
                                            &mut retained,
                                            &mut server,
                                            max_connections,
                                            now,
                                        );
                                    }
                                } else {
                                    debug!("no session found with id {id:?}");
                                }
                            }
                            RakServerOutput::SessionClosing(session) => {
                                let id = session.id();
                                let (session, tx) =
                                    spawn_closing(session, dgram_tx.clone(), disconnect_tx.clone());
                                retention::insert(
                                    id,
                                    Route {
                                        input: tx,
                                        task: session.task(),
                                        lifetime: session.lifetime(Some(&retirement)),
                                    },
                                    &mut sessions,
                                    &retained,
                                );
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
                                    retention::insert(
                                        id,
                                        Route {
                                            input: tx,
                                            task: session.task(),
                                            lifetime: session.lifetime(Some(&retirement)),
                                        },
                                        &mut sessions,
                                        &retained,
                                    );
                                    continue;
                                }

                                retention::insert(
                                    id,
                                    Route {
                                        input: tx,
                                        task: session.task(),
                                        lifetime: session.lifetime(Some(&retirement)),
                                    },
                                    &mut sessions,
                                    &retained,
                                );

                                if let Some(reply) = adoptions.remove(&id) {
                                    let _ = reply.send(Ok(RestoredSession::Established(session)));
                                } else {
                                    let _ = session_tx.send(session);
                                }
                            }
                        }
                    }
                }

                #[cfg(test)]
                crate::session::physical_send_tests::finished(&send_gate);
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
    /// Reads the actual route map after earlier endpoint work without modifying it.
    #[cfg(test)]
    pub(crate) fn route_count_probe(&self) -> tokio::sync::oneshot::Receiver<usize> {
        let (send, receive) = tokio::sync::oneshot::channel();
        let _ = self
            .state
            .msg_tx
            .try_send(RakServerMsg::InspectRoutes(send));
        receive
    }

    /// Reads the original accepted FIFO without taking or acknowledging a handle.
    #[cfg(test)]
    pub(crate) fn unaccepted_count(&self) -> usize {
        self.state.session_rx.len()
    }

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

    /// Waits for the next established peer and returns its message session.
    ///
    /// # Errors
    /// Returns `RakServerError::Closed` when the accepting task has ended.
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
