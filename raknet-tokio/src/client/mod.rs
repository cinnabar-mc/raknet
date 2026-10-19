// Modified by Cinnabar contributors for caller-driven connection negotiation.
mod driver;
mod msg;
mod phases;
pub use phases::{ConfiguredClient, RunningClient};

use crate::client::msg::RakClientMsg;
use crate::prelude::{RakServerError, RakSession};
use crate::session::{RestoredSession, SessionEvent, spawn_closing, spawn_established};
use driver::Driver;
use raknet::prelude::{
    ClientGuid, Established, RakClientConfig, RakClientError, RakClientInput, RakClientOutput,
    RakClock, RakSessionId, RakSessionInput, RakSessionSnapshot,
    RestoredSession as CoreRestoredSession, canonical_socket_addr, constants,
    is_mtu_probe_size_error,
};
use std::collections::HashMap;
use std::mem::take;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Sender, unbounded_channel};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tracing::debug;

struct ClientSockets {
    v4: UdpSocket,
    v6: Option<UdpSocket>,
}

impl ClientSockets {
    async fn bind(port: u16) -> std::io::Result<Self> {
        let v4 = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
        let v6 = UdpSocket::bind((Ipv6Addr::UNSPECIFIED, port)).await.ok();

        Ok(Self { v4, v6 })
    }

    async fn recv(
        &self,
        buf4: &mut [u8],
        buf6: &mut [u8],
    ) -> std::io::Result<(Box<[u8]>, SocketAddr)> {
        tokio::select! {
            received = self.v4.recv_from(buf4) => {
                received.map(|(len, addr)| (buf4[..len].into(), addr))
            }
            received = async {
                match &self.v6 {
                    Some(socket) => socket.recv_from(buf6).await,
                    None => std::future::pending().await,
                }
            } => received.map(|(len, addr)| (buf6[..len].into(), addr)),
        }
    }

    /// Selects the socket matching the canonical endpoint family.
    fn socket(&self, addr: SocketAddr) -> std::io::Result<&UdpSocket> {
        if canonical_socket_addr(addr).is_ipv6() {
            self.v6.as_ref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "IPv6 client socket is unavailable",
                )
            })
        } else {
            Ok(&self.v4)
        }
    }

    /// Returns the selected unconnected socket's actual bound endpoint.
    fn local_addr(&self, addr: SocketAddr) -> std::io::Result<SocketAddr> {
        self.socket(addr)?.local_addr().map(canonical_socket_addr)
    }

    async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> std::io::Result<usize> {
        let addr = canonical_socket_addr(addr);
        self.socket(addr)?.send_to(buf, addr).await
    }
}

const PING_TIMEOUT: Duration = Duration::from_secs(5);
type PingResult = Result<(Box<[u8]>, Duration), RakClientError>;

struct PendingPing {
    sent_ms: u64,
    sender: oneshot::Sender<PingResult>,
}

/// Reclaims canceled discovery metadata and derives the count from its sole owning map.
fn reap_pings(pings: &mut HashMap<SocketAddr, Vec<PendingPing>>) -> usize {
    let mut retained = 0_usize;
    pings.retain(|_, queue| {
        queue.retain(|ping| !ping.sender.is_closed());
        retained = retained.saturating_add(queue.len());
        !queue.is_empty()
    });
    retained
}

/// Builds ping input and reply metadata, or reports generation failure to the caller.
fn prepare_ping(
    guid: std::io::Result<ClientGuid>,
    _remote: SocketAddr,
    now: std::time::Instant,
    clock: RakClock,
    sender: oneshot::Sender<PingResult>,
) -> Option<(ClientGuid, PendingPing)> {
    match guid {
        Ok(guid) => Some((
            guid,
            PendingPing {
                sent_ms: clock.timestamp(now),
                sender,
            },
        )),
        Err(error) => {
            let _ = sender.send(Err(RakClientError::IdentityGeneration(error)));
            None
        }
    }
}

fn fail_pending_connect(
    client: &mut Driver,
    connect: &mut Option<oneshot::Sender<Result<RakSession<Established>, RakClientError>>>,
    result: Result<(), RakClientError>,
) {
    let Err(e) = result else {
        return;
    };

    debug!("client failed to handle input: {e}");

    if !client.is_connecting()
        && let Some(sender) = connect.take()
    {
        let _ = sender.send(Err(e));
        client.reset();
    }
}

/// A configuration or started socket owner with phase-specific operations.
///
/// ```compile_fail
/// use raknet_tokio::prelude::*;
/// fn before_start(client: RakClient<ConfiguredClient>, peer: std::net::SocketAddr) {
///     client.ping(peer);
/// }
/// ```
pub struct RakClient<S> {
    state: S,
}

impl RakClient<ConfiguredClient> {
    pub fn new<T>(conf: T) -> Self
    where
        T: FnOnce(&mut RakClientConfig),
    {
        let mut config = RakClientConfig::default();
        conf(&mut config);

        Self {
            state: ConfiguredClient { config },
        }
    }

    /// Opens the automatic socket owner, retaining configuration on validation or bind failure.
    /// Manual session flushing is unsupported because this handle exposes no caller tick.
    pub async fn start(self) -> Result<RakClient<RunningClient>, ClientStartError> {
        if let Err(error) = self.state.config.validate() {
            let cause = std::io::Error::new(std::io::ErrorKind::InvalidInput, error).into();
            return Err(ClientStartError {
                client: Box::new(self),
                cause,
            });
        }
        if let Err(cause) =
            crate::configuration::require_automatic_flush(&self.state.config.session)
        {
            return Err(ClientStartError {
                client: Box::new(self),
                cause: cause.into(),
            });
        }
        let sockets = match ClientSockets::bind(self.state.config.local_port).await {
            Ok(sockets) => sockets,
            Err(error) => {
                return Err(ClientStartError {
                    client: Box::new(self),
                    cause: error.into(),
                });
            }
        };
        let config = self.state.config;
        let (msg_tx, msg_rx) = unbounded_channel();

        let handle = tokio::spawn({
            let sockets = sockets;
            let mut msg_rx = msg_rx;

            async move {
                let mut session_tx: Option<Sender<SessionEvent>> = None;
                let mut pings: HashMap<SocketAddr, Vec<PendingPing>> = HashMap::new();
                let mut connect: Option<
                    oneshot::Sender<Result<RakSession<Established>, RakClientError>>,
                > = None;

                let mut buf4 = vec![0u8; config.max_mtu_size as usize];
                let mut buf6 = vec![0u8; config.max_mtu_size as usize];
                let clock = RakClock::new(Instant::now().into_std());
                let mut client =
                    Driver::new(config, clock).expect("configuration was validated before binding");
                let mut adopting: Option<oneshot::Sender<Result<RestoredSession, RakClientError>>> =
                    None;

                let (dgram_tx, mut dgram_rx) = unbounded_channel::<(Box<[u8]>, SocketAddr)>();
                let (disconnect_tx, mut disconnect_rx) = unbounded_channel::<RakSessionId>();

                let timer = sleep(Duration::ZERO);
                tokio::pin!(timer);

                let mut stop_deadline: Option<Instant> = None;

                loop {
                    tokio::select! {
                        Ok((datagram, addr)) = sockets.recv(&mut buf4, &mut buf6) => {
                            let now = Instant::now().into_std();

                            let result = client.handle(RakClientInput::Datagram(datagram, addr, now));
                            fail_pending_connect(&mut client, &mut connect, result);
                        }
                        Some((buf, addr)) = dgram_rx.recv() => {
                            let _ = sockets.send_to(buf.as_ref(), addr).await;
                        }
                        Some(_) = disconnect_rx.recv() => {
                            session_tx = None;
                            client.reset();
                        }
                        Some(msg) = msg_rx.recv() => {
                            let now = Instant::now().into_std();
                            match msg {
                                RakClientMsg::Ping(addr, sender) => {
                                    let addr = canonical_socket_addr(addr);
                                    if reap_pings(&mut pings) >= constants::MAX_RECEIVE_MESSAGES {
                                        let _ = sender.send(Err(RakClientError::PendingPingCapacityExceeded));
                                    } else if let Some((guid, ping)) = prepare_ping(ClientGuid::generate(), addr, now, clock, sender) {
                                        let _ = client.ping(addr, guid, now);
                                        if !ping.sender.is_closed() {
                                            pings.entry(addr).or_default().push(ping);
                                        }
                                    }
                                }
                                #[cfg(test)]
                                RakClientMsg::InspectPings(reply) => {
                                    let _ = reply.send(bounded_pings_tests::PingState {
                                        rows: pings.len(),
                                        requests: pings.values().map(Vec::len).sum(),
                                        canceled: pings.values().flatten()
                                            .filter(|ping| ping.sender.is_closed()).count(),
                                    });
                                }
                                #[cfg(test)]
                                RakClientMsg::InspectCommands(reply) => {
                                    let _ = reply.send(crate::endpoint_command_tests::QueueState {
                                        queued: msg_rx.len(),
                                        address: sockets.v4.local_addr().ok(),
                                    });
                                }
                                RakClientMsg::Connect(addr, sender) => {
                                    let addr = canonical_socket_addr(addr);
                                    let result = sockets.local_addr(addr)
                                        .map_err(|_| RakClientError::InvalidConfig("client socket address is unavailable"))
                                        .and_then(|local| {
                                            let guid = ClientGuid::generate().map_err(RakClientError::IdentityGeneration)?;
                                            client.connect(addr, local, guid, now)
                                        });
                                    match result {
                                        Ok(()) => connect = Some(sender),
                                        Err(e) => {
                                            let _ = sender.send(Err(e));
                                        }
                                    }
                                }
                                RakClientMsg::Adopt(session, reply) => {
                                    let result = match session {
                                        CoreRestoredSession::Established(session) => client.adopt(session),
                                        CoreRestoredSession::Closing(session) => client.adopt(session),
                                        CoreRestoredSession::Closed(terminal) => {
                                            let _ = reply.send(Ok(RestoredSession::Closed(terminal)));
                                            continue;
                                        }
                                    };
                                    match result {
                                        Ok(()) => adopting = Some(reply),
                                        Err(error) => { let _ = reply.send(Err(error)); }
                                    }
                                }
                                RakClientMsg::Stop => {
                                    if let Some(session) = &session_tx {
                                        let _ = session.try_send(SessionEvent::Close(now));
                                    }

                                    stop_deadline = Some(
                                        Instant::now() + constants::CLOSE_TIMEOUT + Duration::from_secs(1),
                                    );
                                }
                            }
                        }
                        _ = &mut timer => {
                            reap_pings(&mut pings);
                            let result = client.handle(RakClientInput::Update(Instant::now().into_std()));
                            fail_pending_connect(&mut client, &mut connect, result);
                        }
                        _ = async {
                            match stop_deadline {
                                Some(deadline) => sleep_until(deadline).await,
                                None => std::future::pending().await,
                            }
                        } => {}
                    }

                    while let Some(msg) = client.poll() {
                        match msg {
                            RakClientOutput::MtuProbe { id, datagram, peer } => {
                                let started = Instant::now();
                                let result = sockets.send_to(&datagram, peer).await;
                                let elapsed = started.elapsed();
                                let input = RakClientInput::MtuProbeResult {
                                    id,
                                    now: Instant::now().into_std(),
                                    elapsed,
                                    too_large: result.as_ref().is_err_and(is_mtu_probe_size_error),
                                };
                                let result = client.handle(input);
                                fail_pending_connect(&mut client, &mut connect, result);
                            }
                            RakClientOutput::SocketDatagram(buf, addr) => {
                                let _ = sockets.send_to(&buf, addr).await;
                            }
                            RakClientOutput::SessionDatagram(buf) => {
                                if let Some(session) = &session_tx {
                                    let now = Instant::now().into_std();

                                    let input = RakSessionInput::Datagram(buf, now);
                                    if let Err(TrySendError::Full(_)) =
                                        session.try_send(SessionEvent::Io(input))
                                    {
                                        debug!("closing the session, its input queue is full");
                                        session_tx = None;
                                    }
                                } else {
                                    debug!("no session found");
                                }
                            }
                            RakClientOutput::SessionConnected(session) => {
                                debug!("session connected");

                                let (session, tx) = spawn_established(
                                    session,
                                    dgram_tx.clone(),
                                    disconnect_tx.clone(),
                                );

                                if stop_deadline.is_some() {
                                    let _ =
                                        tx.try_send(SessionEvent::Close(Instant::now().into_std()));
                                    session_tx = Some(tx);
                                    continue;
                                }

                                session_tx = Some(tx);

                                if let Some(reply) = adopting.take() {
                                    let _ = reply.send(Ok(RestoredSession::Established(session)));
                                } else if let Some(sender) = take(&mut connect) {
                                    let _ = sender.send(Ok(session));
                                }
                            }
                            RakClientOutput::SessionClosing(session) => {
                                let (session, tx) =
                                    spawn_closing(session, dgram_tx.clone(), disconnect_tx.clone());
                                session_tx = Some(tx);
                                if let Some(reply) = adopting.take() {
                                    let _ = reply.send(Ok(RestoredSession::Closing(session)));
                                }
                            }
                            RakClientOutput::Wait(duration) => {
                                timer.as_mut().reset(Instant::now() + duration);
                            }
                            RakClientOutput::Pong {
                                addr,
                                message,
                                echoed_timestamp,
                                latency,
                            } => {
                                if let Some(queue) = pings.get_mut(&addr) {
                                    queue.retain(|ping| !ping.sender.is_closed());

                                    if let Some(index) = queue
                                        .iter()
                                        .position(|ping| ping.sent_ms == echoed_timestamp)
                                    {
                                        let ping = queue.remove(index);
                                        let _ = ping.sender.send(Ok((message, latency)));
                                    }
                                    if queue.is_empty() {
                                        pings.remove(&addr);
                                    }
                                }
                            }
                        }
                    }

                    if let Some(deadline) = stop_deadline
                        && (session_tx.is_none() || Instant::now() >= deadline)
                    {
                        break;
                    }
                }

                while let Ok((buf, addr)) = dgram_rx.try_recv() {
                    let _ = sockets.send_to(buf.as_ref(), addr).await;
                }
            }
        });

        Ok(RakClient {
            state: RunningClient {
                handle: Some(handle),
                msg_tx,
            },
        })
    }
}

impl RakClient<RunningClient> {
    /// Reads only the original queue at this probe's FIFO position and the same bound socket.
    #[cfg(test)]
    pub(crate) fn command_queue_probe(
        &self,
    ) -> oneshot::Receiver<crate::endpoint_command_tests::QueueState> {
        let (send, receive) = oneshot::channel();
        let _ = self.state.msg_tx.send(RakClientMsg::InspectCommands(send));
        receive
    }

    /// Requests graceful stop and consumes the running capability.
    pub async fn stop(mut self) {
        let _ = self.state.msg_tx.send(RakClientMsg::Stop);
        if let Some(handle) = self.state.handle.take() {
            let _ = handle.await;
        }
    }

    /// Awaits an original discovery reply under the existing finite timeout.
    ///
    /// At most `constants::MAX_RECEIVE_MESSAGES` reply records are retained by the endpoint.
    /// Excess requests fail with `PendingPingCapacityExceeded` before a wire request is issued.
    /// Canceling an admitted wait does not retract an already queued wire request.
    pub async fn ping(&self, addr: SocketAddr) -> Result<(Box<[u8]>, Duration), RakClientError> {
        let msg_tx = &self.state.msg_tx;

        let (tx, rx) = oneshot::channel();

        let _ = msg_tx.send(RakClientMsg::Ping(addr, tx));
        timeout(PING_TIMEOUT, rx)
            .await
            .map_err(|_| RakClientError::Timeout)?
            .map_err(|_| RakClientError::Closed)?
    }

    /// Resumes a session captured with [`RakSession::snapshot`] on another client.
    ///
    /// Manual-flush captures are returned unchanged in [`ClientAdoptError::UnsupportedConfiguration`]
    /// before restoration or dispatch. Other errors consume the capture, including
    /// [`RakClientError::AlreadyConnected`] if this endpoint is already in use.
    /// The caller arranges peer routing; `elapsed` is the time since capture.
    pub async fn adopt(
        &self,
        snapshot: RakSessionSnapshot,
        elapsed: Duration,
    ) -> Result<RestoredSession, ClientAdoptError> {
        if let Err(cause) = crate::configuration::require_automatic_flush(snapshot.config()) {
            return Err(ClientAdoptError::UnsupportedConfiguration {
                snapshot: Box::new(snapshot),
                cause,
            });
        }
        let msg_tx = &self.state.msg_tx;

        let session = snapshot
            .restore(Instant::now().into_std(), elapsed)
            .map_err(RakClientError::from)?;
        let (tx, rx) = oneshot::channel();

        msg_tx
            .send(RakClientMsg::Adopt(session, tx))
            .map_err(|_| RakClientError::Closed)?;

        Ok(rx.await.map_err(|_| RakClientError::Closed)??)
    }

    pub async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<RakSession<Established>, RakClientError> {
        let msg_tx = &self.state.msg_tx;

        let (tx, rx) = oneshot::channel();

        let _ = msg_tx.send(RakClientMsg::Connect(addr, tx));
        rx.await.map_err(|_| RakClientError::Closed)?
    }
}

/// An adapter-policy rejection retains the original capture before any restore or actor dispatch.
#[derive(Debug, thiserror::Error)]
pub enum ClientAdoptError {
    /// This automatic endpoint cannot drive the saved manual-flush policy.
    #[error("client snapshot configuration is unsupported: {cause}")]
    UnsupportedConfiguration {
        /// The exact unconsumed application capture, without a rebased clock.
        snapshot: Box<RakSessionSnapshot>,
        /// Adapter-owned local validation failure.
        #[source]
        cause: std::io::Error,
    },
    /// Restoration or actor admission consumed the capture and failed.
    #[error(transparent)]
    Session(#[from] RakClientError),
}

/// A failed socket start retains the original configured client.
#[derive(thiserror::Error)]
#[error("client start failed: {cause}")]
pub struct ClientStartError {
    client: Box<RakClient<ConfiguredClient>>,
    #[source]
    cause: RakServerError,
}

impl std::fmt::Debug for ClientStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientStartError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl ClientStartError {
    /// Recovers the configuration and original bind/validation failure.
    pub fn into_parts(self) -> (RakClient<ConfiguredClient>, RakServerError) {
        (*self.client, self.cause)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod bounded_pings_tests;
