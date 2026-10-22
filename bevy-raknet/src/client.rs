// Modified by Cinnabar contributors for caller-driven connection negotiation.
use crate::delivery::ReceiveBudget;
use crate::session::SessionSlot;
use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use raknet::prelude::*;
mod driver;
use driver::Driver;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};
use tracing::debug;

#[cfg(test)]
pub(crate) mod bounded_delivery_tests;

const MAX_DATAGRAMS_PER_TICK: usize = 1024;

/// Updates a client resource in `PreUpdate` and publishes its connection messages.
pub struct RakClientPlugin;

impl Plugin for RakClientPlugin {
    fn build(&self, app: &mut App) {
        app.add_message::<RakClientEvent>();
        app.add_systems(
            PreUpdate,
            Self::update
                .in_set(RakClientSet)
                .run_if(resource_exists::<RakClient>),
        );
    }
}

impl RakClientPlugin {
    fn update(mut client: ResMut<RakClient>, mut events: MessageWriter<RakClientEvent>) {
        client.update();

        while let Some(event) = client.next_event() {
            events.write(event);
        }
    }
}

/// PreUpdate set containing RakClientPlugin's update system. Order your own
/// systems `.after(RakClientSet)` to see this tick's events/received data.
#[derive(SystemSet, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RakClientSet;

#[derive(Message, Clone, Copy, Debug)]
pub enum RakClientEvent {
    Connected,
    Disconnected { reason: RakDisconnectReason },
}

/// A nonblocking IPv4 client resource for one connection at a time.
/// Add `RakClientPlugin`, start through `idle`, then exchange messages through
/// `connected` and `recv` while the app keeps updating.
#[derive(Resource)]
pub struct RakClient {
    intl: Driver,
    socket: UdpSocket,
    session: Option<SessionSlot>,
    delivery: Option<ReceiveBudget>,
    received: VecDeque<Box<[u8]>>,
    pongs: VecDeque<(SocketAddr, Box<[u8]>, Duration)>,
    pong_budget: ReceiveBudget,
    dropped_pongs: u64,
    events: VecDeque<RakClientEvent>,
    buffer: Box<[u8]>,
}

impl RakClient {
    /// Binds a nonblocking IPv4 socket with the supplied configuration.
    ///
    /// # Errors
    /// Returns configuration and I/O failures through `ClientStartError::into_parts`.
    /// Manual flushing is unsupported and rejected before binding.
    pub fn new<T>(conf: T) -> Result<Self, ClientStartError>
    where
        T: FnOnce(&mut RakClientConfig),
    {
        let mut config = RakClientConfig::default();
        conf(&mut config);

        if let Err(error) = config.validate() {
            return Err(ClientStartError::new(
                config,
                std::io::Error::new(ErrorKind::InvalidInput, error),
            ));
        }
        if let Err(cause) = crate::configuration::require_automatic_flush(&config.session) {
            return Err(ClientStartError::new(config, cause));
        }
        let socket = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, config.local_port)) {
            Ok(socket) => socket,
            Err(cause) => return Err(ClientStartError::new(config, cause)),
        };
        if let Err(cause) = socket.set_nonblocking(true) {
            return Err(ClientStartError::new(config, cause));
        }

        Ok(Self {
            buffer: vec![0; config.max_mtu_size as usize].into_boxed_slice(),
            socket,
            session: None,
            delivery: None,
            received: VecDeque::new(),
            pongs: VecDeque::new(),
            pong_budget: ReceiveBudget::new(config.session.max_queued_bytes),
            dropped_pongs: 0,
            events: VecDeque::new(),
            intl: Driver::new(config, RakClock::new(Instant::now()))
                .expect("the same pure client configuration was validated before binding"),
        })
    }

    /// Returns a way to start a connection once earlier data and events have drained.
    pub fn idle(&mut self) -> Option<RakClientIdle<'_>> {
        (self.delivery.is_none() && self.session.is_none() && self.intl.is_idle())
            .then_some(RakClientIdle { client: self })
    }

    /// Borrows the established session for sending and receiving messages.
    pub fn connected(&mut self) -> Option<RakClientSession<'_, Established>> {
        self.session
            .as_ref()
            .is_some_and(SessionSlot::established)
            .then_some(RakClientSession {
                client: self,
                phase: std::marker::PhantomData,
            })
    }

    /// Borrows a locally closing slot without granting application-send permission.
    pub fn closing(&mut self) -> Option<RakClientSession<'_, Closing>> {
        self.session
            .as_ref()
            .is_some_and(|session| !session.established())
            .then_some(RakClientSession {
                client: self,
                phase: std::marker::PhantomData,
            })
    }

    /// Starts from the capability's real configured core owner.
    fn begin_connect(&mut self, addr: SocketAddr) -> Result<(), RakClientError> {
        let local = self
            .socket
            .local_addr()
            .map_err(|_| RakClientError::InvalidConfig("client socket address is unavailable"))?;
        let guid = ClientGuid::generate().map_err(RakClientError::IdentityGeneration)?;
        self.intl.connect(addr, local, guid, Instant::now())
    }

    pub fn ping(&mut self, addr: SocketAddr) {
        let guid = match ClientGuid::generate() {
            Ok(guid) => guid,
            Err(error) => {
                debug!("client ping identity generation failed: {error}");
                return;
            }
        };
        let _ = self.intl.ping(addr, guid, Instant::now());
        // Discovery is best effort, but repeated calls before a frame cannot retain its output.
        self.drain_endpoint(Instant::now());
    }

    /// Observes network establishment without granting a session capability.
    pub fn is_connecting(&self) -> bool {
        self.intl.is_connecting()
    }

    pub fn is_connected(&self) -> bool {
        self.session.is_some()
    }

    pub fn rtt(&self) -> Option<Duration> {
        self.session.as_ref().map(SessionSlot::rtt)
    }

    fn send<T>(
        &mut self,
        buf: T,
        reliability: RakReliability,
        priority: RakPriority,
    ) -> Result<(), RakSessionError>
    where
        T: Into<Box<[u8]>>,
    {
        let Some(session) = self.session.as_mut() else {
            return Err(RakSessionError::Closed);
        };

        let now = Instant::now();
        session.send(buf.into(), reliability, priority, now)?;
        self.drain_current(now);

        Ok(())
    }

    /// Takes the oldest accepted body, releasing its original delivery charge even after close.
    pub fn recv(&mut self) -> Option<Box<[u8]>> {
        let bytes = self.received.pop_front()?;
        self.delivery
            .as_mut()
            .expect("the original body retains its delivery owner")
            .release(bytes.len());
        self.reap_delivery();
        Some(bytes)
    }

    /// Takes a bounded discovery reply and releases its count and byte charge.
    pub fn recv_pong(&mut self) -> Option<(SocketAddr, Box<[u8]>, Duration)> {
        let pong = self.pongs.pop_front()?;
        self.pong_budget.release(pong.1.len());
        Some(pong)
    }

    /// Consumes the owned core phase before any output is drained.
    fn begin_close(&mut self) -> Result<(), RakSessionError> {
        let session = self.session.take().ok_or(RakSessionError::Closed)?;
        let now = Instant::now();
        let (session, result) = session.close(now, false);
        self.session = Some(session);
        self.drain_current(now);
        result
    }

    /// Takes a lifecycle notification; unread notifications retain the preceding allocation.
    pub fn next_event(&mut self) -> Option<RakClientEvent> {
        let event = self.events.pop_front()?;
        self.reap_delivery();
        Some(event)
    }

    /// Releases the one lifecycle reservation only after its original data and notifications drain.
    fn reap_delivery(&mut self) {
        if self.session.is_none() && self.received.is_empty() && self.events.is_empty() {
            self.delivery = None;
        }
    }

    /// Retires only the current typed slot, retaining the accepted final FIFO and its terminal.
    fn drain_current(&mut self, now: Instant) {
        let Some(delivery) = self.delivery.as_mut() else {
            return;
        };
        if let Some(reason) = drain_session(
            &mut self.session,
            &self.socket,
            &mut self.received,
            delivery,
            now,
        ) {
            self.session = None;
            self.intl.reset();
            self.events
                .push_back(RakClientEvent::Disconnected { reason });
        }
    }

    pub fn update(&mut self) {
        let now = Instant::now();

        for _ in 0..MAX_DATAGRAMS_PER_TICK {
            match self.socket.recv_from(&mut self.buffer) {
                Ok((len, addr)) => {
                    if let Err(e) = self.intl.handle(RakClientInput::Datagram(
                        self.buffer[..len].into(),
                        addr,
                        now,
                    )) {
                        debug!("client failed to handle inbound datagram: {e}");
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                _ => {}
            }
        }

        if let Err(e) = self.intl.handle(RakClientInput::Update(now)) {
            debug!("client failed to handle update: {e}");
        }

        self.drain_endpoint(now);

        if let Some(session) = self.session.as_mut() {
            let _ = session.handle(RakSessionInput::Update(now));
            self.drain_current(now);
        }
    }

    /// Drains the original endpoint output immediately without staging another discovery queue.
    fn drain_endpoint(&mut self, now: Instant) {
        while let Some(output) = self.intl.poll() {
            match output {
                RakClientOutput::MtuProbe { id, datagram, peer } => {
                    let started = Instant::now();
                    let result = self.socket.send_to(&datagram, peer);
                    let elapsed = started.elapsed();
                    let input = RakClientInput::MtuProbeResult {
                        id,
                        now: Instant::now(),
                        elapsed,
                        too_large: result.as_ref().is_err_and(is_mtu_probe_size_error),
                    };
                    if let Err(error) = self.intl.handle(input) {
                        debug!("client failed to complete an MTU probe: {error}");
                    }
                }
                RakClientOutput::SocketDatagram(buf, addr) => {
                    let _ = self.socket.send_to(&buf, addr);
                }
                RakClientOutput::SessionDatagram(buf) => {
                    if let Some(session) = self.session.as_mut() {
                        let _ = session.handle(RakSessionInput::Datagram(buf, now));
                    }
                }
                RakClientOutput::SessionConnected(session) => {
                    debug!("session connected");

                    let session = SessionSlot::Established(session);
                    self.delivery = Some(session.receive_budget());
                    self.session = Some(session);
                    self.events.push_back(RakClientEvent::Connected);
                }
                RakClientOutput::SessionClosing(session) => {
                    let session = SessionSlot::Closing(session);
                    self.delivery = Some(session.receive_budget());
                    self.session = Some(session);
                }
                RakClientOutput::Wait(_) => {}
                RakClientOutput::Pong {
                    addr,
                    message,
                    latency,
                    ..
                } => {
                    if self.pong_budget.admit(message.len()) {
                        self.pongs.push_back((addr, message, latency));
                    } else {
                        self.dropped_pongs = self.dropped_pongs.saturating_add(1);
                        debug!(
                            discarded = self.dropped_pongs,
                            "discarding an excess discovery reply"
                        );
                    }
                }
            }
        }
    }
}

/// A failed startup retains the unstarted client configuration and original cause.
pub struct ClientStartError {
    config: Box<RakClientConfig>,
    cause: std::io::Error,
}

impl ClientStartError {
    /// Keeps the configured value intact through validation and socket setup failures.
    fn new(config: RakClientConfig, cause: std::io::Error) -> Self {
        Self {
            config: Box::new(config),
            cause,
        }
    }

    /// Recovers the original configuration and failure for an explicit retry or retirement.
    pub fn into_parts(self) -> (RakClientConfig, std::io::Error) {
        (*self.config, self.cause)
    }
}

impl std::fmt::Debug for ClientStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientStartError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ClientStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client startup failed: {}", self.cause)
    }
}

impl std::error::Error for ClientStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

impl Drop for RakClient {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let (mut closing, _) = session.close(Instant::now(), true);
        while let Some(output) = closing.poll() {
            if let RakSessionOutput::Datagram(bytes, peer) = output {
                let _ = self.socket.send_to(&bytes, peer);
            }
        }
    }
}

/// Transfers the original output FIFO, permanently sealing delivery at its first rejected body.
fn drain_session(
    session: &mut Option<SessionSlot>,
    socket: &UdpSocket,
    received: &mut VecDeque<Box<[u8]>>,
    delivery: &mut ReceiveBudget,
    now: Instant,
) -> Option<RakDisconnectReason> {
    let mut disconnected = None;
    while let Some(output) = session.as_mut().and_then(SessionSlot::poll) {
        match output {
            RakSessionOutput::Datagram(buf, addr) => {
                let _ = socket.send_to(&buf, addr);
            }
            RakSessionOutput::Packet(buf) => {
                if delivery.admit(buf.len()) {
                    received.push_back(buf);
                } else if delivery.seal() {
                    let original = session
                        .take()
                        .expect("the output retains its original core");
                    let (closing, _) = original.close(now, true);
                    *session = Some(closing);
                }
            }
            RakSessionOutput::Disconnected(_, reason) => {
                disconnected.get_or_insert(delivery.reason(reason));
            }
            RakSessionOutput::Wait(_) => {}
        }
    }
    disconnected.or_else(|| delivery.failure())
}

/// An exclusive idle endpoint capability, consumed by connection start.
pub struct RakClientIdle<'a> {
    client: &'a mut RakClient,
}

impl RakClientIdle<'_> {
    /// Starts one attempt; plugin updates perform the actual network handshake.
    pub fn connect(self, peer: SocketAddr) -> Result<(), RakClientError> {
        self.client.begin_connect(peer)
    }
}

/// An exclusive application capability tied to the resource's actual typed slot.
pub struct RakClientSession<'a, S> {
    client: &'a mut RakClient,
    phase: std::marker::PhantomData<S>,
}

impl<S> RakClientSession<'_, S> {
    /// Takes the next already-produced application message.
    pub fn recv(&mut self) -> Option<Box<[u8]>> {
        self.client.recv()
    }
    /// Observes whether the plugin has retired this route.
    pub fn is_closed(&self) -> bool {
        self.client.session.is_none()
    }
}

impl<'a> RakClientSession<'a, Established> {
    /// Sends only while holding exclusive established permission.
    pub fn send<T: Into<Box<[u8]>>>(
        &mut self,
        bytes: T,
        reliability: RakReliability,
        priority: RakPriority,
    ) -> Result<(), RakSessionError> {
        self.client.send(bytes, reliability, priority)
    }
    /// Consumes the connected capability and retains the closing view on failure.
    pub fn close(self) -> Result<RakClientSession<'a, Closing>, ClientCloseError<'a>> {
        let result = self.client.begin_close();
        let session = RakClientSession {
            client: self.client,
            phase: std::marker::PhantomData,
        };
        match result {
            Ok(()) => Ok(session),
            Err(cause) => Err(ClientCloseError { session, cause }),
        }
    }
}

/// A failed close retains the exclusive closing capability and its receive prefix.
pub struct ClientCloseError<'a> {
    session: RakClientSession<'a, Closing>,
    cause: RakSessionError,
}

impl<'a> ClientCloseError<'a> {
    /// Recovers the closing view and original failure.
    pub fn into_parts(self) -> (RakClientSession<'a, Closing>, RakSessionError) {
        (self.session, self.cause)
    }
}

impl std::fmt::Debug for ClientCloseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCloseError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ClientCloseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}

impl std::error::Error for ClientCloseError<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
