use crate::delivery::ReceiveBudget;
use crate::session::SessionSlot;
use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use raknet::prelude::RakServerInput::{SetMaxConnections, SetMessage};
use raknet::prelude::{RakServer as RakServerIntl, *};
use std::collections::{HashMap, VecDeque};
use std::io::ErrorKind;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};
use tracing::debug;

#[cfg(test)]
pub(crate) mod bounded_delivery_tests;

const MAX_DATAGRAMS_PER_TICK: usize = 1024;

/// Updates a server resource in `PreUpdate` and publishes its connection messages.
pub struct RakServerPlugin;

impl Plugin for RakServerPlugin {
    fn build(&self, app: &mut App) {
        app.add_message::<RakServerEvent>();
        app.add_systems(
            PreUpdate,
            Self::update
                .in_set(RakServerSet)
                .run_if(resource_exists::<RakServer>),
        );
    }
}

impl RakServerPlugin {
    fn update(mut server: ResMut<RakServer>, mut events: MessageWriter<RakServerEvent>) {
        server.update();

        while let Some(event) = server.next_event() {
            events.write(event);
        }
    }
}

/// PreUpdate set containing RakServerPlugin's update system. Order your own
/// systems `.after(RakServerSet)` to see this tick's events/received data.
#[derive(SystemSet, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RakServerSet;

#[derive(Message, Clone, Copy, Debug)]
pub enum RakServerEvent {
    SessionConnected {
        id: RakSessionId,
        addr: SocketAddr,
        guid: u64,
    },
    SessionDisconnected {
        id: RakSessionId,
        reason: RakDisconnectReason,
    },
}

struct SessionEntry {
    session: Option<SessionSlot>,
    delivery: ReceiveBudget,
    notifications: u8,
}

impl SessionEntry {
    /// Keeps one original allocation until its final data and reserved lifecycle drain.
    fn new(session: SessionSlot, notifications: u8) -> Self {
        Self {
            delivery: session.receive_budget(),
            session: Some(session),
            notifications,
        }
    }
}

/// A nonblocking UDP server resource driven by `RakServerPlugin`.
/// Read `RakServerEvent` messages for peer IDs, send through `connected(id)`
/// and drain received messages through `recv`.
#[derive(Resource)]
pub struct RakServer {
    intl: RakServerIntl,
    socket: UdpSocket,
    sessions: HashMap<RakSessionId, SessionEntry>,
    max_connections: usize,
    received: VecDeque<(RakSessionId, Box<[u8]>)>,
    events: VecDeque<RakServerEvent>,
    buffer: Box<[u8]>,
}

impl RakServer {
    /// Binds a nonblocking UDP socket at the supplied address.
    ///
    /// # Errors
    /// Returns the address, configuration and I/O cause through `ServerStartError::into_parts`.
    /// Manual flushing is unsupported and rejected before binding.
    pub fn new<T>(addr: SocketAddr, conf: T) -> Result<Self, ServerStartError>
    where
        T: FnOnce(&mut RakServerConfig),
    {
        let mut config = RakServerConfig::default();
        conf(&mut config);

        if let Err(error) = config.session.validate() {
            return Err(ServerStartError::new(
                addr,
                config,
                std::io::Error::new(ErrorKind::InvalidInput, error),
            ));
        }
        if let Err(cause) = crate::configuration::require_automatic_flush(&config.session) {
            return Err(ServerStartError::new(addr, config, cause));
        }
        let socket = match UdpSocket::bind(addr) {
            Ok(socket) => socket,
            Err(cause) => return Err(ServerStartError::new(addr, config, cause)),
        };
        if let Err(cause) = socket.set_nonblocking(true) {
            return Err(ServerStartError::new(addr, config, cause));
        }

        Ok(Self {
            socket,
            buffer: vec![0; config.max_mtu_size as usize].into_boxed_slice(),
            sessions: HashMap::new(),
            max_connections: config.max_connections,
            received: VecDeque::new(),
            events: VecDeque::new(),
            intl: RakServerIntl::new(config, addr, RakClock::new(Instant::now()), Instant::now())
                .expect("the same pure session configuration was validated before binding"),
        })
    }

    pub fn set_message<T>(&mut self, val: T)
    where
        T: Into<Box<[u8]>>,
    {
        let _ = self.intl.handle(SetMessage(val.into()));
    }

    /// Changes future admission without releasing live or unread retired allocations.
    pub fn set_max_connections(&mut self, val: usize) {
        self.max_connections = val;
        self.sync_admission();
    }

    fn send<T>(
        &mut self,
        id: RakSessionId,
        buf: T,
        reliability: RakReliability,
        priority: RakPriority,
    ) -> Result<(), RakSessionError>
    where
        T: Into<Box<[u8]>>,
    {
        let now = Instant::now();
        let mut disconnected = Vec::new();

        {
            let Some(entry) = self.sessions.get_mut(&id) else {
                return Err(RakSessionError::Closed);
            };
            let session = entry.session.as_mut().ok_or(RakSessionError::Closed)?;
            session.send(buf.into(), reliability, priority, now)?;
            if let Some(reason) = drain_session(entry, &self.socket, &mut self.received, now) {
                disconnected.push((id, reason));
            }
        }

        self.remove_sessions(disconnected, now);

        Ok(())
    }

    /// Borrows an established peer's session for sending and receiving messages.
    pub fn connected(&mut self, id: RakSessionId) -> Option<RakServerSession<'_, Established>> {
        self.sessions
            .get(&id)
            .and_then(|entry| entry.session.as_ref())
            .is_some_and(SessionSlot::established)
            .then_some(RakServerSession {
                server: self,
                id,
                phase: std::marker::PhantomData,
            })
    }

    /// Borrows a closing route without exposing application-send methods.
    pub fn closing(&mut self, id: RakSessionId) -> Option<RakServerSession<'_, Closing>> {
        self.sessions
            .get(&id)
            .and_then(|entry| entry.session.as_ref())
            .is_some_and(|session| !session.established())
            .then_some(RakServerSession {
                server: self,
                id,
                phase: std::marker::PhantomData,
            })
    }

    /// Takes the oldest accepted body, including the retained prefix of a retired peer.
    pub fn recv(&mut self) -> Option<(RakSessionId, Box<[u8]>)> {
        let (id, bytes) = self.received.pop_front()?;
        self.release_body(id, bytes.len());
        Some((id, bytes))
    }

    /// Releases a body removed from either global or per-peer public receive.
    fn release_body(&mut self, id: RakSessionId, bytes: usize) {
        self.sessions
            .get_mut(&id)
            .expect("the original body retains its admission entry")
            .delivery
            .release(bytes);
        self.reap(id);
    }

    /// Moves local permission to Closing before draining any produced output.
    fn begin_close(&mut self, id: RakSessionId) -> Result<(), RakSessionError> {
        let now = Instant::now();
        let entry = self.sessions.get_mut(&id).ok_or(RakSessionError::Closed)?;
        let session = entry.session.take().ok_or(RakSessionError::Closed)?;
        let (session, result) = session.close(now, false);
        entry.session = Some(session);
        if let Some(reason) = drain_session(entry, &self.socket, &mut self.received, now) {
            self.remove_sessions(vec![(id, reason)], now);
        }
        result
    }

    pub fn sessions(&self) -> impl Iterator<Item = RakSessionId> + '_ {
        self.sessions
            .iter()
            .filter_map(|(&id, entry)| entry.session.as_ref().map(|_| id))
    }

    pub fn rtt(&self, id: RakSessionId) -> Option<Duration> {
        self.sessions
            .get(&id)
            .and_then(|entry| entry.session.as_ref())
            .map(SessionSlot::rtt)
    }

    /// Takes a lifecycle notification, releasing its original allocation only after data drains.
    pub fn next_event(&mut self) -> Option<RakServerEvent> {
        let event = self.events.pop_front()?;
        let id = match event {
            RakServerEvent::SessionConnected { id, .. }
            | RakServerEvent::SessionDisconnected { id, .. } => id,
        };
        let entry = self
            .sessions
            .get_mut(&id)
            .expect("notification retains its original entry");
        entry.notifications = entry
            .notifications
            .checked_sub(1)
            .expect("notification was reserved");
        self.reap(id);
        Some(event)
    }

    /// Retained retired allocations reduce only future protocol admission, never existing ownership.
    fn sync_admission(&mut self) {
        let retained = self
            .sessions
            .values()
            .filter(|entry| entry.session.is_none())
            .count();
        let _ = self.intl.handle(SetMaxConnections(
            self.max_connections.saturating_sub(retained),
        ));
    }

    /// Releases a slot only after both original final owners have been consumed.
    fn reap(&mut self, id: RakSessionId) {
        if self.sessions.get(&id).is_some_and(|entry| {
            entry.session.is_none() && entry.delivery.is_empty() && entry.notifications == 0
        }) {
            self.sessions.remove(&id);
            self.sync_admission();
        }
    }

    /// Withdraws live routes once while retaining their accepted bodies and one terminal each.
    fn remove_sessions(&mut self, ids: Vec<(RakSessionId, RakDisconnectReason)>, now: Instant) {
        if ids.is_empty() {
            return;
        }
        for (id, reason) in ids {
            let Some(entry) = self.sessions.get_mut(&id) else {
                continue;
            };
            if entry.session.take().is_none() {
                continue;
            }
            let _ = self.intl.handle(RakServerInput::RemoveSession(id, now));
            entry.notifications += 1;
            self.events
                .push_back(RakServerEvent::SessionDisconnected { id, reason });
        }
        self.sync_admission();
    }

    pub fn update(&mut self) {
        let now = Instant::now();

        for _ in 0..MAX_DATAGRAMS_PER_TICK {
            match self.socket.recv_from(&mut self.buffer) {
                Ok((len, addr)) => {
                    if let Err(e) = self.intl.handle(RakServerInput::Datagram(
                        self.buffer[..len].into(),
                        addr,
                        now,
                    )) {
                        debug!("server failed to handle inbound datagram: {e}");
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                _ => {}
            }
        }

        if let Err(e) = self.intl.handle(RakServerInput::Update(now)) {
            debug!("server failed to update: {e}");
        }

        while let Some(output) = self.intl.poll() {
            match output {
                RakServerOutput::SocketDatagram(buf, addr) => {
                    let _ = self.socket.send_to(&buf, addr);
                }
                RakServerOutput::SessionDatagram(buf, id) => {
                    if let Some(session) = self
                        .sessions
                        .get_mut(&id)
                        .and_then(|entry| entry.session.as_mut())
                    {
                        let _ = session.handle(RakSessionInput::Datagram(buf, now));
                    } else {
                        debug!("no session found with id {id:?}");
                    }
                }
                RakServerOutput::SessionClosing(session) => {
                    self.sessions.insert(
                        session.id(),
                        SessionEntry::new(SessionSlot::Closing(session), 0),
                    );
                }
                RakServerOutput::SessionConnected(session) => {
                    debug!("session {:?} connected", session.id());

                    self.events.push_back(RakServerEvent::SessionConnected {
                        id: session.id(),
                        addr: session.addr(),
                        guid: session.guid(),
                    });

                    self.sessions.insert(
                        session.id(),
                        SessionEntry::new(SessionSlot::Established(session), 1),
                    );
                }
            }
        }

        let mut disconnected = Vec::new();
        for (&id, entry) in &mut self.sessions {
            if let Some(session) = entry.session.as_mut() {
                let _ = session.handle(RakSessionInput::Update(now));
                if let Some(reason) = drain_session(entry, &self.socket, &mut self.received, now) {
                    disconnected.push((id, reason));
                }
            }
        }

        self.remove_sessions(disconnected, now);
    }
}

/// A failed startup retains the original bind address, server configuration and cause.
pub struct ServerStartError {
    addr: SocketAddr,
    config: Box<RakServerConfig>,
    cause: std::io::Error,
}

impl ServerStartError {
    /// Keeps the configured value intact through validation and socket setup failures.
    fn new(addr: SocketAddr, config: RakServerConfig, cause: std::io::Error) -> Self {
        Self {
            addr,
            config: Box::new(config),
            cause,
        }
    }

    /// Recovers the original address/configuration and failure for an explicit retry or retirement.
    pub fn into_parts(self) -> (SocketAddr, RakServerConfig, std::io::Error) {
        (self.addr, *self.config, self.cause)
    }
}

impl std::fmt::Debug for ServerStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerStartError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ServerStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "server startup failed: {}", self.cause)
    }
}

impl std::error::Error for ServerStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

impl Drop for RakServer {
    fn drop(&mut self) {
        let now = Instant::now();
        for (_, mut entry) in self.sessions.drain() {
            let Some(session) = entry.session.take() else {
                continue;
            };
            let (mut closing, _) = session.close(now, true);
            while let Some(output) = closing.poll() {
                if let RakSessionOutput::Datagram(bytes, peer) = output {
                    let _ = self.socket.send_to(&bytes, peer);
                }
            }
        }
    }
}

/// Transfers one original output FIFO, permanently refusing successors after a delivery gap.
fn drain_session(
    entry: &mut SessionEntry,
    socket: &UdpSocket,
    received: &mut VecDeque<(RakSessionId, Box<[u8]>)>,
    now: Instant,
) -> Option<RakDisconnectReason> {
    let id = entry.session.as_ref()?.id();
    let mut disconnected = None;
    while let Some(output) = entry.session.as_mut().and_then(SessionSlot::poll) {
        match output {
            RakSessionOutput::Datagram(buf, addr) => {
                let _ = socket.send_to(&buf, addr);
            }
            RakSessionOutput::Packet(buf) => {
                if entry.delivery.admit(buf.len()) {
                    received.push_back((id, buf));
                } else if entry.delivery.seal() {
                    let original = entry
                        .session
                        .take()
                        .expect("the output retains its original core");
                    let (closing, _) = original.close(now, true);
                    entry.session = Some(closing);
                }
            }
            RakSessionOutput::Disconnected(_, reason) => {
                disconnected.get_or_insert(entry.delivery.reason(reason));
            }
            RakSessionOutput::Wait(_) => {}
        }
    }
    disconnected.or_else(|| entry.delivery.failure())
}

/// An exclusive per-peer capability borrowed from the server's actual typed slot.
///
/// ```compile_fail
/// use bevy_raknet::prelude::*;
/// fn after_close(mut session: RakServerSession<'_, Closing>) {
///     session.send(Box::from([1_u8]), RakReliability::ReliableOrdered, RakPriority::Normal);
/// }
/// ```
pub struct RakServerSession<'a, S> {
    server: &'a mut RakServer,
    id: RakSessionId,
    phase: std::marker::PhantomData<S>,
}

impl<S> RakServerSession<'_, S> {
    /// Returns this endpoint's per-peer route identifier.
    pub fn id(&self) -> RakSessionId {
        self.id
    }
    /// Returns the peer endpoint while the route remains owned.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.server
            .sessions
            .get(&self.id)
            .and_then(|entry| entry.session.as_ref())
            .map(SessionSlot::addr)
    }
    /// Returns the negotiated peer identity while the route remains owned.
    pub fn peer_guid(&self) -> Option<u64> {
        self.server
            .sessions
            .get(&self.id)
            .and_then(|entry| entry.session.as_ref())
            .map(SessionSlot::guid)
    }
    /// Takes this peer's next message without reordering its retained prefix.
    pub fn recv(&mut self) -> Option<Box<[u8]>> {
        let index = self
            .server
            .received
            .iter()
            .position(|(id, _)| *id == self.id)?;
        let (_, bytes) = self.server.received.remove(index)?;
        self.server.release_body(self.id, bytes.len());
        Some(bytes)
    }
    /// Observes whether the plugin has retired this route.
    pub fn is_closed(&self) -> bool {
        self.server
            .sessions
            .get(&self.id)
            .is_none_or(|entry| entry.session.is_none())
    }
}

impl<'a> RakServerSession<'a, Established> {
    /// Sends only through an exclusive established capability.
    pub fn send<T: Into<Box<[u8]>>>(
        &mut self,
        bytes: T,
        reliability: RakReliability,
        priority: RakPriority,
    ) -> Result<(), RakSessionError> {
        self.server.send(self.id, bytes, reliability, priority)
    }
    /// Consumes established permission and returns a closing view on every path.
    pub fn close(self) -> Result<RakServerSession<'a, Closing>, ServerCloseError<'a>> {
        let result = self.server.begin_close(self.id);
        let session = RakServerSession {
            server: self.server,
            id: self.id,
            phase: std::marker::PhantomData,
        };
        match result {
            Ok(()) => Ok(session),
            Err(cause) => Err(ServerCloseError { session, cause }),
        }
    }
}

/// A failed close retains the actual exclusive closing view.
pub struct ServerCloseError<'a> {
    session: RakServerSession<'a, Closing>,
    cause: RakSessionError,
}

impl<'a> ServerCloseError<'a> {
    /// Recovers the closing view and original protocol failure.
    pub fn into_parts(self) -> (RakServerSession<'a, Closing>, RakSessionError) {
        (self.session, self.cause)
    }
}

impl std::fmt::Debug for ServerCloseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerCloseError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ServerCloseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}

impl std::error::Error for ServerCloseError<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
