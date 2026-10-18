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

const MAX_DATAGRAMS_PER_TICK: usize = 1024;

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

#[derive(Resource)]
pub struct RakServer {
    intl: RakServerIntl,
    socket: UdpSocket,
    sessions: HashMap<RakSessionId, SessionSlot>,
    received: VecDeque<(RakSessionId, Box<[u8]>)>,
    events: VecDeque<RakServerEvent>,
    buffer: Box<[u8]>,
}

impl RakServer {
    pub fn new<T>(addr: SocketAddr, conf: T) -> std::io::Result<Self>
    where
        T: FnOnce(&mut RakServerConfig),
    {
        let mut config = RakServerConfig::default();
        conf(&mut config);

        config
            .session
            .validate()
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidInput, error))?;
        let socket = UdpSocket::bind(addr)?;
        socket.set_nonblocking(true)?;

        Ok(Self {
            socket,
            buffer: vec![0; config.max_mtu_size as usize].into_boxed_slice(),
            sessions: HashMap::new(),
            received: VecDeque::new(),
            events: VecDeque::new(),
            intl: RakServerIntl::new(config, addr, RakClock::new(Instant::now()), Instant::now())
                .map_err(|error| std::io::Error::new(ErrorKind::InvalidInput, error))?,
        })
    }

    pub fn set_message<T>(&mut self, val: T)
    where
        T: Into<Box<[u8]>>,
    {
        let _ = self.intl.handle(SetMessage(val.into()));
    }

    pub fn set_max_connections(&mut self, val: usize) {
        let _ = self.intl.handle(SetMaxConnections(val));
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
            let Some(session) = self.sessions.get_mut(&id) else {
                return Err(RakSessionError::Closed);
            };

            session.send(buf.into(), reliability, priority, now)?;

            drain_session(session, &self.socket, &mut self.received, &mut disconnected);
        }

        self.remove_sessions(disconnected, now);

        Ok(())
    }

    /// Borrows established permission from the actual route's typed session slot.
    pub fn connected(&mut self, id: RakSessionId) -> Option<RakServerSession<'_, Established>> {
        self.sessions
            .get(&id)
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
            .is_some_and(|session| !session.established())
            .then_some(RakServerSession {
                server: self,
                id,
                phase: std::marker::PhantomData,
            })
    }

    pub fn recv(&mut self) -> Option<(RakSessionId, Box<[u8]>)> {
        self.received.pop_front()
    }

    /// Moves local permission to Closing before draining any produced output.
    fn begin_close(&mut self, id: RakSessionId) -> Result<(), RakSessionError> {
        let now = Instant::now();
        let session = self.sessions.remove(&id).ok_or(RakSessionError::Closed)?;
        let (mut session, result) = session.close(now, false);
        let mut disconnected = Vec::new();
        drain_session(
            &mut session,
            &self.socket,
            &mut self.received,
            &mut disconnected,
        );
        if disconnected.is_empty() {
            self.sessions.insert(id, session);
        }
        self.remove_sessions(disconnected, now);
        result
    }

    pub fn sessions(&self) -> impl Iterator<Item = RakSessionId> + '_ {
        self.sessions.keys().copied()
    }

    pub fn rtt(&self, id: RakSessionId) -> Option<Duration> {
        self.sessions.get(&id).map(SessionSlot::rtt)
    }

    pub fn next_event(&mut self) -> Option<RakServerEvent> {
        self.events.pop_front()
    }

    fn remove_sessions(&mut self, ids: Vec<(RakSessionId, RakDisconnectReason)>, now: Instant) {
        for (id, reason) in ids {
            self.sessions.remove(&id);
            let _ = self.intl.handle(RakServerInput::RemoveSession(id, now));
            self.events
                .push_back(RakServerEvent::SessionDisconnected { id, reason });
        }
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
                    if let Some(session) = self.sessions.get_mut(&id) {
                        let _ = session.handle(RakSessionInput::Datagram(buf, now));
                    } else {
                        debug!("no session found with id {id:?}");
                    }
                }
                RakServerOutput::SessionClosing(session) => {
                    self.sessions
                        .insert(session.id(), SessionSlot::Closing(session));
                }
                RakServerOutput::SessionConnected(session) => {
                    debug!("session {:?} connected", session.id());

                    self.events.push_back(RakServerEvent::SessionConnected {
                        id: session.id(),
                        addr: session.addr(),
                        guid: session.guid(),
                    });

                    self.sessions
                        .insert(session.id(), SessionSlot::Established(session));
                }
            }
        }

        let mut disconnected = Vec::new();
        for session in self.sessions.values_mut() {
            let _ = session.handle(RakSessionInput::Update(now));

            drain_session(session, &self.socket, &mut self.received, &mut disconnected);
        }

        self.remove_sessions(disconnected, now);
    }
}

impl Drop for RakServer {
    fn drop(&mut self) {
        let now = Instant::now();
        for (_, session) in self.sessions.drain() {
            let (mut closing, _) = session.close(now, true);
            while let Some(output) = closing.poll() {
                if let RakSessionOutput::Datagram(bytes, peer) = output {
                    let _ = self.socket.send_to(&bytes, peer);
                }
            }
        }
    }
}

fn drain_session(
    session: &mut SessionSlot,
    socket: &UdpSocket,
    received: &mut VecDeque<(RakSessionId, Box<[u8]>)>,
    disconnected: &mut Vec<(RakSessionId, RakDisconnectReason)>,
) {
    let id = session.id();
    while let Some(output) = session.poll() {
        match output {
            RakSessionOutput::Datagram(buf, addr) => {
                let _ = socket.send_to(&buf, addr);
            }
            RakSessionOutput::Packet(buf) => received.push_back((id, buf)),
            RakSessionOutput::Disconnected(id, reason) => disconnected.push((id, reason)),
            RakSessionOutput::Wait(_) => {}
        }
    }
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
        self.server.sessions.get(&self.id).map(SessionSlot::addr)
    }
    /// Returns the negotiated peer identity while the route remains owned.
    pub fn peer_guid(&self) -> Option<u64> {
        self.server.sessions.get(&self.id).map(SessionSlot::guid)
    }
    /// Takes this peer's next message without reordering its retained prefix.
    pub fn recv(&mut self) -> Option<Box<[u8]>> {
        let index = self
            .server
            .received
            .iter()
            .position(|(id, _)| *id == self.id)?;
        self.server.received.remove(index).map(|(_, bytes)| bytes)
    }
    /// Observes whether the plugin has retired this route.
    pub fn is_closed(&self) -> bool {
        !self.server.sessions.contains_key(&self.id)
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
