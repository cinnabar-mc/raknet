// Modified by Cinnabar contributors for caller-driven connection negotiation.
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

const MAX_DATAGRAMS_PER_TICK: usize = 1024;

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

#[derive(Resource)]
pub struct RakClient {
    intl: Driver,
    socket: UdpSocket,
    session: Option<SessionSlot>,
    received: VecDeque<Box<[u8]>>,
    pongs: VecDeque<(SocketAddr, Box<[u8]>, Duration)>,
    events: VecDeque<RakClientEvent>,
    buffer: Box<[u8]>,
}

impl RakClient {
    pub fn new<T>(conf: T) -> std::io::Result<Self>
    where
        T: FnOnce(&mut RakClientConfig),
    {
        let mut config = RakClientConfig::default();
        conf(&mut config);

        config
            .validate()
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidInput, error))?;
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, config.local_port))?;
        socket.set_nonblocking(true)?;

        Ok(Self {
            buffer: vec![0; config.max_mtu_size as usize].into_boxed_slice(),
            socket,
            session: None,
            received: VecDeque::new(),
            pongs: VecDeque::new(),
            events: VecDeque::new(),
            intl: Driver::new(config, RakClock::new(Instant::now()))
                .map_err(|error| std::io::Error::new(ErrorKind::InvalidInput, error))?,
        })
    }

    /// Borrows the actual idle endpoint's one connection-start capability.
    pub fn idle(&mut self) -> Option<RakClientIdle<'_>> {
        (self.session.is_none() && self.intl.is_idle()).then_some(RakClientIdle { client: self })
    }

    /// Borrows application-send permission only from an established slot.
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

        session.send(buf.into(), reliability, priority, Instant::now())?;

        if let Some(reason) = drain_session(session, &self.socket, &mut self.received) {
            self.session = None;
            self.intl.reset();
            self.events
                .push_back(RakClientEvent::Disconnected { reason });
        }

        Ok(())
    }

    pub fn recv(&mut self) -> Option<Box<[u8]>> {
        self.received.pop_front()
    }

    pub fn recv_pong(&mut self) -> Option<(SocketAddr, Box<[u8]>, Duration)> {
        self.pongs.pop_front()
    }

    /// Consumes the owned core phase before any output is drained.
    fn begin_close(&mut self) -> Result<(), RakSessionError> {
        let session = self.session.take().ok_or(RakSessionError::Closed)?;
        let (mut session, result) = session.close(Instant::now(), false);
        if let Some(reason) = drain_session(&mut session, &self.socket, &mut self.received) {
            self.intl.reset();
            self.events
                .push_back(RakClientEvent::Disconnected { reason });
        } else {
            self.session = Some(session);
        }
        result
    }

    pub fn next_event(&mut self) -> Option<RakClientEvent> {
        self.events.pop_front()
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

                    self.session = Some(SessionSlot::Established(session));
                    self.events.push_back(RakClientEvent::Connected);
                }
                RakClientOutput::SessionClosing(session) => {
                    self.session = Some(SessionSlot::Closing(session));
                }
                RakClientOutput::Wait(_) => {}
                RakClientOutput::Pong {
                    addr,
                    message,
                    latency,
                    ..
                } => self.pongs.push_back((addr, message, latency)),
            }
        }

        if let Some(session) = self.session.as_mut() {
            let _ = session.handle(RakSessionInput::Update(now));

            if let Some(reason) = drain_session(session, &self.socket, &mut self.received) {
                self.session = None;
                self.intl.reset();
                self.events
                    .push_back(RakClientEvent::Disconnected { reason });
            }
        }
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

fn drain_session(
    session: &mut SessionSlot,
    socket: &UdpSocket,
    received: &mut VecDeque<Box<[u8]>>,
) -> Option<RakDisconnectReason> {
    let mut disconnected = None;
    while let Some(output) = session.poll() {
        match output {
            RakSessionOutput::Datagram(buf, addr) => {
                let _ = socket.send_to(&buf, addr);
            }
            RakSessionOutput::Packet(buf) => received.push_back(buf),
            RakSessionOutput::Disconnected(_, reason) => disconnected = Some(reason),
            RakSessionOutput::Wait(_) => {}
        }
    }
    disconnected
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
