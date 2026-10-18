// Modified by Cinnabar contributors for caller-driven connection negotiation.
pub mod config;
pub mod error;
mod guid;
pub mod input;
pub(crate) mod lifecycle;
pub mod output;
mod pending;
mod probe;
mod state;

pub use guid::ClientGuid;
use pending::PendingPackets;
pub use probe::MtuProbeId;
use probe::{MtuProbes, deadline};

use crate::client::config::RakClientConfig;
use crate::client::error::RakClientError;
use crate::client::input::ClientInput;
use crate::client::output::ClientOutput;
use crate::client::state::{ClientPeer, RakClientState};
use crate::clock::RakClock;
use crate::protocol::codec::{RakCodec, RakCodecHelper};
use crate::protocol::error::RakCodecError;
use crate::protocol::packets::connection_request::ConnectionRequest;
use crate::protocol::packets::connection_request_accepted::ConnectionRequestAccepted;
use crate::protocol::packets::new_incoming_connection::NewIncomingConnection;
use crate::protocol::packets::open_connection_reply_1::OpenConnectionReply1;
use crate::protocol::packets::open_connection_reply_2::OpenConnectionReply2;
use crate::protocol::packets::open_connection_request_1::OpenConnectionRequest1;
use crate::protocol::packets::open_connection_request_2::OpenConnectionRequest2;
use crate::protocol::packets::unconnected_ping::UnconnectedPing;
use crate::protocol::packets::unconnected_pong::UnconnectedPong;
use crate::sans::Sans;
use crate::session::RakSessionId;
use crate::session::{
    SessionEngine, input::SessionInput, output::RakSessionOutput, state::RakSessionState,
};
use crate::types::{RakPriority, RakReliability};
use crate::util::socket_addr::{canonical_socket_addr, get_overhead};
use crate::util::{constants, packet_id};
use std::collections::VecDeque;
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tracing::debug;

#[derive(Clone, Debug)]
pub(crate) struct ClientEngine {
    config: RakClientConfig,
    clock: RakClock,

    state: RakClientState,
    probes: Option<MtuProbes>,
    next_probe_id: u64,
    request_two_at: Option<Instant>,
    mtu: u16,
    cookie: Option<i32>,
    session: Option<SessionEngine>,
    pending_packets: PendingPackets,

    connect_deadline: Option<Instant>,

    output: VecDeque<ClientOutput>,
}

impl Sans for ClientEngine {
    type Input = ClientInput;
    type Output = ClientOutput;
    type Error = RakClientError;

    fn handle(&mut self, msg: Self::Input) -> Result<(), Self::Error> {
        match msg {
            ClientInput::MtuProbeResult {
                id,
                now,
                elapsed,
                too_large,
            } => {
                if let Some(probes) = self.probes.as_mut() {
                    match probes.completed(id, now, elapsed, too_large) {
                        Ok(false) => return Ok(()),
                        Ok(true) => self.handle_timeout(now)?,
                        Err(error) => return Err(self.abort(error)),
                    }
                }
            }
            ClientInput::Ping { remote, guid, now } => {
                if !matches!(self.state, RakClientState::Unconnected) {
                    return Ok(());
                }

                let ping = UnconnectedPing {
                    timestamp: self.clock.timestamp(now),
                    client: guid.as_u64(),
                };

                self.output.push_back(ClientOutput::SocketDatagram(
                    ping.serialized()?,
                    canonical_socket_addr(remote),
                ))
            }
            ClientInput::Connect {
                remote,
                local,
                guid,
                now,
            } => {
                if !matches!(self.state, RakClientState::Unconnected) {
                    return Err(RakClientError::AlreadyConnected);
                }

                self.config.validate()?;
                let remote = canonical_socket_addr(remote);
                let local = canonical_socket_addr(local);
                if remote.is_ipv4() != local.is_ipv4() {
                    return Err(RakClientError::InvalidConfig(
                        "local and remote address families differ",
                    ));
                }
                let timeout = deadline(now, self.config.conn_attempt_timeout)?;
                self.clear_connection_outputs();
                self.state = RakClientState::Handshake1(ClientPeer {
                    remote,
                    local,
                    guid,
                });
                self.mtu = 0;
                self.cookie = None;
                self.request_two_at = None;
                self.probes = Some(MtuProbes::new(&self.config, now));
                self.connect_deadline = Some(timeout);
                self.handle_timeout(now)?;
            }
            ClientInput::Datagram(buf, addr, now) => match self.state {
                RakClientState::HandshakeCompleted(peer) => {
                    let ClientPeer { remote, local, .. } = peer;
                    if remote != canonical_socket_addr(addr) {
                        return Ok(());
                    }

                    let mut success: Option<bool> = None;
                    let mut terminal = None;
                    let mut terminal_seen = false;
                    match self.session.as_mut() {
                        Some(session) => {
                            if let Err(error) = session.handle(SessionInput::Datagram(buf, now)) {
                                return Err(self.abort(error.into()));
                            }
                            if session.get_state() == RakSessionState::Disconnected {
                                return Err(self.abort(RakClientError::ConnectionFailed));
                            }

                            while let Some(msg) = session.poll() {
                                match msg {
                                    RakSessionOutput::Datagram(buf, addr) => self
                                        .output
                                        .push_back(ClientOutput::SocketDatagram(buf, addr)),
                                    RakSessionOutput::Packet(buf) => {
                                        if terminal_seen {
                                            continue;
                                        }
                                        match session.try_handle_control(&buf, now) {
                                            Ok(true) => {
                                                terminal_seen = session.get_state()
                                                    == RakSessionState::Disconnected;
                                                continue;
                                            }
                                            Ok(false) => {}
                                            Err(error) => return Err(self.abort(error.into())),
                                        }
                                        if let Some(&b) = buf.first() {
                                            let mut cursor = Cursor::new(buf.as_ref());
                                            match b {
                                                packet_id::CONNECTION_REQUEST
                                                | packet_id::NEW_INCOMING_CONNECTION => {}
                                                packet_id::CONNECTION_REQUEST_ACCEPTED
                                                    if success.is_none() =>
                                                {
                                                    if let Err(error) =
                                                        Self::handle_connection_request_accepted(
                                                            session,
                                                            remote,
                                                            local,
                                                            &mut cursor,
                                                            now,
                                                        )
                                                    {
                                                        return Err(self.abort(error));
                                                    }
                                                    success = Some(true);
                                                }
                                                packet_id::CONNECTION_ATTEMPT_FAILED => {
                                                    session
                                                        .handle(SessionInput::DisconnectNow(now))?;
                                                    success = Some(false);
                                                    terminal_seen = true;
                                                }
                                                packet_id::CONNECTION_REQUEST_ACCEPTED => {}
                                                _ => {
                                                    if let Err(error) =
                                                        self.pending_packets.push(buf, &self.config)
                                                    {
                                                        return Err(self.abort(error));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    RakSessionOutput::Disconnected(id, reason) => {
                                        terminal = Some((id, reason));
                                        terminal_seen = true;
                                    }
                                    RakSessionOutput::Wait(_) => {}
                                }
                            }
                        }
                        None => return Err(self.abort(RakClientError::ConnectionFailed)),
                    }

                    if terminal.is_some() && success.is_none() {
                        return Err(self.abort(RakClientError::ConnectionFailed));
                    }

                    if let Some(succeeded) = success
                        && let Some(mut session) = self.session.take()
                    {
                        match succeeded {
                            true => {
                                session.restore_handshake_outputs(
                                    self.pending_packets.take(),
                                    terminal,
                                );
                                self.state = RakClientState::Connected(remote);
                                self.output
                                    .push_back(ClientOutput::SessionConnected(Box::new(session)));
                            }
                            false => {
                                debug!("connection attempt failed");
                                return Err(self.abort(RakClientError::ConnectionAttemptFailed));
                            }
                        }
                    }
                }
                RakClientState::Connected(remote) => {
                    if canonical_socket_addr(addr) == remote {
                        self.output.push_back(ClientOutput::SessionDatagram(buf));
                    }
                }
                RakClientState::Unconnected => {
                    if let Some(&b) = buf.first() {
                        let mut cursor = Cursor::new(buf.as_ref());
                        if b == packet_id::UNCONNECTED_PONG {
                            let pong = UnconnectedPong::deserialize(&mut cursor)?;

                            self.output.push_back(ClientOutput::Pong {
                                addr,
                                message: pong.message,
                                echoed_timestamp: pong.timestamp,
                                latency: self
                                    .clock
                                    .elapsed(now)
                                    .saturating_sub(Duration::from_millis(pong.timestamp)),
                            })
                        }
                    }
                }
                RakClientState::Handshake1(peer) | RakClientState::Handshake2(peer) => {
                    if peer.remote != canonical_socket_addr(addr) {
                        return Ok(());
                    }

                    if let Some(&b) = buf.first() {
                        let mut cursor = Cursor::new(buf.as_ref());
                        let replied = match b {
                            packet_id::OPEN_CONNECTION_REPLY_1 => {
                                self.handle_open_connection_reply_1(peer, &mut cursor, now)
                            }
                            packet_id::OPEN_CONNECTION_REPLY_2
                                if matches!(self.state, RakClientState::Handshake2(_)) =>
                            {
                                self.handle_open_connection_reply_2(peer, &mut cursor, now)
                            }
                            packet_id::INCOMPATIBLE_PROTOCOL => {
                                debug!(
                                    "RakClient connection failed due to incompatible protocol version"
                                );
                                return Err(self.abort(RakClientError::IncompatibleProtocol));
                            }
                            packet_id::ALREADY_CONNECTED => {
                                debug!(
                                    "RakClient connection failed because this IP is already connected"
                                );
                                return Err(self.abort(RakClientError::AlreadyConnected));
                            }
                            packet_id::NO_FREE_INCOMING_CONNECTIONS => {
                                debug!(
                                    "RakClient connection failed because the server has no free connections"
                                );
                                return Err(self.abort(RakClientError::NoFreeIncomingConnections));
                            }
                            packet_id::IP_RECENTLY_CONNECTED => {
                                debug!(
                                    "RakClient connection failed because this IP recently connected"
                                );
                                return Err(self.abort(RakClientError::RecentlyConnected));
                            }
                            _ => Ok(()),
                        };

                        match replied {
                            Err(RakClientError::RakCodecError(RakCodecError::UnexpectedMagic)) => {}
                            Err(error) => return Err(self.abort(error)),
                            Ok(()) => {}
                        }
                    }
                }
            },
            ClientInput::Update(now) => self.handle_timeout(now)?,
            ClientInput::Disconnect => {
                self.state = RakClientState::Unconnected;
                self.session = None;
                self.pending_packets.clear();
                self.probes = None;
                self.request_two_at = None;
                self.connect_deadline = None;
                self.clear_connection_outputs();
            }
        }
        Ok(())
    }

    fn poll(&mut self) -> Option<Self::Output> {
        self.output.pop_front()
    }
}

impl ClientEngine {
    /// Creates a passive client using the caller's monotonic wire-counter origin.
    pub fn new(config: RakClientConfig, clock: RakClock) -> Self {
        Self {
            config,
            clock,
            state: RakClientState::Unconnected,
            probes: None,
            next_probe_id: 0,
            request_two_at: None,
            mtu: 0,
            cookie: None,
            session: None,
            pending_packets: PendingPackets::default(),
            connect_deadline: None,
            output: VecDeque::new(),
        }
    }

    fn abort(&mut self, error: RakClientError) -> RakClientError {
        self.state = RakClientState::Unconnected;
        self.session = None;
        self.pending_packets.clear();
        self.probes = None;
        self.request_two_at = None;
        self.connect_deadline = None;
        self.clear_connection_outputs();
        self.output
            .push_back(ClientOutput::Wait(self.config.conn_attempt_interval));
        error
    }

    /// Returns whether the initial Reply 1 / Reply 2 negotiation is still active.
    ///
    /// Socket adapters may retry a bounded number of transient reads only during
    /// this phase. It ends when Reply 2 starts the reliable connected handshake.
    pub fn is_offline_negotiating(&self) -> bool {
        matches!(
            self.state,
            RakClientState::Handshake1(_) | RakClientState::Handshake2(_)
        )
    }

    pub fn is_connecting(&self) -> bool {
        match self.state {
            RakClientState::Handshake1(_) | RakClientState::Handshake2(_) => true,
            RakClientState::HandshakeCompleted(_) => true,
            RakClientState::Unconnected | RakClientState::Connected(_) => false,
        }
    }

    /// Drives both retry schedules and the connected handshake under one deadline.
    fn handle_timeout(&mut self, now: Instant) -> Result<(), RakClientError> {
        match self.drive(now) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.abort(error)),
        }
    }

    /// Emits due protocol work without reading a clock or blocking on socket writes.
    fn drive(&mut self, now: Instant) -> Result<(), RakClientError> {
        let connected = matches!(self.state, RakClientState::Connected(_));
        if connected || matches!(self.state, RakClientState::Unconnected) {
            self.output
                .push_back(ClientOutput::Wait(self.config.conn_attempt_interval));
            return Ok(());
        }
        let timeout = self
            .connect_deadline
            .ok_or(RakClientError::ConnectionFailed)?;
        if now >= timeout {
            return Err(RakClientError::ConnectionFailed);
        }
        let mut next = timeout;
        if let (RakClientState::HandshakeCompleted(_), Some(session)) =
            (&self.state, self.session.as_mut())
        {
            session.handle(SessionInput::Update(now))?;
            while let Some(output) = session.poll() {
                match output {
                    RakSessionOutput::Datagram(bytes, peer) => self
                        .output
                        .push_back(ClientOutput::SocketDatagram(bytes, peer)),
                    RakSessionOutput::Wait(wait) => next = next.min(deadline(now, wait)?),
                    RakSessionOutput::Disconnected(..) => {
                        return Err(RakClientError::ConnectionFailed);
                    }
                    RakSessionOutput::Packet(packet) => {
                        self.pending_packets.push(packet, &self.config)?
                    }
                }
            }
        } else if let RakClientState::Handshake1(peer) | RakClientState::Handshake2(peer) =
            self.state
        {
            let id = MtuProbeId(self.next_probe_id);
            if let Some(mtu) = self.probes.as_mut().and_then(|probes| probes.take(now, id)) {
                self.next_probe_id = self
                    .next_probe_id
                    .checked_add(1)
                    .ok_or(RakClientError::ConnectionFailed)?;
                let request = OpenConnectionRequest1 {
                    protocol: self.config.protocol,
                    mtu: mtu - constants::UDP_HEADER_SIZE - get_overhead(&peer.remote),
                };
                self.output.push_back(ClientOutput::MtuProbe {
                    id,
                    datagram: request.serialized()?,
                    peer: peer.remote,
                });
            }
            if self.request_two_at.is_some_and(|at| at <= now) {
                self.send_open_connection_request_2(peer, self.mtu, self.cookie)?;
                self.request_two_at = Some(deadline(now, self.config.conn_attempt_interval)?);
            }
            if let Some(at) = self.probes.as_ref().and_then(MtuProbes::next) {
                next = next.min(at);
            }
            if let Some(at) = self.request_two_at {
                next = next.min(at);
            }
        }
        self.output
            .push_back(ClientOutput::Wait(next.saturating_duration_since(now)));
        Ok(())
    }

    /// Cancels writes and unclaimed session handoffs while preserving completed discovery results.
    fn clear_connection_outputs(&mut self) {
        self.output
            .retain(|output| matches!(output, ClientOutput::Pong { .. }));
    }

    /// Discards unsent offline work when a connection ends or hands off to its session.
    fn clear_offline_outputs(&mut self) {
        self.output.retain(|output| !matches!(output, ClientOutput::MtuProbe { .. })
            && !matches!(output, ClientOutput::SocketDatagram(bytes, _) if bytes.first() == Some(&packet_id::OPEN_CONNECTION_REQUEST_2)));
    }

    /// Removes Request 2 writes that still carry a superseded grant or cookie.
    fn clear_request_two_outputs(&mut self) {
        self.output.retain(|output| !matches!(output, ClientOutput::SocketDatagram(bytes, _) if bytes.first() == Some(&packet_id::OPEN_CONNECTION_REQUEST_2)));
    }

    /// Keeps raw protection challenges separate from an accepted grant and its retry state.
    fn handle_open_connection_reply_1(
        &mut self,
        peer: ClientPeer,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
    ) -> Result<(), RakClientError> {
        let reply = OpenConnectionReply1::deserialize(buf)?;
        let minimum = self.config.min_mtu_size.max(constants::MIN_MTU_SIZE);
        if reply.guid == 0 || reply.mtu < minimum || reply.mtu > 1500 {
            if matches!(self.state, RakClientState::Handshake1(_)) {
                self.send_open_connection_request_2(peer, reply.mtu, reply.cookie)?;
            }
            return Ok(());
        }
        if reply.mtu > self.config.max_mtu_size {
            return Ok(());
        }
        self.clear_request_two_outputs();
        self.mtu = reply.mtu;
        self.cookie = reply.cookie;
        self.state = RakClientState::Handshake2(peer);
        self.send_open_connection_request_2(peer, self.mtu, self.cookie)?;
        self.request_two_at = Some(deadline(now, self.config.conn_attempt_interval)?);
        self.drive(now)
    }

    /// Encodes the advertised grant or protection challenge without using it as a payload capacity.
    fn send_open_connection_request_2(
        &mut self,
        peer: ClientPeer,
        mtu: u16,
        cookie: Option<i32>,
    ) -> Result<(), RakClientError> {
        let request = OpenConnectionRequest2 {
            cookie,
            addr: peer.remote,
            mtu,
            client: peer.guid.as_u64(),
        };
        self.output.push_back(ClientOutput::SocketDatagram(
            request.serialized()?,
            peer.remote,
        ));
        Ok(())
    }

    fn handle_open_connection_reply_2(
        &mut self,
        peer: ClientPeer,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
    ) -> Result<(), RakClientError> {
        let addr = peer.remote;
        let reply = OpenConnectionReply2::deserialize(buf)?;

        if reply.security {
            debug!("RakClient failed to connect due to security exception");
            return Err(RakClientError::SecurityUnsupported);
        }

        let floor = self.config.min_mtu_size.max(constants::MIN_PROBE_MTU_SIZE);
        self.mtu = reply.mtu.clamp(floor, self.config.max_mtu_size);
        self.probes = None;
        self.request_two_at = None;
        self.clear_offline_outputs();
        self.state = RakClientState::HandshakeCompleted(peer);

        debug!(
            "establishing connection to {} with mtu size of {}",
            addr, self.mtu
        );

        let mut session = SessionEngine::new(
            RakSessionId(0),
            addr,
            peer.guid.as_u64(),
            self.mtu,
            now,
            self.clock,
            self.config.session.clone(),
        );

        session.state = RakSessionState::Connecting;
        let req = ConnectionRequest {
            security: false,
            client_timestamp: self.clock.timestamp(now),
            client_guid: peer.guid.as_u64(),
        };

        session.handle(SessionInput::Send(
            req.serialized()?,
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))?;

        while let Some(msg) = session.poll() {
            if let RakSessionOutput::Datagram(buf, addr) = msg {
                self.output
                    .push_back(ClientOutput::SocketDatagram(buf, addr))
            }
        }

        self.session = Some(session);

        self.drive(now)
    }

    /// Takes over a session from another client, continuing its protocol state rather
    /// than handshaking the peer again.
    #[cfg(test)]
    pub fn adopt(&mut self, session: SessionEngine) -> Result<(), RakClientError> {
        self.adopt_owned(session).map_err(|(_, error)| error)
    }

    /// Returns a rejected owner intact instead of discarding its pending output.
    pub(crate) fn adopt_owned(
        &mut self,
        session: SessionEngine,
    ) -> Result<(), (Box<SessionEngine>, RakClientError)> {
        if !matches!(self.state, RakClientState::Unconnected) {
            return Err((Box::new(session), RakClientError::AlreadyConnected));
        }

        if session.get_state() == RakSessionState::Connecting {
            return Err((
                Box::new(session),
                RakClientError::InvalidConfig("an incomplete client handshake cannot be adopted"),
            ));
        }
        let addr = session.addr;

        self.mtu = session.mtu;
        self.state = RakClientState::Connected(canonical_socket_addr(addr));

        debug!("adopting a session with {} at mtu {}", addr, self.mtu);

        self.output
            .push_back(ClientOutput::SessionConnected(Box::new(session)));

        Ok(())
    }

    fn handle_connection_request_accepted(
        session: &mut SessionEngine,
        addr: SocketAddr,
        local: SocketAddr,
        buf: &mut Cursor<&[u8]>,
        now: Instant,
    ) -> Result<(), RakClientError> {
        let acc = ConnectionRequestAccepted::deserialize(buf)?;

        session.state = RakSessionState::Connected;

        let mut internal_addresses =
            vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)); constants::SYSTEM_ADDRESS_COUNT];
        internal_addresses[0] = canonical_socket_addr(local);
        let incoming = NewIncomingConnection {
            server_address: addr,
            internal_addresses,
            incoming_timestamp: acc.timestamp,
            server_timestamp: session.clock.timestamp(now),
        };

        session.handle(SessionInput::Send(
            incoming.serialized()?,
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod negotiation_tests;

#[cfg(test)]
mod endpoint_tests;

#[cfg(test)]
mod handoff_tests;

#[cfg(test)]
pub(crate) const TEST_CLIENT_GUID: ClientGuid = ClientGuid::from_raw((1_u64 << 63) | 7);
