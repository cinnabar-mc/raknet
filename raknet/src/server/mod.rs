// Modified by Cinnabar contributors for caller-driven connection negotiation.
pub mod config;
pub mod error;
pub mod input;
pub mod output;

use crate::protocol::codec::{RakCodec, RakCodecHelper};
use crate::protocol::packets::already_connected::AlreadyConnected;
use crate::protocol::packets::connection_request::ConnectionRequest;
use crate::protocol::packets::connection_request_accepted::ConnectionRequestAccepted;
use crate::protocol::packets::incompatible_protocol::IncompatibleProtocol;
use crate::protocol::packets::ip_recently_connected::IpRecentlyConnected;
use crate::protocol::packets::new_incoming_connection::NewIncomingConnection;
use crate::protocol::packets::no_free_incoming_connections::NoFreeIncomingConnections;
use crate::protocol::packets::open_connection_reply_1::OpenConnectionReply1;
use crate::protocol::packets::open_connection_reply_2::OpenConnectionReply2;
use crate::protocol::packets::open_connection_request_1::OpenConnectionRequest1;
use crate::protocol::packets::open_connection_request_2::OpenConnectionRequest2;
use crate::protocol::packets::unconnected_ping::UnconnectedPing;
use crate::protocol::packets::unconnected_pong::UnconnectedPong;
use crate::sans::Sans;
use crate::server::error::RakServerError;
use crate::server::input::RakServerInput;
use crate::session::input::RakSessionInput;
use crate::session::output::RakSessionOutput;
use crate::session::state::RakSessionState;
use crate::session::{RakSession, RakSessionId};
use crate::types::priority::RakPriority;
use crate::types::reliability::RakReliability;
use crate::util::socket_addr::get_overhead;
use crate::util::{constants, flags, packet_id};
use config::RakServerConfig;
use output::RakServerOutput;
use rand::random;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;

const RECENTLY_CONNECTED_COOLDOWN: Duration = Duration::from_millis(5000);
const OFFLINE_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
const PENDING_CONNECTION_TIMEOUT: Duration = Duration::from_millis(10_000);
const COOKIE_ROTATION_INTERVAL: Duration = Duration::from_secs(2);

pub struct RakServer {
    addr: SocketAddr,
    config: RakServerConfig,

    session_id: RakSessionId,
    session_map: HashMap<SocketAddr, RakSessionId>,
    session_addr: HashMap<RakSessionId, SocketAddr>,
    session_pending: HashMap<SocketAddr, (SystemTime, RakSession)>,

    recently_disconnected: HashMap<SocketAddr, SystemTime>,

    offline_window: SystemTime,
    offline_total: i32,
    offline_per_ip: HashMap<IpAddr, i32>,

    online_window: SystemTime,
    online_per_ip: HashMap<IpAddr, i32>,
    blocked: HashMap<IpAddr, SystemTime>,

    cookie_key: RandomState,
    cookie_salts: [u64; 2],
    cookie_rotated: SystemTime,

    output: VecDeque<RakServerOutput>,
}

impl Sans for RakServer {
    type Input = RakServerInput;
    type Output = RakServerOutput;
    type Error = RakServerError;

    fn handle(&mut self, msg: Self::Input) -> Result<(), Self::Error> {
        match msg {
            RakServerInput::Datagram(buf, addr, now) => {
                let Some(&header) = buf.first() else {
                    return Ok(());
                };

                if self.blocked(addr.ip(), now) {
                    return Ok(());
                }

                match header & flags::VALID {
                    0 => self.handle_offline_datagram(&buf, addr, now)?,
                    _ => self.handle_online_datagram(buf, addr, now)?,
                }
            }
            RakServerInput::Block(ip, duration, now) => {
                self.blocked.insert(ip.to_canonical(), now + duration);
            }
            RakServerInput::Unblock(ip) => {
                self.blocked.remove(&ip.to_canonical());
            }
            RakServerInput::SetMaxConnections(n) => {
                self.config.max_connections = n;
            }
            RakServerInput::SetMessage(msg) => {
                self.config.message = msg;
            }
            RakServerInput::RemoveSession(id, now) => {
                if let Some(addr) = self.session_addr.remove(&id) {
                    self.session_map.remove(&addr);
                    self.session_pending.remove(&addr);
                    self.recently_disconnected.insert(addr, now);
                }

                self.recently_disconnected
                    .retain(|_, &mut disconnected_at| {
                        now.duration_since(disconnected_at).unwrap_or_default()
                            < RECENTLY_CONNECTED_COOLDOWN
                    });
            }
            RakServerInput::Update(now) => {
                self.rotate_cookie_salt(now);
                self.blocked.retain(|_, until| now < *until);
                self.evict_stale_temp_sessions(now);
                self.tick_temp_sessions(now)?;
            }
        };
        Ok(())
    }

    fn poll(&mut self) -> Option<Self::Output> {
        self.output.pop_front()
    }
}

impl RakServer {
    pub fn new(config: RakServerConfig, addr: SocketAddr) -> Self {
        Self {
            config,
            addr,

            session_id: RakSessionId(0),
            session_map: HashMap::new(),
            session_addr: HashMap::new(),
            session_pending: HashMap::new(),
            recently_disconnected: HashMap::new(),

            offline_window: SystemTime::UNIX_EPOCH,
            offline_total: 0,
            offline_per_ip: HashMap::new(),
            online_window: SystemTime::UNIX_EPOCH,
            online_per_ip: HashMap::new(),
            blocked: HashMap::new(),

            cookie_key: RandomState::new(),
            cookie_salts: [random(), random()],
            cookie_rotated: SystemTime::UNIX_EPOCH,

            output: VecDeque::new(),
        }
    }

    /// Takes over a session from another `RakServer`, continuing its protocol state
    /// rather than handshaking the peer again.
    ///
    /// The session is given a fresh local [`RakSessionId`], since the two servers count
    /// ids independently, and is emitted as [`RakServerOutput::SessionConnected`] so it
    /// arrives through the same `poll()` path as one this server handshaked itself.
    pub fn adopt(&mut self, mut session: RakSession) {
        let id = self.session_id.increment();

        let addr = session.addr;
        session.id = id;

        self.session_map.insert(addr, id);
        self.session_addr.insert(id, addr);

        self.output
            .push_back(RakServerOutput::SessionConnected(Box::new(session)));
    }

    fn rotate_cookie_salt(&mut self, now: SystemTime) {
        let elapsed = now.duration_since(self.cookie_rotated).unwrap_or_default();

        if elapsed >= COOKIE_ROTATION_INTERVAL * 2 {
            self.cookie_salts = [random(), random()];
            self.cookie_rotated = now;
        } else if elapsed >= COOKIE_ROTATION_INTERVAL {
            self.cookie_salts = [random(), self.cookie_salts[0]];
            self.cookie_rotated = now;
        }
    }

    fn cookie(&self, salt: u64, addr: SocketAddr) -> i32 {
        self.cookie_key
            .hash_one((salt, addr.ip().to_canonical(), addr.port())) as i32
    }

    fn cookie_valid(&self, cookie: Option<i32>, addr: SocketAddr) -> bool {
        cookie.is_some_and(|cookie| {
            self.cookie_salts
                .iter()
                .any(|&salt| self.cookie(salt, addr) == cookie)
        })
    }

    fn counts_for_ip(&self, ip: IpAddr) -> usize {
        let ip = ip.to_canonical();

        self.session_map
            .keys()
            .filter(|k| k.ip().to_canonical() == ip)
            .count()
    }

    fn forget(&mut self, addr: SocketAddr) {
        self.session_pending.remove(&addr);
        if let Some(id) = self.session_map.remove(&addr) {
            self.session_addr.remove(&id);
        }
    }

    fn rate_limited(&mut self, addr: SocketAddr, now: SystemTime) -> bool {
        if now.duration_since(self.offline_window).unwrap_or_default() >= OFFLINE_RATE_LIMIT_WINDOW
        {
            self.offline_window = now;
            self.offline_total = 0;
            self.offline_per_ip.clear();
        }

        self.offline_total += 1;
        if self.offline_total > self.config.total_packet_limit {
            return true;
        }

        let count = self
            .offline_per_ip
            .entry(addr.ip().to_canonical())
            .or_insert(0);
        *count += 1;

        *count > self.config.packet_limit
    }

    fn online_rate_limited(&mut self, ip: IpAddr, now: SystemTime) -> bool {
        if now.duration_since(self.online_window).unwrap_or_default()
            >= constants::ONLINE_RATE_LIMIT_WINDOW
        {
            self.online_window = now;
            self.online_per_ip.clear();
        }

        let count = self.online_per_ip.entry(ip.to_canonical()).or_insert(0);
        *count += 1;
        *count > self.config.online_packet_limit
    }

    fn blocked(&mut self, ip: IpAddr, now: SystemTime) -> bool {
        let ip = ip.to_canonical();
        match self.blocked.get(&ip) {
            Some(&until) if now < until => true,
            Some(_) => {
                self.blocked.remove(&ip);
                false
            }
            None => false,
        }
    }

    fn tick_temp_sessions(&mut self, now: SystemTime) -> Result<(), RakServerError> {
        let mut closed = Vec::new();

        for (&addr, (_, session)) in self.session_pending.iter_mut() {
            session.handle(RakSessionInput::Update(now))?;

            while let Some(msg) = session.poll() {
                match msg {
                    RakSessionOutput::Datagram(buf, to) => self
                        .output
                        .push_back(RakServerOutput::SocketDatagram(buf, to)),
                    RakSessionOutput::Disconnected(..) => closed.push(addr),
                    _ => {}
                }
            }
        }

        for addr in closed {
            self.forget(addr);
        }
        Ok(())
    }

    fn evict_stale_temp_sessions(&mut self, now: SystemTime) {
        let stale: Vec<SocketAddr> = self
            .session_pending
            .iter()
            .filter(|(_, (created, _))| {
                now.duration_since(*created).unwrap_or_default() >= PENDING_CONNECTION_TIMEOUT
            })
            .map(|(&addr, _)| addr)
            .collect();

        for addr in stale {
            debug!("evicting stale pending connection from {}", addr);

            self.forget(addr);
        }
    }

    fn handle_offline_datagram(
        &mut self,
        buf: &[u8],
        addr: SocketAddr,
        now: SystemTime,
    ) -> Result<(), RakServerError> {
        if self.rate_limited(addr, now) {
            return Ok(());
        }

        if let Some(&id) = buf.first() {
            let mut cursor = Cursor::new(buf);
            match id {
                packet_id::UNCONNECTED_PING => self.handle_unconnected_ping(&mut cursor, addr)?,
                packet_id::OPEN_CONNECTION_REQUEST_1 => {
                    self.handle_open_connection_request_1(&mut cursor, addr, now)?
                }
                packet_id::OPEN_CONNECTION_REQUEST_2 => {
                    self.handle_open_connection_request_2(&mut cursor, addr, now)?
                }
                _ => debug!(
                    "received unknown offline packet from {}, id: {:#04X}",
                    addr, id
                ),
            }
        }
        Ok(())
    }

    fn handle_online_datagram(
        &mut self,
        buf: Box<[u8]>,
        addr: SocketAddr,
        now: SystemTime,
    ) -> Result<(), RakServerError> {
        if self.online_rate_limited(addr.ip(), now) {
            debug!(
                "blocking {} for exceeding the online packet limit",
                addr.ip()
            );
            self.blocked
                .insert(addr.ip().to_canonical(), now + self.config.block_duration);
            return Ok(());
        }

        if let Entry::Occupied(mut entry) = self.session_pending.entry(addr) {
            let mut success = false;
            let mut closed = false;

            let (_, session) = entry.get_mut();

            session.handle(RakSessionInput::Datagram(buf, now))?;

            while let Some(msg) = session.poll() {
                match msg {
                    RakSessionOutput::Datagram(buf, addr) => self
                        .output
                        .push_back(RakServerOutput::SocketDatagram(buf, addr)),
                    RakSessionOutput::Packet(buf) => {
                        if let Some(&b) = buf.first() {
                            let mut cursor = Cursor::new(buf.as_ref());
                            match b {
                                packet_id::CONNECTION_REQUEST => Self::handle_connection_request(
                                    session,
                                    addr,
                                    &mut cursor,
                                    now,
                                )?,
                                packet_id::NEW_INCOMING_CONNECTION => {
                                    Self::handle_new_incoming_connection(
                                        session,
                                        addr,
                                        &mut cursor,
                                    )?;

                                    success = true;
                                    break;
                                }
                                _ => {
                                    debug!(
                                        "unexpected packet {:02X} received from {} during connection phase",
                                        b, addr
                                    );
                                }
                            }
                        }
                    }
                    RakSessionOutput::Disconnected(..) => closed = true,
                    RakSessionOutput::Wait(_) => {}
                }
            }

            if closed {
                debug!("pending connection from {} closed during handshake", addr);

                entry.remove();
                self.forget(addr);
                return Ok(());
            }

            if success {
                let (_, session) = entry.remove();

                self.output
                    .push_back(RakServerOutput::SessionConnected(Box::new(session)));
            }

            return Ok(());
        }

        if let Some(&id) = self.session_map.get(&addr) {
            self.output
                .push_back(RakServerOutput::SessionDatagram(buf, id));
        }
        Ok(())
    }

    fn handle_unconnected_ping(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        addr: SocketAddr,
    ) -> Result<(), RakServerError> {
        let ping = UnconnectedPing::deserialize(cursor)?;

        let pong = UnconnectedPong {
            timestamp: ping.timestamp,
            guid: self.config.guid,
            message: self.config.message.clone(),
        };

        self.output
            .push_back(RakServerOutput::SocketDatagram(pong.serialized()?, addr));
        Ok(())
    }

    fn handle_open_connection_request_1(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        addr: SocketAddr,
        now: SystemTime,
    ) -> Result<(), RakServerError> {
        let request = OpenConnectionRequest1::deserialize(cursor)?;

        let req_protocol = request.protocol;
        if !self.config.protocols.contains(&req_protocol) {
            let incompatible = IncompatibleProtocol {
                protocol: constants::PROTOCOL,
                guid: self.config.guid,
            };

            debug!(
                "refusing connection from {} due to incompatible protocol {}, expected {}",
                addr,
                req_protocol,
                constants::PROTOCOL
            );

            self.output.push_back(RakServerOutput::SocketDatagram(
                incompatible.serialized()?,
                addr,
            ));

            return Ok(());
        }

        if let Some(&disconnected_at) = self.recently_disconnected.get(&addr)
            && now.duration_since(disconnected_at).unwrap_or_default() < RECENTLY_CONNECTED_COOLDOWN
        {
            debug!("refusing connection from {} due to recent disconnect", addr);

            let recently = IpRecentlyConnected {
                guid: self.config.guid,
            };

            self.output.push_back(RakServerOutput::SocketDatagram(
                recently.serialized()?,
                addr,
            ));

            return Ok(());
        }

        self.rotate_cookie_salt(now);

        let reply1 = OpenConnectionReply1 {
            guid: self.config.guid,
            cookie: self
                .config
                .cookies
                .then(|| self.cookie(self.cookie_salts[0], addr)),
            mtu: (request.mtu + constants::UDP_HEADER_SIZE + get_overhead(&addr))
                .clamp(self.config.min_mtu_size, self.config.max_mtu_size),
        };

        self.output
            .push_back(RakServerOutput::SocketDatagram(reply1.serialized()?, addr));

        Ok(())
    }

    fn handle_open_connection_request_2(
        &mut self,
        cursor: &mut Cursor<&[u8]>,
        addr: SocketAddr,
        now: SystemTime,
    ) -> Result<(), RakServerError> {
        let request = OpenConnectionRequest2::deserialize(cursor)?;

        if self.config.cookies {
            self.rotate_cookie_salt(now);

            if !self.cookie_valid(request.cookie, addr) {
                debug!(
                    "ignoring connection request from {} with an invalid cookie",
                    addr
                );
                return Ok(());
            }
        }

        if self.config.require_dialled_port && request.addr.port() != self.addr.port() {
            return Err(RakServerError::RefusingConnection(format!(
                "refusing connection from {} due to port mismatch",
                addr
            )));
        }

        let mtu = request.mtu;

        if !(self.config.min_mtu_size..=self.config.max_mtu_size).contains(&mtu) {
            return Err(RakServerError::RefusingConnection(format!(
                "refusing connection from {} due to invalid mtu size",
                addr
            )));
        }

        if self.session_map.contains_key(&addr) {
            debug!(
                "refusing connection from {} due to existing connection",
                addr
            );

            let already = AlreadyConnected {
                guid: self.config.guid,
            };

            self.output
                .push_back(RakServerOutput::SocketDatagram(already.serialized()?, addr));

            return Ok(());
        }

        let full = self.session_map.len() >= self.config.max_connections;
        let full_ip = self
            .config
            .max_connections_per_ip
            .is_some_and(|max| self.counts_for_ip(addr.ip()) >= max);

        if full || full_ip {
            debug!("refusing connection from {} due to max connections", addr);

            let no_free = NoFreeIncomingConnections {
                guid: self.config.guid,
            };

            self.output
                .push_back(RakServerOutput::SocketDatagram(no_free.serialized()?, addr));

            return Ok(());
        }

        debug!(
            "establishing connection from {} with mtu size of {}",
            addr, mtu
        );

        let reply2 = OpenConnectionReply2::new(self.config.guid, addr, mtu, false);

        self.output
            .push_back(RakServerOutput::SocketDatagram(reply2.serialized()?, addr));

        let id = self.session_id.increment();

        self.session_map.insert(addr, id);
        self.session_addr.insert(id, addr);

        let session = RakSession::new(id, addr, request.client, request.mtu, now, |conf| {
            conf.ordering_channels = self.config.max_ordering_channels;
        });

        self.session_pending.insert(addr, (now, session));

        Ok(())
    }

    fn handle_connection_request(
        session: &mut RakSession,
        addr: SocketAddr,
        buf: &mut Cursor<&[u8]>,
        now: SystemTime,
    ) -> Result<(), RakServerError> {
        let request = ConnectionRequest::deserialize(buf)?;

        debug!("handling connection request from {}", addr);

        let accepted = ConnectionRequestAccepted {
            client_address: addr,
            system_index: 0,
            system_addresses: vec![SocketAddr::new(unspecified_ip_like(addr), 0); 10],
            request_timestamp: request.client_timestamp,
            timestamp: now.duration_since(UNIX_EPOCH)?.as_millis() as u64,
        };

        session.handle(RakSessionInput::Send(
            accepted.serialized()?,
            RakReliability::ReliableOrdered,
            RakPriority::Immediate,
            now,
        ))?;

        Ok(())
    }

    fn handle_new_incoming_connection(
        session: &mut RakSession,
        addr: SocketAddr,
        buf: &mut Cursor<&[u8]>,
    ) -> Result<(), RakServerError> {
        let _ = NewIncomingConnection::deserialize(buf)?;

        debug!("handling new incoming connection from {}", addr);

        session.state = RakSessionState::Connected;

        Ok(())
    }
}

fn unspecified_ip_like(addr: SocketAddr) -> IpAddr {
    match addr {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    }
}

#[cfg(test)]
mod tests;
