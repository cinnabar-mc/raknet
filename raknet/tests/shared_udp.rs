//! A controlled caller loop for already established sessions on one socket per family.
//! Remote fixture sockets represent distinct servers, not per-client application sockets.

use raknet::prelude::*;
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

const PEERS: usize = 4;
const IO_BUDGET: usize = 64;
const MESSAGE_LIMIT: usize = 1024;
const RECEIVE_LIMIT: usize = 16;
const STEP: Duration = Duration::from_millis(10);
type Pending = Option<(Box<[u8]>, SocketAddr)>;

struct Live {
    session: RakSession,
    due: Option<Instant>,
}

/// Test-owned socket and routing table; no task, runtime, timer wheel or protocol implementation.
struct Caller {
    socket: UdpSocket,
    live: BTreeMap<SocketAddr, Live>,
    pending: Pending,
    received: VecDeque<(SocketAddr, Box<[u8]>)>,
    closed: Vec<(SocketAddr, RakDisconnectReason)>,
    unknown: usize,
}

impl Caller {
    /// Binds one explicit family; the example does not assume a dual-stack socket.
    fn bind(ip: IpAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind((ip, 0))?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            live: BTreeMap::new(),
            pending: None,
            received: VecDeque::new(),
            closed: Vec::new(),
            unknown: 0,
        })
    }

    /// Installs an owned handoff, rejecting ambiguous tuples before replacing any state.
    fn insert(&mut self, mut session: RakSession, now: Instant) -> io::Result<()> {
        let peer = canonical_socket_addr(session.addr);
        if peer.is_ipv4() != self.socket.local_addr()?.is_ipv4() {
            return Err(ErrorKind::InvalidInput.into());
        }
        if self.live.contains_key(&peer) {
            return Err(ErrorKind::AlreadyExists.into());
        }
        if self.live.len() == PEERS {
            return Err(ErrorKind::WouldBlock.into());
        }
        session.addr = peer;
        self.live.insert(
            peer,
            Live {
                session,
                due: Some(now),
            },
        );
        Ok(())
    }

    /// Admits one bounded reliable application message per peer; refusal retains caller ownership.
    fn try_send(
        &mut self,
        peer: SocketAddr,
        bytes: Box<[u8]>,
        now: Instant,
    ) -> Result<(), Box<[u8]>> {
        let peer = canonical_socket_addr(peer);
        let Some(live) = self.live.get_mut(&peer) else {
            return Err(bytes);
        };
        if self.pending.is_some()
            || live.session.get_state() != RakSessionState::Connected
            || live.session.outbound_bytes() != 0
            || bytes.len() > MESSAGE_LIMIT
            || bytes.len() > live.session.max_message_len()
        {
            return Err(bytes);
        }
        live.session
            .handle(RakSessionInput::Send(
                bytes,
                RakReliability::ReliableOrdered,
                RakPriority::Normal,
                now,
            ))
            .unwrap();
        Ok(())
    }

    /// Returns a caller deadline suitable for a frame scheduler or existing timer wheel.
    fn next_deadline(&self) -> Option<Instant> {
        self.live.values().filter_map(|live| live.due).min()
    }

    /// Drains accepted outputs before terminal removal; a blocked write stays owned for the next call.
    fn drain_with(
        &mut self,
        now: Instant,
        mut send: impl FnMut(&UdpSocket, &[u8], SocketAddr) -> io::Result<usize>,
    ) -> io::Result<bool> {
        if !write_pending(&mut self.pending, |bytes, peer| {
            send(&self.socket, bytes, peer)
        })? {
            return Ok(false);
        }
        let keys: Vec<_> = self.live.keys().copied().collect();
        for peer in keys {
            let live = self.live.get_mut(&peer).unwrap();
            let mut disconnected = false;
            while let Some(output) = live.session.poll() {
                match output {
                    RakSessionOutput::Datagram(bytes, destination) => {
                        assert_eq!(canonical_socket_addr(destination), peer);
                        self.pending = Some((bytes, peer));
                        if !write_pending(&mut self.pending, |bytes, peer| {
                            send(&self.socket, bytes, peer)
                        })? {
                            return Ok(false);
                        }
                    }
                    RakSessionOutput::Packet(bytes) => {
                        assert!(
                            self.received.len() < RECEIVE_LIMIT,
                            "fixture sink must be drained"
                        );
                        self.received.push_back((peer, bytes));
                    }
                    RakSessionOutput::Wait(after) => {
                        let next = now.checked_add(after).unwrap();
                        live.due = Some(live.due.map_or(next, |due| due.min(next)));
                    }
                    RakSessionOutput::Disconnected(_, reason) => {
                        self.closed.push((peer, reason));
                        disconnected = true;
                        break;
                    }
                }
            }
            if disconnected {
                self.live.remove(&peer);
            }
        }
        Ok(true)
    }

    /// Routes only known endpoints, then drives due sessions with the same explicit caller time.
    fn step(&mut self, now: Instant) -> io::Result<()> {
        self.step_with_send(now, |socket, bytes, peer| socket.send_to(bytes, peer))
    }

    /// Lets a controlled write failure exercise this same public-session/socket scheduling path.
    fn step_with_send(
        &mut self,
        now: Instant,
        mut send: impl FnMut(&UdpSocket, &[u8], SocketAddr) -> io::Result<usize>,
    ) -> io::Result<()> {
        if !self.drain_with(now, &mut send)? {
            return Ok(());
        }
        // A full UDP-sized buffer avoids accepting a silently truncated prefix.
        let mut buffer = [0; 65_536];
        for _ in 0..IO_BUDGET {
            match self.socket.recv_from(&mut buffer) {
                Ok((len, peer)) => {
                    let peer = canonical_socket_addr(peer);
                    if let Some(live) = self.live.get_mut(&peer) {
                        if live.session.get_state() != RakSessionState::Disconnected {
                            live.session
                                .handle(RakSessionInput::Datagram(buffer[..len].into(), now))
                                .unwrap();
                        }
                    } else {
                        self.unknown += 1;
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        for live in self.live.values_mut() {
            if live.session.get_state() != RakSessionState::Disconnected
                && live.due.is_some_and(|due| now >= due)
            {
                live.due = None;
                live.session.handle(RakSessionInput::Update(now)).unwrap();
            }
        }
        self.drain_with(now, send)?;
        Ok(())
    }
}

/// Keeps the exact datagram on WouldBlock/Interrupted; UDP writes must otherwise be complete.
fn write_pending(
    pending: &mut Pending,
    send: impl FnOnce(&[u8], SocketAddr) -> io::Result<usize>,
) -> io::Result<bool> {
    let Some((bytes, peer)) = pending.as_ref() else {
        return Ok(true);
    };
    match send(bytes, *peer) {
        Ok(len) if len == bytes.len() => {
            *pending = None;
            Ok(true)
        }
        Ok(_) => Err(ErrorKind::WriteZero.into()),
        Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

/// Models the established sessions transferred by RakClientOutput::SessionConnected.
fn established(peer: SocketAddr, now: Instant) -> RakSession {
    let config = RakSessionConfig {
        max_queued_bytes: 64 * 1024,
        max_outbound_bytes: 64 * 1024,
        ..Default::default()
    };
    RakSession::new(
        RakSessionId(0),
        peer,
        1 << 63,
        constants::MAX_MTU_SIZE,
        now,
        RakClock::new(now),
        config,
    )
}

struct Fixture {
    application: Caller,
    remotes: Vec<Caller>,
    paused: Option<usize>,
    now: Instant,
}

impl Fixture {
    /// Remote sockets stand for different servers; only application owns the shared client socket.
    fn new(ip: IpAddr) -> io::Result<Self> {
        Self::with_application(Caller::bind(ip)?)
    }

    /// Reuses the already bound family fixture; subsequent setup errors must fail the test.
    fn with_application(mut application: Caller) -> io::Result<Self> {
        let now = Instant::now();
        let local = application.socket.local_addr()?;
        let mut remotes = Vec::new();
        for _ in 0..PEERS {
            let mut remote = Caller::bind(local.ip())?;
            application.insert(established(remote.socket.local_addr()?, now), now)?;
            remote.insert(established(local, now), now)?;
            remotes.push(remote);
        }
        Ok(Self {
            application,
            remotes,
            paused: None,
            now,
        })
    }

    /// Polls finite local I/O and advances synthetic transport time without sleeping.
    fn step(&mut self) {
        self.now += STEP;
        self.application.step(self.now).unwrap();
        for (index, remote) in self.remotes.iter_mut().enumerate() {
            if self.paused == Some(index) {
                continue;
            }
            remote.step(self.now).unwrap();
            while let Some((peer, bytes)) = remote.received.pop_front() {
                assert_eq!(
                    bytes[1] as usize, index,
                    "application data crossed peer routes"
                );
                remote.try_send(peer, bytes, self.now).unwrap();
            }
        }
        std::thread::yield_now();
    }

    /// Bounds a controlled local progress witness independently of wall-clock elapsed time.
    fn until(&mut self, condition: impl Fn(&Self) -> bool) {
        for _ in 0..1000 {
            if condition(self) {
                return;
            }
            self.step();
        }
        assert!(
            condition(self),
            "controlled shared-UDP progress did not complete"
        );
    }

    /// Checks one distinct reliable stream per remote with refusal before another message is admitted.
    fn exchange(&mut self) {
        for (index, remote) in self.remotes.iter().enumerate() {
            let peer = remote.socket.local_addr().unwrap();
            let bytes: Box<[u8]> = vec![0xfe, index as u8].into();
            self.application.try_send(peer, bytes, self.now).unwrap();
            let refused: Box<[u8]> = vec![0xfe, index as u8, 99].into();
            assert_eq!(
                self.application.try_send(peer, refused.clone(), self.now),
                Err(refused)
            );
        }
        self.until(|fixture| fixture.application.received.len() == PEERS);
        let mut received = BTreeMap::new();
        while let Some((peer, bytes)) = self.application.received.pop_front() {
            assert!(received.insert(peer, bytes).is_none());
        }
        for (index, remote) in self.remotes.iter().enumerate() {
            assert_eq!(
                received[&remote.socket.local_addr().unwrap()].as_ref(),
                [0xfe, index as u8]
            );
        }
        self.until(|fixture| {
            fixture
                .application
                .live
                .values()
                .all(|live| live.session.outbound_bytes() == 0)
        });
        assert!(
            self.application
                .next_deadline()
                .is_some_and(|due| due >= self.now)
        );
    }
}

#[test]
fn shared_udp_four_ipv4_sessions_route_independently_and_apply_backpressure() {
    Fixture::new(Ipv4Addr::LOCALHOST.into()).unwrap().exchange();
}

#[test]
fn shared_udp_four_ipv6_sessions_use_one_matching_family_socket() {
    let Ok(application) = Caller::bind(Ipv6Addr::LOCALHOST.into()) else {
        eprintln!("missing fixture: IPv6 loopback UDP sockets are unavailable");
        return;
    };
    Fixture::with_application(application).unwrap().exchange();
}

#[test]
fn shared_udp_cancel_and_timeout_are_owned_by_the_peer_route() {
    let mut fixture = Fixture::new(Ipv4Addr::LOCALHOST.into()).unwrap();
    fixture.exchange();
    let canceled = fixture.remotes[0].socket.local_addr().unwrap();
    fixture
        .application
        .live
        .get_mut(&canceled)
        .unwrap()
        .session
        .handle(RakSessionInput::DisconnectNow(fixture.now))
        .unwrap();
    fixture.until(|fixture| !fixture.application.live.contains_key(&canceled));
    fixture.paused = Some(1);
    let expired = fixture.remotes[1].socket.local_addr().unwrap();
    for _ in 0..2000 {
        fixture.step();
    }
    assert!(
        fixture
            .application
            .closed
            .contains(&(canceled, RakDisconnectReason::Requested))
    );
    assert!(
        fixture
            .application
            .closed
            .contains(&(expired, RakDisconnectReason::Timeout))
    );
    assert_eq!(fixture.application.live.len(), PEERS - 2);
    let active = fixture.remotes[2].socket.local_addr().unwrap();
    fixture
        .application
        .try_send(active, vec![0xfe, 2, 7].into(), fixture.now)
        .unwrap();
    fixture.until(|fixture| !fixture.application.received.is_empty());
    assert_eq!(
        fixture.application.received.pop_front(),
        Some((active, vec![0xfe, 2, 7].into()))
    );
}

#[test]
fn shared_udp_refuses_duplicate_and_wrong_family_routes_and_ignores_unknown_senders() {
    let mut fixture = Fixture::new(Ipv4Addr::LOCALHOST.into()).unwrap();
    let peer = fixture.remotes[0].socket.local_addr().unwrap();
    let mapped = SocketAddr::new(Ipv4Addr::LOCALHOST.to_ipv6_mapped().into(), peer.port());
    assert_eq!(
        fixture
            .application
            .insert(established(mapped, fixture.now), fixture.now)
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        fixture
            .application
            .insert(
                established("[::1]:19132".parse().unwrap(), fixture.now),
                fixture.now
            )
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    let stranger = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    stranger
        .send_to(&[0xfe], fixture.application.socket.local_addr().unwrap())
        .unwrap();
    fixture.until(|fixture| fixture.application.unknown == 1);
    assert_eq!(fixture.application.live.len(), PEERS);
    assert!(fixture.application.received.is_empty());
}

#[test]
fn shared_udp_retains_a_blocked_datagram_until_the_same_socket_accepts_it() {
    let source = Caller::bind(Ipv4Addr::LOCALHOST.into()).unwrap();
    let destination = Caller::bind(Ipv4Addr::LOCALHOST.into()).unwrap();
    let peer = destination.socket.local_addr().unwrap();
    let mut pending = Some((vec![0xfe, 1, 2].into_boxed_slice(), peer));
    assert!(!write_pending(&mut pending, |_, _| Err(ErrorKind::WouldBlock.into())).unwrap());
    assert!(!write_pending(&mut pending, |_, _| Err(ErrorKind::Interrupted.into())).unwrap());
    assert_eq!(pending.as_ref().unwrap().1, peer);
    assert_eq!(pending.as_ref().unwrap().0.as_ref(), [0xfe, 1, 2]);
    assert!(
        write_pending(&mut pending, |bytes, peer| source
            .socket
            .send_to(bytes, peer))
        .unwrap()
    );
    assert!(pending.is_none());
    let mut buffer = [0; 16];
    let mut received = None;
    for _ in 0..1000 {
        match destination.socket.recv_from(&mut buffer) {
            Ok((len, from)) => {
                received = Some((buffer[..len].to_vec(), from));
                break;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => std::thread::yield_now(),
            Err(error) => panic!("controlled UDP receive failed: {error}"),
        }
    }
    assert_eq!(
        received,
        Some((vec![0xfe, 1, 2], source.socket.local_addr().unwrap()))
    );
}

#[test]
fn shared_udp_retains_the_real_session_deadline_across_a_blocked_datagram() {
    let mut application = Caller::bind(Ipv4Addr::LOCALHOST.into()).unwrap();
    let mut remote = Caller::bind(Ipv4Addr::LOCALHOST.into()).unwrap();
    let now = Instant::now();
    let local = application.socket.local_addr().unwrap();
    let peer = remote.socket.local_addr().unwrap();
    application.insert(established(peer, now), now).unwrap();
    remote.insert(established(local, now), now).unwrap();
    let message: Box<[u8]> = vec![0xfe, 7, 8].into();
    application.try_send(peer, message.clone(), now).unwrap();
    let update_at = now + STEP;
    let original_deadline = update_at + RakSessionConfig::default().autoflush_interval_ms;
    let mut blocked_writes = 0;
    application
        .step_with_send(update_at, |_, _, _| {
            blocked_writes += 1;
            Err(ErrorKind::WouldBlock.into())
        })
        .unwrap();
    assert_eq!(blocked_writes, 1);
    let retained = application.pending.as_ref().unwrap().clone();
    assert!(
        application
            .next_deadline()
            .is_some_and(|due| due <= original_deadline),
        "a blocked real-session datagram must retain its timer obligation"
    );
    let retry_at = update_at + Duration::from_secs(1);
    assert!(
        !application
            .drain_with(retry_at, |_, _, _| Err(ErrorKind::Interrupted.into()))
            .unwrap()
    );
    assert_eq!(application.pending.as_ref(), Some(&retained));
    assert!(
        application
            .drain_with(retry_at, |socket, bytes, peer| socket.send_to(bytes, peer))
            .unwrap()
    );
    assert!(application.pending.is_none());
    assert_eq!(
        application.next_deadline(),
        Some(original_deadline),
        "a queued Wait must remain anchored to its generating Update"
    );
    for _ in 0..1000 {
        remote.step(retry_at).unwrap();
        if !remote.received.is_empty() {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(remote.received.pop_front(), Some((local, message)));
    assert!(remote.received.is_empty());
    application.step(retry_at).unwrap();
    assert!(
        application
            .next_deadline()
            .is_some_and(|due| due >= retry_at)
    );
}
