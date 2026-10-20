//! Rust and go-raknet endpoints exchanging traffic through the impairment proxy.
//!
//! The Go side is `tools/go-raknet/cmd/peer`. Set `RAKNET_GO_PEER` to a built peer,
//! or put `go` on PATH to build it; otherwise every test skips with a named reason,
//! or fails when `RAKNET_REQUIRE_GO_PEER` is set.
//!
//! Delivery tests assert intact transfer in both roles. Edge-case tests observe one
//! endpoint at a time, Rust and then go-raknet in the same role, and compare the pair
//! with `EXPECTED`; a fix that changes either outcome forces its entry to be updated.

use raknet::protocol::codec::RakCodec;
use raknet::protocol::packets::ack::Ack;
use raknet::protocol::packets::frame_set::FrameSet;
use raknet::protocol::types::frame::Frame;
use raknet::test_support::pattern;
use raknet_tokio::impairment::{Direction, Impairment, UdpImpairment, Verdict};
use raknet_tokio::prelude::*;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// What the endpoint under test did with follow-up traffic, or how its peer saw it close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Delivered,
    /// The session or connection ended; a peer-side send error counts as this too.
    Closed,
    TimedOut,
}

use Outcome::{Closed, Delivered, TimedOut};

/// Observed `(rust, go)` outcomes per `case/role`, with Rust and Go each playing that role.
///
/// Equal pairs are parity; unequal pairs are recorded divergences.
const EXPECTED: &[(&str, Outcome, Outcome)] = &[
    ("loss_burst_receiver/client", Delivered, Delivered),
    // New: the client recovers from a gap over 2048 datagrams since the 50000 gap
    // fix (6fc4d7c), but a server session still delivers nothing after one.
    ("loss_burst_receiver/server", TimedOut, Delivered),
    // Known: the disconnect notification is sent once, not until acknowledged (98dc6d1).
    // The outcome is whether the closer's peer saw the close within 2 s.
    ("lost_disconnect_closer/client", TimedOut, Closed),
    ("lost_disconnect_closer/server", TimedOut, Closed),
    ("truncated_frame_datagram/client", Delivered, Delivered),
    // New: a go-raknet listener closes the connection when it rejects a datagram;
    // the Rust server skips it and keeps the session.
    ("truncated_frame_datagram/server", Delivered, Closed),
    ("reversed_ack_range/client", Delivered, Delivered),
    ("reversed_ack_range/server", Delivered, Closed),
    // Known: empty or count-mismatched split fragments end the session (250a0fc).
    ("empty_split_fragment/client", Closed, Delivered),
    ("empty_split_fragment/server", Closed, Delivered),
    ("mismatched_split_count/client", Closed, Delivered),
    ("mismatched_split_count/server", Closed, Delivered),
    // Known: a 15 s silence timeout against go-raknet's 5 s plus two round trips.
    // The outcome is whether the endpoint closed a silent peer within 12 s.
    ("dead_peer_detection/client", TimedOut, Closed),
    ("dead_peer_detection/server", TimedOut, Closed),
];

/// Checks one edge case's observed outcomes against `EXPECTED` for each role.
fn assert_expected(name: &str, rust: [Option<Outcome>; 2], go: [Option<Outcome>; 2]) {
    let mut problems = Vec::new();
    for role in ROLES {
        let id = case(name, role);
        let observed = (
            rust[role.index()].expect("rust outcome recorded"),
            go[role.index()].expect("go outcome recorded"),
        );
        match EXPECTED.iter().find(|(c, ..)| *c == id) {
            Some(&(_, r, g)) if (r, g) == observed => eprintln!("{id}: rust {r:?}, go {g:?}"),
            Some(&(_, r, g)) => problems.push(format!(
                "{id}: expected rust {r:?}, go {g:?}; observed rust {:?}, go {:?}",
                observed.0, observed.1
            )),
            None => problems.push(format!("{id}: no expected entry for {observed:?}")),
        }
    }
    assert!(
        problems.is_empty(),
        "go-raknet parity:\n{}",
        problems.join("\n")
    );
}

/// Locates or builds the Go peer once per test binary.
fn peer_binary() -> Option<PathBuf> {
    static PEER: OnceLock<Option<PathBuf>> = OnceLock::new();
    PEER.get_or_init(|| {
        if let Some(path) = std::env::var_os("RAKNET_GO_PEER") {
            return Some(PathBuf::from(path));
        }
        let out = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("go-raknet-peer");
        let status = std::process::Command::new("go")
            .args(["build", "-o"])
            .arg(&out)
            .arg("./cmd/peer")
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../tools/go-raknet"))
            .status();
        matches!(status, Ok(s) if s.success()).then_some(out)
    })
    .clone()
}

macro_rules! require_peer {
    () => {
        match peer_binary() {
            Some(path) => path,
            None => {
                let reason = "missing fixture go-raknet peer (set RAKNET_GO_PEER or install Go to build tools/go-raknet/cmd/peer)";
                // CI sets this so a broken peer build fails instead of skipping.
                assert!(std::env::var_os("RAKNET_REQUIRE_GO_PEER").is_none(), "{reason}");
                eprintln!("skipping: {reason}");
                return;
            }
        }
    };
}

/// Messages one side accepted in order, plus anything else that reached its application.
#[derive(Debug, Default)]
struct Received {
    delivered: usize,
    unexpected: Vec<String>,
    /// Why receiving stopped early: the session closed or time ran out.
    ended: Option<String>,
}

impl Received {
    /// Reduces receipt of `count` messages to an outcome; corrupt or stray messages always fail.
    fn outcome(&self, count: usize) -> Outcome {
        assert!(self.unexpected.is_empty(), "unexpected messages: {self:?}");
        match &self.ended {
            _ if self.delivered == count => Delivered,
            Some(ended) if ended.starts_with("closed") || ended.starts_with("error") => Closed,
            _ => TimedOut,
        }
    }
}

/// A running Go peer process and its event lines.
struct GoPeer {
    child: Child,
    stdin: ChildStdin,
    events: mpsc::UnboundedReceiver<String>,
}

impl GoPeer {
    async fn spawn(binary: &PathBuf, args: &[&str]) -> Self {
        let mut child = Command::new(binary)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn go-raknet peer");
        let stdin = child.stdin.take().unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let (tx, events) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Self {
            child,
            stdin,
            events,
        }
    }

    async fn command(&mut self, line: &str) {
        self.stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("write peer command");
    }

    /// Waits for the next event matching one of `prefixes`; None on timeout or exit.
    async fn expect_any(&mut self, prefixes: &[&str], within: Duration) -> Option<String> {
        timeout(within, async {
            while let Some(line) = self.events.recv().await {
                if prefixes.iter().any(|p| line.starts_with(p)) {
                    return Some(line);
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
    }

    async fn expect(&mut self, prefix: &str, within: Duration) -> Option<String> {
        self.expect_any(&[prefix], within).await
    }

    /// Collects `recv` and `unexpected` events until `count` arrived, the peer closed or time ran out.
    async fn receive(&mut self, size: usize, count: usize, within: Duration) -> Received {
        let deadline = tokio::time::Instant::now() + within;
        let mut received = Received::default();
        while received.delivered < count {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self
                .expect_any(&["recv", "unexpected", "closed", "error"], left)
                .await
            {
                Some(line) if line == format!("recv {size} ok") => received.delivered += 1,
                Some(line) if line.starts_with("closed") || line.starts_with("error") => {
                    received.ended = Some(line);
                    break;
                }
                Some(line) => received.unexpected.push(line),
                None => {
                    received.ended = Some("timeout".into());
                    break;
                }
            }
        }
        received
    }
}

impl Drop for GoPeer {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Reserves a free loopback port; the adapter does not report its bound address.
fn free_addr() -> SocketAddr {
    std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|s| s.local_addr())
        .unwrap()
}

/// Which endpoint the Rust adapter plays.
#[derive(Clone, Copy, Debug)]
enum Role {
    RustClient,
    RustServer,
}

const ROLES: [Role; 2] = [Role::RustClient, Role::RustServer];

impl Role {
    fn index(self) -> usize {
        match self {
            Role::RustClient => 0,
            Role::RustServer => 1,
        }
    }

    /// The role the Go peer plays while Rust plays this one.
    fn opposite(self) -> Role {
        match self {
            Role::RustClient => Role::RustServer,
            Role::RustServer => Role::RustClient,
        }
    }

    /// Names the endpoint role itself, independent of which implementation plays it.
    fn name(self) -> &'static str {
        match self {
            Role::RustClient => "client",
            Role::RustServer => "server",
        }
    }
}

/// Names a case by the role being compared, e.g. `name/server`.
fn case(name: &str, role: Role) -> String {
    format!("{name}/{}", role.name())
}

/// A connected Rust session, the Go peer and the proxy between them.
struct Link {
    session: RakSession<Established>,
    go: GoPeer,
    shim: UdpImpairment,
    /// Direction the Rust endpoint's datagrams travel through the proxy.
    rust_out: Direction,
    /// Pattern counters, cumulative like the Go peer's.
    rust_sent: usize,
    rust_received: usize,
    _client: Option<RakClient<RunningClient>>,
    _server: Option<RakServer<RunningServer>>,
}

impl Link {
    async fn connect(peer: &PathBuf, role: Role, seed: u64) -> Link {
        let (session, go, shim, rust_out, client, server) = match role {
            Role::RustClient => {
                let mut go =
                    GoPeer::spawn(peer, &["-mode", "listen", "-addr", "127.0.0.1:0"]).await;
                let line = go
                    .expect("listening", Duration::from_secs(10))
                    .await
                    .expect("go listener address");
                let upstream: SocketAddr = line.trim_start_matches("listening ").parse().unwrap();
                let shim = UdpImpairment::start(upstream, seed).await.unwrap();
                let client = RakClient::new(|_| {}).start().await.unwrap();
                let session = timeout(Duration::from_secs(15), client.connect(shim.local_addr()))
                    .await
                    .expect("connect within the handshake timeout")
                    .expect("connect to go-raknet listener");
                (session, go, shim, Direction::ToUpstream, Some(client), None)
            }
            Role::RustServer => {
                let addr = free_addr();
                let mut server = RakServer::new(addr, |c| c.max_connections = 1)
                    .start()
                    .await
                    .unwrap();
                let shim = UdpImpairment::start(addr, seed).await.unwrap();
                let target = shim.local_addr().to_string();
                let go = GoPeer::spawn(peer, &["-mode", "dial", "-addr", &target]).await;
                let session = timeout(Duration::from_secs(15), server.accept())
                    .await
                    .expect("accept within the handshake timeout")
                    .expect("accept go-raknet dialer");
                (session, go, shim, Direction::ToClient, None, Some(server))
            }
        };
        let mut link = Link {
            session,
            go,
            shim,
            rust_out,
            rust_sent: 0,
            rust_received: 0,
            _client: client,
            _server: server,
        };
        link.go
            .expect("connected", Duration::from_secs(10))
            .await
            .expect("go connected");
        link
    }

    fn towards_rust(&self) -> Direction {
        match self.rust_out {
            Direction::ToUpstream => Direction::ToClient,
            Direction::ToClient => Direction::ToUpstream,
        }
    }

    /// Queues `count` pattern messages; false once the session refuses.
    async fn rust_send(&mut self, size: usize, count: usize) -> bool {
        for _ in 0..count {
            let message = pattern(size, self.rust_sent);
            let sent = self
                .session
                .send(
                    message,
                    RakReliability::ReliableOrdered,
                    RakPriority::Normal,
                )
                .await;
            if sent.is_err() {
                return false;
            }
            self.rust_sent += 1;
        }
        true
    }

    /// Receives up to `count` pattern messages on the Rust session.
    async fn rust_receive(&mut self, size: usize, count: usize, within: Duration) -> Received {
        let deadline = tokio::time::Instant::now() + within;
        let mut received = Received::default();
        while received.delivered < count {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match timeout(left, self.session.recv::<Box<[u8]>>()).await {
                Ok(Ok(message)) if message[..] == pattern(size, self.rust_received)[..] => {
                    received.delivered += 1;
                    self.rust_received += 1;
                }
                Ok(Ok(message)) => received.unexpected.push(format!(
                    "{} bytes starting {:02x?}",
                    message.len(),
                    &message[..message.len().min(4)]
                )),
                Ok(Err(error)) => {
                    received.ended = Some(format!("closed: {error}"));
                    break;
                }
                Err(_) => {
                    received.ended = Some("timeout".into());
                    break;
                }
            }
        }
        received
    }

    /// Sends `count` messages each way and reports what each side received.
    async fn exchange(&mut self, size: usize, count: usize, within: Duration) -> Exchange {
        let queued = self.rust_send(size, count).await;
        self.go.command(&format!("send {size} {count}")).await;
        let at_rust = self.rust_receive(size, count, within).await;
        let at_go = self.go.receive(size, count, within).await;
        Exchange {
            count,
            queued,
            at_rust,
            at_go,
        }
    }
}

/// Both receive sides of one `Link::exchange`.
struct Exchange {
    count: usize,
    queued: bool,
    at_rust: Received,
    at_go: Received,
}

impl Exchange {
    /// Asserts both directions arrived intact and nothing else reached either application.
    fn assert_ok(&self, what: &str) {
        let ok = self.queued
            && self.at_rust.delivered == self.count
            && self.at_go.delivered == self.count
            && self.at_rust.unexpected.is_empty()
            && self.at_go.unexpected.is_empty();
        assert!(
            ok,
            "{what}: rust sent all: {}; at rust {:?}; at go {:?}",
            self.queued, self.at_rust, self.at_go
        );
    }
}

/// Clean handshake and ordered delivery both ways, in both roles.
#[tokio::test(flavor = "multi_thread")]
async fn handshake_and_ordered_delivery() {
    let peer = require_peer!();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 1).await;
        let exchange = link.exchange(300, 50, Duration::from_secs(10)).await;
        exchange.assert_ok(&format!("ordered delivery, rust as {}", role.name()));
    }
}

/// Four 1 MiB messages each way under random loss, duplication and reordering.
#[tokio::test(flavor = "multi_thread")]
async fn bulk_under_random_impairment() {
    let peer = require_peer!();
    let impairment = Impairment {
        loss_per_mille: 30,
        duplicate_per_mille: 10,
        reorder_per_mille: 20,
        reorder_distance: 3,
        delay: Duration::from_millis(2),
        jitter: Duration::from_millis(3),
    };
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 7).await;
        link.shim.set(Direction::ToUpstream, impairment);
        link.shim.set(Direction::ToClient, impairment);
        let exchange = link.exchange(1 << 20, 4, Duration::from_secs(60)).await;
        exchange.assert_ok(&format!("bulk transfer, rust as {}", role.name()));
    }
}

/// Rewrites the datagram number of one data datagram to land `jump` past its original.
fn jump_sequence(bytes: &[u8], jump: u32) -> Vec<u8> {
    let mut out = bytes.to_vec();
    let seq = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], 0]).wrapping_add(jump) & 0xff_ffff;
    out[1..4].copy_from_slice(&seq.to_le_bytes()[..3]);
    out
}

/// One datagram numbered more than 2048 ahead stalls neither receiver, in both roles.
#[tokio::test(flavor = "multi_thread")]
async fn datagram_far_ahead() {
    let peer = require_peer!();
    for role in ROLES {
        for to_rust in [true, false] {
            let mut link = Link::connect(&peer, role, 13).await;
            let towards = if to_rust {
                link.towards_rust()
            } else {
                link.rust_out
            };
            let seen = Arc::new(AtomicUsize::new(0));
            link.shim.set_filter(Some(Box::new(move |direction, bytes| {
                if direction == towards
                    && is_data(bytes)
                    && seen.fetch_add(1, Ordering::Relaxed) == 20
                {
                    Verdict::Replace(jump_sequence(bytes, 3000))
                } else {
                    Verdict::Forward
                }
            })));
            let exchange = link.exchange(2000, 100, Duration::from_secs(20)).await;
            exchange.assert_ok(&format!(
                "datagram far ahead towards {}, rust as {}",
                if to_rust { "rust" } else { "go" },
                role.name()
            ));
        }
    }
}

/// A one second port outage (ICMP unreachable, transient socket errors) during transfer.
#[tokio::test(flavor = "multi_thread")]
async fn transient_socket_errors() {
    let peer = require_peer!();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 17).await;
        link.shim.outage(Duration::from_secs(1)).await;
        let exchange = link.exchange(1000, 200, Duration::from_secs(20)).await;
        exchange.assert_ok(&format!("port outage, rust as {}", role.name()));
    }
}

/// Is `bytes` a data datagram (not an ACK or NACK)?
fn is_data(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0] & 0x80 != 0 && bytes[0] & 0x60 == 0
}

/// Datagram numbers a loss burst skips: past the Rust receive window of 2048.
const BURST: u32 = 2100;

/// Is `bytes` an ACK or NACK?
fn is_ack(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes[0] & 0x80 != 0 && bytes[0] & 0x60 != 0
}

/// Renumbers data datagrams towards `towards` as if `BURST` consecutive ones were lost
/// after the first 20, and maps the receiver's ACKs and NACKs back to the sender's numbers.
///
/// This reproduces what a sender with megabytes in flight emits across a long loss
/// burst, since every retransmission takes a new number, without waiting out its timers.
fn skip_after_burst(towards: Direction) -> raknet_tokio::impairment::Filter {
    let mut seen = 0;
    let mut start: Option<u32> = None;
    Box::new(move |direction, bytes| {
        if direction == towards && is_data(bytes) {
            seen += 1;
            if seen <= 20 {
                return Verdict::Forward;
            }
            start.get_or_insert(u32::from_le_bytes([bytes[1], bytes[2], bytes[3], 0]));
            return Verdict::Replace(jump_sequence(bytes, BURST));
        }
        let (Some(start), true) = (start, direction != towards && is_ack(bytes)) else {
            return Verdict::Forward;
        };
        let Ok(ack) = Ack::deserialize(&mut &bytes[..]) else {
            return Verdict::Forward;
        };
        let sequences: Vec<u32> = ack
            .sequences
            .into_iter()
            .filter_map(|seq| match seq.wrapping_sub(start) & 0xff_ffff {
                // Numbers inside the skipped burst were never sent.
                offset if offset < BURST => None,
                offset if offset < 0x80_0000 => Some(seq.wrapping_sub(BURST) & 0xff_ffff),
                _ => Some(seq),
            })
            .collect();
        if sequences.is_empty() {
            return Verdict::Drop;
        }
        let mut out = Vec::new();
        match Ack::new(sequences, ack.is_nack).serialize(&mut out) {
            Ok(()) => Verdict::Replace(out),
            Err(_) => Verdict::Drop,
        }
    })
}

/// The receiver's outcome for 4 MiB sent towards it across a skipped burst.
async fn after_burst(link: &mut Link, rust_receives: bool) -> Outcome {
    let towards = if rust_receives {
        link.towards_rust()
    } else {
        link.rust_out
    };
    link.shim.set_filter(Some(skip_after_burst(towards)));
    if rust_receives {
        link.go.command(&format!("send {} 4", 1 << 20)).await;
        let received = link.rust_receive(1 << 20, 4, Duration::from_secs(20)).await;
        received.outcome(4)
    } else {
        link.rust_send(1 << 20, 4).await;
        let received = link.go.receive(1 << 20, 4, Duration::from_secs(20)).await;
        received.outcome(4)
    }
}

/// A receiver delivers 4 MiB across a loss burst longer than 2048 datagrams.
#[tokio::test(flavor = "multi_thread")]
async fn loss_burst_with_megabytes_in_flight() {
    let peer = require_peer!();
    let (mut rust, mut go) = ([None; 2], [None; 2]);
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 11).await;
        rust[role.index()] = Some(after_burst(&mut link, true).await);
        // Go plays this role in the link where Rust plays the other one.
        let mut link = Link::connect(&peer, role.opposite(), 12).await;
        go[role.index()] = Some(after_burst(&mut link, false).await);
    }
    assert_expected("loss_burst_receiver", rust, go);
}

/// Does this data datagram carry a disconnect notification frame?
fn carries_disconnect(bytes: &[u8]) -> bool {
    is_data(bytes)
        && FrameSet::deserialize(&mut &bytes[..]).is_ok_and(|set| {
            set.frames
                .iter()
                .any(|f| !f.is_split() && f.payload.first() == Some(&0x15))
        })
}

/// Drops the first disconnect notification travelling `towards`.
fn drop_first_disconnect(link: &Link, towards: Direction) {
    let counter = Arc::new(AtomicUsize::new(0));
    link.shim.set_filter(Some(Box::new(move |direction, bytes| {
        if direction == towards
            && carries_disconnect(bytes)
            && counter.fetch_add(1, Ordering::Relaxed) == 0
        {
            Verdict::Drop
        } else {
            Verdict::Forward
        }
    })));
}

/// How long a closer's peer may take to notice the close before it counts as timed out.
const CLOSE_NOTICE: Duration = Duration::from_secs(2);

/// Whether a closer whose first disconnect notification is lost still closes its peer promptly.
#[tokio::test(flavor = "multi_thread")]
async fn lost_disconnect_notification() {
    let peer = require_peer!();
    let (mut rust, mut go) = ([None; 2], [None; 2]);
    for role in ROLES {
        // Rust closes in this role; the Go peer observes.
        let link = Link::connect(&peer, role, 20).await;
        drop_first_disconnect(&link, link.rust_out);
        let Link {
            session,
            go: mut peer_go,
            shim: _shim,
            _client,
            _server,
            ..
        } = link;
        let _closing = session.close().expect("close");
        let noticed = peer_go.expect_any(&["closed", "error"], CLOSE_NOTICE).await;
        rust[role.index()] = Some(if noticed.is_some() { Closed } else { TimedOut });

        // Go closes in this role, from the link where Rust plays the other one.
        let mut link = Link::connect(&peer, role.opposite(), 19).await;
        drop_first_disconnect(&link, link.towards_rust());
        link.go.command("close").await;
        go[role.index()] = Some(
            match timeout(CLOSE_NOTICE, link.session.recv::<Box<[u8]>>()).await {
                Ok(Err(_)) => Closed,
                Ok(Ok(_)) => Delivered,
                Err(_) => TimedOut,
            },
        );
    }
    assert_expected("lost_disconnect_closer", rust, go);
}

/// Encodes one datagram carrying `frames`, numbered `seq`.
fn datagram(seq: u32, frames: Vec<Frame>) -> Vec<u8> {
    let mut out = Vec::new();
    FrameSet::new(seq, frames, false, true, false)
        .serialize(&mut out)
        .unwrap();
    out
}

/// Injects crafted datagrams into the endpoint under test, then sends follow-up traffic only to it.
///
/// `crafted` takes datagram numbers from the allocator it is given. Each is distinct and
/// 100000 behind the newest number sent that way, so no receiver can treat it as a
/// duplicate or as too far ahead, whatever the scheduling. Each injection is on the wire
/// before the follow-up traffic starts.
async fn after_injection(
    link: &mut Link,
    rust_under_test: bool,
    crafted: &impl Fn(&mut dyn FnMut() -> u32) -> Vec<Vec<u8>>,
) -> Outcome {
    let towards = if rust_under_test {
        link.towards_rust()
    } else {
        link.rust_out
    };
    let last = Arc::new(AtomicUsize::new(0));
    let observed = last.clone();
    link.shim.set_filter(Some(Box::new(move |direction, bytes| {
        if direction == towards && is_data(bytes) {
            let seq = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], 0]);
            observed.store(seq as usize, Ordering::Relaxed);
        }
        Verdict::Forward
    })));
    let warm_up = link.exchange(100, 20, Duration::from_secs(10)).await;
    warm_up.assert_ok("warm-up before injection");
    let mut next = (last.load(Ordering::Relaxed) as u32).wrapping_sub(100_000) & 0xff_ffff;
    let mut allocate = move || {
        next = next.wrapping_sub(1) & 0xff_ffff;
        next
    };
    for bytes in crafted(&mut allocate) {
        link.shim.inject(towards, bytes).await;
    }
    if rust_under_test {
        link.go.command("send 100 20").await;
        let received = link.rust_receive(100, 20, Duration::from_secs(10)).await;
        received.outcome(20)
    } else {
        link.rust_send(100, 20).await;
        let received = link.go.receive(100, 20, Duration::from_secs(10)).await;
        received.outcome(20)
    }
}

/// Injects the same crafted datagrams into Rust and into Go playing the same role.
async fn injection_case(
    peer: &PathBuf,
    name: &str,
    seed: u64,
    crafted: impl Fn(&mut dyn FnMut() -> u32) -> Vec<Vec<u8>>,
) {
    let (mut rust, mut go) = ([None; 2], [None; 2]);
    for role in ROLES {
        let mut link = Link::connect(peer, role, seed).await;
        rust[role.index()] = Some(after_injection(&mut link, true, &crafted).await);
        let mut link = Link::connect(peer, role.opposite(), seed + 1).await;
        go[role.index()] = Some(after_injection(&mut link, false, &crafted).await);
    }
    assert_expected(name, rust, go);
}

/// A frame header announcing more payload than the datagram holds.
#[tokio::test(flavor = "multi_thread")]
async fn truncated_frame_datagram() {
    let peer = require_peer!();
    injection_case(&peer, "truncated_frame_datagram", 23, |seq| {
        let mut truncated = datagram(seq(), vec![]);
        truncated.extend_from_slice(&[0x00, 0x40, 0x00, 0x01]);
        vec![truncated]
    })
    .await;
}

/// An ACK whose range ends before it starts.
#[tokio::test(flavor = "multi_thread")]
async fn reversed_ack_range() {
    let peer = require_peer!();
    injection_case(&peer, "reversed_ack_range", 25, |_| {
        vec![vec![0xc0, 0x00, 0x01, 0x00, 9, 0, 0, 3, 0, 0]]
    })
    .await;
}

fn split_frame(id: u16, count: u32, index: u32, payload: &[u8]) -> Frame {
    let mut frame = Frame::new(RakReliability::Unreliable, payload.into());
    frame.split_id = id;
    frame.split_size = count;
    frame.split_index = index;
    frame
}

/// A split fragment with an empty payload.
#[tokio::test(flavor = "multi_thread")]
async fn empty_split_fragment() {
    let peer = require_peer!();
    injection_case(&peer, "empty_split_fragment", 29, |seq| {
        vec![datagram(seq(), vec![split_frame(4001, 2, 0, &[])])]
    })
    .await;
}

/// Two fragments of one split that disagree on its fragment count.
#[tokio::test(flavor = "multi_thread")]
async fn mismatched_split_count() {
    let peer = require_peer!();
    injection_case(&peer, "mismatched_split_count", 31, |seq| {
        vec![
            datagram(seq(), vec![split_frame(4002, 3, 0, &[1, 2, 3])]),
            datagram(seq(), vec![split_frame(4002, 4, 1, &[4, 5, 6])]),
        ]
    })
    .await;
}

/// How long an endpoint may take to close a silent peer before it counts as timed out:
/// go-raknet's 5 s of silence plus up to 5 s of close drain, with margin.
const DEAD_PEER_NOTICE: Duration = Duration::from_secs(12);

/// Whether each endpoint closes a silent peer within go-raknet's bound.
#[tokio::test(flavor = "multi_thread")]
async fn dead_peer_detection() {
    let peer = require_peer!();
    let (mut rust, mut go) = ([None; 2], [None; 2]);
    for role in ROLES {
        // Neither endpoint gets any traffic, so both are observed in the same link.
        let mut link = Link::connect(&peer, role, 37).await;
        let blackhole = Impairment {
            loss_per_mille: 1000,
            ..Default::default()
        };
        link.shim.set(Direction::ToUpstream, blackhole);
        link.shim.set(Direction::ToClient, blackhole);
        let (go_closed, rust_closed) = tokio::join!(
            link.go.expect_any(&["closed", "error"], DEAD_PEER_NOTICE),
            timeout(DEAD_PEER_NOTICE, link.session.recv::<Box<[u8]>>()),
        );
        rust[role.index()] = Some(match rust_closed {
            Ok(Err(_)) => Closed,
            Ok(Ok(_)) => Delivered,
            Err(_) => TimedOut,
        });
        go[role.opposite().index()] = Some(if go_closed.is_some() {
            Closed
        } else {
            TimedOut
        });
    }
    assert_expected("dead_peer_detection", rust, go);
}
