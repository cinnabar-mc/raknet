//! Rust and go-raknet endpoints exchanging traffic through the impairment proxy.
//!
//! The Go side is `tools/go-raknet/cmd/peer`. Set `RAKNET_GO_PEER` to a built peer,
//! or put `go` on PATH to build it; otherwise every test skips with a named reason,
//! or fails when `RAKNET_REQUIRE_GO_PEER` is set.
//! A case listed in `DIVERGENCES` must still fail its go-raknet parity check, so a
//! fix forces removal of its entry.

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
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Cases known to differ from go-raknet, with the exact observation each must keep producing.
const DIVERGENCES: &[(&str, &str)] = &[
    // New: the client recovers from a gap over 2048 datagrams since the 50000 gap
    // fix (6fc4d7c), but a server session still delivers nothing after one.
    ("loss_burst_to_rust/rust_server", "rust timeout"),
    // Known: the disconnect notification is sent once, not until acknowledged (98dc6d1).
    (
        "lost_disconnect_rust_closes/rust_client",
        "go closed after its own timeout",
    ),
    (
        "lost_disconnect_rust_closes/rust_server",
        "go closed after its own timeout",
    ),
    // Known: empty or count-mismatched split fragments end the session (250a0fc).
    ("empty_split_fragment/rust_client", "rust closed"),
    ("empty_split_fragment/rust_server", "rust closed"),
    ("mismatched_split_count/rust_client", "rust closed"),
    ("mismatched_split_count/rust_server", "rust closed"),
    // Known: 15 s silence timeout against go-raknet's 5 s plus two round trips.
    (
        "dead_peer_detection/rust_client",
        "rust closed at the 15 s timeout",
    ),
    (
        "dead_peer_detection/rust_server",
        "rust closed at the 15 s timeout",
    ),
];

/// Collects parity verdicts so one run reports every case; listed divergences must still reproduce.
#[derive(Default)]
struct Verdicts(Vec<String>);

impl Verdicts {
    /// `observed` summarises the outcome; a listed divergence must reproduce it exactly.
    fn check(&mut self, case: &str, matches_go: bool, observed: &str, detail: &str) {
        match (DIVERGENCES.iter().find(|(c, _)| *c == case), matches_go) {
            (Some(_), true) => self.0.push(format!(
                "{case} now matches go-raknet ({detail}); remove its divergence entry"
            )),
            (Some((_, expected)), false) if *expected != observed => self.0.push(format!(
                "{case} divergence changed: expected `{expected}`, observed `{observed}` ({detail})"
            )),
            (None, false) => self.0.push(format!(
                "{case} diverges from go-raknet: {observed} ({detail})"
            )),
            _ => eprintln!("{case}: {observed} ({detail})"),
        }
    }

    fn finish(self) {
        assert!(
            self.0.is_empty(),
            "go-raknet parity:\n{}",
            self.0.join("\n")
        );
    }
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
    /// Classifies the outcome without counts, which vary with scheduling.
    fn summary(&self, count: usize) -> &'static str {
        match &self.ended {
            _ if !self.unexpected.is_empty() => "unexpected messages",
            _ if self.delivered == count => "all",
            Some(ended) if ended.starts_with("closed") => "closed",
            _ => "timeout",
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
                assert!(!line.starts_with("error"), "go peer: {line}");
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
                .expect_any(&["recv", "unexpected", "closed"], left)
                .await
            {
                Some(line) if line == format!("recv {size} ok") => received.delivered += 1,
                Some(line) if line.starts_with("closed") => {
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

fn case(name: &str, role: Role) -> String {
    let role = match role {
        Role::RustClient => "rust_client",
        Role::RustServer => "rust_server",
    };
    format!("{name}/{role}")
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

    /// Sends `count` messages each way; true when both arrived intact and nothing else did.
    ///
    /// The observation summarises the Rust side, whose behaviour the divergences describe.
    async fn exchange(
        &mut self,
        size: usize,
        count: usize,
        within: Duration,
    ) -> (bool, String, String) {
        let queued = self.rust_send(size, count).await;
        self.go.command(&format!("send {size} {count}")).await;
        let at_rust = self.rust_receive(size, count, within).await;
        let at_go = self.go.receive(size, count, within).await;
        let ok = queued
            && at_rust.delivered == count
            && at_go.delivered == count
            && at_rust.unexpected.is_empty()
            && at_go.unexpected.is_empty();
        (
            ok,
            format!("rust {}", at_rust.summary(count)),
            format!("rust sent all: {queued}; at rust {at_rust:?}; at go {at_go:?}"),
        )
    }
}

/// Clean handshake and ordered delivery both ways, in both roles.
#[tokio::test(flavor = "multi_thread")]
async fn handshake_and_ordered_delivery() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 1).await;
        let (ok, observed, detail) = link.exchange(300, 50, Duration::from_secs(10)).await;
        verdicts.check(
            &case("handshake_and_ordered_delivery", role),
            ok,
            &observed,
            &detail,
        );
    }
    verdicts.finish();
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
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 7).await;
        link.shim.set(Direction::ToUpstream, impairment);
        link.shim.set(Direction::ToClient, impairment);
        let (ok, observed, detail) = link.exchange(1 << 20, 4, Duration::from_secs(60)).await;
        verdicts.check(
            &case("bulk_under_random_impairment", role),
            ok,
            &observed,
            &detail,
        );
    }
    verdicts.finish();
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

/// Sends 4 MiB towards `towards` across a skipped burst; true when it all arrived.
async fn burst(link: &mut Link, towards: Direction) -> (bool, String, String) {
    link.shim.set_filter(Some(skip_after_burst(towards)));
    let (side, received) = if towards == link.towards_rust() {
        link.go.command(&format!("send {} 4", 1 << 20)).await;
        (
            "rust",
            link.rust_receive(1 << 20, 4, Duration::from_secs(20)).await,
        )
    } else {
        link.rust_send(1 << 20, 4).await;
        (
            "go",
            link.go.receive(1 << 20, 4, Duration::from_secs(20)).await,
        )
    };
    (
        received.summary(4) == "all",
        format!("{side} {}", received.summary(4)),
        format!("{received:?}"),
    )
}

/// The receive window survives a loss burst longer than 2048 datagrams with 4 MiB in flight.
#[tokio::test(flavor = "multi_thread")]
async fn loss_burst_with_megabytes_in_flight() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 11).await;
        let towards = link.towards_rust();
        let (ok, observed, detail) = burst(&mut link, towards).await;
        verdicts.check(&case("loss_burst_to_rust", role), ok, &observed, &detail);
        let mut link = Link::connect(&peer, role, 12).await;
        let towards = link.rust_out;
        let (ok, observed, detail) = burst(&mut link, towards).await;
        verdicts.check(&case("loss_burst_to_go", role), ok, &observed, &detail);
    }
    verdicts.finish();
}

/// Rewrites the datagram number of one data datagram to land `jump` past its original.
fn jump_sequence(bytes: &[u8], jump: u32) -> Vec<u8> {
    let mut out = bytes.to_vec();
    let seq = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], 0]).wrapping_add(jump) & 0xff_ffff;
    out[1..4].copy_from_slice(&seq.to_le_bytes()[..3]);
    out
}

/// One datagram numbered more than 2048 ahead does not stall later delivery.
#[tokio::test(flavor = "multi_thread")]
async fn datagram_far_ahead() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
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
            let (ok, observed, detail) = link.exchange(2000, 100, Duration::from_secs(20)).await;
            let name = if to_rust {
                "datagram_far_ahead_to_rust"
            } else {
                "datagram_far_ahead_to_go"
            };
            verdicts.check(&case(name, role), ok, &observed, &detail);
        }
    }
    verdicts.finish();
}

/// A one second port outage (ICMP unreachable, transient socket errors) during transfer.
#[tokio::test(flavor = "multi_thread")]
async fn transient_socket_errors() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 17).await;
        link.shim.outage(Duration::from_secs(1)).await;
        let (ok, observed, detail) = link.exchange(1000, 200, Duration::from_secs(20)).await;
        verdicts.check(
            &case("transient_socket_errors", role),
            ok,
            &observed,
            &detail,
        );
    }
    verdicts.finish();
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

/// Dropping the first disconnect notification still closes the peer promptly.
#[tokio::test(flavor = "multi_thread")]
async fn lost_disconnect_notification() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 19).await;
        drop_first_disconnect(&link, link.towards_rust());
        let started = Instant::now();
        link.go.command("close").await;
        let closed = timeout(Duration::from_secs(30), link.session.recv::<Box<[u8]>>()).await;
        let elapsed = started.elapsed();
        let prompt = matches!(closed, Ok(Err(_))) && elapsed < Duration::from_secs(2);
        verdicts.check(
            &case("lost_disconnect_go_closes", role),
            prompt,
            if prompt { "prompt" } else { "rust closed late" },
            &format!("rust noticed after {elapsed:?}"),
        );

        let link = Link::connect(&peer, role, 20).await;
        drop_first_disconnect(&link, link.rust_out);
        let Link {
            session,
            mut go,
            shim: _shim,
            _client,
            _server,
            ..
        } = link;
        let started = Instant::now();
        let _closing = session.close().expect("close");
        let line = go.expect("closed", Duration::from_secs(30)).await;
        let elapsed = started.elapsed();
        let observed = match line {
            None => "go never closed",
            Some(_) if elapsed < Duration::from_secs(2) => "prompt",
            // go-raknet closes a silent peer after 5 s, then drains for up to 5 s more.
            Some(_) if elapsed >= Duration::from_secs(5) => "go closed after its own timeout",
            Some(_) => "go closed late",
        };
        verdicts.check(
            &case("lost_disconnect_rust_closes", role),
            observed == "prompt",
            observed,
            &format!("go noticed after {elapsed:?}"),
        );
    }
    verdicts.finish();
}

/// Encodes one datagram carrying `frames`, numbered `seq`.
fn datagram(seq: u32, frames: Vec<Frame>) -> Vec<u8> {
    let mut out = Vec::new();
    FrameSet::new(seq, frames, false, true, false)
        .serialize(&mut out)
        .unwrap();
    out
}

/// Injects crafted datagrams towards Rust, then checks delivery continues.
///
/// `crafted` takes datagram numbers from the allocator it is given. Each is distinct and
/// 100000 behind the newest number sent, so no receiver can treat it as a duplicate or
/// as too far ahead, whatever the scheduling.
async fn survives_injection(
    link: &mut Link,
    crafted: impl Fn(&mut dyn FnMut() -> u32) -> Vec<Vec<u8>>,
) -> (bool, String, String) {
    let towards = link.towards_rust();
    let last = Arc::new(AtomicUsize::new(0));
    let observed = last.clone();
    link.shim.set_filter(Some(Box::new(move |direction, bytes| {
        if direction == towards && is_data(bytes) {
            let seq = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], 0]);
            observed.store(seq as usize, Ordering::Relaxed);
        }
        Verdict::Forward
    })));
    let (ok, _, detail) = link.exchange(100, 20, Duration::from_secs(10)).await;
    assert!(ok, "warm-up traffic: {detail}");
    let mut next = (last.load(Ordering::Relaxed) as u32).wrapping_sub(100_000) & 0xff_ffff;
    let mut allocate = move || {
        next = next.wrapping_sub(1) & 0xff_ffff;
        next
    };
    // Each injection is on the wire before the follow-up traffic starts.
    for bytes in crafted(&mut allocate) {
        link.shim.inject(towards, bytes).await;
    }
    link.exchange(100, 20, Duration::from_secs(10)).await
}

/// A malformed connected datagram is skipped rather than ending the session.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_online_datagram() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        // A frame header announcing more payload than the datagram holds.
        let mut link = Link::connect(&peer, role, 23).await;
        let (ok, observed, detail) = survives_injection(&mut link, |seq| {
            let mut truncated = datagram(seq(), vec![]);
            truncated.extend_from_slice(&[0x00, 0x40, 0x00, 0x01]);
            vec![truncated]
        })
        .await;
        verdicts.check(
            &case("truncated_frame_datagram", role),
            ok,
            &observed,
            &detail,
        );

        // An ACK whose range ends before it starts.
        let mut link = Link::connect(&peer, role, 24).await;
        let (ok, observed, detail) = survives_injection(&mut link, |_| {
            vec![vec![0xc0, 0x00, 0x01, 0x00, 9, 0, 0, 3, 0, 0]]
        })
        .await;
        verdicts.check(&case("reversed_ack_range", role), ok, &observed, &detail);
    }
    verdicts.finish();
}

fn split_frame(id: u16, count: u32, index: u32, payload: &[u8]) -> Frame {
    let mut frame = Frame::new(RakReliability::Unreliable, payload.into());
    frame.split_id = id;
    frame.split_size = count;
    frame.split_index = index;
    frame
}

/// Empty and count-mismatched split fragments are dropped rather than ending the session.
#[tokio::test(flavor = "multi_thread")]
async fn bad_split_fragments() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 29).await;
        let (ok, observed, detail) = survives_injection(&mut link, |seq| {
            vec![datagram(seq(), vec![split_frame(4001, 2, 0, &[])])]
        })
        .await;
        verdicts.check(&case("empty_split_fragment", role), ok, &observed, &detail);

        let mut link = Link::connect(&peer, role, 31).await;
        let (ok, observed, detail) = survives_injection(&mut link, |seq| {
            vec![
                datagram(seq(), vec![split_frame(4002, 3, 0, &[1, 2, 3])]),
                datagram(seq(), vec![split_frame(4002, 4, 1, &[4, 5, 6])]),
            ]
        })
        .await;
        verdicts.check(
            &case("mismatched_split_count", role),
            ok,
            &observed,
            &detail,
        );
    }
    verdicts.finish();
}

/// Both endpoints notice a silent peer; Rust should not take longer than go-raknet.
#[tokio::test(flavor = "multi_thread")]
async fn dead_peer_detection() {
    let peer = require_peer!();
    let mut verdicts = Verdicts::default();
    for role in ROLES {
        let mut link = Link::connect(&peer, role, 37).await;
        let blackhole = Impairment {
            loss_per_mille: 1000,
            ..Default::default()
        };
        let started = Instant::now();
        link.shim.set(Direction::ToUpstream, blackhole);
        link.shim.set(Direction::ToClient, blackhole);
        let (go_closed, rust_closed) = tokio::join!(
            async {
                link.go
                    .expect("closed", Duration::from_secs(40))
                    .await
                    .map(|_| started.elapsed())
            },
            async {
                timeout(Duration::from_secs(40), link.session.recv::<Box<[u8]>>())
                    .await
                    .ok()
                    .map(|_| started.elapsed())
            },
        );
        let go_closed = go_closed.expect("go-raknet must close a silent peer");
        let observed = match rust_closed {
            None => "rust never closed".to_string(),
            Some(t) if (14..=17).contains(&t.as_secs()) => "rust closed at the 15 s timeout".into(),
            Some(t) => format!("rust closed after {}s", t.as_secs()),
        };
        verdicts.check(
            &case("dead_peer_detection", role),
            rust_closed.is_some_and(|t| t <= go_closed + Duration::from_secs(1)),
            &observed,
            &format!("rust closed after {rust_closed:?}, go after {go_closed:?}"),
        );
    }
    verdicts.finish();
}
