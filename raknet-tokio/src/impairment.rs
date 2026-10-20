//! Deterministic UDP impairment proxy for interop tests.
//!
//! The proxy forwards datagrams between one client and a fixed upstream. Loss,
//! duplication, reordering and delay decisions come from a seeded generator
//! consumed once per datagram, so a given datagram order always meets the same
//! impairments; only the wall-clock timing of delayed sends varies.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};

/// The side a datagram travels towards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    /// From the client to the upstream endpoint.
    ToUpstream,
    /// From the upstream endpoint back to the client.
    ToClient,
}

impl Direction {
    fn index(self) -> usize {
        match self {
            Direction::ToUpstream => 0,
            Direction::ToClient => 1,
        }
    }
}

/// Random impairments for one direction; rates are per mille.
#[derive(Clone, Copy, Debug, Default)]
pub struct Impairment {
    pub loss_per_mille: u32,
    pub duplicate_per_mille: u32,
    /// A reordered datagram is held until `reorder_distance` later datagrams pass it.
    pub reorder_per_mille: u32,
    pub reorder_distance: usize,
    pub delay: Duration,
    /// Extra delay drawn uniformly from zero to this bound.
    pub jitter: Duration,
}

/// A filter's decision for one datagram, applied before random impairments.
pub enum Verdict {
    Forward,
    Drop,
    /// Forward these bytes instead of the original.
    Replace(Vec<u8>),
}

/// Inspects every datagram in order; it may drop or rewrite it.
pub type Filter = Box<dyn FnMut(Direction, &[u8]) -> Verdict + Send>;

/// Per-direction counters of what the proxy did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DirectionStats {
    pub received: u64,
    pub forwarded: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub reordered: u64,
}

struct Shared {
    impairments: [Impairment; 2],
    filter: Option<Filter>,
    stats: [DirectionStats; 2],
    client: Option<SocketAddr>,
}

enum Command {
    Inject(Direction, Vec<u8>, tokio::sync::oneshot::Sender<()>),
    Outage(Duration, tokio::sync::oneshot::Sender<()>),
}

/// A running proxy; dropping it stops forwarding.
pub struct UdpImpairment {
    addr: SocketAddr,
    upstream: SocketAddr,
    shared: Arc<Mutex<Shared>>,
    commands: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
}

impl Drop for UdpImpairment {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl UdpImpairment {
    /// Binds a loopback port of the upstream's family and forwards to `upstream`.
    pub async fn start(upstream: SocketAddr, seed: u64) -> io::Result<Self> {
        let bind: SocketAddr = match upstream {
            SocketAddr::V4(_) => (Ipv4Addr::LOCALHOST, 0).into(),
            SocketAddr::V6(_) => (Ipv6Addr::LOCALHOST, 0).into(),
        };
        let socket = UdpSocket::bind(bind).await?;
        let addr = socket.local_addr()?;
        let shared = Arc::new(Mutex::new(Shared {
            impairments: [Impairment::default(); 2],
            filter: None,
            stats: [DirectionStats::default(); 2],
            client: None,
        }));
        let (commands, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(run(socket, addr, upstream, seed, shared.clone(), receiver));
        Ok(Self {
            addr,
            upstream,
            shared,
            commands,
            task,
        })
    }

    /// Returns the address clients should send to instead of the upstream.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the forwarded-to endpoint.
    pub fn upstream(&self) -> SocketAddr {
        self.upstream
    }

    /// Replaces one direction's random impairments for subsequent datagrams.
    pub fn set(&self, direction: Direction, impairment: Impairment) {
        self.shared.lock().unwrap().impairments[direction.index()] = impairment;
    }

    /// Installs or removes the datagram filter.
    pub fn set_filter(&self, filter: Option<Filter>) {
        self.shared.lock().unwrap().filter = filter;
    }

    /// Returns the counters observed so far.
    pub fn stats(&self, direction: Direction) -> DirectionStats {
        self.shared.lock().unwrap().stats[direction.index()]
    }

    /// Returns the client endpoint, once one has sent a datagram.
    pub fn client(&self) -> Option<SocketAddr> {
        self.shared.lock().unwrap().client
    }

    /// Sends raw bytes towards one side, as if the other side had sent them.
    ///
    /// Returns once they are written, ahead of any datagram the proxy forwards later.
    pub async fn inject(&self, direction: Direction, bytes: Vec<u8>) {
        let (sent, wait) = tokio::sync::oneshot::channel();
        if self
            .commands
            .send(Command::Inject(direction, bytes, sent))
            .is_ok()
        {
            let _ = wait.await;
        }
    }

    /// Closes the proxy port for `duration`, returning once it is closed; it then rebinds.
    ///
    /// Peers sending meanwhile receive ICMP port-unreachable replies, which
    /// connected sockets report as transient send or receive errors.
    pub async fn outage(&self, duration: Duration) {
        let (closed, wait) = tokio::sync::oneshot::channel();
        if self
            .commands
            .send(Command::Outage(duration, closed))
            .is_ok()
        {
            let _ = wait.await;
        }
    }
}

/// SplitMix64: small, seedable and stable across platforms.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn per_mille(&mut self) -> u32 {
        (self.next() % 1000) as u32
    }
}

struct Held {
    bytes: Vec<u8>,
    remaining: usize,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Scheduled {
    due: Reverse<Instant>,
    order: Reverse<u64>,
    direction: usize,
    bytes: Vec<u8>,
}

async fn run(
    mut socket: UdpSocket,
    addr: SocketAddr,
    upstream: SocketAddr,
    seed: u64,
    shared: Arc<Mutex<Shared>>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let mut rngs = [Rng(seed), Rng(seed ^ 0xA5A5_A5A5_A5A5_A5A5)];
    let mut held: [Vec<Held>; 2] = [Vec::new(), Vec::new()];
    let mut queue: BinaryHeap<Scheduled> = BinaryHeap::new();
    let mut order = 0u64;
    let mut buf = vec![0u8; 65_536];
    loop {
        let next_due = queue.peek().map(|s| s.due.0);
        tokio::select! {
            received = socket.recv_from(&mut buf) => {
                let Ok((len, from)) = received else { continue };
                let direction = if from == upstream { Direction::ToClient } else { Direction::ToUpstream };
                let mut state = shared.lock().unwrap();
                if direction == Direction::ToUpstream {
                    state.client = Some(from);
                }
                let d = direction.index();
                state.stats[d].received += 1;
                let mut bytes = buf[..len].to_vec();
                if let Some(filter) = state.filter.as_mut() {
                    match filter(direction, &bytes) {
                        Verdict::Forward => {}
                        Verdict::Drop => {
                            state.stats[d].dropped += 1;
                            continue;
                        }
                        Verdict::Replace(replacement) => bytes = replacement,
                    }
                }
                let impairment = state.impairments[d];
                // Draw a fixed number of values per datagram so later decisions never shift.
                let rng = &mut rngs[d];
                let (loss, dup, reorder, jitter) = (rng.per_mille(), rng.per_mille(), rng.per_mille(), rng.next());
                if loss < impairment.loss_per_mille {
                    state.stats[d].dropped += 1;
                    continue;
                }
                let mut ready = Vec::new();
                for hold in &mut held[d] {
                    hold.remaining = hold.remaining.saturating_sub(1);
                }
                if reorder < impairment.reorder_per_mille && impairment.reorder_distance > 0 {
                    state.stats[d].reordered += 1;
                    held[d].push(Held { bytes: bytes.clone(), remaining: impairment.reorder_distance });
                } else {
                    ready.push(bytes.clone());
                    if dup < impairment.duplicate_per_mille {
                        state.stats[d].duplicated += 1;
                        ready.push(bytes);
                    }
                }
                let (released, kept): (Vec<_>, Vec<_>) = held[d].drain(..).partition(|h| h.remaining == 0);
                held[d] = kept;
                ready.extend(released.into_iter().map(|h| h.bytes));
                let extra = match impairment.jitter.as_nanos() {
                    0 => Duration::ZERO,
                    bound => Duration::from_nanos(jitter % bound as u64),
                };
                let due = Instant::now() + impairment.delay + extra;
                state.stats[d].forwarded += ready.len() as u64;
                drop(state);
                for bytes in ready {
                    order += 1;
                    queue.push(Scheduled { due: Reverse(due), order: Reverse(order), direction: d, bytes });
                }
            }
            _ = async { sleep_until(next_due.unwrap()).await }, if next_due.is_some() => {}
            command = commands.recv() => match command {
                Some(Command::Inject(direction, bytes, sent)) => {
                    let target = match direction {
                        Direction::ToUpstream => Some(upstream),
                        Direction::ToClient => shared.lock().unwrap().client,
                    };
                    if let Some(target) = target {
                        let _ = socket.send_to(&bytes, target).await;
                    }
                    let _ = sent.send(());
                }
                Some(Command::Outage(duration, closed)) => {
                    drop(socket);
                    let _ = closed.send(());
                    sleep(duration).await;
                    socket = match UdpSocket::bind(addr).await {
                        Ok(socket) => socket,
                        Err(_) => return,
                    };
                    queue.clear();
                }
                None => return,
            },
        }
        let now = Instant::now();
        while queue.peek().is_some_and(|s| s.due.0 <= now) {
            let item = queue.pop().unwrap();
            let target = if item.direction == Direction::ToUpstream.index() {
                Some(upstream)
            } else {
                shared.lock().unwrap().client
            };
            if let Some(target) = target {
                let _ = socket.send_to(&item.bytes, target).await;
            }
        }
    }
}
