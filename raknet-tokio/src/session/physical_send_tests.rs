//! A task-local edge gate holds the original UDP send after genuine negotiation.

use super::*;
use crate::prelude::{RakClient, RakServer, RunningClient, RunningServer};
use crate::stop::StopState;
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

tokio::task_local! {
    static SEND_GATE: Arc<SendGate>;
}

/// One explicit endpoint's test edge; it never replaces the socket or reports a send.
pub(crate) struct SendGate {
    armed: AtomicBool,
    released: AtomicBool,
    changed: Notify,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    stop: OnceLock<Arc<StopState>>,
    finished: AtomicBool,
}

impl SendGate {
    /// Creates an initially transparent gate and one causal first-entry receipt.
    fn new() -> (Arc<Self>, oneshot::Receiver<()>) {
        let (send, receive) = oneshot::channel();
        (
            Arc::new(Self {
                armed: AtomicBool::new(false),
                released: AtomicBool::new(false),
                changed: Notify::new(),
                entered: Mutex::new(Some(send)),
                stop: OnceLock::new(),
                finished: AtomicBool::new(false),
            }),
            receive,
        )
    }

    /// Releases every borrowed wait on this same endpoint, including cleanup sends.
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

/// Captures only the calling start future's opt-in gate into its original spawned task.
pub(crate) fn capture(stop: &Arc<StopState>) -> Option<Arc<SendGate>> {
    SEND_GATE
        .try_with(|gate| {
            assert!(gate.stop.set(Arc::clone(stop)).is_ok());
            Arc::clone(gate)
        })
        .ok()
}

/// Pauses immediately before the original socket call; release still performs that real call.
pub(crate) async fn before_send(gate: &Option<Arc<SendGate>>) {
    let Some(gate) = gate
        .as_ref()
        .filter(|gate| gate.armed.load(Ordering::Acquire))
    else {
        return;
    };
    if let Some(entered) = gate.entered.lock().unwrap().take() {
        let _ = entered.send(());
    }
    loop {
        let changed = gate.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if gate.released.load(Ordering::Acquire) {
            return;
        }
        changed.await;
    }
}

/// Records normal completion at the original endpoint task's final boundary.
pub(crate) fn finished(gate: &Option<Arc<SendGate>>) {
    if let Some(gate) = gate {
        gate.finished.store(true, Ordering::Release);
    }
}

/// Owns the actual public endpoints, established handles and original actor joins.
struct Pair {
    client: Option<RakClient<RunningClient>>,
    server: Option<RakServer<RunningServer>>,
    local: Option<RakSession<Established>>,
    remote: Option<RakSession<Established>>,
    actors: Vec<JoinHandle<()>>,
}

impl Pair {
    /// Selects a gate on exactly one endpoint; both owners still bind original loopback sockets.
    async fn start(gate: &Arc<SendGate>, gated_client: bool) -> Self {
        let configured = RakServer::new((Ipv4Addr::LOCALHOST, 0).into(), |_| {});
        let server = if gated_client {
            configured.start().await
        } else {
            SEND_GATE.scope(Arc::clone(gate), configured.start()).await
        }
        .unwrap();
        let configured = RakClient::new(|_| {});
        let client = if gated_client {
            SEND_GATE.scope(Arc::clone(gate), configured.start()).await
        } else {
            configured.start().await
        }
        .unwrap();
        Self {
            client: Some(client),
            server: Some(server),
            local: None,
            remote: None,
            actors: Vec::new(),
        }
    }

    /// Completes real offline/online negotiation and one reliable round trip before any hold.
    async fn negotiate(&mut self) -> Result<(), String> {
        let address = self
            .server
            .as_ref()
            .unwrap()
            .command_queue_probe()
            .await
            .map_err(|_| "bound endpoint probe closed")?
            .address
            .ok_or("no bound endpoint")?;
        let mut local = self
            .client
            .as_ref()
            .unwrap()
            .connect(address)
            .await
            .map_err(|_| "original connect failed")?;
        self.actors
            .push(local.actor.take().ok_or("local actor owner missing")?);
        self.local = Some(local);
        let mut remote = self
            .server
            .as_mut()
            .unwrap()
            .accept()
            .await
            .map_err(|_| "original accept failed")?;
        self.actors
            .push(remote.actor.take().ok_or("remote actor owner missing")?);
        self.remote = Some(remote);
        self.echo(b"original traffic before physical hold").await
    }

    /// Checks full bytes through both existing reliable ordered receive owners.
    async fn echo(&mut self, marker: &[u8]) -> Result<(), String> {
        self.local
            .as_ref()
            .unwrap()
            .send(
                marker.to_vec().into_boxed_slice(),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await
            .map_err(|_| "original send failed")?;
        let request = self
            .remote
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|_| "original peer receive failed")?;
        if request.as_ref() != marker {
            return Err("original request bytes changed".into());
        }
        self.remote
            .as_ref()
            .unwrap()
            .send(
                request,
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await
            .map_err(|_| "original return send failed")?;
        let response = self
            .local
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|_| "original response failed")?;
        if response.as_ref() != marker {
            return Err("original response bytes changed".into());
        }
        Ok(())
    }

    /// Closes every original capability, stops remaining sockets and joins both original actors.
    async fn finish(mut self) -> Result<(), String> {
        for session in [self.local.take(), self.remote.take()]
            .into_iter()
            .flatten()
        {
            match session.close() {
                Ok(closing) => drop(closing),
                Err(error) => drop(error.into_parts().0),
            }
        }
        if let Some(client) = self.client.take() {
            client.stop().await;
        }
        if let Some(server) = self.server.take() {
            server.stop().await;
        }
        let mut failed = false;
        for actor in self.actors {
            failed |= actor.await.is_err();
        }
        if failed {
            Err("original session actor did not join".into())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct StopObservation {
    initially_pending: bool,
    first_cutoff_retained: bool,
    completed_by_cutoff: bool,
}

/// Polls the same consuming stop through its original cutoff, then releases and joins it.
async fn observe_stop(stop: impl Future<Output = ()>, gate: &SendGate) -> StopObservation {
    tokio::pin!(stop);
    let mut done = std::future::poll_fn(|cx| Poll::Ready(stop.as_mut().poll(cx).is_ready())).await;
    let initially_pending = !done;
    let owner = gate
        .stop
        .get()
        .expect("the actual endpoint captured this gate");
    let cutoff = owner
        .deadline()
        .expect("the original stop captured its cutoff");
    tokio::time::advance(cutoff.saturating_duration_since(Instant::now())).await;
    owner.request();
    let first_cutoff_retained = owner.deadline() == Some(cutoff);
    let mut completed_by_cutoff = false;
    if !done {
        for _ in 0..constants::MAX_RECEIVE_MESSAGES {
            done = std::future::poll_fn(|cx| Poll::Ready(stop.as_mut().poll(cx).is_ready())).await;
            if done {
                completed_by_cutoff = Instant::now() <= cutoff;
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    gate.release();
    if !done {
        stop.await;
    }
    StopObservation {
        initially_pending,
        first_cutoff_retained,
        completed_by_cutoff,
    }
}

/// Records the cutoff failure only after the original endpoints and actors have retired.
async fn held_send_stop(gated_client: bool) {
    let (gate, entered) = SendGate::new();
    let mut pair = Pair::start(&gate, gated_client).await;
    let setup = tokio::time::timeout(Duration::from_secs(10), async {
        pair.negotiate().await?;
        gate.armed.store(true, Ordering::Release);
        let sender = if gated_client {
            pair.local.as_ref()
        } else {
            pair.remote.as_ref()
        }
        .unwrap();
        sender
            .send(
                Box::<[u8]>::from(&b"one original pending reliable datagram"[..]),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await
            .map_err(|_| "pending body was not locally admitted")?;
        entered
            .await
            .map_err(|_| "original physical send never entered")?;
        Ok::<_, String>(())
    })
    .await;
    let observation = if matches!(&setup, Ok(Ok(()))) {
        tokio::time::pause();
        let observed = if gated_client {
            observe_stop(pair.client.take().unwrap().stop(), &gate).await
        } else {
            observe_stop(pair.server.take().unwrap().stop(), &gate).await
        };
        Some(observed)
    } else {
        None
    };
    gate.release();
    let cleanup = tokio::time::timeout(Duration::from_secs(20), pair.finish()).await;
    setup
        .expect("finite actual handshake/send fixture")
        .expect("actual wire preconditions");
    cleanup
        .expect("original endpoint cleanup cutoff")
        .expect("original endpoint/actor cleanup");
    assert!(
        gate.finished.load(Ordering::Acquire),
        "original endpoint task did not finish normally"
    );
    let observation = observation.unwrap();
    assert!(
        observation.initially_pending && observation.first_cutoff_retained,
        "{observation:?}"
    );
    assert!(
        observation.completed_by_cutoff,
        "physical UDP send stranded the original endpoint beyond its first stop cutoff: {observation:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn client_physical_send_keeps_the_first_stop_cutoff() {
    held_send_stop(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn server_physical_send_keeps_the_first_stop_cutoff() {
    held_send_stop(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn released_physical_send_preserves_original_reliable_bytes_and_next_body() {
    let (gate, entered) = SendGate::new();
    let mut pair = Pair::start(&gate, true).await;
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        pair.negotiate().await?;
        gate.armed.store(true, Ordering::Release);
        let marker = b"original held body released before stop";
        pair.local
            .as_ref()
            .unwrap()
            .send(
                Box::<[u8]>::from(&marker[..]),
                RakReliability::ReliableOrdered,
                RakPriority::Immediate,
            )
            .await
            .map_err(|_| "positive send failed")?;
        entered
            .await
            .map_err(|_| "positive original send never entered")?;
        gate.release();
        let body = pair
            .remote
            .as_mut()
            .unwrap()
            .recv::<Box<[u8]>>()
            .await
            .map_err(|_| "released original body was not received")?;
        if body.as_ref() != marker {
            return Err("released original bytes changed".into());
        }
        pair.echo(b"next original body proves no replay").await
    })
    .await;
    gate.release();
    let cleanup = tokio::time::timeout(Duration::from_secs(20), pair.finish()).await;
    cleanup
        .expect("positive cleanup cutoff")
        .expect("positive original cleanup");
    assert!(
        gate.finished.load(Ordering::Acquire),
        "positive original endpoint task did not finish normally"
    );
    observed
        .expect("finite release fixture")
        .expect("release/prefix/next-body control");
}
