//! Typed application handles backed by one private session actor.

mod admission;
mod command;
mod driver;
mod send;
pub(crate) mod wire;

use crate::inline::Task;
use admission::{ActorAdmission, Admission, HandleLifetime, ReceiveCredit, SendCredit};
pub(crate) use admission::{SessionLifetime, SessionRetirement};
use command::RakSessionMsg;
use driver::Driver;
use raknet::prelude::*;
pub use send::{SendPermit, SendReceipt, SessionSender};
use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc::error::TrySendError;
#[cfg(test)]
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender, channel};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep};
use tracing::debug;

const SESSION_QUEUE_CAPACITY: usize = constants::MAX_RECEIVE_MESSAGES;

/// Drops rejected or unread body storage before returning its queue credit.
struct Received {
    bytes: Box<[u8]>,
    credit: ReceiveCredit,
}

pub(crate) enum SessionEvent {
    Io(RakSessionInput),
    Close(std::time::Instant),
}

/// One established or locally closing message session.
///
/// `recv` reads complete messages. Established sessions can also send; consuming
/// `close` returns a closing session that retains unread messages.
///
/// ```compile_fail
/// use raknet_tokio::prelude::*;
/// fn closed_handle(session: RakSession<Closing>) {
///     session.send(Box::from([1_u8]), RakReliability::ReliableOrdered, RakPriority::Normal);
/// }
/// ```
pub struct RakSession<S: SessionPhase> {
    task: Weak<Task>,
    msg_tx: Sender<RakSessionMsg>,
    admission: Arc<Admission>,
    buf_rx: Receiver<Received>,
    terminal_rx: oneshot::Receiver<ClosedSession>,
    close_ack: Option<oneshot::Receiver<Result<(), RakSessionError>>>,
    addr: SocketAddr,
    max_message_len: usize,
    #[cfg(test)]
    actor: Option<tokio::task::JoinHandle<()>>,
    phase: PhantomData<S>,
    handle: Arc<HandleLifetime>,
}

impl<S: SessionPhase> fmt::Debug for RakSession<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RakSession")
            .field("peer", &self.addr)
            .finish_non_exhaustive()
    }
}

/// Spawns the established actor only from the core's genuine owned handoff.
pub(crate) fn spawn_established(
    session: raknet::prelude::RakSession<Established>,
    datagrams: wire::Route,
    disconnected: UnboundedSender<RakSessionId>,
) -> (RakSession<Established>, Sender<SessionEvent>) {
    let addr = session.addr();
    let id = session.id();
    let limit = session.max_message_len();
    let mtu = session.mtu();
    spawn(
        Driver::established(session),
        addr,
        id,
        limit,
        mtu,
        datagrams,
        disconnected,
    )
}

/// Spawns a restored closing actor while withholding all application-send methods.
pub(crate) fn spawn_closing(
    session: raknet::prelude::RakSession<Closing>,
    datagrams: wire::Route,
    disconnected: UnboundedSender<RakSessionId>,
) -> (RakSession<Closing>, Sender<SessionEvent>) {
    let addr = session.addr();
    let id = session.id();
    let limit = session.max_message_len();
    let mtu = session.mtu();
    spawn(
        Driver::closing(session),
        addr,
        id,
        limit,
        mtu,
        datagrams,
        disconnected,
    )
}

/// Keeps all protocol state inside the actor and its typed local phase slot.
fn spawn<S: SessionPhase>(
    mut driver: Driver,
    addr: SocketAddr,
    id: RakSessionId,
    max_message_len: usize,
    mtu: u16,
    datagram_tx: wire::Route,
    disconnect_tx: UnboundedSender<RakSessionId>,
) -> (RakSession<S>, Sender<SessionEvent>) {
    let receive_maximum = usize::try_from(driver.config().max_queued_bytes).unwrap_or(0);
    let admission = Admission::new(
        driver.config().max_outbound_bytes,
        driver.config().close_timeout,
        driver.state() == RakSessionState::Connected,
    );
    let actor_admission = ActorAdmission(Arc::clone(&admission));
    let handle = admission.handle();
    let (msg_tx, mut msg_rx) = channel(SESSION_QUEUE_CAPACITY);
    let (buf_tx, buf_rx) = channel(SESSION_QUEUE_CAPACITY);
    let (tx, mut rx) = channel(SESSION_QUEUE_CAPACITY);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let (_actor, task) = Task::spawn(|_| async move {
        let admission_owner = actor_admission;
        let admission = &admission_owner.0;
        let timer = sleep(std::time::Duration::ZERO);
        tokio::pin!(timer);
        let mut pending = None;
        let mut application_open = true;
        let mut wire_open = true;
        let mut expiry = None;
        let closing = admission.close_ready();
        let application_closed = buf_tx.closed();
        let wire_closed = datagram_tx.closed();
        tokio::pin!(closing, application_closed, wire_closed);
        let mut close_consumed = false;
        loop {
            let blocked = pending.is_some();
            let terminal = driver.state() == RakSessionState::Disconnected;
            tokio::select! {
                biased;
                close = &mut closing, if !close_consumed => {
                    close_consumed = true;
                    let result = driver.close(close.started.into_std(), false);
                    // Age the same close immediately; FIFO draining never rebases its cutoff.
                    let _ = driver.handle(RakSessionInput::Update(Instant::now().into_std()));
                    expiry = None;
                    let _ = close.reply.send(result);
                }
                _ = &mut application_closed, if application_open => {
                    application_open = false;
                    let _ = driver.close(Instant::now().into_std(), true);
                }
                _ = &mut wire_closed, if wire_open => {
                    wire_open = false;
                    pending = None;
                    let _ = driver.close(Instant::now().into_std(), true);
                }
                result = wire::forward(&datagram_tx, admission, mtu, id, &mut pending),
                    if pending.is_some() && wire_open => {
                    if result.is_err() {
                        wire_open = false;
                        pending = None;
                        let _ = driver.close(Instant::now().into_std(), true);
                    }
                    #[cfg(test)]
                    if result.is_ok() && driver.state() == RakSessionState::Closing
                        && let Some(gate) = &mut driver.wait_gate
                    {
                        gate.admitted = true;
                    }
                }
                // Try admitting the terminal wire prefix above first. A genuinely
                // blocked route cannot withhold its already-produced local terminal.
                _ = std::future::ready(()), if terminal && pending.is_some() => {
                    wire_open = false;
                    pending = None;
                }
                _ = async {
                    if blocked {
                        // Observe deadlines only after real wire admission is Pending.
                        // Ordinary retransmits must not grow the core's blocked FIFO.
                        expiry = driver.expiry_deadline(Instant::now().into_std()).map(Instant::from_std);
                        match expiry {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                        let _ = driver.handle(RakSessionInput::Update(Instant::now().into_std()));
                        expiry = None;
                        return;
                    }
                    tokio::select! {
                        Some(message) = msg_rx.recv() => match message {
                            RakSessionMsg::Send(bytes, reliability, priority, reply, credit) => {
                                let result = driver.send(bytes, reliability, priority, Instant::now().into_std());
                                expiry = None;
                                drop(credit);
                                let _ = reply.send(result);
                            }
                            RakSessionMsg::IsClosed(reply) => {
                                let _ = reply.send(driver.state() != RakSessionState::Connected);
                            }
                            RakSessionMsg::Snapshot(reply) => {
                                let result = if buf_tx.capacity() != buf_tx.max_capacity() {
                                    Err(RakSessionCaptureError::PendingOutput)
                                } else {
                                    driver.snapshot(Instant::now().into_std())
                                };
                                let _ = reply.send(result);
                            }
                            #[cfg(test)]
                            RakSessionMsg::InspectCommands(reply) => {
                                let _ = reply.send(msg_rx.len());
                            }
                        },
                        event = rx.recv() => match event {
                            Some(SessionEvent::Io(input)) => {
                                let _ = driver.handle(input);
                                expiry = None;
                            }
                            Some(SessionEvent::Close(now)) => {
                                let _ = driver.close(now, false);
                                expiry = None;
                            }
                            None => { let _ = driver.close(Instant::now().into_std(), true); }
                        },
                        _ = &mut timer => {
                            let _ = driver.handle(RakSessionInput::Update(Instant::now().into_std()));
                            expiry = None;
                        }
                    }
                } => {},
            }
            if driver.state() != RakSessionState::Connected {
                admission.seal();
            }
            // Drain the original output before accepting another mutation. A blocked
            // wire front must not move growth back into the core's output FIFO.
            while pending.is_none()
                && let Some(output) = driver.poll()
            {
                match output {
                    RakSessionOutput::Wait(after) => {
                        #[cfg(test)]
                        if let Some(gate) = &mut driver.wait_gate {
                            gate.before_wait(after).await;
                        }
                        let now = Instant::now();
                        let deadline = now + after;
                        // Delayed relative output cannot postpone the core's original cutoff.
                        let deadline = driver
                            .expiry_deadline(now.into_std())
                            .map_or(deadline, |expiry| deadline.min(Instant::from_std(expiry)));
                        timer
                            .as_mut()
                            .reset(expiry.map_or(deadline, |expiry| expiry.min(deadline)));
                    }
                    RakSessionOutput::Datagram(bytes, peer) if wire_open => {
                        pending = Some((bytes, peer));
                    }
                    RakSessionOutput::Datagram(..) => {}
                    RakSessionOutput::Packet(bytes) => {
                        if bytes.is_empty() || !application_open {
                            continue;
                        }
                        let full = match admission.try_receive(bytes.len(), receive_maximum) {
                            Some(credit) => matches!(
                                buf_tx.try_send(Received { bytes, credit }),
                                Err(TrySendError::Full(_))
                            ),
                            None => {
                                drop(bytes);
                                true
                            }
                        };
                        if full {
                            application_open = false;
                            debug!("closing session with {addr}, its packets are not being read");
                            let _ = driver.close(Instant::now().into_std(), true);
                        }
                        #[cfg(test)]
                        {
                            let state = driver.state();
                            if let Some(gate) = &mut driver.receive_gate {
                                gate.after_attempt(
                                    buf_tx.max_capacity() - buf_tx.capacity(),
                                    state,
                                )
                                .await;
                            }
                        }
                    }
                    RakSessionOutput::Disconnected(..) => {
                        let _ = disconnect_tx.send(id);
                        if let Some(terminal) = driver.finish() {
                            let _ = terminal_tx.send(terminal);
                        }
                        return;
                    }
                }
            }
        }
    });
    (
        RakSession {
            task,
            msg_tx,
            admission,
            buf_rx,
            terminal_rx,
            close_ack: None,
            addr,
            max_message_len,
            #[cfg(test)]
            actor: Some(_actor),
            phase: PhantomData,
            handle,
        },
        tx,
    )
}

impl<S: SessionPhase> RakSession<S> {
    /// Borrows the original actor's inline capability without retaining its lifetime.
    pub(crate) fn task(&self) -> Weak<Task> {
        self.task.clone()
    }
    /// Observes the original allocation without retaining its public or actor ownership.
    pub(crate) fn lifetime(&self, retirement: Option<&Arc<SessionRetirement>>) -> SessionLifetime {
        self.admission.lifetime(&self.handle, retirement)
    }

    /// Waits for one complete message, reading queued messages before terminal failure.
    /// Canceling a pending receive does not consume a message.
    ///
    /// # Errors
    /// Returns `Closed` after the receive queue drains, or the close operation's
    /// error. Use `finish` after draining to obtain the protocol disconnection reason.
    pub async fn recv<T>(&mut self) -> Result<T, RakSessionError>
    where
        Box<[u8]>: Into<T>,
    {
        if let Some(Received { bytes, credit }) = self.buf_rx.recv().await {
            drop(credit);
            return Ok(bytes.into());
        }
        if let Some(ack) = &mut self.close_ack {
            let result = ack.await;
            self.close_ack = None;
            result.map_err(|_| RakSessionError::Closed)??;
        }
        Err(RakSessionError::Closed)
    }
    /// Observes the actor's current network state.
    pub async fn is_closed(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self.msg_tx.send(RakSessionMsg::IsClosed(tx)).await;
        rx.await.unwrap_or(true)
    }
    /// Returns the negotiated peer address.
    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }
    /// Returns the send limit captured from the negotiated core state.
    pub fn max_message_len(&self) -> usize {
        self.max_message_len
    }
    /// Captures trusted state without stopping its current owner.
    pub async fn snapshot(&self) -> Result<RakSessionSnapshot, SnapshotError> {
        if !self.buf_rx.is_empty() {
            return Err(RakSessionCaptureError::PendingOutput.into());
        }
        let (tx, rx) = oneshot::channel();
        self.msg_tx
            .send(RakSessionMsg::Snapshot(tx))
            .await
            .map_err(|_| SnapshotError::Session(RakSessionError::Closed))?;
        Ok(rx
            .await
            .map_err(|_| SnapshotError::Session(RakSessionError::Closed))??)
    }
    /// Takes disconnection metadata after retirement and all received messages have drained.
    /// Returns the session unchanged while either is incomplete.
    pub fn finish(mut self) -> Result<ClosedSession, Self> {
        if !self.buf_rx.is_closed() || !self.buf_rx.is_empty() {
            return Err(self);
        }
        match self.terminal_rx.try_recv() {
            Ok(terminal) => Ok(terminal),
            Err(_) => Err(self),
        }
    }
}

impl RakSession<Established> {
    /// Creates an owned send-only capability without sharing receive or consuming close ownership.
    pub fn sender(&self) -> SessionSender {
        SessionSender::new(self)
    }

    /// Admits one message under the original command-count and outbound-byte budgets.
    ///
    /// Waiting retains the body in this caller's future. Cancellation before queue
    /// commitment releases its reservations; cancellation afterwards does not undo it.
    /// A body exceeding the negotiated limit or the entire queue byte budget returns
    /// `MessageSizeExceeded` without entering the queue.
    /// Admitted writes drive sending without waiting for the maintenance timer;
    /// congestion control and physical socket admission can still defer delivery.
    pub async fn send<T>(
        &self,
        bytes: T,
        reliability: RakReliability,
        priority: RakPriority,
    ) -> Result<(), RakSessionError>
    where
        T: Into<Box<[u8]>>,
    {
        let bytes = bytes.into();
        self.sender()
            .reserve_send(bytes.len())
            .await?
            .commit(bytes, reliability, priority)?
            .admitted()
            .await
    }
    /// Starts graceful close, consuming this session's application-send methods.
    /// Keep reading from the returned closing session until it ends.
    ///
    /// # Errors
    /// A rejected close returns the same closing session and cause through
    /// `SessionCloseError::into_parts`, preserving its unread messages.
    pub fn close(self) -> Result<RakSession<Closing>, SessionCloseError> {
        let (tx, rx) = oneshot::channel();
        let closing = RakSession {
            task: self.task,
            msg_tx: self.msg_tx,
            admission: self.admission,
            buf_rx: self.buf_rx,
            terminal_rx: self.terminal_rx,
            close_ack: Some(rx),
            addr: self.addr,
            max_message_len: self.max_message_len,
            #[cfg(test)]
            actor: self.actor,
            phase: PhantomData,
            handle: self.handle,
        };
        match closing.admission.request_close(tx) {
            Ok(()) => Ok(closing),
            Err(cause) => Err(SessionCloseError {
                session: Box::new(closing),
                cause,
            }),
        }
    }
}

/// A rejected close command retains the locally closing handle and receive FIFO.
#[derive(Debug, thiserror::Error)]
#[error("session close failed: {cause}")]
pub struct SessionCloseError {
    session: Box<RakSession<Closing>>,
    #[source]
    cause: RakSessionError,
}

impl SessionCloseError {
    /// Recovers the closing owner and original command error.
    pub fn into_parts(self) -> (RakSession<Closing>, RakSessionError) {
        (*self.session, self.cause)
    }
}

/// A capture failure leaves both the live task and already-produced messages owned.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Session(#[from] RakSessionError),
    #[error(transparent)]
    Capture(#[from] RakSessionCaptureError),
}

/// Restored application handles retain the snapshot's saved local phase.
#[derive(Debug)]
pub enum RestoredSession {
    Established(RakSession<Established>),
    Closing(RakSession<Closing>),
    Closed(ClosedSession),
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod command_byte_tests;

#[cfg(test)]
mod admission_tests;

#[cfg(test)]
mod accepted_lifetime_tests;

#[cfg(test)]
mod lifetime_tests;

#[cfg(test)]
mod router_wire_tests;

#[cfg(test)]
pub(crate) mod physical_send_tests;

#[cfg(test)]
mod wait_cutoff_tests;
