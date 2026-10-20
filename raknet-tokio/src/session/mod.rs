//! Typed application handles backed by one private session actor.

mod admission;
mod command;
mod driver;

use admission::{ActorAdmission, Admission};
pub(crate) use admission::{SessionLifetime, SessionRetirement};
use command::RakSessionMsg;
use driver::Driver;
use raknet::prelude::*;
use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
#[cfg(test)]
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender, channel};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep};
use tracing::debug;

const SESSION_QUEUE_CAPACITY: usize = constants::MAX_RECEIVE_MESSAGES;

pub(crate) enum SessionEvent {
    Io(RakSessionInput),
    Close(std::time::Instant),
}

/// An established or locally closing application handle, never clonable.
///
/// ```compile_fail
/// use raknet_tokio::prelude::*;
/// fn closed_handle(session: RakSession<Closing>) {
///     session.send(Box::from([1_u8]), RakReliability::ReliableOrdered, RakPriority::Normal);
/// }
/// ```
pub struct RakSession<S: SessionPhase> {
    msg_tx: Sender<RakSessionMsg>,
    admission: Arc<Admission>,
    buf_rx: Receiver<Box<[u8]>>,
    terminal_rx: oneshot::Receiver<ClosedSession>,
    close_ack: Option<oneshot::Receiver<Result<(), RakSessionError>>>,
    addr: SocketAddr,
    max_message_len: usize,
    #[cfg(test)]
    actor: Option<tokio::task::JoinHandle<()>>,
    phase: PhantomData<S>,
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
    datagrams: UnboundedSender<(Box<[u8]>, SocketAddr)>,
    disconnected: UnboundedSender<RakSessionId>,
) -> (RakSession<Established>, Sender<SessionEvent>) {
    let addr = session.addr();
    let id = session.id();
    let limit = session.max_message_len();
    spawn(
        Driver::established(session),
        addr,
        id,
        limit,
        datagrams,
        disconnected,
    )
}

/// Spawns a restored closing actor while withholding all application-send methods.
pub(crate) fn spawn_closing(
    session: raknet::prelude::RakSession<Closing>,
    datagrams: UnboundedSender<(Box<[u8]>, SocketAddr)>,
    disconnected: UnboundedSender<RakSessionId>,
) -> (RakSession<Closing>, Sender<SessionEvent>) {
    let addr = session.addr();
    let id = session.id();
    let limit = session.max_message_len();
    spawn(
        Driver::closing(session),
        addr,
        id,
        limit,
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
    datagram_tx: UnboundedSender<(Box<[u8]>, SocketAddr)>,
    disconnect_tx: UnboundedSender<RakSessionId>,
) -> (RakSession<S>, Sender<SessionEvent>) {
    let admission = Admission::new(
        driver.config().max_outbound_bytes,
        driver.config().close_timeout,
        driver.state() == RakSessionState::Connected,
    );
    let actor_admission = ActorAdmission(Arc::clone(&admission));
    let (msg_tx, mut msg_rx) = channel(SESSION_QUEUE_CAPACITY);
    let (buf_tx, buf_rx) = channel(SESSION_QUEUE_CAPACITY);
    let (tx, mut rx) = channel(SESSION_QUEUE_CAPACITY);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let _actor = tokio::spawn(async move {
        let admission_owner = actor_admission;
        let admission = &admission_owner.0;
        let timer = sleep(std::time::Duration::ZERO);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                biased;
                close = admission.close_ready() => {
                    let result = driver.close(close.started.into_std(), false);
                    // Age the same close immediately; FIFO draining never rebases its cutoff.
                    let _ = driver.handle(RakSessionInput::Update(Instant::now().into_std()));
                    let _ = close.reply.send(result);
                }
                _ = async {
                    tokio::select! {
                        Some(message) = msg_rx.recv() => match message {
                            RakSessionMsg::Send(bytes, reliability, priority, reply, credit) => {
                                let result = driver.send(bytes, reliability, priority, Instant::now().into_std());
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
                            Some(SessionEvent::Io(input)) => { let _ = driver.handle(input); }
                            Some(SessionEvent::Close(now)) => { let _ = driver.close(now, false); }
                            None => { let _ = driver.close(Instant::now().into_std(), true); }
                        },
                        _ = buf_tx.closed() => { let _ = driver.close(Instant::now().into_std(), true); }
                        _ = &mut timer => { let _ = driver.handle(RakSessionInput::Update(Instant::now().into_std())); }
                    }
                } => {},
            }
            if driver.state() != RakSessionState::Connected {
                admission.seal();
            }
            while let Some(output) = driver.poll() {
                match output {
                    RakSessionOutput::Wait(after) => timer.as_mut().reset(Instant::now() + after),
                    RakSessionOutput::Datagram(bytes, peer) => {
                        let _ = datagram_tx.send((bytes, peer));
                    }
                    RakSessionOutput::Packet(bytes) => {
                        if bytes.is_empty() {
                            continue;
                        }
                        if let Err(TrySendError::Full(_)) = buf_tx.try_send(bytes) {
                            debug!("closing session with {addr}, its packets are not being read");
                            let _ = driver.close(Instant::now().into_std(), true);
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
        },
        tx,
    )
}

impl<S: SessionPhase> RakSession<S> {
    /// Observes the original allocation without retaining its public or actor ownership.
    pub(crate) fn lifetime(&self, retirement: &Arc<SessionRetirement>) -> SessionLifetime {
        self.admission.lifetime(retirement)
    }

    /// Receives complete messages, retaining the prefix before terminal failure.
    pub async fn recv<T>(&mut self) -> Result<T, RakSessionError>
    where
        Box<[u8]>: Into<T>,
    {
        if let Some(bytes) = self.buf_rx.recv().await {
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
    /// Takes terminal metadata once the received-message prefix has been consumed.
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
    /// Admits one message under the original command-count and outbound-byte budgets.
    ///
    /// Waiting retains the body in this caller's future. Cancellation before queue
    /// commitment releases its reservations; cancellation afterwards does not undo it.
    /// A body exceeding the negotiated limit or the entire queue byte budget returns
    /// `MessageSizeExceeded` without entering the queue.
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
        let mut credit = self
            .admission
            .reserve(bytes.len(), self.max_message_len)
            .await?;
        let permit = tokio::select! {
            biased;
            _ = self.admission.closed() => return Err(RakSessionError::Closed),
            permit = self.msg_tx.reserve() => permit.map_err(|_| RakSessionError::Closed)?,
        };
        let (tx, rx) = oneshot::channel();
        credit.commit()?;
        permit.send(RakSessionMsg::Send(
            bytes,
            reliability,
            priority,
            tx,
            credit,
        ));
        rx.await.map_err(|_| RakSessionError::Closed)?
    }
    /// Synchronously consumes send permission before any cancellable close wait.
    pub fn close(self) -> Result<RakSession<Closing>, SessionCloseError> {
        let (tx, rx) = oneshot::channel();
        let closing = RakSession {
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
        };
        match closing.admission.request_close(tx) {
            Ok(()) => Ok(closing),
            Err(cause) => Err(SessionCloseError {
                session: closing,
                cause,
            }),
        }
    }
}

/// A rejected close command retains the locally closing handle and receive FIFO.
#[derive(Debug, thiserror::Error)]
#[error("session close failed: {cause}")]
pub struct SessionCloseError {
    session: RakSession<Closing>,
    #[source]
    cause: RakSessionError,
}

impl SessionCloseError {
    /// Recovers the closing owner and original command error.
    pub fn into_parts(self) -> (RakSession<Closing>, RakSessionError) {
        (self.session, self.cause)
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
