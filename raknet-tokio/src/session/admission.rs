//! Original session admission, first close and metadata-only lifetime observation.

use raknet::prelude::{RakSessionError, constants};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

pub(super) struct Admission {
    maximum: Option<usize>,
    close_timeout: Duration,
    state: Mutex<State>,
    changed: Notify,
    closing_changed: Notify,
    retirement: OnceLock<Arc<SessionRetirement>>,
    direct_wire: AtomicBool,
}

/// The actual receive/terminal owner, separate from detached send admission.
pub(super) struct HandleLifetime(Arc<Admission>);

/// A weak observation cannot keep the original handle, actor or queued body alive.
pub(crate) struct SessionLifetime {
    admission: Weak<Admission>,
    handle: Weak<HandleLifetime>,
}

impl SessionLifetime {
    /// Mirrors this exact route's socket permission while its endpoint lock is held.
    pub(crate) fn set_direct_wire(&self, allowed: bool) {
        if let Some(admission) = self.admission.upgrade() {
            admission.direct_wire.store(allowed, Ordering::Release);
        }
    }
    /// Compares the original allocation without extending either route's lifetime.
    pub(super) fn matches(&self, admission: &Arc<Admission>) -> bool {
        self.admission.ptr_eq(&Arc::downgrade(admission))
    }

    /// Observes the actual actor guard even before its receiver finishes dropping.
    pub(crate) fn actor_retired(&self) -> bool {
        let Some(admission) = self.admission.upgrade() else {
            return true;
        };
        admission
            .state
            .lock()
            .expect("session admission mutex poisoned")
            .retired
    }

    /// Final handle disposal releases its slot once original actor and storage have retired.
    /// Revoked senders and uncommitted permits retain bookkeeping, not accepted ownership.
    pub(crate) fn released(&self) -> bool {
        if self.handle.strong_count() != 0 {
            return false;
        }
        let Some(admission) = self.admission.upgrade() else {
            return true;
        };
        let state = admission
            .state
            .lock()
            .expect("session admission mutex poisoned");
        state.retired
            && state.committed == 0
            && state.wire_messages == 0
            && state.received_bytes == 0
    }
}

impl Drop for HandleLifetime {
    fn drop(&mut self) {
        self.0.notify_retirement();
    }
}

/// Coalesces only retirement/release notifications for one original server endpoint.
#[derive(Default)]
pub(crate) struct SessionRetirement {
    changed: AtomicBool,
    wake: Notify,
}

impl SessionRetirement {
    /// Marks a finite ownership transition without adding another lifecycle queue.
    fn notify(&self) {
        self.changed.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    /// Tests the ownership changes before actual protocol admission.
    pub(crate) fn take(&self) -> bool {
        self.changed.swap(false, Ordering::AcqRel)
    }

    /// Waits outside the payload queues; notify_one retains a pre-registration wake.
    pub(crate) async fn wait(&self) {
        self.wake.notified().await;
    }
}

struct State {
    open: bool,
    retired: bool,
    bytes: usize,
    committed: usize,
    wire_bytes: usize,
    wire_messages: usize,
    received_bytes: usize,
    close: Option<Close>,
}

pub(super) struct Close {
    pub(super) started: Instant,
    pub(super) reply: oneshot::Sender<Result<(), RakSessionError>>,
}

/// Credit follows the same body until the actor transfers or rejects it.
pub(super) struct SendCredit {
    admission: Arc<Admission>,
    bytes: usize,
    committed: bool,
}

/// Keeps the encoded datagram and original route charged through router send or drop.
pub(super) struct WireCredit {
    admission: Arc<Admission>,
    bytes: usize,
}

/// Charges only the original final receive FIFO, until pop or disposal.
pub(super) struct ReceiveCredit {
    admission: Arc<Admission>,
    bytes: usize,
}

/// Seals blocked callers even if the original actor is aborted or unwinds.
pub(super) struct ActorAdmission(pub(super) Arc<Admission>);

impl Admission {
    /// Creates the sole application-handle lifetime; consuming phase changes transfer it.
    pub(super) fn handle(self: &Arc<Self>) -> Arc<HandleLifetime> {
        Arc::new(HandleLifetime(Arc::clone(self)))
    }

    /// Retains the original byte policy without semaphore platform restrictions.
    pub(super) fn new(maximum: i32, close_timeout: Duration, open: bool) -> Arc<Self> {
        Arc::new(Self {
            maximum: usize::try_from(maximum)
                .ok()
                .filter(|maximum| *maximum != 0),
            close_timeout,
            state: Mutex::new(State {
                open,
                retired: false,
                bytes: 0,
                committed: 0,
                wire_bytes: 0,
                wire_messages: 0,
                received_bytes: 0,
                close: None,
            }),
            changed: Notify::new(),
            closing_changed: Notify::new(),
            retirement: OnceLock::new(),
            direct_wire: AtomicBool::new(false),
        })
    }

    /// Admits an aggregate prefix or one already-valid body alone, without waiting.
    pub(super) fn try_receive(
        self: &Arc<Self>,
        bytes: usize,
        maximum: usize,
    ) -> Option<ReceiveCredit> {
        let mut state = self.state.lock().expect("session admission mutex poisoned");
        if state.received_bytes != 0
            && (state.received_bytes > maximum || bytes > maximum - state.received_bytes)
        {
            return None;
        }
        state.received_bytes = state.received_bytes.checked_add(bytes)?;
        Some(ReceiveCredit {
            admission: Arc::clone(self),
            bytes,
        })
    }

    /// Links a weak lifetime observation to the endpoint's single retirement wake.
    pub(super) fn lifetime(
        self: &Arc<Self>,
        handle: &Arc<HandleLifetime>,
        retirement: Option<&Arc<SessionRetirement>>,
    ) -> SessionLifetime {
        if let Some(retirement) = retirement {
            let retirement = self.retirement.get_or_init(|| Arc::clone(retirement));
            let retired = self
                .state
                .lock()
                .expect("session admission mutex poisoned")
                .retired;
            if retired {
                retirement.notify();
            }
        }
        SessionLifetime {
            admission: Arc::downgrade(self),
            handle: Arc::downgrade(handle),
        }
    }

    /// Wakes only the existing endpoint retention owner after a finite lifetime change.
    fn notify_retirement(&self) {
        if let Some(retirement) = self.retirement.get() {
            retirement.notify();
        }
    }

    /// Reserves exact encoded bytes independently of caller-owned application bodies.
    /// Original protocol output may drain after local sending has been sealed.
    pub(super) async fn reserve_wire(
        self: &Arc<Self>,
        bytes: usize,
        mtu: u16,
    ) -> Result<WireCredit, RakSessionError> {
        let maximum = self
            .maximum
            .ok_or(RakSessionError::InvalidConfig(
                "session byte capacities must be positive",
            ))?
            .max(usize::from(mtu));
        if bytes > maximum {
            return Err(RakSessionError::MessageSizeExceeded(bytes));
        }
        if let Some(credit) = self.try_wire(bytes, maximum) {
            return Ok(credit);
        }
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(credit) = self.try_wire(bytes, maximum) {
                return Ok(credit);
            }
            notified.await;
        }
    }

    /// Takes immediately available wire credit before allocating a capacity waiter.
    fn try_wire(self: &Arc<Self>, bytes: usize, maximum: usize) -> Option<WireCredit> {
        let mut state = self.state.lock().expect("session admission mutex poisoned");
        if state.wire_messages >= constants::MAX_RECEIVE_MESSAGES
            || bytes > maximum - state.wire_bytes
        {
            return None;
        }
        state.wire_bytes += bytes;
        state.wire_messages += 1;
        Some(WireCredit {
            admission: Arc::clone(self),
            bytes,
        })
    }

    /// Reserves actual body bytes while leaving uncommitted bodies caller-owned.
    pub(super) async fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        message_limit: usize,
    ) -> Result<SendCredit, RakSessionError> {
        if let Some(credit) = self.try_reserve(bytes, message_limit)? {
            return Ok(credit);
        }
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(credit) = self.try_reserve(bytes, message_limit)? {
                return Ok(credit);
            }
            notified.await;
        }
    }

    /// Takes available body credit under the same close fence before registering a waiter.
    fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
        message_limit: usize,
    ) -> Result<Option<SendCredit>, RakSessionError> {
        let mut state = self.state.lock().expect("session admission mutex poisoned");
        if !state.open {
            return Err(RakSessionError::Closed);
        }
        let maximum = self.maximum.ok_or(RakSessionError::InvalidConfig(
            "session byte capacities must be positive",
        ))?;
        if bytes > message_limit || bytes > maximum {
            return Err(RakSessionError::MessageSizeExceeded(bytes));
        }
        if bytes > maximum - state.bytes {
            return Ok(None);
        }
        state.bytes += bytes;
        Ok(Some(SendCredit {
            admission: Arc::clone(self),
            bytes,
            committed: false,
        }))
    }

    /// Waits for sealing independently of available command or byte capacity.
    pub(super) async fn closed(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self
                .state
                .lock()
                .expect("session admission mutex poisoned")
                .open
            {
                return;
            }
            notified.await;
        }
    }

    /// Captures the sole close before returning the locally closing capability.
    pub(super) fn request_close(
        &self,
        reply: oneshot::Sender<Result<(), RakSessionError>>,
    ) -> Result<(), RakSessionError> {
        {
            let mut state = self.state.lock().expect("session admission mutex poisoned");
            if state.retired || state.close.is_some() {
                return Err(RakSessionError::Closed);
            }
            state.open = false;
            state.close = Some(Close {
                started: Instant::now(),
                reply,
            });
        }
        self.changed.notify_waiters();
        self.closing_changed.notify_one();
        Ok(())
    }

    /// Drains earlier committed sends under the original close duration.
    pub(super) async fn close_ready(&self) -> Close {
        loop {
            let notified = self.closing_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let remaining = {
                let mut state = self.state.lock().expect("session admission mutex poisoned");
                let remaining = state
                    .close
                    .as_ref()
                    .map(|close| self.close_timeout.saturating_sub(close.started.elapsed()));
                if remaining.is_some_and(|remaining| remaining.is_zero() || state.committed == 0) {
                    return state.close.take().expect("the original close is pending");
                }
                remaining
            };
            match remaining {
                Some(remaining) => {
                    // A huge configured duration need not fit an absolute Instant.
                    match Instant::now().checked_add(remaining) {
                        Some(deadline) => tokio::select! {
                            _ = &mut notified => {},
                            _ = tokio::time::sleep_until(deadline) => {},
                        },
                        None => notified.await,
                    }
                }
                None => notified.await,
            }
        }
    }

    /// Stops future send commitment without discarding committed FIFO entries.
    pub(super) fn seal(&self) {
        self.state
            .lock()
            .expect("session admission mutex poisoned")
            .open = false;
        self.changed.notify_waiters();
    }
}

impl SendCredit {
    /// Linearizes commitment against the consuming close's admission fence.
    pub(super) fn commit(&mut self) -> Result<(), RakSessionError> {
        let mut state = self
            .admission
            .state
            .lock()
            .expect("session admission mutex poisoned");
        if !state.open {
            return Err(RakSessionError::Closed);
        }
        state.committed = state
            .committed
            .checked_add(1)
            .expect("committed commands fit their bounded FIFO");
        self.committed = true;
        Ok(())
    }
}

impl Drop for SendCredit {
    fn drop(&mut self) {
        let (closing, retired) = {
            let mut state = self
                .admission
                .state
                .lock()
                .expect("session admission mutex poisoned");
            state.bytes = state
                .bytes
                .checked_sub(self.bytes)
                .expect("each body returns its original byte credit once");
            if self.committed {
                state.committed = state
                    .committed
                    .checked_sub(1)
                    .expect("each committed body leaves the FIFO once");
            }
            (state.close.is_some(), state.retired)
        };
        self.admission.changed.notify_waiters();
        if closing {
            self.admission.closing_changed.notify_one();
        }
        if retired {
            self.admission.notify_retirement();
        }
    }
}

impl WireCredit {
    /// Checks the original producer's permission, never another session with the same ID.
    pub(super) fn direct_allowed(&self) -> bool {
        self.admission.direct_wire.load(Ordering::Acquire)
    }
    /// Rejects a reused numeric session ID belonging to a different original allocation.
    pub(super) fn belongs_to(&self, lifetime: &SessionLifetime) -> bool {
        lifetime.matches(&self.admission)
    }
}

impl Drop for WireCredit {
    fn drop(&mut self) {
        let retired = {
            let mut state = self
                .admission
                .state
                .lock()
                .expect("session admission mutex poisoned");
            state.wire_bytes = state
                .wire_bytes
                .checked_sub(self.bytes)
                .expect("each original datagram returns its byte credit once");
            state.wire_messages = state
                .wire_messages
                .checked_sub(1)
                .expect("each original datagram returns its message credit once");
            state.retired
        };
        self.admission.changed.notify_waiters();
        if retired {
            self.admission.notify_retirement();
        }
    }
}

impl Drop for ReceiveCredit {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .state
            .lock()
            .expect("session admission mutex poisoned");
        state.received_bytes = state
            .received_bytes
            .checked_sub(self.bytes)
            .expect("each final received body returns its byte credit once");
        let retired = state.retired;
        drop(state);
        if retired {
            self.admission.notify_retirement();
        }
    }
}

impl Drop for ActorAdmission {
    fn drop(&mut self) {
        let close = {
            let mut state = self
                .0
                .state
                .lock()
                .expect("session admission mutex poisoned");
            state.open = false;
            state.retired = true;
            state.close.take()
        };
        self.0.changed.notify_waiters();
        if let Some(retirement) = self.0.retirement.get() {
            retirement.notify();
        }
        drop(close);
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(retirement) = self.retirement.get() {
            retirement.notify();
        }
    }
}
