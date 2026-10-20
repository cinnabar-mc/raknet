//! The original encoded datagram and its route credit remain one router-owned value.

use super::admission::{Admission, SessionLifetime, WireCredit};
use raknet::prelude::{RakSessionError, RakSessionId, constants};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender, channel};

/// Carries the original allocation through queueing and the actual socket send.
pub(crate) struct Datagram {
    bytes: Box<[u8]>,
    peer: SocketAddr,
    id: RakSessionId,
    credit: WireCredit,
}

impl Datagram {
    /// Borrows the encoded bytes without releasing their route's reservation.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the original destination, not a replacement session's endpoint.
    pub(crate) fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Returns the actual producing core's local session identity.
    pub(crate) fn id(&self) -> RakSessionId {
        self.id
    }

    /// Validates the exact original owner in addition to its numeric session identity.
    pub(crate) fn belongs_to(&self, lifetime: &SessionLifetime) -> bool {
        self.credit.belongs_to(lifetime)
    }

    /// Transfers the same bytes into a real fixture peer while its credit remains in scope.
    #[cfg(test)]
    pub(super) fn into_parts(self) -> (Box<[u8]>, SocketAddr, WireCredit) {
        (self.bytes, self.peer, self.credit)
    }
}

/// Constructs the single existing endpoint queue with its canonical message bound.
pub(crate) fn datagrams() -> (Sender<Datagram>, Receiver<Datagram>) {
    channel(constants::MAX_RECEIVE_MESSAGES)
}

/// Commits one original output only after both wire-byte and channel admission.
/// Cancellation retains its original bytes and releases any uncommitted credit.
pub(super) async fn forward(
    sender: &Sender<Datagram>,
    admission: &Arc<Admission>,
    mtu: u16,
    id: RakSessionId,
    pending: &mut Option<(Box<[u8]>, SocketAddr)>,
) -> Result<(), RakSessionError> {
    let length = pending.as_ref().expect("one original wire output").0.len();
    let credit = admission.reserve_wire(length, mtu).await?;
    let slot = sender
        .reserve()
        .await
        .map_err(|_| RakSessionError::Closed)?;
    let (bytes, peer) = pending.take().expect("same admitted wire output");
    slot.send(Datagram {
        bytes,
        peer,
        id,
        credit,
    });
    Ok(())
}
