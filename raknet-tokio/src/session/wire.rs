//! The original encoded datagram and its route credit remain one router-owned value.

mod socket;
pub(crate) use socket::SocketWriter;

use super::admission::{Admission, SessionLifetime, WireCredit};
use crate::inline::Task;
use raknet::prelude::{RakSessionError, RakSessionId, constants};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
use tokio::sync::mpsc::{Receiver, Sender, channel};

/// Keeps the bounded wire queue while allowing its endpoint to run at admission.
#[derive(Clone)]
pub(crate) struct Route {
    sender: Sender<Datagram>,
    endpoint: Weak<Task>,
    socket: Option<Arc<SocketWriter>>,
}

impl Route {
    /// Observes socket-owner retirement through the original queue receiver.
    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }
}

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

/// The one physical send head retains either its original route credit or core-owned bytes.
pub(crate) enum SendDatagram {
    Session(Datagram),
    Socket(Box<[u8]>, SocketAddr),
}

impl SendDatagram {
    /// Borrows the same encoded allocation while socket readiness or stop is pending.
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Session(datagram) => datagram.bytes(),
            Self::Socket(bytes, _) => bytes,
        }
    }

    /// Keeps the original destination through route retirement and canceled send polls.
    pub(crate) fn peer(&self) -> SocketAddr {
        match self {
            Self::Session(datagram) => datagram.peer(),
            Self::Socket(_, peer) => *peer,
        }
    }
}

/// Constructs the single existing endpoint queue with its canonical message bound.
#[cfg(test)]
pub(crate) fn datagrams() -> (Route, Receiver<Datagram>) {
    datagrams_on(Weak::new(), None)
}

/// Attaches the endpoint's weak inline capability to its existing bounded queue.
pub(crate) fn datagrams_on(
    endpoint: Weak<Task>,
    socket: Option<Arc<SocketWriter>>,
) -> (Route, Receiver<Datagram>) {
    let (sender, receiver) = channel(constants::MAX_RECEIVE_MESSAGES);
    (
        Route {
            sender,
            endpoint,
            socket,
        },
        receiver,
    )
}

/// Holds original wire-byte credit through a direct send or bounded queue admission.
/// Cancellation retains its original bytes and releases any uncommitted credit.
pub(super) async fn forward(
    sender: &Route,
    admission: &Arc<Admission>,
    mtu: u16,
    id: RakSessionId,
    pending: &mut Option<(Box<[u8]>, SocketAddr)>,
) -> Result<(), RakSessionError> {
    let length = pending.as_ref().expect("one original wire output").0.len();
    let credit = admission.reserve_wire(length, mtu).await?;
    let sent = Task::dispatch(&sender.endpoint, |exclusive| {
        let (bytes, peer) = pending.as_ref().expect("same original wire output");
        let sent = exclusive
            && credit.direct_allowed()
            && sender.sender.capacity() == sender.sender.max_capacity()
            && sender
                .socket
                .as_ref()
                .is_some_and(|socket| socket.try_send(bytes, *peer));
        (sent, false)
    });
    if sent {
        pending.take();
        return Ok(());
    }
    let slot = sender
        .sender
        .reserve()
        .await
        .map_err(|_| RakSessionError::Closed)?;
    let (bytes, peer) = pending.take().expect("same admitted wire output");
    let datagram = Datagram {
        bytes,
        peer,
        id,
        credit,
    };
    Task::drive(&sender.endpoint, || {
        slot.send(datagram);
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn withdrawn_original_route_cannot_use_a_replacements_direct_permission() {
        let socket = Arc::new(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap());
        socket.writable().await.unwrap();
        let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let destination = peer.local_addr().unwrap();
        let weak = Arc::downgrade(&socket);
        let writer = SocketWriter::new(
            Arc::new(crate::stop::StopState::default()),
            move |bytes, peer| weak.upgrade().unwrap().try_send_to(bytes, peer),
            None,
        );
        let (runner, endpoint) = Task::spawn(|_| std::future::pending());
        let (route, mut received) = datagrams_on(endpoint, Some(writer));
        let original = Admission::new(2048, std::time::Duration::from_secs(1), true);
        let replacement = Admission::new(2048, std::time::Duration::from_secs(1), true);
        let original_handle = original.handle();
        let replacement_handle = replacement.handle();
        let lifetime = original.lifetime(&original_handle, None);
        lifetime.set_direct_wire(true);
        let id = RakSessionId(7);
        let mut pending = Some((Box::from(&b"original"[..]), destination));
        forward(&route, &original, 1500, id, &mut pending)
            .await
            .unwrap();
        let mut bytes = [0; 64];
        let (length, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..length], b"original");
        assert!(received.try_recv().is_err());
        lifetime.set_direct_wire(false);
        replacement
            .lifetime(&replacement_handle, None)
            .set_direct_wire(true);
        let mut pending = Some((Box::from(&b"retired original"[..]), destination));
        forward(&route, &original, 1500, id, &mut pending)
            .await
            .unwrap();
        let queued = received
            .try_recv()
            .expect("old allocation goes through route validation");
        assert_eq!(queued.bytes(), b"retired original");
        assert_eq!(
            peer.try_recv_from(&mut bytes).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        runner.abort();
        let _ = runner.await;
    }
}
