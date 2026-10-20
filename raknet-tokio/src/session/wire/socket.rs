//! Writable UDP output can finish under the original endpoint's exclusive poll lock.

use crate::stop::StopState;
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::time::Instant;

type SendFn = dyn Fn(&[u8], SocketAddr) -> io::Result<usize> + Send + Sync;

pub(crate) struct SocketWriter {
    pending: AtomicBool,
    stop: Arc<StopState>,
    send: Box<SendFn>,
    #[cfg(test)]
    gate: Option<Arc<super::super::physical_send_tests::SendGate>>,
}

impl SocketWriter {
    /// Borrows the endpoint's real sockets through a callback holding only weak references.
    pub(crate) fn new(
        stop: Arc<StopState>,
        send: impl Fn(&[u8], SocketAddr) -> io::Result<usize> + Send + Sync + 'static,
        #[cfg(test)] gate: Option<Arc<super::super::physical_send_tests::SendGate>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pending: AtomicBool::new(false),
            stop,
            send: Box::new(send),
            #[cfg(test)]
            gate,
        })
    }

    /// Records the actual physical head before its endpoint can return Pending.
    pub(crate) fn set_pending(&self, pending: bool) {
        self.pending.store(pending, Ordering::Release);
    }

    /// Attempts the same UDP call only while the endpoint lock excludes every other send.
    pub(super) fn try_send(&self, bytes: &[u8], peer: SocketAddr) -> bool {
        if self.pending.load(Ordering::Acquire)
            || self
                .stop
                .deadline()
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return false;
        }
        #[cfg(test)]
        if super::super::physical_send_tests::is_held(&self.gate) {
            return false;
        }
        // The endpoint also retires a physical head after a failed atomic send.
        !matches!((self.send)(bytes, peer), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio::net::UdpSocket;

    #[tokio::test(start_paused = true)]
    async fn synchronous_writes_keep_the_physical_head_and_first_stop_cutoff() {
        let socket = Arc::new(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap());
        socket.writable().await.unwrap();
        let address = socket.local_addr().unwrap();
        let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let destination = peer.local_addr().unwrap();
        let weak = Arc::downgrade(&socket);
        let stop = Arc::new(StopState::default());
        let writer = SocketWriter::new(
            Arc::clone(&stop),
            move |bytes, peer| weak.upgrade().unwrap().try_send_to(bytes, peer),
            None,
        );
        writer.set_pending(true);
        assert!(!writer.try_send(b"original", destination));
        writer.set_pending(false);
        assert!(writer.try_send(b"original", destination));
        let mut bytes = [0; 64];
        let (length, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..length], b"original");
        stop.request();
        let deadline = stop.deadline().unwrap();
        tokio::time::advance(deadline - Instant::now()).await;
        stop.request();
        assert_eq!(stop.deadline(), Some(deadline));
        assert!(!writer.try_send(b"after cutoff", destination));
        assert_eq!(
            peer.try_recv_from(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(socket);
        UdpSocket::bind(address).await.unwrap();
    }
}
