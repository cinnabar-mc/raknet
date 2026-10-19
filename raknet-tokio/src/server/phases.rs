//! The data owned by configured and running Tokio server phases.

use super::msg::RakServerMsg;
use crate::session::RakSession;
use crate::stop::StopState;
use raknet::prelude::{Established, RakServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};
use tokio::task::JoinHandle;

/// Server settings before socket binding and task creation.
pub struct ConfiguredServer {
    pub(super) addr: SocketAddr,
    pub(super) config: RakServerConfig,
}

/// A started socket task and its exclusive accepted-session receiver.
pub struct RunningServer {
    pub(super) handle: Option<JoinHandle<()>>,
    pub(super) session_rx: UnboundedReceiver<RakSession<Established>>,
    pub(super) msg_tx: Sender<RakServerMsg>,
    pub(super) stop: Arc<StopState>,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop.request();
        }
    }
}
