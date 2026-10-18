//! Owned setup and started task states for the Tokio client.

use super::msg::RakClientMsg;
use raknet::prelude::RakClientConfig;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

/// A Tokio client configuration that has not opened sockets or spawned a task.
pub struct ConfiguredClient {
    pub(super) config: RakClientConfig,
}

/// The running socket task and its private command capability.
pub struct RunningClient {
    pub(super) handle: Option<JoinHandle<()>>,
    pub(super) msg_tx: UnboundedSender<RakClientMsg>,
}

impl Drop for RunningClient {
    fn drop(&mut self) {
        if self.handle.is_some() {
            let _ = self.msg_tx.send(RakClientMsg::Stop);
        }
    }
}
