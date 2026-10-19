//! Owned setup and started task states for the Tokio client.

use super::msg::RakClientMsg;
use crate::stop::StopState;
use raknet::prelude::RakClientConfig;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

/// A Tokio client configuration that has not opened sockets or spawned a task.
pub struct ConfiguredClient {
    pub(super) config: RakClientConfig,
}

/// The running socket task and its private command capability.
pub struct RunningClient {
    pub(super) handle: Option<JoinHandle<()>>,
    pub(super) msg_tx: Sender<RakClientMsg>,
    pub(super) stop: Arc<StopState>,
}

impl Drop for RunningClient {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop.request();
        }
    }
}
