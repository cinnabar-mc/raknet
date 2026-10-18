//! Session policies supported by this frame-driven socket adapter.

use raknet::prelude::RakSessionConfig;
use std::io;

/// Rejects manual flushing before binding a socket whose session has no caller tick.
pub(crate) fn require_automatic_flush(config: &RakSessionConfig) -> io::Result<()> {
    if !config.autoflush {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Bevy RakNet sessions require automatic flushing",
        ));
    }
    Ok(())
}
