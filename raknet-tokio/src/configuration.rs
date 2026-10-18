//! Session policies supported by this automatically scheduled socket adapter.

use raknet::prelude::RakSessionConfig;
use std::io;

/// Rejects manual flushing before opening or dispatching an automatic session owner.
pub(crate) fn require_automatic_flush(config: &RakSessionConfig) -> io::Result<()> {
    if !config.autoflush {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Tokio RakNet sessions require automatic flushing",
        ));
    }
    Ok(())
}
