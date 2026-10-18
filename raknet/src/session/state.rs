// Modified by Cinnabar contributors to preserve connection handshake ordering.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, facet::Facet)]
#[repr(u8)]
pub enum RakSessionState {
    Connected,
    Disconnected,
    Closing,
    /// Reliable client establishment before application ownership transfers.
    Connecting,
}
