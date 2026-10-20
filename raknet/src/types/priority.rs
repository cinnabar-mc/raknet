#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
/// Selects scheduling order when flow control retains application writes.
pub enum RakPriority {
    /// Highest scheduling priority, within congestion and receive-window limits.
    Immediate,
    High,
    Normal,
    Low,
}
