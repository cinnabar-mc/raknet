/// A protocol state machine driven by application-supplied input.
///
/// After each input, drain `poll` in order, including when `handle` returns an
/// error. Retain output until the socket or application accepts it. Implementors
/// do not perform I/O or schedule their own timer updates.
pub trait Sans {
    type Input;
    type Output;
    type Error;

    /// Applies one input; queued output remains available even if this returns an error.
    fn handle(&mut self, msg: Self::Input) -> Result<(), Self::Error>;

    /// Takes the next output, or returns `None` when all current output has drained.
    fn poll(&mut self) -> Option<Self::Output>;
}
