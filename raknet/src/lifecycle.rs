//! Local ownership phases; peer availability remains a runtime result.

mod private {
    pub trait Sealed {}
}

/// Configuration owns no started operation.
#[derive(Debug)]
pub struct Configured;

/// A client attempt or its established route has been started.
#[derive(Debug)]
pub struct Active;

/// Establishment or trusted adoption completed, with local send permission.
#[derive(Debug)]
pub struct Established;

/// Local send permission has been consumed; protocol output remains drainable.
#[derive(Debug)]
pub struct Closing;

/// The session phases that can service protocol input and output.
pub trait SessionPhase: private::Sealed + std::fmt::Debug {
    #[doc(hidden)]
    const CLOSING: bool;
}

impl private::Sealed for Established {}
impl private::Sealed for Closing {}
impl SessionPhase for Established {
    const CLOSING: bool = false;
}
impl SessionPhase for Closing {
    const CLOSING: bool = true;
}
