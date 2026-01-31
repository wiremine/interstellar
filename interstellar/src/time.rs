//! Platform-agnostic time utilities.
//!
//! On native platforms, uses `std::time`.
//! On WASM, uses `web-time` crate.

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_arch = "wasm32")]
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};
