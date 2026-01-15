//! Storage backend integration tests.
//!
//! This module contains tests for both storage backends:
//! - In-memory storage (always available)
//! - Memory-mapped persistent storage (requires "mmap" feature)

mod common;

#[path = "storage/inmemory.rs"]
mod inmemory;

#[path = "storage/mmap.rs"]
#[cfg(feature = "mmap")]
mod mmap;
