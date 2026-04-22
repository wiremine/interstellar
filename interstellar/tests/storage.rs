//! Storage backend integration tests.
//!
//! This module contains tests for all storage backends:
//! - Copy-on-write in-memory storage (Graph)
//! - Memory-mapped persistent storage (requires "mmap" feature)
//! - Copy-on-write persistent storage (PersistentGraph, requires "mmap" feature)

mod common;

#[path = "storage/index_integration.rs"]
mod index_integration;

#[path = "storage/text_search_integration.rs"]
#[cfg(feature = "full-text")]
mod text_search_integration;

#[path = "storage/cow.rs"]
mod cow;

#[path = "storage/mmap.rs"]
#[cfg(feature = "mmap")]
mod mmap;

#[path = "storage/cow_mmap.rs"]
#[cfg(feature = "mmap")]
mod cow_mmap;
