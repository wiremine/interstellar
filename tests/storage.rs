//! Storage backend integration tests.
//!
//! This module contains tests for all storage backends:
//! - In-memory storage (always available)
//! - Memory-mapped persistent storage (requires "mmap" feature)
//! - Copy-on-write in-memory storage (CowGraph)
//! - Copy-on-write persistent storage (CowMmapGraph, requires "mmap" feature)

mod common;

#[path = "storage/inmemory.rs"]
mod inmemory;

#[path = "storage/index_integration.rs"]
mod index_integration;

#[path = "storage/cow.rs"]
mod cow;

#[path = "storage/mmap.rs"]
#[cfg(feature = "mmap")]
mod mmap;

#[path = "storage/cow_mmap.rs"]
#[cfg(feature = "mmap")]
mod cow_mmap;
