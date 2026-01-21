//! # Rhai Scripting Integration
//!
//! This module provides Rhai scripting support for Interstellar's graph traversal API.
//!
//! Rhai is an embedded scripting language that exposes Interstellar's Gremlin-style
//! traversal API to scripts, enabling interactive exploration and dynamic query construction.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use interstellar::prelude::*;
//! use interstellar::rhai::RhaiEngine;
//!
//! let graph = Graph::in_memory();
//! let engine = RhaiEngine::new();
//!
//! let script = r#"
//!     let g = graph.gremlin();
//!     g.v().has_label("person").values("name").to_list()
//! "#;
//!
//! let result = engine.eval(&graph.snapshot(), script)?;
//! ```
//!
//! ## Storage Backend Support
//!
//! The Rhai integration supports both in-memory (`Graph`) and memory-mapped (`CowMmapGraph`)
//! storage backends. The script syntax is identical regardless of the underlying storage.
//!
//! ```rust,ignore
//! // In-memory graph
//! let engine = RhaiEngine::new();
//! let graph = Arc::new(Graph::new());
//! let result = engine.eval_with_graph(graph, script)?;
//!
//! // Persistent mmap graph (requires "mmap" feature)
//! #[cfg(feature = "mmap")]
//! {
//!     let mmap_graph = Arc::new(CowMmapGraph::open("data.db")?);
//!     let result = engine.eval_with_mmap_graph(mmap_graph, script)?;
//! }
//! ```
//!
//! ## Module Structure
//!
//! - [`engine`] - The main `RhaiEngine` type for script execution
//! - [`types`] - Type registrations for `Value`, `VertexId`, `EdgeId`
//! - [`traversal`] - Traversal wrapper types for Rhai
//! - [`predicates`] - Predicate function bindings (`eq`, `gt`, `within`, etc.)
//! - [`anonymous`] - Anonymous traversal factory (`__`)
//! - [`error`] - Error types for Rhai integration

mod anonymous;
mod engine;
mod error;
mod predicates;
mod traversal;
mod types;

pub use anonymous::{create_anonymous_factory, register_anonymous, AnonymousTraversalFactory};
pub use engine::RhaiEngine;
pub use error::*;
pub use predicates::*;
pub use traversal::*;
pub use types::*;
