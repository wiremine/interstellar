//! # Rhai Scripting Integration
//!
//! This module provides Rhai scripting support for Intersteller's graph traversal API.
//!
//! Rhai is an embedded scripting language that exposes Intersteller's Gremlin-style
//! traversal API to scripts, enabling interactive exploration and dynamic query construction.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use intersteller::prelude::*;
//! use intersteller::rhai::RhaiEngine;
//!
//! let graph = Graph::in_memory();
//! let engine = RhaiEngine::new();
//!
//! let script = r#"
//!     let g = graph.traversal();
//!     g.v().has_label("person").values("name").to_list()
//! "#;
//!
//! let result = engine.eval(&graph.snapshot(), script)?;
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
