//! Interstellar Graph Database - Native Node.js Bindings
//!
//! High-performance native bindings for Node.js using napi-rs.
//!
//! # Quick Start
//!
//! ```javascript
//! import { Graph, P, __ } from '@interstellar/node';
//!
//! // Create a new graph
//! const graph = new Graph();
//!
//! // Add vertices
//! const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
//! const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
//!
//! // Add edge
//! graph.addEdge(alice, bob, 'knows', { since: 2020 });
//!
//! // Query with Gremlin-style traversals
//! const friends = graph.V(alice)
//!     .out('knows')
//!     .hasLabel('person')
//!     .values('name')
//!     .toList();
//!
//! console.log(friends); // ['Bob']
//! ```
//!
//! # Features
//!
//! - **Graph Operations**: Create, read, update, delete vertices and edges
//! - **Gremlin-style API**: Familiar traversal DSL with method chaining
//! - **Predicates (P)**: Comparison, range, collection, and string predicates
//! - **Anonymous Traversals (__)**: Composable traversal fragments
//! - **High Performance**: Native Rust implementation with zero-copy where possible

#![feature(impl_trait_in_assoc_type)]
#![deny(clippy::all)]

mod anonymous;
mod error;
mod graph;
mod predicate;
#[cfg(feature = "full-text")]
mod text_query;
mod traversal;
mod value;

// Re-export all napi types
pub use anonymous::AnonymousFactory;
pub use graph::JsGraph;
pub use predicate::{JsPredicate, P};
#[cfg(feature = "full-text")]
pub use text_query::{JsTextQuery, TextQ};
pub use traversal::JsTraversal;
