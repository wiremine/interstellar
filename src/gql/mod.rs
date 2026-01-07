//! GQL (Graph Query Language) support for RustGremlin.
//!
//! This module provides parsing and execution of GQL queries against
//! a graph snapshot. GQL is a declarative query language for property graphs.
//!
//! # Example
//!
//! ```rust
//! use rustgremlin::prelude::*;
//! use rustgremlin::storage::InMemoryGraph;
//! use std::sync::Arc;
//!
//! // Create storage with data
//! let mut storage = InMemoryGraph::new();
//! let mut props = std::collections::HashMap::new();
//! props.insert("name".to_string(), Value::from("Alice"));
//! storage.add_vertex("Person", props);
//!
//! // Wrap in Graph for querying
//! let graph = Graph::new(Arc::new(storage));
//!
//! // Query with GQL
//! let snapshot = graph.snapshot();
//! let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
//! assert_eq!(results.len(), 1);
//! ```
//!
//! # Supported Syntax (Week 1 Spike)
//!
//! Currently supports minimal GQL syntax:
//!
//! ```text
//! MATCH (variable:Label) RETURN variable
//! MATCH (variable) RETURN variable
//! ```
//!
//! Future phases will add:
//! - Edge patterns: `(a)-[r:TYPE]->(b)`
//! - WHERE clauses: `WHERE n.age > 30`
//! - ORDER BY and LIMIT
//! - Aggregations: `count()`, `sum()`, etc.
//! - Variable-length paths: `(a)-[*1..3]->(b)`

mod ast;
mod compiler;
mod error;
mod parser;

pub use ast::*;
pub use compiler::compile;
pub use error::{CompileError, GqlError, ParseError};
pub use parser::parse;
