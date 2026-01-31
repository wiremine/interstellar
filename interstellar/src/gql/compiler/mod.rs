//! GQL Compiler module - transforms GQL AST to traversal execution.
//!
//! This module provides the compiler that takes a parsed GQL [`Query`] and executes
//! it against a [`GraphSnapshot`], returning results as `Vec<Value>`.
//!
//! # Module Structure
//!
//! The compiler is organized into several submodules:
//!
//! - `core`: Core compiler struct and main compile logic
//! - `helpers`: Helper types and functions for value comparison and operations
//! - `math`: Math expression evaluation
//! - `pattern`: Pattern matching and compilation
//! - `expression`: Expression evaluation (row-based, path-based, element-based)
//! - `clauses`: UNWIND, LET, WITH clause handling
//! - `call`: CALL subquery handling
//! - `optional`: OPTIONAL MATCH handling
//! - `aggregation`: GROUP BY, aggregates, HAVING
//!
//! [`Query`]: crate::gql::ast::Query
//! [`GraphSnapshot`]: crate::storage::GraphSnapshot

// Helper types and functions
mod helpers;

// Math expression evaluation
mod math;

// Re-export everything from the legacy file during migration
#[path = "../compiler_legacy.rs"]
mod legacy;

pub use legacy::*;
