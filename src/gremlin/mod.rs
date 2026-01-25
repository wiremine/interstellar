//! # Gremlin Text Parser
//!
//! TinkerPop-compatible Gremlin query parsing for Interstellar.
//!
//! This module provides a parser for Gremlin query strings, allowing users to write
//! queries as text and have them parsed, compiled, and executed against the graph.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use interstellar::prelude::*;
//! use interstellar::gremlin;
//!
//! let graph = Graph::new();
//! // ... populate graph ...
//!
//! // Parse a Gremlin query
//! let ast = gremlin::parse("g.V().hasLabel('person').values('name')")?;
//!
//! // Compile and execute
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
//! let compiled = gremlin::compile(&ast, &g)?;
//! let results = compiled.execute()?;
//! ```
//!
//! ## Supported Syntax
//!
//! The parser supports a subset of TinkerPop Gremlin syntax:
//!
//! - **Source steps**: `g.V()`, `g.E()`, `g.addV()`, `g.addE()`, `g.inject()`
//! - **Navigation**: `out()`, `in()`, `both()`, `outE()`, `inE()`, `outV()`, `inV()`
//! - **Filtering**: `has()`, `hasLabel()`, `hasId()`, `where()`, `is()`, `and()`, `or()`, `not()`
//! - **Limiting**: `limit()`, `skip()`, `range()`, `dedup()`
//! - **Transform**: `values()`, `valueMap()`, `id()`, `label()`, `count()`, `fold()`
//! - **Branch**: `union()`, `coalesce()`, `choose()`, `optional()`
//! - **Repeat**: `repeat()`, `times()`, `until()`, `emit()`
//! - **Side-effect**: `as()`, `select()`, `aggregate()`, `store()`, `cap()`
//! - **Mutation**: `property()`, `from()`, `to()`, `drop()`
//! - **Terminal**: `toList()`, `next()`, `iterate()`, `hasNext()`
//! - **Predicates**: `P.eq()`, `P.gt()`, `P.within()`, `TextP.containing()`, etc.
//! - **Anonymous traversals**: `__.out()`, `__.values()`, etc.

mod ast;
mod compiler;
mod error;
mod parser;

pub use ast::*;
pub use compiler::{compile, CompiledTraversal, ExecutionResult};
pub use error::{CompileError, GremlinError, ParseError};
pub use parser::parse;

#[cfg(test)]
mod tests;
