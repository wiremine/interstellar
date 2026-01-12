//! # GQL Parser, Compiler, and Mutation Executor
//!
//! This module provides GQL (Graph Query Language) support for Intersteller.
//! GQL is a declarative query language for property graphs, offering a
//! SQL-like syntax for pattern matching, data retrieval, and mutations.
//!
//! ## Overview
//!
//! The GQL implementation follows a pipeline architecture:
//!
//! ```text
//! GQL Query Text → Parser → AST → Compiler/Executor → Results
//! ```
//!
//! 1. **Parser** ([`parse`], [`parse_statement`]): Converts GQL text into a typed AST
//! 2. **Compiler** ([`compile`]): Transforms read-only AST into traversal operations
//! 3. **Mutation Executor** ([`execute_mutation`]): Executes mutation statements
//! 4. **Execution**: The traversal engine or mutation executor processes the query
//!
//! ## Quick Start - Read Queries
//!
//! The simplest way to execute a GQL query is through [`GraphSnapshot::gql()`](crate::graph::GraphSnapshot::gql):
//!
//! ```rust
//! use intersteller::prelude::*;
//! use intersteller::storage::InMemoryGraph;
//! use std::sync::Arc;
//!
//! // Create a graph with data
//! let mut storage = InMemoryGraph::new();
//! let mut props = std::collections::HashMap::new();
//! props.insert("name".to_string(), Value::from("Alice"));
//! props.insert("age".to_string(), Value::from(30i64));
//! storage.add_vertex("Person", props);
//!
//! let graph = Graph::new(storage);
//! let snapshot = graph.snapshot();
//!
//! // Execute GQL query
//! let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
//! assert_eq!(results.len(), 1);
//! ```
//!
//! ## Quick Start - Mutations
//!
//! For mutations (CREATE, SET, DELETE, etc.), use [`execute_mutation`] with mutable storage:
//!
//! ```rust
//! use intersteller::gql::{parse_statement, execute_mutation};
//! use intersteller::storage::{GraphStorage, InMemoryGraph};
//!
//! let mut storage = InMemoryGraph::new();
//!
//! // CREATE a new vertex
//! let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
//! execute_mutation(&stmt, &mut storage).unwrap();
//!
//! assert_eq!(storage.vertex_count(), 1);
//!
//! // UPDATE with SET
//! let stmt = parse_statement("MATCH (n:Person {name: 'Alice'}) SET n.age = 31").unwrap();
//! execute_mutation(&stmt, &mut storage).unwrap();
//!
//! // DELETE
//! let stmt = parse_statement("MATCH (n:Person {name: 'Alice'}) DELETE n").unwrap();
//! execute_mutation(&stmt, &mut storage).unwrap();
//!
//! assert_eq!(storage.vertex_count(), 0);
//! ```
//!
//! ## Supported Features
//!
//! ### MATCH Clause - Pattern Matching
//!
//! The `MATCH` clause specifies patterns to find in the graph.
//!
//! **Node patterns:**
//! ```text
//! (n)                         -- Any vertex, bound to variable 'n'
//! (n:Person)                  -- Vertex with label 'Person'
//! (n:Person:Employee)         -- Vertex with multiple labels
//! (n {name: 'Alice'})         -- Vertex with property constraint
//! (n:Person {name: 'Alice'})  -- Label and property constraint
//! (:Person)                   -- Anonymous (unbound) vertex
//! ```
//!
//! **Edge patterns:**
//! ```text
//! -[:KNOWS]->                 -- Outgoing edge with label 'KNOWS'
//! <-[:KNOWS]-                 -- Incoming edge
//! -[:KNOWS]-                  -- Bidirectional (either direction)
//! -[e:KNOWS]->                -- Edge bound to variable 'e'
//! -[]->                       -- Any outgoing edge
//! ```
//!
//! **Variable-length paths:**
//! ```text
//! -[:KNOWS*]->                -- Any number of hops (default max: 10)
//! -[:KNOWS*2]->               -- Exactly 2 hops
//! -[:KNOWS*1..3]->            -- 1 to 3 hops
//! -[:KNOWS*..5]->             -- 0 to 5 hops (includes start vertex)
//! -[:KNOWS*2..]->             -- 2 or more hops
//! ```
//!
//! **Complete pattern example:**
//! ```rust
//! # use intersteller::prelude::*;
//! # let graph = Graph::in_memory();
//! # let snapshot = graph.snapshot();
//! let results = snapshot.gql(r#"
//!     MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
//!     RETURN friend
//! "#);
//! ```
//!
//! ### WHERE Clause - Filtering
//!
//! The `WHERE` clause filters results using boolean expressions.
//!
//! **Comparison operators:**
//! - `=`, `<>`, `!=` - Equality and inequality
//! - `<`, `<=`, `>`, `>=` - Numeric/string comparison
//!
//! **Logical operators:**
//! - `AND` - Logical conjunction
//! - `OR` - Logical disjunction
//! - `NOT` - Logical negation
//!
//! **String operators:**
//! - `CONTAINS` - Substring match
//! - `STARTS WITH` - Prefix match
//! - `ENDS WITH` - Suffix match
//!
//! **Null checks:**
//! - `IS NULL` - Check for missing property
//! - `IS NOT NULL` - Check for existing property
//!
//! **List membership:**
//! - `IN [...]` - Value in list
//! - `NOT IN [...]` - Value not in list
//!
//! **Examples:**
//! ```rust
//! # use intersteller::prelude::*;
//! # let graph = Graph::in_memory();
//! # let snapshot = graph.snapshot();
//! // Numeric comparison
//! let _ = snapshot.gql("MATCH (p:Person) WHERE p.age > 25 RETURN p");
//!
//! // Combined conditions
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     WHERE p.age >= 25 AND p.age <= 35
//!     RETURN p
//! "#);
//!
//! // String matching
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     WHERE p.name STARTS WITH 'A'
//!     RETURN p
//! "#);
//!
//! // Null check
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     WHERE p.email IS NOT NULL
//!     RETURN p
//! "#);
//!
//! // List membership
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     WHERE p.status IN ['active', 'pending']
//!     RETURN p
//! "#);
//! ```
//!
//! ### RETURN Clause - Result Projection
//!
//! The `RETURN` clause specifies what data to return.
//!
//! **Return types:**
//! - Variables: `RETURN n` - Returns the vertex/edge
//! - Properties: `RETURN n.name` - Returns property value
//! - Aliases: `RETURN n.name AS personName` - Rename in output
//! - Multiple items: `RETURN n.name, n.age` - Returns a map
//! - Literals: `RETURN 'constant'` - Returns constant value
//! - Distinct: `RETURN DISTINCT n.city` - Deduplicate results
//!
//! **Aggregate functions:**
//! - `COUNT(*)` - Count all results
//! - `COUNT(expr)` - Count non-null values
//! - `COUNT(DISTINCT expr)` - Count unique values
//! - `SUM(expr)` - Sum numeric values
//! - `AVG(expr)` - Average of numeric values
//! - `MIN(expr)` - Minimum value
//! - `MAX(expr)` - Maximum value
//! - `COLLECT(expr)` - Collect values into a list
//!
//! **Examples:**
//! ```rust
//! # use intersteller::prelude::*;
//! # let graph = Graph::in_memory();
//! # let snapshot = graph.snapshot();
//! // Return multiple properties as a map
//! let _ = snapshot.gql("MATCH (p:Person) RETURN p.name, p.age");
//!
//! // With aliases
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     RETURN p.name AS name, p.age AS years
//! "#);
//!
//! // Aggregation
//! let _ = snapshot.gql("MATCH (p:Person) RETURN count(*)");
//! let _ = snapshot.gql("MATCH (p:Person) RETURN avg(p.age)");
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     RETURN count(DISTINCT p.city) AS uniqueCities
//! "#);
//! ```
//!
//! ### ORDER BY Clause - Sorting
//!
//! Sort results by one or more expressions.
//!
//! ```rust
//! # use intersteller::prelude::*;
//! # let graph = Graph::in_memory();
//! # let snapshot = graph.snapshot();
//! // Ascending (default)
//! let _ = snapshot.gql("MATCH (p:Person) RETURN p ORDER BY p.age");
//!
//! // Descending
//! let _ = snapshot.gql("MATCH (p:Person) RETURN p ORDER BY p.age DESC");
//!
//! // Multiple sort keys
//! let _ = snapshot.gql(r#"
//!     MATCH (p:Person)
//!     RETURN p
//!     ORDER BY p.age DESC, p.name ASC
//! "#);
//! ```
//!
//! ### LIMIT/OFFSET Clause - Pagination
//!
//! Limit the number of results and skip initial results.
//!
//! ```rust
//! # use intersteller::prelude::*;
//! # let graph = Graph::in_memory();
//! # let snapshot = graph.snapshot();
//! // First 10 results
//! let _ = snapshot.gql("MATCH (p:Person) RETURN p LIMIT 10");
//!
//! // Pagination: skip 20, take 10
//! let _ = snapshot.gql("MATCH (p:Person) RETURN p LIMIT 10 OFFSET 20");
//! ```
//!
//! ## Complete Query Example
//!
//! ```rust
//! # use intersteller::prelude::*;
//! # let graph = Graph::in_memory();
//! # let snapshot = graph.snapshot();
//! let results = snapshot.gql(r#"
//!     MATCH (p:Person)-[:KNOWS]->(friend:Person)
//!     WHERE p.age > 25 AND friend.city = 'NYC'
//!     RETURN p.name AS person, friend.name AS friendName, friend.age
//!     ORDER BY friend.age DESC
//!     LIMIT 10
//! "#);
//! ```
//!
//! ## Error Handling
//!
//! GQL operations can fail with two types of errors:
//!
//! - [`ParseError`] - Syntax errors in the query text
//! - [`CompileError`] - Semantic errors (undefined variables, type mismatches)
//!
//! Both are wrapped in [`GqlError`] when using [`GraphSnapshot::gql()`](crate::graph::GraphSnapshot::gql).
//!
//! ```rust
//! use intersteller::prelude::*;
//! use intersteller::gql::GqlError;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//!
//! match snapshot.gql("MATCH (n:Person) RETURN x") {
//!     Ok(results) => println!("Found {} results", results.len()),
//!     Err(GqlError::Parse(e)) => eprintln!("Syntax error: {}", e),
//!     Err(GqlError::Compile(e)) => eprintln!("Compilation error: {}", e),
//! }
//! ```
//!
//! ## Mutation Clauses
//!
//! GQL mutations allow you to modify the graph declaratively.
//!
//! ### CREATE - Add New Elements
//!
//! ```text
//! CREATE (n:Person {name: 'Alice', age: 30})
//! CREATE (a:Person)-[:KNOWS]->(b:Person)
//! CREATE (n:Person {name: 'Alice'}) RETURN n
//! ```
//!
//! ### SET - Update Properties
//!
//! ```text
//! MATCH (n:Person {name: 'Alice'}) SET n.age = 31
//! MATCH (n:Person) SET n.updated = true, n.timestamp = 123
//! ```
//!
//! ### REMOVE - Remove Properties
//!
//! ```text
//! MATCH (n:Person) REMOVE n.temporary_field
//! ```
//!
//! ### DELETE - Remove Elements
//!
//! ```text
//! MATCH (n:Person {status: 'inactive'}) DELETE n
//! MATCH ()-[r:OLD_RELATION]->() DELETE r
//! ```
//!
//! Note: DELETE fails if a vertex has connected edges. Use DETACH DELETE instead.
//!
//! ### DETACH DELETE - Remove Vertices with Edges
//!
//! ```text
//! MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
//! ```
//!
//! DETACH DELETE removes the vertex and all connected edges automatically.
//!
//! ### MERGE - Upsert Operation
//!
//! ```text
//! MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true
//! MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.lastSeen = 123
//! MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.new = true ON MATCH SET n.existing = true
//! ```
//!
//! MERGE creates the pattern if it doesn't exist, or matches existing elements.
//!
//! ## Limitations
//!
//! The current implementation does not support:
//!
//! - **UNWIND**: No list unpacking in queries
//! - **Subqueries**: No nested queries or `CALL` procedures
//! - **Multiple graphs**: Single graph queries only
//! - **Path expressions**: Cannot return paths directly (use variable-length
//!   patterns and return endpoints)
//! - **Comma-separated MATCH patterns**: Use edge patterns instead of `MATCH (a), (b)`
//! - **Anonymous endpoint patterns**: `MATCH ()-[r]->()` requires explicit labels
//!
//! ## Architecture
//!
//! For implementers, the module is organized as:
//!
//! - [`ast`](ast) - AST type definitions ([`Query`], [`Pattern`], [`Expression`], etc.)
//! - [`parser`](parser) - pest-based parser (grammar in `grammar.pest`)
//! - [`compiler`](compiler) - AST to traversal compiler for read queries
//! - [`mutation`](mutation) - Mutation execution engine ([`execute_mutation`], [`MutationContext`])
//! - [`error`](error) - Error types with source span information
//!
//! The parser uses the [pest](https://pest.rs) parsing library with a PEG grammar.
//! The compiler transforms read AST nodes into calls to the traversal API.
//! The mutation executor directly modifies storage via [`GraphStorageMut`](crate::storage::GraphStorageMut).

mod ast;
mod compiler;
mod error;
mod mutation;
mod parser;

pub use ast::*;
pub use compiler::{compile, compile_statement};
pub use error::{CompileError, GqlError, ParseError, Span};
pub use mutation::{
    execute_mutation, execute_mutation_query, Element, MutationContext, MutationError,
};
pub use parser::{parse, parse_statement};
