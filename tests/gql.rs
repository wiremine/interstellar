//! GQL (Graph Query Language) integration tests.
//!
//! Tests are organized by GQL feature area:
//! - `basics` - Basic queries, parsing, errors, API
//! - `match_clause` - MATCH patterns, edge traversal
//! - `where_clause` - WHERE predicates, boolean logic
//! - `return_clause` - RETURN expressions, property access
//! - `ordering` - ORDER BY, LIMIT, SKIP/OFFSET
//! - `aggregation` - COUNT, SUM, AVG, GROUP BY, HAVING
//! - `patterns` - Variable-length paths, DISTINCT
//! - `expressions` - CASE, COALESCE, type functions, EXISTS
//! - `clauses` - UNION, OPTIONAL MATCH, UNWIND, WITH
//! - `collections` - List operations, string functions, maps

mod common;

#[path = "gql/basics.rs"]
mod basics;

#[path = "gql/match_clause.rs"]
mod match_clause;

#[path = "gql/where_clause.rs"]
mod where_clause;

#[path = "gql/ordering.rs"]
mod ordering;

#[path = "gql/aggregation.rs"]
mod aggregation;

#[path = "gql/patterns.rs"]
mod patterns;

#[path = "gql/expressions.rs"]
mod expressions;

#[path = "gql/clauses.rs"]
mod clauses;

#[path = "gql/collections.rs"]
mod collections;

#[path = "gql/edge_cases.rs"]
mod edge_cases;

#[path = "gql/mutations.rs"]
mod mutations;

#[path = "gql/snapshots.rs"]
mod snapshots;

#[path = "gql/compiler_coverage.rs"]
mod compiler_coverage;

#[path = "gql/mutation_coverage.rs"]
mod mutation_coverage;

#[path = "gql/parser_coverage.rs"]
mod parser_coverage;

#[path = "gql/compiler_coverage_extended.rs"]
mod compiler_coverage_extended;

#[path = "gql/compiler_unit.rs"]
mod compiler_unit;
