//! Integration tests for Rhai scripting.
//!
//! These tests verify the Rhai scripting integration works correctly
//! with realistic graph scenarios.

#![allow(unused_variables)]
#![allow(unused_imports)]
mod anonymous;
mod errors;
mod predicates;
#[cfg(feature = "mmap")]
mod storage_backends;
mod traversal;
mod types;

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use std::collections::HashMap;
use std::sync::Arc;

/// Create a social graph for testing.
///
/// Graph structure:
/// ```text
///   Alice(30) --knows--> Bob(25)
///      |                   |
///      +---knows--> Carol(35) <--knows--+
///      |
///      +---works_at--> Acme(company)
///
///   Dave(40) --knows--> Eve(28)
/// ```
pub fn create_social_graph() -> Arc<Graph> {
    let graph = Graph::new();

    // People
    let alice = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let bob = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let carol = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Carol".to_string())),
            ("age".to_string(), Value::Int(35)),
            ("active".to_string(), Value::Bool(false)),
        ]),
    );

    let dave = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Dave".to_string())),
            ("age".to_string(), Value::Int(40)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let eve = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Eve".to_string())),
            ("age".to_string(), Value::Int(28)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    // Company
    let acme = graph.add_vertex(
        "company",
        HashMap::from([
            ("name".to_string(), Value::String("Acme Corp".to_string())),
            (
                "industry".to_string(),
                Value::String("Technology".to_string()),
            ),
        ]),
    );

    // Edges
    graph
        .add_edge(
            alice,
            bob,
            "knows",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    graph
        .add_edge(
            alice,
            carol,
            "knows",
            HashMap::from([("since".to_string(), Value::Int(2018))]),
        )
        .unwrap();

    graph.add_edge(bob, carol, "knows", HashMap::new()).unwrap();

    graph.add_edge(dave, eve, "knows", HashMap::new()).unwrap();

    graph
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();

    Arc::new(graph)
}

/// Create an empty graph for testing edge cases.
pub fn create_empty_graph() -> Arc<Graph> {
    Arc::new(Graph::new())
}

/// Create a simple chain graph for path testing.
///
/// Graph structure:
/// ```text
/// A --> B --> C --> D --> E
/// ```
pub fn create_chain_graph() -> Arc<Graph> {
    let graph = Graph::new();

    let a = graph.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("A".to_string()))]),
    );
    let b = graph.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("B".to_string()))]),
    );
    let c = graph.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("C".to_string()))]),
    );
    let d = graph.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("D".to_string()))]),
    );
    let e = graph.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("E".to_string()))]),
    );

    graph.add_edge(a, b, "next", HashMap::new()).unwrap();
    graph.add_edge(b, c, "next", HashMap::new()).unwrap();
    graph.add_edge(c, d, "next", HashMap::new()).unwrap();
    graph.add_edge(d, e, "next", HashMap::new()).unwrap();

    Arc::new(graph)
}
