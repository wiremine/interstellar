//! Integration tests for Rhai scripting.
//!
//! These tests verify the Rhai scripting integration works correctly
//! with realistic graph scenarios.

mod anonymous;
mod errors;
mod predicates;
mod traversal;
mod types;

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use interstellar::storage::InMemoryGraph;
use std::collections::HashMap;

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
pub fn create_social_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // People
    let alice = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let bob = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let carol = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Carol".to_string())),
            ("age".to_string(), Value::Int(35)),
            ("active".to_string(), Value::Bool(false)),
        ]),
    );

    let dave = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Dave".to_string())),
            ("age".to_string(), Value::Int(40)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let eve = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Eve".to_string())),
            ("age".to_string(), Value::Int(28)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    // Company
    let acme = storage.add_vertex(
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
    storage
        .add_edge(
            alice,
            bob,
            "knows",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    storage
        .add_edge(
            alice,
            carol,
            "knows",
            HashMap::from([("since".to_string(), Value::Int(2018))]),
        )
        .unwrap();

    storage
        .add_edge(bob, carol, "knows", HashMap::new())
        .unwrap();

    storage
        .add_edge(dave, eve, "knows", HashMap::new())
        .unwrap();

    storage
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();

    Graph::new(storage)
}

/// Create an empty graph for testing edge cases.
pub fn create_empty_graph() -> Graph {
    Graph::in_memory()
}

/// Create a simple chain graph for path testing.
///
/// Graph structure:
/// ```text
/// A --> B --> C --> D --> E
/// ```
pub fn create_chain_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let a = storage.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("A".to_string()))]),
    );
    let b = storage.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("B".to_string()))]),
    );
    let c = storage.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("C".to_string()))]),
    );
    let d = storage.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("D".to_string()))]),
    );
    let e = storage.add_vertex(
        "node",
        HashMap::from([("name".to_string(), Value::String("E".to_string()))]),
    );

    storage.add_edge(a, b, "next", HashMap::new()).unwrap();
    storage.add_edge(b, c, "next", HashMap::new()).unwrap();
    storage.add_edge(c, d, "next", HashMap::new()).unwrap();
    storage.add_edge(d, e, "next", HashMap::new()).unwrap();

    Graph::new(storage)
}
