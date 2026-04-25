//! # Traversal Explain Example
//!
//! Demonstrates the `explain()` terminal method which returns a structured
//! description of a traversal pipeline without executing it.
//!
//! Run: `cargo run --example explain`

use interstellar::prelude::*;
use interstellar::traversal::p;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    let graph = Arc::new(Graph::new());

    // Add some data so the graph isn't empty
    let alice = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".into(), Value::from("Alice")),
            ("age".into(), Value::from(35i64)),
        ]),
    );
    let bob = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".into(), Value::from("Bob")),
            ("age".into(), Value::from(28i64)),
        ]),
    );
    graph
        .add_edge(
            alice,
            bob,
            "knows",
            HashMap::from([("since".into(), Value::from(2020i64))]),
        )
        .unwrap();

    let snap = graph.snapshot();
    let g = snap.gremlin();

    // -------------------------------------------------------------------------
    // 1. Simple filter traversal
    // -------------------------------------------------------------------------
    println!("--- Example 1: Simple filter ---\n");
    let explanation = g.v().has_label("person").values("name").explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 2. Navigation with predicate
    // -------------------------------------------------------------------------
    println!("--- Example 2: Navigation with predicate ---\n");
    let explanation = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .out_labels(&["knows"])
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 3. Aggregation (contains barrier)
    // -------------------------------------------------------------------------
    println!("--- Example 3: Traversal with barrier step ---\n");
    let explanation = g
        .v()
        .has_label("person")
        .out()
        .order()
        .by_value_asc()
        .build()
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 4. Complex traversal with limit
    // -------------------------------------------------------------------------
    println!("--- Example 4: Traversal with limit ---\n");
    let explanation = g
        .v()
        .has_label("person")
        .out()
        .dedup()
        .limit(10)
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 5. Anonymous traversal (no source)
    // -------------------------------------------------------------------------
    println!("--- Example 5: Anonymous traversal ---\n");
    let explanation = __.out().has_label("person").values("name").explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 6. Compare explain with actual execution
    // -------------------------------------------------------------------------
    println!("--- Example 6: Explain vs Execute ---\n");
    let traversal_desc = "g.V().hasLabel('person').has('age', gt(30)).values('name')";
    println!("Query: {traversal_desc}\n");

    let explanation = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .values("name")
        .explain();
    println!("{explanation}");

    let results = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .values("name")
        .to_list();
    println!("Results: {results:?}");
}
