//! # Traversal Explain Example
//!
//! Demonstrates the `explain()` terminal method which returns a structured
//! description of a traversal pipeline without executing it.
//!
//! Run: `cargo run -p interstellar --features gremlin --example explain`
//! With full-text search: `cargo run -p interstellar --features "gremlin,full-text" --example explain`

use interstellar::prelude::*;
use interstellar::traversal::p;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    let graph = Arc::new(Graph::new());

    // Build a small social graph
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
    let charlie = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".into(), Value::from("Charlie")),
            ("age".into(), Value::from(42i64)),
        ]),
    );
    let proj = graph.add_vertex(
        "project",
        HashMap::from([("name".into(), Value::from("Interstellar"))]),
    );

    graph
        .add_edge(alice, bob, "knows", HashMap::from([("since".into(), Value::from(2020i64))]))
        .unwrap();
    graph
        .add_edge(bob, charlie, "knows", HashMap::from([("since".into(), Value::from(2022i64))]))
        .unwrap();
    graph
        .add_edge(alice, proj, "created", Default::default())
        .unwrap();

    let snap = graph.snapshot();
    let g = snap.gremlin();

    // -------------------------------------------------------------------------
    // 1. Simple filter traversal
    // -------------------------------------------------------------------------
    println!("=== Example 1: Simple filter ===");
    println!("Query: g.V().hasLabel('person').values('name')\n");
    let explanation = g.v().has_label("person").values("name").explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 2. Navigation with predicate
    // -------------------------------------------------------------------------
    println!("=== Example 2: Navigation with predicate ===");
    println!("Query: g.V().hasLabel('person').has('age', gt(30)).out('knows').values('name')\n");
    let explanation = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .out_labels(&["knows"])
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 3. Barrier step (order)
    // -------------------------------------------------------------------------
    println!("=== Example 3: Traversal with barrier step ===");
    println!("Query: g.V().hasLabel('person').out().order().by(asc).values('name')\n");
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
    // 4. Dedup + limit
    // -------------------------------------------------------------------------
    println!("=== Example 4: Dedup + limit ===");
    println!("Query: g.V().hasLabel('person').out().dedup().limit(10).values('name')\n");
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
    // 5. Edge traversal with E()
    // -------------------------------------------------------------------------
    println!("=== Example 5: Edge traversal ===");
    println!("Query: g.E().hasLabel('knows').has('since', gte(2021)).inV().values('name')\n");
    let explanation = g
        .e()
        .has_label("knows")
        .has_where("since", p::gte(2021i64))
        .in_v()
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 6. E() with outV navigation
    // -------------------------------------------------------------------------
    println!("=== Example 6: E() with outV ===");
    println!("Query: g.E().hasLabel('knows').outV().dedup().values('name')\n");
    let explanation = g
        .e()
        .has_label("knows")
        .out_v()
        .dedup()
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 7. Where filter (traversal-based)
    // -------------------------------------------------------------------------
    println!("=== Example 7: Where filter ===");
    println!("Query: g.V().hasLabel('person').where(__.out('created')).values('name')\n");
    let explanation = g
        .v()
        .has_label("person")
        .where_(__.out_labels(&["created"]))
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 8. Repeat/until pattern
    // -------------------------------------------------------------------------
    println!("=== Example 8: Repeat/until ===");
    println!("Query: g.V().has('name','Alice').repeat(__.out('knows')).until(__.has('name','Charlie')).times(5).values('name')\n");
    let explanation = g
        .v()
        .has_value("name", Value::from("Alice"))
        .repeat(__.out_labels(&["knows"]))
        .until(__.has_value("name", Value::from("Charlie")))
        .times(5)
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 9. Repeat/times pattern
    // -------------------------------------------------------------------------
    println!("=== Example 9: Repeat/times ===");
    println!("Query: g.V().has('name','Alice').repeat(__.out('knows')).times(3).values('name')\n");
    let explanation = g
        .v()
        .has_value("name", Value::from("Alice"))
        .repeat(__.out_labels(&["knows"]))
        .times(3)
        .values("name")
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 10. Union branching
    // -------------------------------------------------------------------------
    println!("=== Example 10: Union branching ===");
    println!("Query: g.V().has('name','Alice').union(__.out('knows').values('name'), __.out('created').values('name'))\n");
    let explanation = g
        .v()
        .has_value("name", Value::from("Alice"))
        .union(vec![
            __.out_labels(&["knows"]).values("name"),
            __.out_labels(&["created"]).values("name"),
        ])
        .explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 11. Anonymous traversal (no source)
    // -------------------------------------------------------------------------
    println!("=== Example 11: Anonymous traversal ===");
    println!("Query: __.out().hasLabel('person').values('name')\n");
    let explanation = __.out().has_label("person").values("name").explain();
    println!("{explanation}");

    // -------------------------------------------------------------------------
    // 12. Explain vs Execute
    // -------------------------------------------------------------------------
    println!("=== Example 12: Explain vs Execute ===");
    println!("Query: g.V().hasLabel('person').has('age', gt(30)).values('name')\n");

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

    // -------------------------------------------------------------------------
    // 13. Explain via Gremlin string query
    // -------------------------------------------------------------------------
    println!("\n=== Example 13: Explain via Gremlin string ===");
    let query = "g.V().hasLabel('person').has('age', P.gt(30)).out('knows').values('name').explain()";
    println!("Query: {query}\n");
    let ast = interstellar::gremlin::parse(query).expect("parse failed");
    let compiled = interstellar::gremlin::compile(&ast, &g).expect("compile failed");
    let result = compiled.execute();
    match result {
        interstellar::gremlin::ExecutionResult::Explain(text) => println!("{text}"),
        other => println!("Unexpected result: {other:?}"),
    }

    // -------------------------------------------------------------------------
    // 14. Index-aware explain
    // -------------------------------------------------------------------------
    println!("=== Example 14: Index-aware explain ===");
    println!("Creating BTree index on person.age...\n");

    use interstellar::index::IndexBuilder;
    let spec = IndexBuilder::vertex()
        .label("person")
        .property("age")
        .build()
        .expect("build index spec");
    graph.create_index(spec).expect("create index");

    // Re-snapshot to pick up the index
    let snap = graph.snapshot();
    let g = interstellar::traversal::GraphTraversalSource::from_snapshot_with_graph(
        &snap,
        Arc::clone(&graph),
    );

    println!("Query: g.V().hasLabel('person').has('age', gt(30)).out('knows').values('name')\n");
    let explanation = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .out_labels(&["knows"])
        .values("name")
        .explain();
    println!("{explanation}");
    println!("Note: The 'has' step on 'age' now shows an index hint!");

    // -------------------------------------------------------------------------
    // 15. Full-text search explain
    // -------------------------------------------------------------------------
    #[cfg(feature = "full-text")]
    {
        use interstellar::storage::text::TextIndexConfig;

        println!("\n=== Example 15: Full-text search explain ===");
        println!("Creating text index on article.body...\n");

        graph
            .create_text_index_v("body", TextIndexConfig::default())
            .expect("create text index");

        // Add some articles
        graph.add_vertex(
            "article",
            HashMap::from([
                ("title".into(), Value::from("Intro to Raft")),
                (
                    "body".into(),
                    Value::from("Raft is a consensus algorithm for replicated logs"),
                ),
            ]),
        );
        graph.add_vertex(
            "article",
            HashMap::from([
                ("title".into(), Value::from("Paxos Made Simple")),
                (
                    "body".into(),
                    Value::from("Paxos is a family of protocols for consensus"),
                ),
            ]),
        );

        // Re-snapshot after adding data
        let snap = graph.snapshot();
        let g = interstellar::traversal::GraphTraversalSource::from_snapshot_with_graph(
            &snap,
            Arc::clone(&graph),
        );

        println!("Query: g.searchText('body', 'consensus', 5).hasLabel('article').values('title')\n");
        let explanation = g
            .search_text("body", "consensus", 5)
            .expect("search_text")
            .has_label("article")
            .values("title")
            .explain();
        println!("{explanation}");

        println!("Query: g.V().hasLabel('article').has('body').values('title')\n");
        let explanation = g
            .v()
            .has_label("article")
            .has("body")
            .values("title")
            .explain();
        println!("{explanation}");
    }
}
