//! # Interstellar Full-Text Search Quickstart
//!
//! A minimal introduction to Interstellar's Tantivy-backed full-text search.
//!
//! This example demonstrates:
//! - Registering a text index on a vertex property
//! - Indexing documents (tokens are extracted on insert / update / remove)
//! - Running BM25-ranked free-text queries
//! - Running a structured phrase query via `TextQuery`
//! - Chaining `search_text` with regular Gremlin steps
//!
//! Run: `cargo run --example quickstart_text_search --features full-text`

use std::collections::HashMap;
use std::sync::Arc;

use interstellar::gremlin::ExecutionResult;
use interstellar::storage::text::{ElementRef, TextIndexConfig, TextQuery};
use interstellar::storage::Graph;
use interstellar::value::Value;

fn main() {
    println!("=== Interstellar Full-Text Search Quickstart ===\n");

    // -------------------------------------------------------------------------
    // 1. Create an in-memory graph and a Gremlin traversal source
    // -------------------------------------------------------------------------
    let graph = Arc::new(Graph::new());
    let g = graph.gremlin(Arc::clone(&graph));

    // -------------------------------------------------------------------------
    // 2. Register a text index on the `body` property
    //
    // The default `TextIndexConfig` uses a standard analyzer (lowercase +
    // unicode tokenizer) and stores positions so phrase queries work.
    //
    // Indexes can be created BEFORE or AFTER vertices exist. If created
    // afterwards, the existing string values are back-filled automatically.
    // -------------------------------------------------------------------------
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .expect("text index creation failed");
    println!("Registered text index on `body`");

    // -------------------------------------------------------------------------
    // 3. Insert some documents
    //
    // The mutation hooks in `Graph::add_vertex` automatically upsert the
    // `body` field into the text index.
    // -------------------------------------------------------------------------
    let _intro = g
        .add_v("article")
        .property("title", "Intro to Raft")
        .property("body", "raft is a consensus algorithm for replicated logs")
        .next()
        .unwrap();

    let _paxos = g
        .add_v("article")
        .property("title", "Paxos Made Simple")
        .property("body", "paxos is the classical consensus protocol")
        .next()
        .unwrap();

    let _gossip = g
        .add_v("article")
        .property("title", "Epidemic Broadcast")
        .property("body", "gossip protocols disseminate state across a cluster")
        .next()
        .unwrap();

    let _note = g
        .add_v("note")
        .property("body", "raft and paxos are both consensus protocols")
        .next()
        .unwrap();

    println!("Indexed 4 documents (3 articles, 1 note)\n");

    // -------------------------------------------------------------------------
    // 4. Free-text search: top-k by BM25 relevance
    //
    // `search_text(property, query, k)` returns a traversal seeded with the
    // top-k matching vertices, sorted by descending BM25 score.
    // -------------------------------------------------------------------------
    println!("-- Free-text search for \"consensus\" (top 5) --\n");
    let hits = g
        .search_text("body", "consensus", 5)
        .expect("search failed")
        .values("title")
        .to_value_list();
    for (rank, title) in hits.iter().enumerate() {
        if let Value::String(s) = title {
            println!("  {}. {}", rank + 1, s);
        }
    }

    // -------------------------------------------------------------------------
    // 5. Chain search results with regular Gremlin steps
    //
    // `search_text` is just another source step; it composes with `has_label`,
    // `out`, `where_`, etc. Here we restrict to articles only.
    // -------------------------------------------------------------------------
    println!("\n-- \"consensus\" restricted to label=article --\n");
    let titles = g
        .search_text("body", "consensus", 10)
        .expect("search failed")
        .has_label("article")
        .values("title")
        .to_value_list();
    for title in &titles {
        if let Value::String(s) = title {
            println!("  - {}", s);
        }
    }

    // -------------------------------------------------------------------------
    // 6. Structured queries via `TextQuery`
    //
    // Use `search_text_query` for phrase, prefix, boolean, and fuzzy queries.
    // -------------------------------------------------------------------------
    println!("\n-- Phrase query: \"replicated logs\" --\n");
    let phrase = TextQuery::Phrase {
        text: "replicated logs".to_string(),
        slop: 0,
    };
    let phrase_hits = g
        .search_text_query("body", &phrase, 10)
        .expect("search failed")
        .values("title")
        .to_value_list();
    for title in &phrase_hits {
        if let Value::String(s) = title {
            println!("  - {}", s);
        }
    }

    // -------------------------------------------------------------------------
    // 7. Updates flow through the index automatically
    //
    // `set_vertex_property` replaces the indexed text. Tokens that no longer
    // appear in the new value stop matching; new tokens become searchable.
    // -------------------------------------------------------------------------
    println!("\n-- Updating a document --\n");
    let gossip_id = g
        .v()
        .has_value("title", Value::from("Epidemic Broadcast"))
        .next()
        .unwrap()
        .id();
    graph
        .set_vertex_property(
            gossip_id,
            "body",
            Value::String("gossip implements eventual consistency for state".into()),
        )
        .unwrap();

    let consistency = g
        .search_text("body", "consistency", 5)
        .expect("search failed")
        .values("title")
        .to_value_list();
    println!("After update, search for \"consistency\":");
    for title in &consistency {
        if let Value::String(s) = title {
            println!("  - {}", s);
        }
    }

    // -------------------------------------------------------------------------
    // 8. Full-text search on EDGES
    //
    // Edges with text payloads (comments, endorsements, annotations) work
    // exactly like vertices. Use the `_e` suffix throughout:
    //   - `create_text_index_e` to register
    //   - `search_text_e` / `search_text_query_e` as source steps
    //   - `text_index_e` for direct programmatic access
    //
    // Property names are GLOBALLY unique across vertex and edge indexes, so
    // we register `note` (not `body`, which is taken by the vertex index).
    // -------------------------------------------------------------------------
    println!("\n-- Edge full-text search --\n");
    graph
        .create_text_index_e("note", TextIndexConfig::default())
        .expect("edge text index creation failed");

    // Two anchor vertices to attach edges to.
    let alice = g
        .add_v("user")
        .property("name", "Alice")
        .next()
        .unwrap()
        .id();
    let bob = g
        .add_v("user")
        .property("name", "Bob")
        .next()
        .unwrap()
        .id();

    // Several `endorses` edges with body text.
    let mk_props = |body: &str| {
        let mut p = HashMap::new();
        p.insert("note".to_string(), Value::String(body.to_string()));
        p
    };
    graph
        .add_edge(
            alice,
            bob,
            "endorses",
            mk_props("raft is the cleanest consensus algorithm"),
        )
        .unwrap();
    graph
        .add_edge(
            alice,
            bob,
            "endorses",
            mk_props("paxos is harder to implement than raft"),
        )
        .unwrap();
    graph
        .add_edge(
            bob,
            alice,
            "endorses",
            mk_props("gossip protocols scale beautifully"),
        )
        .unwrap();

    // Free-text edge search.
    println!("Free-text edge search for \"raft\":");
    let raft_edges = g
        .search_text_e("note", "raft", 10)
        .expect("edge search failed")
        .values("note")
        .to_value_list();
    for (rank, note) in raft_edges.iter().enumerate() {
        if let Value::String(s) = note {
            println!("  {}. {}", rank + 1, s);
        }
    }

    // Phrase query on edges.
    println!("\nPhrase query on edges: \"consensus algorithm\"");
    let phrase_q = TextQuery::Phrase {
        text: "consensus algorithm".to_string(),
        slop: 0,
    };
    let phrase_edges = g
        .search_text_query_e("note", &phrase_q, 10)
        .expect("edge phrase search failed")
        .values("note")
        .to_value_list();
    for note in &phrase_edges {
        if let Value::String(s) = note {
            println!("  - {}", s);
        }
    }

    // Programmatic access: inspect raw scores via `text_index_e`.
    //
    // The handle returned by `text_index_e` exposes `search`, which yields
    // `TextHit { element, score }`. For edge indexes, `element` is always
    // `ElementRef::Edge(EdgeId)`.
    println!("\nRaw BM25 scores for \"raft\" on edges:");
    let edge_index = graph.text_index_e("note").expect("edge index missing");
    let raw_hits = edge_index
        .search(&TextQuery::Match("raft".to_string()), 10)
        .expect("raw edge search failed");
    for hit in &raw_hits {
        if let ElementRef::Edge(eid) = hit.element {
            println!("  edge {:?}: score = {:.4}", eid, hit.score);
        }
    }

    // -------------------------------------------------------------------------
    // 6. Query-language surfaces (spec-55c): Gremlin and GQL
    //
    // The same FTS engine is reachable from both query languages. The Rust
    // API stays the most expressive (compound And/Or/Not is Gremlin or
    // Rust-only), but the language surfaces cover the common cases.
    // -------------------------------------------------------------------------
    println!("\n=== Query languages ===");

    // -- Gremlin: bare-string sugars to TextQ.match --------------------------
    println!("\nGremlin g.searchTextV('body', 'raft', 5).values('title'):");
    let titles = graph
        .execute_script("g.searchTextV('body', 'raft', 5).values('title')")
        .expect("gremlin failed");
    if let ExecutionResult::List(values) = &titles.result {
        for v in values {
            if let Value::String(s) = v {
                println!("  - {s}");
            }
        }
    }

    // -- Gremlin: structured TextQ.phrase + textScore() ----------------------
    println!("\nGremlin g.searchTextV(... TextQ.phrase('consensus algorithm') ...).textScore():");
    let scores = graph
        .execute_script(
            "g.searchTextV('body', TextQ.phrase('consensus algorithm'), 5).textScore()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(values) = &scores.result {
        for (rank, v) in values.iter().enumerate() {
            if let Value::Float(s) = v {
                println!("  {}. score = {:.4}", rank + 1, s);
            }
        }
    }

    // -- GQL: CALL procedure with YIELD elemId, score ------------------------
    println!("\nGQL CALL interstellar.searchTextV('body', 'raft', 5):");
    let rows = graph
        .gql(
            "MATCH (anchor) WHERE id(anchor) = 0 \
             CALL interstellar.searchTextV('body', 'raft', 5) \
             YIELD elemId, score RETURN elemId, score",
        )
        .expect("gql failed");
    for row in &rows {
        if let Value::Map(m) = row {
            let id = m.get("elemId");
            let score = m.get("score");
            println!("  elemId={id:?}, score={score:?}");
        }
    }

    // -- GQL: edge-side prefix expansion -------------------------------------
    println!("\nGQL CALL interstellar.searchTextPrefixE('note', 'consen', 5):");
    let edge_rows = graph
        .gql(
            "MATCH (anchor) WHERE id(anchor) = 0 \
             CALL interstellar.searchTextPrefixE('note', 'consen', 5) \
             YIELD elemId, score RETURN elemId, score",
        )
        .expect("gql failed");
    for row in &edge_rows {
        if let Value::Map(m) = row {
            println!("  {:?}", m);
        }
    }

    println!("\n=== Quickstart Complete ===");
}
