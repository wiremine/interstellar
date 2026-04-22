//! GQL CALL procedure integration tests for spec-55c full-text search.
//!
//! Covers the eight `interstellar.searchText{,All,Phrase,Prefix}{V,E}`
//! procedures end-to-end against a live `Graph` via the graph-bound
//! `Graph::gql` entry point.
//!
//! YIELD aliases under test:
//! - `elem`   → fully materialized vertex/edge property map
//! - `elemId` → bare `Value::Vertex(VertexId)` / `Value::Edge(EdgeId)`
//! - `score`  → BM25 score as `Value::Float` (descending order)
//!
//! Compound queries (`TextQ.and/or/not`) are intentionally NOT exposed
//! through GQL (spec-55c §D4) — those are Gremlin / Rust-API only.

use interstellar::storage::text::TextIndexConfig;
use interstellar::storage::Graph;
use interstellar::value::{EdgeId, Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// Build a small graph with a vertex text index on `body` and three articles
/// with predictable token content.
fn vertex_corpus() -> (Arc<Graph>, Vec<VertexId>) {
    let g = Arc::new(Graph::new());
    g.create_text_index_v("body", TextIndexConfig::default())
        .expect("vertex index creation");

    let mut ids = Vec::new();
    let docs = [
        ("doc1", "raft consensus algorithm"),
        ("doc2", "raft is a distributed consensus protocol"),
        ("doc3", "the quick brown fox jumps over the lazy dog"),
    ];
    for (title, body) in docs {
        let mut props = HashMap::new();
        props.insert("title".to_string(), Value::String(title.to_string()));
        props.insert("body".to_string(), Value::String(body.to_string()));
        ids.push(g.add_vertex("article", props));
    }
    (g, ids)
}

/// Build a small graph with an edge text index on `note` and three edges.
fn edge_corpus() -> (Arc<Graph>, Vec<EdgeId>) {
    let g = Arc::new(Graph::new());
    g.create_text_index_e("note", TextIndexConfig::default())
        .expect("edge index creation");

    let v: Vec<VertexId> = (0..3)
        .map(|_| g.add_vertex("user", HashMap::new()))
        .collect();

    let mut eids = Vec::new();
    let notes = [
        "alice sent a friendly hello",
        "bob ignored the greeting",
        "carol responded with a hello back",
    ];
    for (i, note) in notes.iter().enumerate() {
        let mut props = HashMap::new();
        props.insert("note".to_string(), Value::String((*note).to_string()));
        eids.push(
            g.add_edge(v[i % 3], v[(i + 1) % 3], "messaged", props)
                .unwrap(),
        );
    }
    (g, eids)
}

/// Single-row helper: extract the inner Value::Map from a result row.
fn row_map(v: &Value) -> &indexmap::IndexMap<String, Value> {
    match v {
        Value::Map(m) => m,
        other => panic!("expected Value::Map, got {other:?}"),
    }
}

fn run(g: &Arc<Graph>, q: &str) -> Vec<Value> {
    g.gql(q).unwrap_or_else(|e| panic!("gql failed: {e:?}\nquery:\n{q}"))
}

/// Build a CALL query that anchors against a single row to avoid the
/// CALL-per-outer-row cartesian product. We pin the outer MATCH to one
/// vertex by id so the CALL fires exactly once.
fn fts_query(anchor_id: u64, body: &str) -> String {
    format!(
        "MATCH (anchor) WHERE id(anchor) = {anchor_id} {body}"
    )
}

// ---------------------------------------------------------------------------
// vertex procedures: happy paths
// ---------------------------------------------------------------------------

#[test]
fn search_text_v_returns_scored_vertex_ids() {
    let (g, ids) = vertex_corpus();
    let rows = run(
        &g,
        &fts_query(
            ids[0].0,
            "CALL interstellar.searchTextV('body', 'raft', 5) YIELD elemId, score \
             RETURN elemId, score",
        ),
    );
    assert!(!rows.is_empty(), "expected at least one match for 'raft'");
    assert!(rows.len() >= 2);
    let mut last_score = f64::INFINITY;
    for row in &rows {
        let m = row_map(row);
        assert!(matches!(m.get("elemId"), Some(Value::Vertex(_))));
        let s = match m.get("score") {
            Some(Value::Float(f)) => *f,
            other => panic!("expected score float, got {other:?}"),
        };
        assert!(s <= last_score, "scores not descending: {s} > {last_score}");
        last_score = s;
    }
}

#[test]
fn search_text_v_yield_elem_materializes_full_record() {
    let (g, ids) = vertex_corpus();
    let rows = run(
        &g,
        &fts_query(
            ids[0].0,
            "CALL interstellar.searchTextV('body', 'raft', 5) YIELD elem RETURN elem",
        ),
    );
    assert!(!rows.is_empty());
    // RETURN with a single Value::Map column flattens the map into the
    // result row's fields. So we expect the row to contain `title` and
    // `body` directly (the property keys of the materialized vertex).
    let m = row_map(&rows[0]);
    assert!(
        m.contains_key("title"),
        "expected materialized vertex to include 'title', got keys {:?}",
        m.keys().collect::<Vec<_>>()
    );
    assert!(m.contains_key("body"));
}

#[test]
fn search_text_all_v_requires_all_terms() {
    let (g, ids) = vertex_corpus();
    let rows = run(
        &g,
        &fts_query(
            ids[0].0,
            "CALL interstellar.searchTextAllV('body', 'raft consensus', 10) \
             YIELD elemId RETURN elemId",
        ),
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn search_text_phrase_v_orders_terms() {
    let (g, ids) = vertex_corpus();
    let rows = run(
        &g,
        &fts_query(
            ids[0].0,
            "CALL interstellar.searchTextPhraseV('body', 'quick brown fox', 10) \
             YIELD elemId RETURN elemId",
        ),
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn search_text_prefix_v_expands_term() {
    let (g, ids) = vertex_corpus();
    let rows = run(
        &g,
        &fts_query(
            ids[0].0,
            "CALL interstellar.searchTextPrefixV('body', 'consen', 10) \
             YIELD elemId RETURN elemId",
        ),
    );
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// edge procedures
// ---------------------------------------------------------------------------

#[test]
fn search_text_e_returns_scored_edge_ids() {
    let (g, _eids) = edge_corpus();
    // Anchor on any vertex (vertex 0 was created by edge_corpus).
    let rows = run(
        &g,
        &fts_query(
            0,
            "CALL interstellar.searchTextE('note', 'hello', 5) \
             YIELD elemId, score RETURN elemId, score",
        ),
    );
    assert_eq!(rows.len(), 2, "two edges mention 'hello'");
    for row in &rows {
        let m = row_map(row);
        assert!(matches!(m.get("elemId"), Some(Value::Edge(_))));
        assert!(matches!(m.get("score"), Some(Value::Float(_))));
    }
}

#[test]
fn search_text_phrase_e_strict_order() {
    let (g, _eids) = edge_corpus();
    let rows = run(
        &g,
        &fts_query(
            0,
            "CALL interstellar.searchTextPhraseE('note', 'hello back', 5) \
             YIELD elemId RETURN elemId",
        ),
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn search_text_prefix_e_expands_term() {
    let (g, _eids) = edge_corpus();
    let rows = run(
        &g,
        &fts_query(
            0,
            "CALL interstellar.searchTextPrefixE('note', 'gree', 5) \
             YIELD elemId RETURN elemId",
        ),
    );
    assert_eq!(rows.len(), 1, "expected 'greeting' from doc2");
}

#[test]
fn search_text_all_e_intersection() {
    let (g, _eids) = edge_corpus();
    let rows = run(
        &g,
        &fts_query(
            0,
            "CALL interstellar.searchTextAllE('note', 'hello back', 5) \
             YIELD elemId RETURN elemId",
        ),
    );
    assert_eq!(rows.len(), 1);
}

// ---------------------------------------------------------------------------
// error branches
// ---------------------------------------------------------------------------

#[test]
fn empty_property_is_rejected() {
    let (g, ids) = vertex_corpus();
    let err = g
        .gql(&fts_query(
            ids[0].0,
            "CALL interstellar.searchTextV('', 'raft', 5) YIELD elemId RETURN elemId",
        ))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("property"),
        "expected error mentioning property, got: {msg}"
    );
}

#[test]
fn k_zero_is_rejected() {
    let (g, ids) = vertex_corpus();
    let err = g
        .gql(&fts_query(
            ids[0].0,
            "CALL interstellar.searchTextV('body', 'raft', 0) YIELD elemId RETURN elemId",
        ))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("k"), "expected error mentioning k, got: {msg}");
}

#[test]
fn negative_k_is_rejected() {
    let (g, ids) = vertex_corpus();
    let err = g
        .gql(&fts_query(
            ids[0].0,
            "CALL interstellar.searchTextV('body', 'raft', -1) YIELD elemId RETURN elemId",
        ))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("k"), "expected error mentioning k, got: {msg}");
}

#[test]
fn unknown_property_is_rejected() {
    let (g, ids) = vertex_corpus();
    let err = g
        .gql(&fts_query(
            ids[0].0,
            "CALL interstellar.searchTextV('nonexistent', 'raft', 5) YIELD elemId RETURN elemId",
        ))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("nonexistent") || msg.contains("no vertex text index"),
        "expected error mentioning missing index, got: {msg}"
    );
}

#[test]
fn vertex_proc_against_edge_index_fails() {
    let (g, _) = edge_corpus();
    let err = g
        .gql(&fts_query(
            0,
            "CALL interstellar.searchTextV('note', 'hello', 5) YIELD elemId RETURN elemId",
        ))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("no vertex text index"),
        "expected vertex-index lookup failure, got: {msg}"
    );
}

#[test]
fn edge_proc_against_vertex_index_fails() {
    let (g, ids) = vertex_corpus();
    let err = g
        .gql(&fts_query(
            ids[0].0,
            "CALL interstellar.searchTextE('body', 'raft', 5) YIELD elemId RETURN elemId",
        ))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("no edge text index"),
        "expected edge-index lookup failure, got: {msg}"
    );
}

#[test]
fn missing_graph_handle_returns_actionable_error() {
    use interstellar::gql::{compile, parse};
    let (g, ids) = vertex_corpus();
    let snap = g.snapshot();
    let q = parse(&fts_query(
        ids[0].0,
        "CALL interstellar.searchTextV('body', 'raft', 5) YIELD elemId RETURN elemId",
    ))
    .unwrap();
    let err = compile(&q, &snap).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("graph-bound") || msg.contains("Graph::gql"),
        "expected graph-handle error, got: {msg}"
    );
}
