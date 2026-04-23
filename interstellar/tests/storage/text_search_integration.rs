//! Integration tests for full-text search (Phase 2 of spec-55, edges from
//! spec-55b).
//!
//! Covers:
//!   - vertex side: `Graph::create_text_index_v` lifecycle, mutation hooks
//!     (`add_vertex` / `set_vertex_property` / `remove_vertex`), and the
//!     `search_text` / `search_text_query` traversal source steps including
//!     BM25 score propagation via the traverser sack;
//!   - edge side: identical surface area mirrored through `_e` helpers
//!     (`create_text_index_e`, `search_text_e`, `search_text_query_e`,
//!     `add_edge` / `set_edge_property` / `remove_edge` mutation hooks);
//!   - cross-element invariants: globally-unique property-name namespace;
//!     vertex and edge indexes coexist without bleed-through.

use std::collections::HashMap;
use std::sync::Arc;

use interstellar::storage::text::{TextIndexConfig, TextIndexError, TextQuery};
use interstellar::storage::Graph;
use interstellar::value::{EdgeId, Value, VertexId};

// =============================================================================
// Helpers
// =============================================================================

fn graph_with_body_index() -> Arc<Graph> {
    let graph = Arc::new(Graph::new());
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();
    graph
}

fn add_doc(graph: &Graph, label: &str, body: &str) -> VertexId {
    let mut props = HashMap::new();
    props.insert("body".to_string(), Value::String(body.to_string()));
    graph.add_vertex(label, props)
}

// =============================================================================
// Index lifecycle
// =============================================================================

#[test]
fn create_then_drop_text_index_round_trips() {
    let graph = Graph::new();
    assert_eq!(graph.text_index_count_v(), 0);
    assert!(!graph.has_text_index_v("body"));

    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();
    assert!(graph.has_text_index_v("body"));
    assert_eq!(graph.text_index_count_v(), 1);
    assert_eq!(graph.list_text_indexes_v(), vec!["body".to_string()]);

    graph.drop_text_index_v("body").unwrap();
    assert!(!graph.has_text_index_v("body"));
    assert_eq!(graph.text_index_count_v(), 0);
}

#[test]
fn create_text_index_rejects_duplicate_property() {
    let graph = Graph::new();
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();
    let err = graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}

#[test]
fn drop_text_index_returns_error_for_unknown_property() {
    let graph = Graph::new();
    let err = graph.drop_text_index_v("missing").unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}

#[test]
fn create_text_index_backfills_existing_string_values() {
    let graph = Arc::new(Graph::new());

    add_doc(&graph, "doc", "the raft consensus protocol");
    add_doc(&graph, "doc", "paxos consensus");
    add_doc(&graph, "doc", "completely unrelated content about cats");

    // Index created AFTER vertices exist must back-fill them.
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text("body", "consensus", 10)
        .unwrap()
        .to_value_list();
    assert_eq!(hits.len(), 2);
}

// =============================================================================
// Mutation hooks
// =============================================================================

#[test]
fn add_vertex_indexes_string_body_property() {
    let graph = graph_with_body_index();
    let v = add_doc(&graph, "doc", "raft consensus algorithm");

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g.search_text("body", "raft", 10).unwrap().to_value_list();
    assert_eq!(hits, vec![Value::Vertex(v)]);
}

#[test]
fn set_vertex_property_updates_text_index() {
    let graph = graph_with_body_index();
    let v = add_doc(&graph, "doc", "original text about apples");

    let g = graph.gremlin(Arc::clone(&graph));
    assert_eq!(
        g.search_text("body", "apples", 10)
            .unwrap()
            .to_value_list()
            .len(),
        1
    );

    // Replace property; old token "apples" must disappear, "bananas" appears.
    graph
        .set_vertex_property(v, "body", Value::String("now about bananas".into()))
        .unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text("body", "apples", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
    assert_eq!(
        g.search_text("body", "bananas", 10)
            .unwrap()
            .to_value_list()
            .len(),
        1
    );
}

#[test]
fn set_vertex_property_to_non_string_removes_from_index() {
    let graph = graph_with_body_index();
    let v = add_doc(&graph, "doc", "indexable text");

    // Overwrite the body property with a non-string value; the indexed
    // tokens must be removed so the document no longer matches.
    graph
        .set_vertex_property(v, "body", Value::Int(42))
        .unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text("body", "indexable", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
}

#[test]
fn remove_vertex_removes_from_text_index() {
    let graph = graph_with_body_index();
    let v = add_doc(&graph, "doc", "ephemeral content");

    graph.remove_vertex(v).unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text("body", "ephemeral", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
}

// =============================================================================
// Traversal source: search_text / search_text_query
// =============================================================================

#[test]
fn search_text_returns_top_k_in_score_order() {
    let graph = graph_with_body_index();
    add_doc(&graph, "doc", "raft raft raft consensus consensus");
    add_doc(&graph, "doc", "raft consensus");
    add_doc(&graph, "doc", "consensus only");
    add_doc(&graph, "doc", "totally unrelated content");

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text("body", "raft consensus", 10)
        .unwrap()
        .to_value_list();
    assert!(hits.len() >= 2, "expected at least two matches, got {hits:?}");

    // Top-k must respect k.
    let g = graph.gremlin(Arc::clone(&graph));
    let top1 = g
        .search_text("body", "raft consensus", 1)
        .unwrap()
        .to_value_list();
    assert_eq!(top1.len(), 1);
}

#[test]
fn search_text_query_supports_phrase_query() {
    let graph = graph_with_body_index();
    let phrase_match = add_doc(&graph, "doc", "the quick brown fox jumps");
    let _scattered = add_doc(&graph, "doc", "brown sugar and quick recipes for fox tacos");

    let g = graph.gremlin(Arc::clone(&graph));
    let q = TextQuery::Phrase {
        text: "quick brown fox".to_string(),
        slop: 0,
    };
    let hits = g
        .search_text_query("body", &q, 10)
        .unwrap()
        .to_value_list();
    assert_eq!(hits, vec![Value::Vertex(phrase_match)]);
}

#[test]
fn search_text_returns_error_for_unknown_property() {
    let graph = Arc::new(Graph::new());
    let g = graph.gremlin(Arc::clone(&graph));
    match g.search_text("missing", "anything", 10) {
        Err(TextIndexError::Storage(_)) => {}
        Err(other) => panic!("unexpected error variant: {other:?}"),
        Ok(_) => panic!("expected error for missing index"),
    }
}

#[test]
fn search_text_with_zero_k_returns_no_hits() {
    let graph = graph_with_body_index();
    add_doc(&graph, "doc", "anything");
    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text("body", "anything", 0)
        .unwrap()
        .to_value_list();
    assert!(hits.is_empty());
}

#[test]
fn search_text_chains_with_filter_steps() {
    let graph = graph_with_body_index();
    let _doc = add_doc(&graph, "doc", "raft consensus");
    let _note = add_doc(&graph, "note", "raft consensus");

    let g = graph.gremlin(Arc::clone(&graph));
    let docs_only = g
        .search_text("body", "raft", 10)
        .unwrap()
        .has_label("doc")
        .to_value_list();
    assert_eq!(docs_only.len(), 1);
}

// =============================================================================
// Score propagation via traverser sack
//
// The `VerticesWithTextScore` source variant attaches each hit's BM25 score
// to the traverser's sack at construction time (verified directly in the
// source-step match arm). At the user-visible API level we observe this
// indirectly via score ordering: Tantivy returns hits sorted by descending
// relevance, so the order of `to_value_list()` output reflects the scores
// that were stamped into each traverser's sack.
// =============================================================================

#[test]
fn search_text_preserves_descending_score_order() {
    let graph = graph_with_body_index();
    let strong = add_doc(&graph, "doc", "raft raft raft consensus consensus consensus");
    let weak = add_doc(&graph, "doc", "raft");

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text("body", "raft consensus", 10)
        .unwrap()
        .to_value_list();

    // Stronger BM25 match must come first.
    assert_eq!(hits.first(), Some(&Value::Vertex(strong)));
    assert!(hits.contains(&Value::Vertex(weak)));
}

// =============================================================================
// Edge-side helpers
// =============================================================================

/// Build a graph with two anchor vertices (so we have somewhere to attach
/// edges) and an edge text index registered on the `body` property.
fn graph_with_edge_body_index() -> (Arc<Graph>, VertexId, VertexId) {
    let graph = Arc::new(Graph::new());
    graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap();
    let a = graph.add_vertex("anchor", HashMap::new());
    let b = graph.add_vertex("anchor", HashMap::new());
    (graph, a, b)
}

fn add_edge_with_body(
    graph: &Graph,
    src: VertexId,
    dst: VertexId,
    label: &str,
    body: &str,
) -> EdgeId {
    let mut props = HashMap::new();
    props.insert("body".to_string(), Value::String(body.to_string()));
    graph.add_edge(src, dst, label, props).unwrap()
}

// =============================================================================
// Edge-side: index lifecycle
// =============================================================================

#[test]
fn create_then_drop_edge_text_index_round_trips() {
    let graph = Graph::new();
    assert_eq!(graph.text_index_count_e(), 0);
    assert!(!graph.has_text_index_e("body"));

    graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap();
    assert!(graph.has_text_index_e("body"));
    assert_eq!(graph.text_index_count_e(), 1);
    assert_eq!(graph.list_text_indexes_e(), vec!["body".to_string()]);

    graph.drop_text_index_e("body").unwrap();
    assert!(!graph.has_text_index_e("body"));
    assert_eq!(graph.text_index_count_e(), 0);
}

#[test]
fn create_edge_text_index_rejects_duplicate_property() {
    let graph = Graph::new();
    graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap();
    let err = graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}

#[test]
fn drop_edge_text_index_returns_error_for_unknown_property() {
    let graph = Graph::new();
    let err = graph.drop_text_index_e("missing").unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}

#[test]
fn create_edge_text_index_backfills_existing_string_values() {
    let graph = Arc::new(Graph::new());
    let a = graph.add_vertex("anchor", HashMap::new());
    let b = graph.add_vertex("anchor", HashMap::new());

    add_edge_with_body(&graph, a, b, "comment", "the raft consensus protocol");
    add_edge_with_body(&graph, a, b, "comment", "paxos consensus");
    add_edge_with_body(&graph, a, b, "comment", "completely unrelated content");

    // Index created AFTER edges exist must back-fill them.
    graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text_e("body", "consensus", 10)
        .unwrap()
        .to_value_list();
    assert_eq!(hits.len(), 2);
}

// =============================================================================
// Edge-side: mutation hooks
// =============================================================================

#[test]
fn add_edge_indexes_string_body_property() {
    let (graph, a, b) = graph_with_edge_body_index();
    let e = add_edge_with_body(&graph, a, b, "comment", "raft consensus algorithm");

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text_e("body", "raft", 10)
        .unwrap()
        .to_value_list();
    assert_eq!(hits, vec![Value::Edge(e)]);
}

#[test]
fn set_edge_property_updates_text_index() {
    let (graph, a, b) = graph_with_edge_body_index();
    let e = add_edge_with_body(&graph, a, b, "comment", "original text about apples");

    let g = graph.gremlin(Arc::clone(&graph));
    assert_eq!(
        g.search_text_e("body", "apples", 10)
            .unwrap()
            .to_value_list()
            .len(),
        1
    );

    // Replace property; old token "apples" must disappear, "bananas" appears.
    graph
        .set_edge_property(e, "body", Value::String("now about bananas".into()))
        .unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text_e("body", "apples", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
    assert_eq!(
        g.search_text_e("body", "bananas", 10)
            .unwrap()
            .to_value_list()
            .len(),
        1
    );
}

#[test]
fn set_edge_property_to_non_string_removes_from_index() {
    let (graph, a, b) = graph_with_edge_body_index();
    let e = add_edge_with_body(&graph, a, b, "comment", "indexable text");

    graph
        .set_edge_property(e, "body", Value::Int(42))
        .unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text_e("body", "indexable", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
}

#[test]
fn remove_edge_removes_from_text_index() {
    let (graph, a, b) = graph_with_edge_body_index();
    let e = add_edge_with_body(&graph, a, b, "comment", "ephemeral content");

    graph.remove_edge(e).unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text_e("body", "ephemeral", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
}

#[test]
fn removing_vertex_cascades_edge_text_index_cleanup() {
    let (graph, a, b) = graph_with_edge_body_index();
    add_edge_with_body(&graph, a, b, "comment", "doomed payload");

    // Removing the source vertex cascades to its incident edges; the edge
    // text index must reflect that removal.
    graph.remove_vertex(a).unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text_e("body", "doomed", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
}

// =============================================================================
// Edge-side: traversal source (search_text_e / search_text_query_e)
// =============================================================================

#[test]
fn search_text_e_returns_top_k_in_score_order() {
    let (graph, a, b) = graph_with_edge_body_index();
    add_edge_with_body(&graph, a, b, "comment", "raft raft raft consensus consensus");
    add_edge_with_body(&graph, a, b, "comment", "raft consensus");
    add_edge_with_body(&graph, a, b, "comment", "consensus only");
    add_edge_with_body(&graph, a, b, "comment", "totally unrelated content");

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text_e("body", "raft consensus", 10)
        .unwrap()
        .to_value_list();
    assert!(hits.len() >= 2);

    let g = graph.gremlin(Arc::clone(&graph));
    let top1 = g
        .search_text_e("body", "raft consensus", 1)
        .unwrap()
        .to_value_list();
    assert_eq!(top1.len(), 1);
}

#[test]
fn search_text_query_e_supports_phrase_query() {
    let (graph, a, b) = graph_with_edge_body_index();
    let phrase_match = add_edge_with_body(&graph, a, b, "comment", "the quick brown fox jumps");
    let _scattered =
        add_edge_with_body(&graph, a, b, "comment", "brown sugar and quick recipes for fox tacos");

    let g = graph.gremlin(Arc::clone(&graph));
    let q = TextQuery::Phrase {
        text: "quick brown fox".to_string(),
        slop: 0,
    };
    let hits = g
        .search_text_query_e("body", &q, 10)
        .unwrap()
        .to_value_list();
    assert_eq!(hits, vec![Value::Edge(phrase_match)]);
}

#[test]
fn search_text_e_returns_error_for_unknown_property() {
    let graph = Arc::new(Graph::new());
    let g = graph.gremlin(Arc::clone(&graph));
    match g.search_text_e("missing", "anything", 10) {
        Err(TextIndexError::Storage(_)) => {}
        Err(other) => panic!("unexpected error variant: {other:?}"),
        Ok(_) => panic!("expected error for missing index"),
    }
}

#[test]
fn search_text_e_with_zero_k_returns_no_hits() {
    let (graph, a, b) = graph_with_edge_body_index();
    add_edge_with_body(&graph, a, b, "comment", "anything");
    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text_e("body", "anything", 0)
        .unwrap()
        .to_value_list();
    assert!(hits.is_empty());
}

#[test]
fn search_text_e_chains_with_filter_steps() {
    let (graph, a, b) = graph_with_edge_body_index();
    let _comment = add_edge_with_body(&graph, a, b, "comment", "raft consensus");
    let _endorses = add_edge_with_body(&graph, a, b, "endorses", "raft consensus");

    let g = graph.gremlin(Arc::clone(&graph));
    let comments_only = g
        .search_text_e("body", "raft", 10)
        .unwrap()
        .has_label("comment")
        .to_value_list();
    assert_eq!(comments_only.len(), 1);
}

#[test]
fn search_text_e_preserves_descending_score_order() {
    let (graph, a, b) = graph_with_edge_body_index();
    let strong = add_edge_with_body(
        &graph,
        a,
        b,
        "comment",
        "raft raft raft consensus consensus consensus",
    );
    let weak = add_edge_with_body(&graph, a, b, "comment", "raft");

    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text_e("body", "raft consensus", 10)
        .unwrap()
        .to_value_list();

    assert_eq!(hits.first(), Some(&Value::Edge(strong)));
    assert!(hits.contains(&Value::Edge(weak)));
}

// =============================================================================
// Cross-element invariants
//
// Two parallel maps mean vertex and edge indexes are independent storage,
// but the spec mandates a globally-unique property-name namespace and
// guarantees zero bleed-through between the two.
// =============================================================================

#[test]
fn vertex_and_edge_indexes_on_different_properties_are_independent() {
    let graph = Arc::new(Graph::new());
    graph
        .create_text_index_v("bio", TextIndexConfig::default())
        .unwrap();
    graph
        .create_text_index_e("note", TextIndexConfig::default())
        .unwrap();

    // Add a vertex with `bio` and an edge with `note`. Neither index should
    // see the other's tokens; cross-search must miss.
    let mut vp = HashMap::new();
    vp.insert("bio".into(), Value::String("alice loves raft".into()));
    let alice = graph.add_vertex("person", vp);
    let bob = graph.add_vertex("person", HashMap::new());

    let mut ep = HashMap::new();
    ep.insert("note".into(), Value::String("paxos is fine too".into()));
    let edge = graph.add_edge(alice, bob, "comment", ep).unwrap();

    let g = graph.gremlin(Arc::clone(&graph));
    let v_hits = g
        .search_text("bio", "raft", 10)
        .unwrap()
        .to_value_list();
    assert_eq!(v_hits, vec![Value::Vertex(alice)]);

    let g = graph.gremlin(Arc::clone(&graph));
    let e_hits = g
        .search_text_e("note", "paxos", 10)
        .unwrap()
        .to_value_list();
    assert_eq!(e_hits, vec![Value::Edge(edge)]);

    // Edge-side search for a token that only the vertex index has must miss.
    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text_e("note", "raft", 10)
        .unwrap()
        .to_value_list()
        .is_empty());

    // And the symmetric direction.
    let g = graph.gremlin(Arc::clone(&graph));
    assert!(g
        .search_text("bio", "paxos", 10)
        .unwrap()
        .to_value_list()
        .is_empty());
}

#[test]
fn property_name_uniqueness_is_global_across_vertex_and_edge_indexes() {
    let graph = Graph::new();

    // Register a vertex index on `body`; a subsequent edge index on the same
    // property must be rejected per the global-uniqueness invariant.
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();
    let err = graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));

    // And the symmetric direction: drop the vertex index, register on edge,
    // then a vertex re-registration must be rejected.
    graph.drop_text_index_v("body").unwrap();
    graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap();
    let err = graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));

    // Sanity: only one of the two maps holds the index at any given time.
    assert_eq!(graph.text_index_count_v(), 0);
    assert_eq!(graph.text_index_count_e(), 1);
}

// =============================================================================
// spec-55c Layer 1h: bridge contract for query-language entry points
//
// These tests pin the invariant that the FTS bridge (`from_snapshot_with_graph`
// vs `from_snapshot`) is wired into the four query-language entry points. The
// Gremlin script and GQL surface syntax for FTS lands in Layers 2-5; until
// then, these tests cover the underlying bridge so regressions surface early.
// =============================================================================

mod fts_bridge_contract {
    use super::*;
    use interstellar::traversal::GraphTraversalSource;

    /// A `GraphTraversalSource` built via `from_snapshot` (no `Graph` handle)
    /// must reject FTS calls with a clear, actionable error.
    #[test]
    fn from_snapshot_without_graph_handle_rejects_search_text() {
        let graph = graph_with_body_index();
        add_doc(&graph, "doc", "raft consensus algorithm");
        let snapshot = graph.snapshot();
        let g = GraphTraversalSource::from_snapshot(&snapshot);

        let err = g.search_text("body", "raft", 10).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("requires a live Graph handle"),
            "expected guidance about Graph handle, got: {msg}"
        );
    }

    /// `from_snapshot_with_graph` must accept FTS calls and route through the
    /// live registry. This is the exact path used by `Graph::query`,
    /// `Graph::execute_script_with_context`, `Graph::gql`, and
    /// `Graph::gql_with_params`.
    #[test]
    fn from_snapshot_with_graph_routes_to_text_index() {
        let graph = graph_with_body_index();
        add_doc(&graph, "doc", "raft consensus algorithm");
        add_doc(&graph, "doc", "paxos consensus");
        let snapshot = graph.snapshot();
        let g = GraphTraversalSource::from_snapshot_with_graph(&snapshot, Arc::clone(&graph));

        let hits = g.search_text("body", "consensus", 10).unwrap().to_list();
        assert_eq!(hits.len(), 2);
    }

    /// `Graph::query` must not panic and must return a successful result for a
    /// non-FTS Gremlin script run on a graph that *does* carry text indexes.
    /// This guards against any accidental coupling between text-index presence
    /// and the bridge plumbing.
    #[cfg(feature = "gremlin")]
    #[test]
    fn graph_query_succeeds_when_text_indexes_present() {
        let graph = graph_with_body_index();
        add_doc(&graph, "doc", "raft consensus algorithm");
        add_doc(&graph, "doc", "paxos consensus");

        let result = graph.query("g.V().count().toList()").unwrap();
        // The exact ExecutionResult shape varies; we only require non-error.
        let _ = result;
    }

    /// `Graph::gql` similarly must not panic when text indexes are registered;
    /// the new `Compiler.graph_handle` plumbing must remain harmless when no
    /// FTS CALL procedure is invoked. Layer 5 will add positive FTS coverage.
    #[cfg(feature = "gql")]
    #[test]
    fn graph_gql_succeeds_when_text_indexes_present() {
        let graph = graph_with_body_index();
        add_doc(&graph, "doc", "raft consensus algorithm");

        let results = graph.gql("MATCH (n) RETURN count(n)").unwrap();
        assert_eq!(results.len(), 1);
    }

    /// The same invariant for edges: a snapshot built without a graph handle
    /// must reject `search_text_e` with the same actionable error.
    #[test]
    fn from_snapshot_without_graph_handle_rejects_search_text_e() {
        let graph = Arc::new(Graph::new());
        graph
            .create_text_index_e("relation", TextIndexConfig::default())
            .unwrap();
        let snapshot = graph.snapshot();
        let g = GraphTraversalSource::from_snapshot(&snapshot);

        let err = g.search_text_e("relation", "anything", 10).unwrap_err();
        assert!(err.to_string().contains("requires a live Graph handle"));
    }
}
