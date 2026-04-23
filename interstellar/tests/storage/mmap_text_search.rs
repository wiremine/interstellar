//! Integration tests for full-text search on the mmap backend (spec-55, Phase 4).
//!
//! Covers:
//!   - create / drop text index lifecycle
//!   - on-disk persistence and recovery across reopen
//!   - mutation hooks (add_vertex, set_vertex_property, remove_vertex)
//!   - edge-side equivalents
//!   - back-fill on index creation
//!   - rebuild from graph data when index dir is missing

use std::collections::HashMap;

use interstellar::storage::text::{TextIndex, TextIndexConfig, TextIndexError, TextQuery};
use interstellar::storage::MmapGraph;
use interstellar::value::{EdgeId, Value, VertexId};

// =============================================================================
// Helpers
// =============================================================================

fn tmp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    (dir, path)
}

fn add_doc(graph: &MmapGraph, label: &str, body: &str) -> VertexId {
    let mut props = HashMap::new();
    props.insert("body".to_string(), Value::String(body.to_string()));
    graph.add_vertex(label, props).unwrap()
}

fn search_body(graph: &MmapGraph, query: &str) -> Vec<(VertexId, f32)> {
    let idx = graph.text_index_v("body").expect("body index missing");
    let hits = idx
        .search(&TextQuery::Match(query.to_string()), 100)
        .unwrap();
    hits.into_iter()
        .map(|h| (h.element.as_vertex().unwrap(), h.score))
        .collect()
}

// =============================================================================
// Index lifecycle
// =============================================================================

#[test]
fn create_then_drop_text_index() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();

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
fn duplicate_index_rejected() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();

    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();
    let err = graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}

#[test]
fn drop_missing_index_errors() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();
    let err = graph.drop_text_index_v("nope").unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}

// =============================================================================
// Search basics + mutation hooks
// =============================================================================

#[test]
fn insert_and_search() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();

    let v1 = add_doc(&graph, "doc", "the quick brown fox");
    let _v2 = add_doc(&graph, "doc", "lazy dog sleeps");

    let hits = search_body(&graph, "fox");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, v1);
}

#[test]
fn property_update_reflected_in_search() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();

    let v1 = add_doc(&graph, "doc", "original text");
    assert_eq!(search_body(&graph, "original").len(), 1);

    graph
        .set_vertex_property(v1, "body", Value::String("replacement words".to_string()))
        .unwrap();

    assert_eq!(search_body(&graph, "original").len(), 0);
    assert_eq!(search_body(&graph, "replacement").len(), 1);
}

#[test]
fn remove_vertex_cleans_index() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();
    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();

    let v1 = add_doc(&graph, "doc", "searchable content");
    assert_eq!(search_body(&graph, "searchable").len(), 1);

    graph.remove_vertex(v1).unwrap();
    assert_eq!(search_body(&graph, "searchable").len(), 0);
}

// =============================================================================
// Back-fill on index creation
// =============================================================================

#[test]
fn backfill_existing_data() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();

    // Add data before creating the index
    let _v1 = add_doc(&graph, "doc", "pre-existing document alpha");
    let _v2 = add_doc(&graph, "doc", "pre-existing document beta");

    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();

    let hits = search_body(&graph, "pre-existing");
    assert_eq!(hits.len(), 2);
}

// =============================================================================
// Persistence across reopen
// =============================================================================

#[test]
fn index_survives_reopen() {
    let (_dir, path) = tmp_db();

    // First session: create index and add data
    {
        let graph = MmapGraph::open(&path).unwrap();
        graph
            .create_text_index_v("body", TextIndexConfig::default())
            .unwrap();
        add_doc(&graph, "doc", "persistent full text data");
    }

    // Second session: index should be loaded from disk
    {
        let graph = MmapGraph::open(&path).unwrap();
        assert!(graph.has_text_index_v("body"));
        let hits = search_body(&graph, "persistent");
        assert_eq!(hits.len(), 1);
    }
}

#[test]
fn index_rebuilt_when_dir_missing() {
    let (_dir, path) = tmp_db();

    // First session: create index and add data
    {
        let graph = MmapGraph::open(&path).unwrap();
        graph
            .create_text_index_v("body", TextIndexConfig::default())
            .unwrap();
        add_doc(&graph, "doc", "rebuild test data");
    }

    // Delete the on-disk index directory to simulate corruption
    let text_dir = path.parent().unwrap().join("text_indexes").join("body");
    if text_dir.exists() {
        std::fs::remove_dir_all(&text_dir).unwrap();
    }

    // Reopen: should rebuild from graph data
    {
        let graph = MmapGraph::open(&path).unwrap();
        assert!(graph.has_text_index_v("body"));
        let hits = search_body(&graph, "rebuild");
        assert_eq!(hits.len(), 1);
    }
}

// =============================================================================
// Edge text indexes
// =============================================================================

#[test]
fn edge_text_index_lifecycle() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();

    graph
        .create_text_index_e("description", TextIndexConfig::default())
        .unwrap();
    assert!(graph.has_text_index_e("description"));

    let v1 = graph.add_vertex("person", HashMap::new()).unwrap();
    let v2 = graph.add_vertex("person", HashMap::new()).unwrap();

    let mut props = HashMap::new();
    props.insert(
        "description".to_string(),
        Value::String("works closely together".to_string()),
    );
    let e1 = graph.add_edge(v1, v2, "knows", props).unwrap();

    let idx = graph.text_index_e("description").unwrap();
    let hits = idx
        .search(&TextQuery::Match("closely".to_string()), 10)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].element.as_edge().unwrap(), e1);

    graph.remove_edge(e1).unwrap();
    let hits = idx
        .search(&TextQuery::Match("closely".to_string()), 10)
        .unwrap();
    assert_eq!(hits.is_empty(), true);

    graph.drop_text_index_e("description").unwrap();
    assert!(!graph.has_text_index_e("description"));
}

// =============================================================================
// Cross-element uniqueness
// =============================================================================

#[test]
fn property_name_unique_across_vertex_and_edge() {
    let (_dir, path) = tmp_db();
    let graph = MmapGraph::open(&path).unwrap();

    graph
        .create_text_index_v("body", TextIndexConfig::default())
        .unwrap();

    // Same property name on edges should fail
    let err = graph
        .create_text_index_e("body", TextIndexConfig::default())
        .unwrap_err();
    assert!(matches!(err, TextIndexError::Storage(_)));
}
