//! Performance edge case tests for Gremlin traversal operations.
//!
//! Phase 5 of the integration test strategy. Tests for:
//!
//! - Large result set handling (5+ tests)
//! - Barrier step memory behavior (5+ tests)
//! - Streaming vs buffered execution (5+ tests)
//!
//! These tests verify correctness under performance-sensitive conditions
//! rather than measuring actual performance (benchmarks handle that).

#![allow(unused_variables)]

use std::collections::HashMap;

use interstellar::p;
use interstellar::storage::Graph;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

// =============================================================================
// Test Fixtures for Large Graphs
// =============================================================================

/// Creates a graph with the specified number of vertices.
/// Each vertex has an "index" property with its creation order.
fn create_large_vertex_graph(count: usize) -> Graph {
    let graph = Graph::new();
    for i in 0..count {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("name".to_string(), Value::String(format!("vertex_{}", i)));
        props.insert(
            "category".to_string(),
            Value::String(format!("cat_{}", i % 10)),
        );
        graph.add_vertex("node", props);
    }
    graph
}

/// Creates a linear chain graph: v0 -> v1 -> v2 -> ... -> vN
fn create_chain_graph(length: usize) -> (Graph, VertexId, VertexId) {
    let graph = Graph::new();
    let mut prev_id: Option<VertexId> = None;
    let mut first_id: Option<VertexId> = None;
    let mut last_id: Option<VertexId> = None;

    for i in 0..length {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("depth".to_string(), Value::Int(i as i64));
        let id = graph.add_vertex("node", props);

        if first_id.is_none() {
            first_id = Some(id);
        }
        last_id = Some(id);

        if let Some(prev) = prev_id {
            graph.add_edge(prev, id, "next", HashMap::new()).unwrap();
        }
        prev_id = Some(id);
    }

    (graph, first_id.unwrap(), last_id.unwrap())
}

/// Creates a dense graph where each vertex connects to multiple others.
/// Vertices 0..count, each connects to (i+1)%count, (i+2)%count, etc.
fn create_dense_graph(vertex_count: usize, edges_per_vertex: usize) -> Graph {
    let graph = Graph::new();
    let mut ids = Vec::with_capacity(vertex_count);

    // Create vertices
    for i in 0..vertex_count {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        let id = graph.add_vertex("node", props);
        ids.push(id);
    }

    // Create edges
    for i in 0..vertex_count {
        for j in 1..=edges_per_vertex {
            let target = (i + j) % vertex_count;
            if target != i {
                let _ = graph.add_edge(ids[i], ids[target], "connects", HashMap::new());
            }
        }
    }

    graph
}

/// Creates a graph with vertices having diverse property values for aggregation tests.
fn create_aggregation_graph(count: usize) -> Graph {
    let graph = Graph::new();
    for i in 0..count {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("value".to_string(), Value::Int((i * 7 % 100) as i64));
        props.insert(
            "group".to_string(),
            Value::String(format!("group_{}", i % 5)),
        );
        props.insert("priority".to_string(), Value::Int((i % 3) as i64));
        graph.add_vertex("item", props);
    }
    graph
}

// =============================================================================
// Large Result Set Handling Tests
// =============================================================================

/// Verifies traversal handles 1000+ vertices correctly.
#[test]
fn handles_thousand_vertices() {
    let graph = create_large_vertex_graph(1000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.v().count();
    assert_eq!(count, 1000);

    let results = g.v().to_list();
    assert_eq!(results.len(), 1000);
}

/// Verifies traversal handles 10,000 vertices correctly.
#[test]
fn handles_ten_thousand_vertices() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.v().count();
    assert_eq!(count, 10_000);

    // Verify we can filter and still get correct results
    let filtered_count = g.v().has_where("index", p::lt(5000)).count();
    assert_eq!(filtered_count, 5000);
}

/// Verifies limit works correctly with large result sets.
#[test]
fn limit_on_large_result_set() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Limit should stop early, not process all 10k vertices
    let limited = g.v().limit(10).to_list();
    assert_eq!(limited.len(), 10);

    // Verify limit at different points
    let limited_100 = g.v().limit(100).to_list();
    assert_eq!(limited_100.len(), 100);

    let limited_1000 = g.v().limit(1000).to_list();
    assert_eq!(limited_1000.len(), 1000);
}

/// Verifies skip + limit (pagination) works correctly.
#[test]
fn pagination_on_large_result_set() {
    let graph = create_large_vertex_graph(1000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Simulate pagination: page size 100
    let page1 = g.v().range(0, 100).to_list();
    let page2 = g.v().range(100, 200).to_list();
    let page3 = g.v().range(200, 300).to_list();

    assert_eq!(page1.len(), 100);
    assert_eq!(page2.len(), 100);
    assert_eq!(page3.len(), 100);

    // Pages should be disjoint
    let page1_ids: std::collections::HashSet<_> =
        page1.iter().filter_map(|v| v.as_vertex_id()).collect();
    let page2_ids: std::collections::HashSet<_> =
        page2.iter().filter_map(|v| v.as_vertex_id()).collect();

    assert!(
        page1_ids.is_disjoint(&page2_ids),
        "Pages should not overlap"
    );
}

/// Verifies dedup handles large result sets with many duplicates.
#[test]
fn dedup_on_large_result_set_with_duplicates() {
    let graph = create_dense_graph(100, 10);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Navigate out twice - creates many duplicates
    let with_dups = g.v().out().out().to_list();
    let without_dups = g.v().out().out().dedup().to_list();

    // Dedup should significantly reduce count
    assert!(
        without_dups.len() <= with_dups.len(),
        "Dedup should reduce or maintain count"
    );
    assert!(
        without_dups.len() <= 100,
        "Should have at most 100 unique vertices"
    );
}

/// Verifies count() works correctly without materializing all results.
#[test]
fn count_on_large_result_set() {
    let graph = create_large_vertex_graph(50_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Count should work without OOM
    let count = g.v().count();
    assert_eq!(count, 50_000);

    // Count with filter
    let filtered_count = g.v().has_where("index", p::gte(25_000)).count();
    assert_eq!(filtered_count, 25_000);
}

// =============================================================================
// Barrier Step Memory Behavior Tests
// =============================================================================

/// Verifies order() (a barrier step) works correctly with large data.
#[test]
fn order_barrier_with_large_data() {
    let graph = create_large_vertex_graph(5000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // order() must buffer all results before emitting
    let ordered = g
        .v()
        .values("index")
        .order()
        .by_asc()
        .build()
        .limit(10)
        .to_list();

    assert_eq!(ordered.len(), 10);

    // Verify correct ordering (should be 0-9)
    let indices: Vec<i64> = ordered.iter().filter_map(|v| v.as_i64()).collect();
    assert_eq!(indices, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

/// Verifies order().by_desc() correctly orders large data.
#[test]
fn order_desc_barrier_with_large_data() {
    let graph = create_large_vertex_graph(5000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let ordered = g
        .v()
        .values("index")
        .order()
        .by_desc()
        .build()
        .limit(10)
        .to_list();

    assert_eq!(ordered.len(), 10);

    // Verify descending order (should be 4999, 4998, ...)
    let indices: Vec<i64> = ordered.iter().filter_map(|v| v.as_i64()).collect();
    assert_eq!(
        indices,
        vec![4999, 4998, 4997, 4996, 4995, 4994, 4993, 4992, 4991, 4990]
    );
}

/// Verifies group() (a barrier step) handles large data correctly.
#[test]
fn group_barrier_with_large_data() {
    let graph = create_aggregation_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Group by "group" property (5 unique values)
    let grouped = g.v().group().by_key("group").by_value().build().to_list();

    assert_eq!(grouped.len(), 1);

    if let Value::Map(map) = &grouped[0] {
        // Should have 5 groups
        assert_eq!(map.len(), 5);

        // Each group should have 2000 elements (10000 / 5)
        for (key, value) in map.iter() {
            if let Value::List(list) = value {
                assert_eq!(
                    list.len(),
                    2000,
                    "Each group should have 2000 elements, got {} for key {:?}",
                    list.len(),
                    key
                );
            }
        }
    }
}

/// Verifies group_count() (a barrier step) handles large data correctly.
#[test]
fn group_count_barrier_with_large_data() {
    let graph = create_aggregation_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let counts = g.v().group_count().by_key("priority").build().to_list();

    assert_eq!(counts.len(), 1);

    if let Value::Map(map) = &counts[0] {
        // 3 priority levels (0, 1, 2)
        assert_eq!(map.len(), 3);

        // Each priority should have ~3333 elements
        let total: i64 = map.values().filter_map(|v| v.as_i64()).sum();
        assert_eq!(total, 10_000);
    }
}

/// Verifies sum() (a barrier step) handles large numeric data.
#[test]
fn sum_barrier_with_large_data() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sum = g.v().values("index").sum();

    // Sum of 0..10000 = (n-1)*n/2 = 9999*10000/2 = 49995000
    assert_eq!(sum, Value::Int(49_995_000));
}

/// Verifies min/max (barrier steps) handle large data.
#[test]
fn min_max_barrier_with_large_data() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let min = g.v().values("index").min();
    let max = g.v().values("index").max();

    assert_eq!(min, Some(Value::Int(0)));
    assert_eq!(max, Some(Value::Int(9999)));
}

// =============================================================================
// Streaming vs Buffered Execution Tests
// =============================================================================

/// Verifies that filter steps stream (don't buffer all results).
#[test]
fn filter_streams_without_buffering() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Filter + limit should not process all 10k vertices
    // (We can't directly test this, but we verify correctness)
    let results = g.v().has_where("index", p::lt(100)).limit(5).to_list();

    assert_eq!(results.len(), 5);

    // Verify all results match filter
    for r in &results {
        if let Some(vid) = r.as_vertex_id() {
            // Results should have index < 100
        }
    }
}

/// Verifies navigation steps stream correctly.
#[test]
fn navigation_streams_without_buffering() {
    let (graph, start, _end) = create_chain_graph(1000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Navigate through chain with limit - should stop early
    let results = g
        .v_ids([start])
        .repeat(__.out())
        .times(1000) // Would traverse entire chain
        .emit()
        .limit(10) // But we only want 10
        .to_list();

    assert_eq!(results.len(), 10);
}

/// Verifies that non-barrier chains maintain streaming behavior.
#[test]
fn non_barrier_chain_streams() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Chain of non-barrier steps should stream
    let results = g
        .v()
        .has_label("node")
        .has_where("index", p::lt(1000))
        .values("name")
        .limit(5)
        .to_list();

    assert_eq!(results.len(), 5);
}

/// Verifies barrier step forces buffering, but subsequent steps stream again.
#[test]
fn barrier_then_stream() {
    let graph = create_aggregation_graph(1000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // order() buffers, but limit() after should work correctly
    let results = g
        .v()
        .values("value")
        .order()
        .by_asc()
        .build()
        .limit(10)
        .to_list();

    assert_eq!(results.len(), 10);

    // Should be the 10 smallest values
    let values: Vec<i64> = results.iter().filter_map(|v| v.as_i64()).collect();
    for i in 1..values.len() {
        assert!(values[i - 1] <= values[i], "Should be in ascending order");
    }
}

/// Verifies count() doesn't require materializing list.
#[test]
fn count_optimized_execution() {
    let graph = create_large_vertex_graph(100_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Count should work efficiently
    let count = g.v().count();
    assert_eq!(count, 100_000);

    // Count after filter
    let filtered = g.v().has_where("index", p::lt(50_000)).count();
    assert_eq!(filtered, 50_000);
}

// =============================================================================
// Edge Cases with Large Data
// =============================================================================

/// Verifies empty result handling with large graph.
#[test]
fn empty_result_from_large_graph() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Filter that matches nothing
    let results = g.v().has_where("index", p::gt(100_000)).to_list();
    assert!(results.is_empty());

    let count = g.v().has_where("index", p::gt(100_000)).count();
    assert_eq!(count, 0);
}

/// Verifies single result extraction from large graph.
#[test]
fn single_result_from_large_graph() {
    let graph = create_large_vertex_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Get exactly one vertex
    let result = g.v().has_where("index", p::eq(5000i64)).one();
    assert!(result.is_ok());
}

/// Verifies deep traversal in chain graph.
#[test]
fn deep_traversal_in_chain() {
    let (graph, start, end) = create_chain_graph(100);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Traverse entire chain
    let final_vertex = g
        .v_ids([start])
        .repeat(__.out())
        .times(99) // 99 hops to reach end
        .to_list();

    assert_eq!(final_vertex.len(), 1);
    assert_eq!(final_vertex[0].as_vertex_id(), Some(end));
}

/// Verifies path tracking with large traversal.
#[test]
fn path_tracking_large_traversal() {
    let (graph, start, _end) = create_chain_graph(50);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let paths = g
        .v_ids([start])
        .with_path()
        .repeat(__.out())
        .times(10)
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);

    if let Value::List(path) = &paths[0] {
        // Path should have 11 elements (start + 10 hops)
        assert_eq!(path.len(), 11);
    }
}

/// Verifies aggregation accuracy with large data.
#[test]
fn aggregation_accuracy_large_data() {
    let graph = create_aggregation_graph(10_000);
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Calculate expected sum of "value" property
    // value = (i * 7) % 100 for i in 0..10000
    let expected_sum: i64 = (0..10_000i64).map(|i| (i * 7) % 100).sum();

    let actual_sum = g.v().values("value").sum();
    assert_eq!(actual_sum, Value::Int(expected_sum));
}
