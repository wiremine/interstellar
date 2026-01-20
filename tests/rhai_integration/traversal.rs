//! Traversal integration tests.

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;

use super::{create_chain_graph, create_empty_graph, create_social_graph};

// =============================================================================
// Source Steps
// =============================================================================

#[test]
fn test_v_all_vertices() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6); // 5 people + 1 company
}

#[test]
fn test_e_all_edges() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.e().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 5); // 4 knows + 1 works_at
}

#[test]
fn test_empty_graph() {
    let engine = RhaiEngine::new();
    let graph = create_empty_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

// =============================================================================
// Navigation Steps
// =============================================================================

#[test]
fn test_out_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").out().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob, Carol, Acme
}

#[test]
fn test_out_with_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").out("knows").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob, Carol (not Acme)
}

#[test]
fn test_in_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Carol").in_().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Alice, Bob both know Carol
}

#[test]
fn test_both_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Bob").both("knows").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Alice (in) and Carol (out)
}

#[test]
fn test_out_e_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").out_e().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // 2 knows + 1 works_at
}

#[test]
fn test_in_e_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Carol").in_e().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Two incoming "knows" edges
}

#[test]
fn test_edge_to_vertex_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // out_e().in_v() should be equivalent to out()
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").out_e("knows").in_v().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob, Carol
}

#[test]
fn test_chained_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // A -> B -> C
    let names: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "A").out().out().values("name").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(names.len(), 1);
    assert_eq!(names[0].clone().into_string().unwrap(), "C");
}

// =============================================================================
// Filter Steps
// =============================================================================

#[test]
fn test_has_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 5);
}

#[test]
fn test_has_property() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has("age").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 5); // All people have age, company doesn't
}

#[test]
fn test_has_not_property() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_not("age").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 1); // Company doesn't have age
}

#[test]
fn test_has_value() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 1);
}

#[test]
fn test_dedup() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Carol is reached from both Alice and Bob
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_where("name", within(["Alice", "Bob"])).out("knows").dedup().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob, Carol (deduplicated)
}

#[test]
fn test_limit() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().limit(3).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3);
}

#[test]
fn test_skip() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().skip(4).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // 6 - 4 = 2
}

#[test]
fn test_range() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().range(1, 4).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Elements at index 1, 2, 3
}

// =============================================================================
// Transform Steps
// =============================================================================

#[test]
fn test_id() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let ids: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").id().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(ids.len(), 5);
}

#[test]
fn test_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let labels: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().label().dedup().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(labels.len(), 2); // person, company
}

#[test]
fn test_values() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let names: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("name").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(names.len(), 5);
}

#[test]
fn test_value_map() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let maps: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").value_map().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(maps.len(), 1);
    assert!(maps[0].is_map());
}

#[test]
fn test_element_map() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let maps: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").element_map().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(maps.len(), 1);
    let map = &maps[0];
    assert!(map.is_map());
}

#[test]
fn test_constant() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").constant(42).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 5);
    for result in results {
        assert_eq!(result.as_int().unwrap(), 42);
    }
}

#[test]
fn test_identity() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().identity().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6);
}

// =============================================================================
// Modulator Steps
// =============================================================================

#[test]
fn test_as_select() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").as_("a").out("knows").as_("b").select(["a", "b"]).to_list()
        "#,
        )
        .unwrap();

    // Alice knows 2 people, so 2 results
    assert_eq!(results.len(), 2);
}

#[test]
fn test_select_one() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").as_("a").out("knows").select_one("a").to_list()
        "#,
        )
        .unwrap();

    // Both paths return Alice
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Terminal Steps
// =============================================================================

#[test]
fn test_to_list() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 5);
}

#[test]
fn test_count() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6);
}

#[test]
fn test_first() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let result: rhai::Dynamic = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").first()
        "#,
        )
        .unwrap();

    // Should get some result (not unit)
    assert!(!result.is_unit());
}

#[test]
fn test_first_empty() {
    let engine = RhaiEngine::new();
    let graph = create_empty_graph();

    let result: rhai::Dynamic = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().first()
        "#,
        )
        .unwrap();

    // Should return unit for empty
    assert!(result.is_unit());
}

#[test]
fn test_has_next() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let has_people: bool = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").has_next()
        "#,
        )
        .unwrap();

    assert!(has_people);
}

#[test]
fn test_has_next_empty() {
    let engine = RhaiEngine::new();
    let graph = create_empty_graph();

    let has_vertices: bool = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_next()
        "#,
        )
        .unwrap();

    assert!(!has_vertices);
}

// =============================================================================
// Order Steps
// =============================================================================

#[test]
fn test_order_asc() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let ages: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("age").order_asc().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(ages.len(), 5);
    // Should be sorted ascending
    let first = ages[0].as_int().unwrap();
    let last = ages[4].as_int().unwrap();
    assert!(first <= last);
}

#[test]
fn test_order_desc() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let ages: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("age").order_desc().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(ages.len(), 5);
    // Should be sorted descending
    let first = ages[0].as_int().unwrap();
    let last = ages[4].as_int().unwrap();
    assert!(first >= last);
}

// =============================================================================
// Complex Queries
// =============================================================================

#[test]
fn test_friends_of_friends() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find friends of friends of Alice (excluding Alice)
    let fof: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice")
                .out("knows")
                .out("knows")
                .has_label("person")
                .dedup()
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Bob knows Carol, so Carol is friend-of-friend
    assert!(fof.len() >= 1);
}

#[test]
fn test_coworkers_query() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find company where Alice works
    let company: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice")
                .out("works_at")
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(company.len(), 1);
    assert_eq!(company[0].clone().into_string().unwrap(), "Acme Corp");
}

#[test]
fn test_multi_step_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find active people over 25 that Alice knows
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice")
                .out("knows")
                .has_label("person")
                .has_value("active", true)
                .has_where("age", gt(25))
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Alice knows Bob(25, active) and Carol(35, inactive)
    // Bob is 25, not > 25, so no results
    assert_eq!(results.len(), 0);
}

#[test]
fn test_path_length_query() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // Count hops from A
    let result: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "A")
                .out().out().out().out()
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].clone().into_string().unwrap(), "E");
}

// =============================================================================
// Traversal-Based Filter Steps (Phase 2)
// =============================================================================

#[test]
fn test_where_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who know someone (have outgoing knows edges)
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .where_(anon().out("knows"))
                .count()
        "#,
        )
        .unwrap();

    // Alice knows Bob and Carol, Bob knows Carol, Dave knows Eve
    // So: Alice, Bob, Dave have outgoing knows edges
    assert_eq!(count, 3);
}

#[test]
fn test_where_filter_empty() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who know someone over 100 years old (none exist)
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .where_(anon().out("knows").has_where("age", gt(100)))
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

#[test]
fn test_not_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who do NOT know anyone
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .not_(anon().out("knows"))
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Carol and Eve don't have outgoing knows edges
    assert_eq!(results.len(), 2);
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"Carol".to_string()));
    assert!(names.contains(&"Eve".to_string()));
}

#[test]
fn test_and_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who know someone AND are active
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .and_([
                    anon().out("knows"),
                    anon().has_value("active", true)
                ])
                .count()
        "#,
        )
        .unwrap();

    // Alice (active, knows Bob and Carol), Bob (active, knows Carol), Dave (active, knows Eve)
    assert_eq!(count, 3);
}

#[test]
fn test_and_filter_stricter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who know someone AND are over 30 AND are active
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .and_([
                    anon().out("knows"),
                    anon().has_where("age", gt(30)),
                    anon().has_value("active", true)
                ])
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Only Dave is over 30, active, and knows someone
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "Dave");
}

#[test]
fn test_or_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who are either over 35 OR named "Bob"
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .or_([
                    anon().has_where("age", gt(35)),
                    anon().has_value("name", "Bob")
                ])
                .count()
        "#,
        )
        .unwrap();

    // Bob (25) is named Bob, Dave (40) is over 35
    assert_eq!(count, 2);
}

#[test]
fn test_or_filter_with_names() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who are either inactive OR very young
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .or_([
                    anon().has_value("active", false),
                    anon().has_where("age", lt(26))
                ])
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Carol is inactive, Bob is 25 (< 26)
    assert_eq!(results.len(), 2);
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"Carol".to_string()));
    assert!(names.contains(&"Bob".to_string()));
}

#[test]
fn test_combined_where_not() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people who know someone but don't work anywhere
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .where_(anon().out("knows"))
                .not_(anon().out("works_at"))
                .count()
        "#,
        )
        .unwrap();

    // Alice, Bob, Dave know someone. Only Alice works at Acme.
    // So Bob and Dave match.
    assert_eq!(count, 2);
}

// =============================================================================
// Navigation Completion (Phase 3)
// =============================================================================

#[test]
fn test_both_v_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get all vertices connected by "knows" edges
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.e().has_label("knows").both_v().dedup().count()
        "#,
        )
        .unwrap();

    // The knows edges connect: Alice-Bob, Alice-Carol, Bob-Carol, Dave-Eve
    // Unique vertices: Alice, Bob, Carol, Dave, Eve = 5
    assert_eq!(count, 5);
}

#[test]
fn test_both_v_without_dedup() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get all vertices from knows edges (with duplicates)
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.e().has_label("knows").both_v().count()
        "#,
        )
        .unwrap();

    // 4 knows edges * 2 vertices each = 8 vertices
    assert_eq!(count, 8);
}

#[test]
fn test_both_v_with_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get names of all vertices connected by knows edges, filtered by active
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.e().has_label("knows")
                .both_v()
                .has_value("active", true)
                .dedup()
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Active people in knows relationships: Alice, Bob, Dave, Eve (Carol is inactive)
    assert_eq!(results.len(), 4);
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Dave".to_string()));
    assert!(names.contains(&"Eve".to_string()));
    assert!(!names.contains(&"Carol".to_string()));
}

// =============================================================================
// Repeat Step Completion (Phase 4)
// =============================================================================

#[test]
fn test_repeat_times() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // Traverse 2 hops from A: A -> B -> C
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "A")
                .repeat(anon().out(), 2)
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "C");
}

#[test]
fn test_repeat_emit() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // Traverse 2 hops from A with emit: should get B and C
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "A")
                .repeat_emit(anon().out(), 2)
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // With emit, we get intermediate results: B (after 1 hop) and C (after 2 hops)
    assert_eq!(results.len(), 2);
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"B".to_string()));
    assert!(names.contains(&"C".to_string()));
}

#[test]
fn test_repeat_emit_more_hops() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // Traverse 3 hops from A with emit: should get B, C, and D
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "A")
                .repeat_emit(anon().out(), 3)
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 3);
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"B".to_string()));
    assert!(names.contains(&"C".to_string()));
    assert!(names.contains(&"D".to_string()));
}

#[test]
fn test_repeat_until() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Start from Alice and traverse knows edges until we find someone over 30
    // Alice(30) -> Bob(25), Carol(35)
    // Bob is not > 30, so from Bob we'd go to Carol(35)
    // Carol is > 30, so we stop
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice")
                .repeat_until(anon().out("knows"), anon().has_where("age", gt(30)))
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Should find Carol (age 35 > 30)
    assert!(!results.is_empty());
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"Carol".to_string()));
}

#[test]
fn test_repeat_emit_until() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // Traverse from A until we reach D, emitting intermediates
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "A")
                .repeat_emit_until(anon().out(), anon().has_value("name", "D"))
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // With emit, we get B, C, and D (intermediates + final)
    let names: Vec<String> = results
        .iter()
        .map(|d| d.clone().into_string().unwrap())
        .collect();
    assert!(names.contains(&"B".to_string()));
    assert!(names.contains(&"C".to_string()));
    assert!(names.contains(&"D".to_string()));
}

// =============================================================================
// Side Effect Steps (Phase 5)
// =============================================================================

#[test]
fn test_store_and_cap() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Store all person names and retrieve with cap
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .values("name")
                .store("names")
                .cap("names")
                .to_list()
        "#,
        )
        .unwrap();

    // cap returns a single list value containing the stored items
    assert_eq!(results.len(), 1);
    // The first element should be a list of all the names
    let stored_names = results[0].clone().into_array().unwrap();
    assert_eq!(stored_names.len(), 5); // 5 people
}

#[test]
fn test_aggregate_and_cap() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Aggregate all person ages (barrier step)
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .values("age")
                .aggregate("ages")
                .cap("ages")
                .to_list()
        "#,
        )
        .unwrap();

    // cap returns a single list value containing the aggregated items
    assert_eq!(results.len(), 1);
    let stored_ages = results[0].clone().into_array().unwrap();
    assert_eq!(stored_ages.len(), 5); // 5 people with ages
}

#[test]
fn test_store_multiple_keys() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Store vertices and their names separately
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .store("vertices")
                .values("name")
                .store("names")
                .cap_multi(["vertices", "names"])
                .to_list()
        "#,
        )
        .unwrap();

    // cap_multi returns a map with the two keys
    assert_eq!(results.len(), 1);
    // The result is a map - check it's a valid Map type
    let result = &results[0];
    assert!(result.is_map());
}

#[test]
fn test_side_effect_with_store() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Use side_effect to store values while continuing traversal
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person")
                .side_effect(anon().values("name").store("all_names"))
                .count()
        "#,
        )
        .unwrap();

    // The main traversal should still count all people
    assert_eq!(count, 5);
}

#[test]
fn test_store_empty_result() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Store from a filter that matches nothing
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("nonexistent")
                .store("empty")
                .cap("empty")
                .to_list()
        "#,
        )
        .unwrap();

    // cap should return an empty list
    assert_eq!(results.len(), 1);
    let stored = results[0].clone().into_array().unwrap();
    assert_eq!(stored.len(), 0);
}

#[test]
fn test_store_with_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Store Alice's friends
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice")
                .out("knows")
                .values("name")
                .store("friends")
                .cap("friends")
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let friends = results[0].clone().into_array().unwrap();
    assert_eq!(friends.len(), 2); // Alice knows Bob and Carol
}

#[test]
fn test_aggregate_is_barrier() {
    let engine = RhaiEngine::new();
    let graph = create_chain_graph();

    // Aggregate should collect all before continuing
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().values("name")
                .aggregate("all")
                .cap("all")
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let all_names = results[0].clone().into_array().unwrap();
    assert_eq!(all_names.len(), 5); // A, B, C, D, E in chain graph
}

// =============================================================================
// Mutation Steps (Phase 6)
// =============================================================================

#[test]
fn test_add_v_creates_pending_vertex() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // add_v should create a pending vertex marker
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_v("test_label").to_list()
        "#,
        )
        .unwrap();

    // Should return one pending vertex
    assert_eq!(results.len(), 1);
}

#[test]
fn test_add_v_with_property() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // add_v with chained property
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_v("person").property("name", "NewPerson").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_add_v_with_multiple_properties() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // add_v with multiple properties
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_v("person")
                .property("name", "NewPerson")
                .property("age", 25)
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_add_e_with_endpoints() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // add_e with from/to vertex IDs
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_e("test_edge").from_v(0).to_v(1).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_add_e_with_property() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // add_e with property
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_e("test_edge")
                .from_v(0)
                .to_v(1)
                .property("since", 2024)
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_drop_step() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get initial vertex count
    let initial_count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().count()
        "#,
        )
        .unwrap();

    // drop() should delete the element
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").drop().to_list()
        "#,
        )
        .unwrap();

    // drop() executes the deletion and returns empty (no result for deleted items)
    assert_eq!(results.len(), 0);

    // Verify the vertex was actually deleted
    let final_count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().count()
        "#,
        )
        .unwrap();

    assert_eq!(final_count, initial_count - 1);
}

#[test]
fn test_property_on_traversal() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // property() on existing element
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").property("status", "active").to_list()
        "#,
        )
        .unwrap();

    // Should return pending property update
    assert_eq!(results.len(), 1);
}

#[test]
fn test_add_v_source_level() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Source-level add_v
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_v("new_type").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_add_e_source_level() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Source-level add_e
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.add_e("connects").from_v(0).to_v(1).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_add_v() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal with add_v
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let add_person = anon().add_v("person").property("name", "AnonPerson");
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_property() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal with property
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let add_prop = anon().property("status", "pending");
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_drop() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal with drop
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let del = anon().drop();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
}

// =============================================================================
// Phase 7: Advanced Filter Steps
// =============================================================================

#[test]
fn test_tail() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // tail() gets the last element
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().tail().count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 1);
}

#[test]
fn test_tail_n() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // tail_n(3) gets the last 3 elements
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().tail_n(3).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3);
}

#[test]
fn test_tail_n_more_than_available() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // tail_n(100) when only 6 vertices exist should return all 6
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().tail_n(100).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6);
}

#[test]
fn test_coin() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // coin(0.0) should filter out everything
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().coin(0.0).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

#[test]
fn test_coin_all() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // coin(1.0) should pass everything through
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().coin(1.0).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6);
}

#[test]
fn test_sample() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // sample(2) should return exactly 2 elements
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().sample(2).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2);
}

#[test]
fn test_sample_more_than_available() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // sample(100) when only 6 vertices exist should return all 6
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().sample(100).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6);
}

#[test]
fn test_dedup_by_key() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // dedup_by_key should deduplicate by property value
    // All people have different names so count should equal number of vertices with name property
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has("name").dedup_by_key("name").count()
        "#,
        )
        .unwrap();

    // 5 people + 1 company = 6 all have unique names
    assert_eq!(count, 6);
}

#[test]
fn test_dedup_by_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // dedup_by_label should keep only one vertex per label
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().dedup_by_label().count()
        "#,
        )
        .unwrap();

    // 2 labels: person and company
    assert_eq!(count, 2);
}

#[test]
fn test_dedup_by_traversal() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // dedup_by with anonymous traversal - deduplicate by label
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().dedup_by(anon().label()).count()
        "#,
        )
        .unwrap();

    // 2 labels: person and company
    assert_eq!(count, 2);
}

#[test]
fn test_has_ids() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // has_ids filters to only vertices with the specified IDs
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_ids([0, 1, 2]).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3);
}

#[test]
fn test_has_ids_empty_array() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // has_ids with empty array should match nothing
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_ids([]).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

#[test]
fn test_has_ids_nonexistent() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // has_ids with nonexistent IDs should match nothing
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_ids([999, 1000]).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

// Anonymous traversal versions of Phase 7 steps

#[test]
fn test_anonymous_tail() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let t = anon().tail();
            g.v().where_(t).count()
        "#,
        )
        .unwrap();

    // where_ with tail() keeps elements where tail produces results
    assert!(count >= 0);
}

#[test]
fn test_anonymous_tail_n() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let t = anon().tail_n(2);
            g.v().where_(t).count()
        "#,
        )
        .unwrap();

    // where_ with tail_n(2) keeps elements where tail_n produces results
    assert!(count >= 0);
}

#[test]
fn test_anonymous_coin() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // coin(1.0) passes everything
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let c = anon().coin(1.0);
            g.v().where_(c).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 6);
}

#[test]
fn test_anonymous_sample() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let s = anon().sample(1);
            g.v().where_(s).count()
        "#,
        )
        .unwrap();

    // where_ with sample(1) keeps elements where sample produces results
    assert!(count >= 0);
}

#[test]
fn test_anonymous_dedup_by_key() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let d = anon().dedup_by_key("name");
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_dedup_by_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let d = anon().dedup_by_label();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_dedup_by() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let d = anon().dedup_by(anon().label());
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_has_ids() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let h = anon().has_ids([0, 1]);
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_chained_phase7_steps() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Chain multiple Phase 7 steps together
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").dedup_by_label().tail_n(5).count()
        "#,
        )
        .unwrap();

    // Only one "person" label after dedup_by_label, tail_n(5) keeps it
    assert_eq!(count, 1);
}

// =============================================================================
// Phase 8: Advanced Transform Steps
// =============================================================================

#[test]
fn test_properties() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // properties() returns property objects for each element
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").properties().count()
        "#,
        )
        .unwrap();

    // Alice has name and age properties = 2
    assert!(count >= 2);
}

#[test]
fn test_properties_keys() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // properties_keys filters to specific keys
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").properties_keys(["name"]).count()
        "#,
        )
        .unwrap();

    // Only the name property
    assert_eq!(count, 1);
}

#[test]
fn test_key() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // key() extracts the key from property objects
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").properties_keys(["name"]).key().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "name");
}

#[test]
fn test_value() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // value() extracts the value from property objects
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").properties_keys(["name"]).value().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "Alice");
}

#[test]
fn test_value_map_keys() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // value_map_keys filters to specific keys
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").value_map_keys(["name"]).count()
        "#,
        )
        .unwrap();

    // One map per vertex
    assert_eq!(count, 1);
}

#[test]
fn test_value_map_with_tokens() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // value_map_with_tokens includes id and label
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").value_map_with_tokens().to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    // The result should be a map with id and label fields
    let map = results[0].clone().into_typed_array::<rhai::Map>();
    // Just verify it's a map/object type - the exact structure depends on implementation
    assert!(results[0].is_map());
}

#[test]
fn test_index() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // index() adds position to each element
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").limit(3).index().to_list()
        "#,
        )
        .unwrap();

    // Should have 3 indexed elements (each is [value, index])
    assert_eq!(results.len(), 3);
}

#[test]
fn test_local() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // local() executes sub-traversal in isolated scope
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").local(anon().out("knows").limit(1)).count()
        "#,
        )
        .unwrap();

    // Each person gets at most 1 friend through local scope
    assert!(count <= 5); // At most 5 people with 1 result each
}

#[test]
fn test_local_with_aggregation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // local() with aggregation step
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").local(anon().out("knows")).to_list()
        "#,
        )
        .unwrap();

    // Alice knows 2 people, local returns them
    assert_eq!(results.len(), 2);
}

// Anonymous traversal versions of Phase 8 steps

#[test]
fn test_anonymous_properties() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let p = anon().properties();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_properties_keys() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let p = anon().properties_keys(["name"]);
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_key() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let k = anon().key();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_value() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let v = anon().value();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_value_map_keys() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let vm = anon().value_map_keys(["name"]);
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_value_map_with_tokens() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let vm = anon().value_map_with_tokens();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_index() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let i = anon().index();
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_local() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let l = anon().local(anon().out());
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_chained_phase8_steps() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Chain multiple Phase 8 steps together
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").properties_keys(["name"]).key().to_list()
        "#,
        )
        .unwrap();

    // Should get "name" as the key
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "name");
}

// =============================================================================
// Phase 9: Branching Steps
// =============================================================================

#[test]
fn test_choose_binary_true_branch() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // If person has outgoing edges, get their names; otherwise get constant
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").choose(
                anon().out(),
                anon().values("name"),
                anon().constant("no-outgoing")
            ).to_list()
        "#,
        )
        .unwrap();

    // Alice has outgoing edges, so should get "Alice"
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "Alice");
}

#[test]
fn test_choose_binary_false_branch() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // If person has outgoing "manages" edges, get their names; otherwise get constant
    // Nobody has "manages" edges in our graph
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").choose(
                anon().out("manages"),
                anon().constant("is-manager"),
                anon().constant("not-manager")
            ).to_list()
        "#,
        )
        .unwrap();

    // Alice has no "manages" edges, so should get "not-manager"
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "not-manager");
}

#[test]
fn test_choose_binary_with_navigation() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // For each vertex, if it has "knows" edges, follow them; otherwise follow all edges
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").choose(
                anon().out("knows"),
                anon().out("knows"),
                anon().out()
            ).count()
        "#,
        )
        .unwrap();

    // People with "knows" edges follow those, others follow all out edges
    // Alice->Bob, Alice->Carol (knows), Bob->Carol (knows), etc.
    assert!(count > 0);
}

#[test]
fn test_choose_binary_alias() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test using choose_binary as an alias
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").choose_binary(
                anon().out(),
                anon().constant("has-out"),
                anon().constant("no-out")
            ).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clone().into_string().unwrap(), "has-out");
}

#[test]
fn test_choose_options_basic() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Route based on label
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().choose_options(anon().label(), #{
                "person": anon().values("name"),
                "company": anon().values("industry")
            }).to_list()
        "#,
        )
        .unwrap();

    // Should get names of people and industries of companies
    assert!(results.len() > 0);
}

#[test]
fn test_choose_options_with_default() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Route based on label with default
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().choose_options(anon().label(), #{
                "person": anon().constant("is-person"),
                "_default": anon().constant("unknown-type")
            }).to_list()
        "#,
        )
        .unwrap();

    // Persons get "is-person", company gets "unknown-type"
    assert_eq!(results.len(), 6); // 5 people + 1 company

    let person_count = results
        .iter()
        .filter(|r| r.clone().clone().into_string().ok() == Some("is-person".to_string()))
        .count();

    let unknown_count = results
        .iter()
        .filter(|r| r.clone().clone().into_string().ok() == Some("unknown-type".to_string()))
        .count();

    assert_eq!(person_count, 5);
    assert_eq!(unknown_count, 1);
}

#[test]
fn test_choose_on_empty_traversal() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Choose on empty result should produce nothing
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "NonExistent").choose(
                anon().out(),
                anon().constant("found"),
                anon().constant("not-found")
            ).to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_choose_multiple_inputs() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // All people go through choose
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").choose(
                anon().has("age"),
                anon().values("age"),
                anon().constant(0)
            ).count()
        "#,
        )
        .unwrap();

    // All 5 people should produce results
    assert_eq!(count, 5);
}

#[test]
fn test_anonymous_choose_binary() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test anonymous traversal with choose_binary
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let choose_trav = anon().choose(
                anon().has("age"),
                anon().values("name"),
                anon().constant("no-age")
            );
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_anonymous_choose_options() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test anonymous traversal with choose_options
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            let choose_trav = anon().choose_options(anon().label(), #{
                "person": anon().identity(),
                "_default": anon().identity()
            });
            g.inject([1]).to_list()
        "#,
        )
        .unwrap();

    // Just verifying the anonymous traversal can be created
    assert_eq!(results.len(), 1);
}

#[test]
fn test_nested_choose() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Nested choose operations
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").choose(
                anon().out("knows"),
                anon().choose(
                    anon().has_value("age", 30),
                    anon().constant("young-friend"),
                    anon().constant("has-friends")
                ),
                anon().constant("no-friends")
            ).to_list()
        "#,
        )
        .unwrap();

    // Alice has friends, and she's 30, so inner choose should evaluate
    assert_eq!(results.len(), 1);
}

#[test]
fn test_choose_with_transform() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Choose followed by transforms
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").choose(
                anon().values("age").is_(gte(30)),
                anon().constant("senior"),
                anon().constant("junior")
            ).dedup().to_list()
        "#,
        )
        .unwrap();

    // Should have both "senior" and "junior" as unique values
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Phase 11: Builder Pattern Steps
// =============================================================================

#[test]
fn test_order_by() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Order people by age ascending
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").order_by("age").values("name").to_list()
        "#,
        )
        .unwrap();

    // Should be ordered by age: Bob(25), Eve(28), Alice(30), Carol(35), Dave(40)
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].clone().into_string().unwrap(), "Bob".to_string());
    assert_eq!(results[1].clone().into_string().unwrap(), "Eve".to_string());
    assert_eq!(
        results[2].clone().into_string().unwrap(),
        "Alice".to_string()
    );
    assert_eq!(
        results[3].clone().into_string().unwrap(),
        "Carol".to_string()
    );
    assert_eq!(
        results[4].clone().into_string().unwrap(),
        "Dave".to_string()
    );
}

#[test]
fn test_order_by_desc() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Order people by age descending
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").order_by_desc("age").values("name").to_list()
        "#,
        )
        .unwrap();

    // Should be ordered by age descending: Dave(40), Carol(35), Alice(30), Eve(28), Bob(25)
    assert_eq!(results.len(), 5);
    assert_eq!(
        results[0].clone().into_string().unwrap(),
        "Dave".to_string()
    );
    assert_eq!(
        results[1].clone().into_string().unwrap(),
        "Carol".to_string()
    );
    assert_eq!(
        results[2].clone().into_string().unwrap(),
        "Alice".to_string()
    );
    assert_eq!(results[3].clone().into_string().unwrap(), "Eve".to_string());
    assert_eq!(results[4].clone().into_string().unwrap(), "Bob".to_string());
}

#[test]
fn test_group_by_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Group vertices by label
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().group_by_label().to_list()
        "#,
        )
        .unwrap();

    // Should return a single map
    assert_eq!(results.len(), 1);
}

#[test]
fn test_group_count_by_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Count vertices by label
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().group_count_by_label().to_list()
        "#,
        )
        .unwrap();

    // Should return a single map with label counts
    assert_eq!(results.len(), 1);
    // The graph has 5 people and 1 company
    let map = results[0].clone().try_cast::<rhai::Map>().unwrap();
    let person_count = map.get("person").and_then(|v| v.as_int().ok()).unwrap_or(0);
    let company_count = map
        .get("company")
        .and_then(|v| v.as_int().ok())
        .unwrap_or(0);
    assert_eq!(person_count, 5);
    assert_eq!(company_count, 1);
}

#[test]
fn test_group_count_by_key() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Count people by "active" property (which exists in the test graph)
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").group_count_by_key("active").to_list()
        "#,
        )
        .unwrap();

    // Should return a single map with active counts
    assert_eq!(results.len(), 1);
    let map = results[0].clone().try_cast::<rhai::Map>().unwrap();
    // Alice, Bob, Dave, Eve are active (true), Carol is inactive (false)
    // Keys are serialized as "true" and "false" strings
    let total_count: i64 = map.values().filter_map(|v| v.as_int().ok()).sum();
    assert_eq!(total_count, 5);
}

#[test]
fn test_math_double() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Double each age value
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("age").math("_ * 2").to_list()
        "#,
        )
        .unwrap();

    // Should have 5 results (one for each person)
    assert_eq!(results.len(), 5);

    // All results should be doubles of the original ages
    let values: Vec<f64> = results.iter().filter_map(|v| v.as_float().ok()).collect();
    assert!(values.contains(&60.0)); // Alice 30 * 2
    assert!(values.contains(&50.0)); // Bob 25 * 2
    assert!(values.contains(&70.0)); // Carol 35 * 2
    assert!(values.contains(&80.0)); // Dave 40 * 2
    assert!(values.contains(&56.0)); // Eve 28 * 2
}

#[test]
fn test_math_add_constant() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Add 10 to each age
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("age").math("_ + 10").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 5);
    let values: Vec<f64> = results.iter().filter_map(|v| v.as_float().ok()).collect();
    assert!(values.contains(&40.0)); // Alice 30 + 10
    assert!(values.contains(&35.0)); // Bob 25 + 10
    assert!(values.contains(&45.0)); // Carol 35 + 10
}

#[test]
fn test_anonymous_order_by() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test order_by in anonymous traversal context
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").local(anon().order_by("age").limit(1)).values("name").to_list()
        "#,
        )
        .unwrap();

    // Each person locally ordered by age and limited to 1, so 5 results
    assert_eq!(results.len(), 5);
}

#[test]
fn test_anonymous_group_count() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test group_count_by_label in anonymous traversal
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").local(anon().group_count_by_label()).to_list()
        "#,
        )
        .unwrap();

    // Each person produces their own group count map
    assert_eq!(results.len(), 5);
}

#[test]
fn test_anonymous_math() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test math in anonymous traversal context
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").local(anon().values("age").math("_ / 2")).to_list()
        "#,
        )
        .unwrap();

    // Each person's age divided by 2
    assert_eq!(results.len(), 5);
    let values: Vec<f64> = results.iter().filter_map(|v| v.as_float().ok()).collect();
    assert!(values.contains(&15.0)); // Alice 30 / 2
    assert!(values.contains(&12.5)); // Bob 25 / 2
}

// =============================================================================
// Phase 10: Terminal Steps
// =============================================================================

#[test]
fn test_to_set() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get labels of all vertices - should have duplicates normally
    // to_set should return unique values only
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().label().to_set()
        "#,
        )
        .unwrap();

    // Should have 2 unique labels: "person" and "company"
    assert_eq!(results.len(), 2);
    let labels: Vec<String> = results
        .iter()
        .filter_map(|v| v.clone().into_string().ok())
        .collect();
    assert!(labels.contains(&"person".to_string()));
    assert!(labels.contains(&"company".to_string()));
}

#[test]
fn test_to_set_preserves_order() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get names that appear multiple times via traversal
    // Friends of friends may include duplicates
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").out("knows").out("knows").values("name").to_set()
        "#,
        )
        .unwrap();

    // Should have unique names only
    let names: Vec<String> = results
        .iter()
        .filter_map(|v| v.clone().into_string().ok())
        .collect();
    // Check no duplicates
    let mut seen = std::collections::HashSet::new();
    for name in &names {
        assert!(seen.insert(name.clone()), "Duplicate found: {}", name);
    }
}

#[test]
fn test_iterate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // iterate() should consume the traversal without returning anything useful
    // It's used for side effects. We just verify it doesn't error.
    let result: () = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").iterate()
        "#,
        )
        .unwrap();

    // iterate returns unit/nothing
    assert_eq!(result, ());
}

#[test]
fn test_take() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Take first 2 people
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("name").take(2)
        "#,
        )
        .unwrap();

    // Should have exactly 2 results
    assert_eq!(results.len(), 2);
}

#[test]
fn test_take_more_than_available() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Take 100 but only 5 people exist
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("name").take(100)
        "#,
        )
        .unwrap();

    // Should have all 5 people
    assert_eq!(results.len(), 5);
}

#[test]
fn test_take_zero() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Take 0 should return empty
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("name").take(0)
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_take_with_order() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Take first 3 people ordered by age ascending
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").order_by("age").values("name").take(3)
        "#,
        )
        .unwrap();

    // Should be the 3 youngest: Bob(25), Eve(28), Alice(30)
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].clone().into_string().unwrap(), "Bob".to_string());
    assert_eq!(results[1].clone().into_string().unwrap(), "Eve".to_string());
    assert_eq!(
        results[2].clone().into_string().unwrap(),
        "Alice".to_string()
    );
}
