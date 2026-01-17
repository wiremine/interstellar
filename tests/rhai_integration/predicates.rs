//! Predicate integration tests.

use intersteller::prelude::*;
use intersteller::rhai::RhaiEngine;

use super::create_social_graph;

// =============================================================================
// Comparison Predicates
// =============================================================================

#[test]
fn test_eq_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", eq(30)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 1); // Alice
}

#[test]
fn test_neq_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_label("person").has_where("age", neq(30)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 4); // Everyone except Alice
}

#[test]
fn test_lt_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", lt(30)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob(25), Eve(28)
}

#[test]
fn test_lte_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", lte(30)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob(25), Eve(28), Alice(30)
}

#[test]
fn test_gt_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", gt(30)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Carol(35), Dave(40)
}

#[test]
fn test_gte_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", gte(30)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Alice(30), Carol(35), Dave(40)
}

// =============================================================================
// Range Predicates
// =============================================================================

#[test]
fn test_between_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", between(25, 35)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob(25), Eve(28), Alice(30) - [25, 35) exclusive end
}

#[test]
fn test_inside_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", inside(25, 35)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Eve(28), Alice(30) - exclusive bounds
}

#[test]
fn test_outside_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", outside(28, 35)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob(25), Dave(40)
}

// =============================================================================
// Collection Predicates
// =============================================================================

#[test]
fn test_within_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("name", within(["Alice", "Bob", "Charlie"])).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Alice, Bob (Charlie doesn't exist)
}

#[test]
fn test_without_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_label("person").has_where("name", without(["Alice", "Bob"])).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Carol, Dave, Eve
}

// =============================================================================
// String Predicates
// =============================================================================

#[test]
fn test_containing_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("name", containing("a")).count()
        "#,
        )
        .unwrap();

    // Carol, Dave (case-sensitive, so not Alice)
    assert!(count >= 2);
}

#[test]
fn test_starting_with_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("name", starting_with("A")).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Alice, Acme Corp
}

#[test]
fn test_ending_with_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_label("person").has_where("name", ending_with("e")).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Alice, Dave, Eve
}

#[test]
fn test_regex_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_label("person").has_where("name", regex("^[A-D]")).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 4); // Alice, Bob, Carol, Dave (names starting with A-D)
}

// =============================================================================
// Logical Predicates
// =============================================================================

#[test]
fn test_not_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", pred_not(gt(30))).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob(25), Eve(28), Alice(30)
}

#[test]
fn test_and_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", pred_and(gte(25), lte(30))).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob(25), Eve(28), Alice(30)
}

#[test]
fn test_or_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            g.v().has_where("age", pred_or(eq(25), eq(40))).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob(25), Dave(40)
}

#[test]
fn test_complex_predicate_combination() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Find people aged 25-30 OR older than 35
    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let g = graph.traversal();
            let young_adults = pred_and(gte(25), lte(30));
            let seniors = gt(35);
            g.v().has_where("age", pred_or(young_adults, seniors)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 4); // Bob(25), Eve(28), Alice(30), Dave(40)
}

// =============================================================================
// Predicates with Variables
// =============================================================================

#[test]
fn test_predicate_with_variable() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            let min_age = 28;
            let max_age = 35;
            let g = graph.traversal();
            g.v().has_where("age", between(min_age, max_age)).count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Eve(28), Alice(30) - [28, 35) exclusive end
}

#[test]
fn test_predicate_in_function() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
            fn count_by_age_range(g, min, max) {
                g.v().has_where("age", between(min, max)).count()
            }
            
            let g = graph.traversal();
            count_by_age_range(g, 25, 30)
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob(25), Eve(28) - [25, 30) exclusive end
}
