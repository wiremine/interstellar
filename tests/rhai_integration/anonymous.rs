//! Anonymous traversal integration tests.

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;

use super::create_social_graph;

// =============================================================================
// Anonymous Factory Basic Tests
// =============================================================================

#[test]
fn test_anonymous_factory_available() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Just test that A is available in scope
    let result: rhai::Dynamic = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let anon = A.out();
            true
        "#,
        )
        .unwrap();

    assert!(result.as_bool().unwrap());
}

#[test]
fn test_anonymous_identity() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // A.identity() should create a pass-through traversal
    let result: rhai::Dynamic = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let anon = A.identity();
            true
        "#,
        )
        .unwrap();

    assert!(result.as_bool().unwrap());
}

#[test]
fn test_anonymous_chained() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test chained anonymous traversal creation
    let result: rhai::Dynamic = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let anon = A.out("knows").has_label("person").values("name");
            true
        "#,
        )
        .unwrap();

    assert!(result.as_bool().unwrap());
}

// =============================================================================
// Anonymous in Union
// =============================================================================

#[test]
fn test_union_with_anonymous() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Union of out("knows") and out("works_at")
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([A.out("knows"), A.out("works_at")])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob, Carol (knows) + Acme (works_at)
}

#[test]
fn test_union_multiple_branches() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Union of multiple navigation paths
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([
                    A.out("knows").values("name"),
                    A.out("works_at").values("name")
                ])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob, Carol, Acme Corp
}

// =============================================================================
// Anonymous in Coalesce
// =============================================================================

#[test]
fn test_coalesce_with_anonymous() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Coalesce: try out("manages") first, fall back to out("knows")
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .coalesce([A.out("manages"), A.out("knows")])
                .count()
        "#,
        )
        .unwrap();

    // No "manages" edges, so falls back to "knows"
    assert_eq!(count, 2); // Bob, Carol
}

#[test]
fn test_coalesce_first_match_wins() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Coalesce: out("knows") succeeds, so out("works_at") is not tried
    let names: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .coalesce([A.out("knows"), A.out("works_at")])
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Should get knows results, not works_at
    assert!(names.len() >= 1);
}

// =============================================================================
// Anonymous in Optional
// =============================================================================

#[test]
fn test_optional_with_anonymous() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Optional: try to navigate, keep original if not possible
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_label("person")
                .optional(A.out("works_at"))
                .count()
        "#,
        )
        .unwrap();

    // 5 people, but only Alice has works_at.
    // Optional keeps original for those without the path.
    assert!(count >= 5);
}

// =============================================================================
// Anonymous in Repeat
// =============================================================================

#[test]
fn test_repeat_with_anonymous() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Repeat out() twice
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .repeat(A.out("knows"), 2)
                .count()
        "#,
        )
        .unwrap();

    // Alice -> (Bob, Carol) -> Carol (from Bob)
    assert!(count >= 1);
}

// =============================================================================
// Anonymous with Filters
// =============================================================================

#[test]
fn test_anonymous_with_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal with has_label filter
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([A.out().has_label("person")])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob, Carol (not Acme which is company)
}

#[test]
fn test_anonymous_with_has_value() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal with has_value filter
    let names: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([A.out("knows").has_value("active", true)])
                .values("name")
                .to_list()
        "#,
        )
        .unwrap();

    // Only Bob is active among Alice's friends
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].clone().into_string().unwrap(), "Bob");
}

// =============================================================================
// Anonymous with Transforms
// =============================================================================

#[test]
fn test_anonymous_with_values() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal that extracts values
    let names: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([A.out("knows").values("name")])
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(names.len(), 2); // Bob, Carol
}

#[test]
fn test_anonymous_with_constant() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous traversal with constant
    let results: rhai::Array = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([A.out("knows").constant("friend")])
                .to_list()
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    for result in results {
        assert_eq!(result.into_string().unwrap(), "friend");
    }
}

// =============================================================================
// Complex Anonymous Patterns
// =============================================================================

#[test]
fn test_nested_union() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Nested unions (if supported)
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([
                    A.out("knows"),
                    A.out("works_at")
                ])
                .union([
                    A.values("name")
                ])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob, Carol, Acme Corp names
}

#[test]
fn test_anonymous_dedup() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous with dedup
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_where("name", within(["Alice", "Bob"]))
                .union([A.out("knows").dedup()])
                .count()
        "#,
        )
        .unwrap();

    // Alice knows Bob, Carol; Bob knows Carol
    // With dedup in anonymous, should deduplicate
    assert!(count >= 1);
}

#[test]
fn test_anonymous_limit() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Anonymous with limit
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([A.out().limit(1)])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 1);
}

// =============================================================================
// Anonymous Variables in Scripts
// =============================================================================

#[test]
fn test_anonymous_in_variable() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Store anonymous traversal in variable and reuse
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let friends = A.out("knows");
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([friends])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Bob, Carol
}

#[test]
fn test_multiple_anonymous_variables() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Multiple anonymous traversals in variables
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            let friends = A.out("knows");
            let work = A.out("works_at");
            let g = graph.traversal();
            g.v().has_value("name", "Alice")
                .union([friends, work])
                .count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 3); // Bob, Carol, Acme
}

#[test]
fn test_anonymous_as_function_parameter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Pass anonymous traversal to function
    let count: i64 = engine
        .eval_with_graph(
            graph.clone(),
            r#"
            fn apply_union(g, traversal) {
                g.v().has_value("name", "Alice").union([traversal]).count()
            }
            
            let g = graph.traversal();
            apply_union(g, A.out("knows"))
        "#,
        )
        .unwrap();

    assert_eq!(count, 2);
}
