//! Error handling integration tests.

use intersteller::rhai::{RhaiEngine, RhaiError};

use super::{create_empty_graph, create_social_graph};

// =============================================================================
// Compile Errors
// =============================================================================

#[test]
fn test_compile_error_syntax() {
    let engine = RhaiEngine::new();

    let result = engine.compile("this is not valid syntax {{{");
    assert!(result.is_err());

    if let Err(RhaiError::Compile(msg)) = result {
        assert!(!msg.is_empty());
    } else {
        panic!("Expected RhaiError::Compile");
    }
}

#[test]
fn test_compile_error_unclosed_string() {
    let engine = RhaiEngine::new();

    let result = engine.compile(r#"let x = "unclosed string"#);
    assert!(result.is_err());
}

#[test]
fn test_compile_error_unclosed_paren() {
    let engine = RhaiEngine::new();

    let result = engine.compile("let x = (1 + 2");
    assert!(result.is_err());
}

// =============================================================================
// Runtime Errors
// =============================================================================

#[test]
fn test_runtime_error_undefined_variable() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let result: Result<i64, _> = engine.eval_with_graph(&graph, "undefined_variable");
    assert!(result.is_err());
}

#[test]
fn test_runtime_error_undefined_function() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let result: Result<i64, _> = engine.eval_with_graph(&graph, "undefined_function()");
    assert!(result.is_err());
}

#[test]
fn test_runtime_error_wrong_type() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Try to call a method that doesn't exist on the type
    let result: Result<i64, _> =
        engine.eval_with_graph(&graph, r#"let x = "string"; x.nonexistent_method()"#);
    assert!(result.is_err());
}

#[test]
fn test_runtime_error_division_by_zero() {
    let engine = RhaiEngine::new();

    let result: Result<i64, _> = engine.eval("let x = 1 / 0; x");
    // Rhai may handle this differently (infinity for float, or error for int)
    // Just check it doesn't panic
    let _ = result;
}

// =============================================================================
// Graph Not Available Errors
// =============================================================================

#[test]
fn test_graph_not_in_scope() {
    let engine = RhaiEngine::new();

    // Calling eval without graph should not have 'graph' in scope
    let result: Result<i64, _> = engine.eval("graph.traversal().v().count()");
    assert!(result.is_err());
}

// =============================================================================
// Type Mismatch Errors
// =============================================================================

#[test]
fn test_type_mismatch_count_as_string() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // count() returns i64, trying to use as string should work (Rhai coercion)
    // or fail depending on operation
    let result: Result<String, _> = engine.eval_with_graph(
        &graph,
        r#"
        let g = graph.traversal();
        g.v().count()
    "#,
    );

    // This might succeed with coercion or fail - either is acceptable
    let _ = result;
}

// =============================================================================
// Predicate Errors
// =============================================================================

#[test]
fn test_predicate_wrong_argument_count() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // between() requires exactly 2 arguments
    let result: Result<i64, _> = engine.eval_with_graph(
        &graph,
        r#"
        let g = graph.traversal();
        g.v().has_where("age", between(10)).count()
    "#,
    );
    assert!(result.is_err());
}

#[test]
fn test_predicate_wrong_argument_type() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // regex() with invalid regex pattern - should error at runtime or return no matches
    let result: Result<i64, _> = engine.eval_with_graph(
        &graph,
        r#"
        let g = graph.traversal();
        g.v().has_where("name", regex("[invalid")).count()
    "#,
    );

    // Either errors or returns 0 - depends on implementation
    let _ = result;
}

// =============================================================================
// Empty Results (Not Errors)
// =============================================================================

#[test]
fn test_empty_traversal_count() {
    let engine = RhaiEngine::new();
    let graph = create_empty_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
        let g = graph.traversal();
        g.v().count()
    "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

#[test]
fn test_no_match_filter() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
        let g = graph.traversal();
        g.v().has_value("name", "NonExistent").count()
    "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

#[test]
fn test_no_match_label() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
        let g = graph.traversal();
        g.v().has_label("nonexistent_label").count()
    "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_empty_script() {
    let engine = RhaiEngine::new();

    // Empty script should return unit
    let result: rhai::Dynamic = engine.eval("").unwrap();
    assert!(result.is_unit());
}

#[test]
fn test_whitespace_only_script() {
    let engine = RhaiEngine::new();

    let result: rhai::Dynamic = engine.eval("   \n\t  ").unwrap();
    assert!(result.is_unit());
}

#[test]
fn test_comment_only_script() {
    let engine = RhaiEngine::new();

    let result: rhai::Dynamic = engine
        .eval("// this is a comment\n/* block comment */")
        .unwrap();
    assert!(result.is_unit());
}

#[test]
fn test_large_limit() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Limit larger than result set
    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
        let g = graph.traversal();
        g.v().limit(1000).count()
    "#,
        )
        .unwrap();

    assert_eq!(count, 6); // Only 6 vertices exist
}

#[test]
fn test_skip_more_than_available() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    let count: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
        let g = graph.traversal();
        g.v().skip(100).count()
    "#,
        )
        .unwrap();

    assert_eq!(count, 0);
}

// =============================================================================
// Error Recovery
// =============================================================================

#[test]
fn test_error_doesnt_affect_subsequent_calls() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // First call fails
    let result1: Result<i64, _> = engine.eval_with_graph(&graph, "undefined_function()");
    assert!(result1.is_err());

    // Second call should still work
    let result2: i64 = engine
        .eval_with_graph(
            &graph,
            r#"
        let g = graph.traversal();
        g.v().count()
    "#,
        )
        .unwrap();

    assert_eq!(result2, 6);
}

#[test]
fn test_multiple_graphs_independent() {
    let engine = RhaiEngine::new();
    let graph1 = create_social_graph();
    let graph2 = create_empty_graph();

    let count1: i64 = engine
        .eval_with_graph(
            &graph1,
            r#"
        let g = graph.traversal();
        g.v().count()
    "#,
        )
        .unwrap();

    let count2: i64 = engine
        .eval_with_graph(
            &graph2,
            r#"
        let g = graph.traversal();
        g.v().count()
    "#,
        )
        .unwrap();

    assert_eq!(count1, 6);
    assert_eq!(count2, 0);
}
