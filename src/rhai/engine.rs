//! The main Rhai engine for script execution.
//!
//! This module provides `RhaiEngine`, the primary interface for executing Rhai scripts
//! with Intersteller's graph traversal API.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use intersteller::prelude::*;
//! use intersteller::rhai::RhaiEngine;
//!
//! // Create an engine
//! let engine = RhaiEngine::new();
//!
//! // Create and populate a graph
//! let mut storage = InMemoryGraph::new();
//! storage.add_vertex("person", [("name", "Alice")].into());
//! let graph = Graph::new(storage);
//!
//! // Execute a script
//! let script = r#"
//!     let g = graph.traversal();
//!     g.v().has_label("person").values("name").to_list()
//! "#;
//!
//! let result = engine.eval_with_graph(&graph, script)?;
//! ```

use rhai::{Dynamic, Engine, Scope, AST};
use std::sync::Arc;

use super::anonymous::{create_anonymous_factory, register_anonymous};
use super::error::{RhaiError, RhaiResult};
use super::predicates::register_predicates;
use super::traversal::{register_traversal, RhaiGraph};
use super::types::register_types;
use crate::graph::Graph;

/// The main Rhai engine for executing scripts with Intersteller graph support.
///
/// `RhaiEngine` wraps a Rhai `Engine` with all Intersteller types and functions
/// pre-registered, making it easy to execute graph traversal scripts.
///
/// # Example
///
/// ```rust,ignore
/// let engine = RhaiEngine::new();
///
/// // Execute a simple expression
/// let result: i64 = engine.eval("1 + 2").unwrap();
/// assert_eq!(result, 3);
///
/// // Execute with a graph
/// let result = engine.eval_with_graph(&graph, r#"
///     let g = graph.traversal();
///     g.v().count()
/// "#).unwrap();
/// ```
pub struct RhaiEngine {
    engine: Engine,
}

impl Default for RhaiEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RhaiEngine {
    /// Create a new RhaiEngine with all Intersteller bindings registered.
    pub fn new() -> Self {
        let mut engine = Engine::new();

        // Register all Intersteller types and functions
        register_types(&mut engine);
        register_predicates(&mut engine);
        register_traversal(&mut engine);
        register_anonymous(&mut engine);

        RhaiEngine { engine }
    }

    /// Create a RhaiEngine from an existing Rhai Engine.
    ///
    /// This adds Intersteller bindings to an existing engine that may have
    /// custom configuration or additional registered functions.
    pub fn with_engine(mut engine: Engine) -> Self {
        register_types(&mut engine);
        register_predicates(&mut engine);
        register_traversal(&mut engine);
        register_anonymous(&mut engine);

        RhaiEngine { engine }
    }

    /// Get a reference to the underlying Rhai engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get a mutable reference to the underlying Rhai engine.
    pub fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }

    /// Compile a script to an AST for caching and repeated execution.
    ///
    /// Pre-compiling scripts improves performance when the same script is
    /// executed multiple times.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let engine = RhaiEngine::new();
    /// let ast = engine.compile(r#"
    ///     let g = graph.traversal();
    ///     g.v().count()
    /// "#)?;
    ///
    /// // Execute multiple times with different graphs
    /// let count1 = engine.eval_ast_with_graph(&graph1, &ast)?;
    /// let count2 = engine.eval_ast_with_graph(&graph2, &ast)?;
    /// ```
    pub fn compile(&self, script: &str) -> RhaiResult<AST> {
        self.engine.compile(script).map_err(RhaiError::from)
    }

    /// Evaluate a script without a graph context.
    ///
    /// This is useful for testing predicates or simple expressions.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let engine = RhaiEngine::new();
    /// let result: i64 = engine.eval("2 + 2")?;
    /// assert_eq!(result, 4);
    /// ```
    pub fn eval<T>(&self, script: &str) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.engine.eval(script).map_err(RhaiError::from)
    }

    /// Evaluate a script with a graph bound to the `graph` variable.
    ///
    /// The script has access to:
    /// - `graph` - A `RhaiGraph` wrapper around the provided graph
    /// - `A` - The anonymous traversal factory (similar to Gremlin's `__`)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let engine = RhaiEngine::new();
    /// let result: i64 = engine.eval_with_graph(&graph, r#"
    ///     let g = graph.traversal();
    ///     g.v().count()
    /// "#)?;
    /// ```
    pub fn eval_with_graph<T>(&self, graph: &Graph, script: &str) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .eval_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Evaluate a script with a graph and return a Dynamic result.
    ///
    /// This is useful when the result type is not known at compile time.
    pub fn eval_with_graph_dynamic(&self, graph: &Graph, script: &str) -> RhaiResult<Dynamic> {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .eval_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Evaluate a pre-compiled AST with a graph context.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let engine = RhaiEngine::new();
    /// let ast = engine.compile("graph.traversal().v().count()")?;
    ///
    /// let count: i64 = engine.eval_ast_with_graph(&graph, &ast)?;
    /// ```
    pub fn eval_ast_with_graph<T>(&self, graph: &Graph, ast: &AST) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .eval_ast_with_scope(&mut scope, ast)
            .map_err(RhaiError::from)
    }

    /// Evaluate a pre-compiled AST with a graph and return a Dynamic result.
    pub fn eval_ast_with_graph_dynamic(&self, graph: &Graph, ast: &AST) -> RhaiResult<Dynamic> {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .eval_ast_with_scope(&mut scope, ast)
            .map_err(RhaiError::from)
    }

    /// Evaluate a script with a shared Arc<Graph>.
    ///
    /// This is more efficient when the graph is already wrapped in an Arc,
    /// as it avoids an extra clone.
    pub fn eval_with_arc_graph<T>(&self, graph: Arc<Graph>, script: &str) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = Scope::new();
        scope.push("graph", RhaiGraph::from_arc(graph));
        scope.push("A", create_anonymous_factory());

        self.engine
            .eval_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Run a script with a graph, ignoring the return value.
    ///
    /// This is useful for scripts that perform side effects without
    /// returning a meaningful value.
    pub fn run_with_graph(&self, graph: &Graph, script: &str) -> RhaiResult<()> {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .run_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Create a scope with the graph and anonymous factory pre-bound.
    fn create_graph_scope(&self, graph: &Graph) -> Scope<'static> {
        let mut scope = Scope::new();

        // Create a RhaiGraph from the graph reference.
        // RhaiGraph stores an Arc<Graph> internally, but Graph doesn't implement Clone.
        // We need to create a new Graph that shares the same storage.
        // The simplest approach is to create a new Graph with the same Arc<storage>.
        let rhai_graph = RhaiGraph::new(graph.share());
        scope.push("graph", rhai_graph);

        // Add the anonymous traversal factory
        scope.push("A", create_anonymous_factory());

        scope
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryGraph;
    use crate::value::Value;
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        let alice = storage.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
        );

        let bob = storage.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::Int(25)),
            ]
            .into_iter()
            .collect(),
        );

        let carol = storage.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::Int(35)),
            ]
            .into_iter()
            .collect(),
        );

        storage
            .add_edge(alice, bob, "knows", HashMap::new())
            .unwrap();
        storage
            .add_edge(alice, carol, "knows", HashMap::new())
            .unwrap();
        storage
            .add_edge(bob, carol, "knows", HashMap::new())
            .unwrap();

        Graph::new(storage)
    }

    #[test]
    fn test_engine_creation() {
        let _engine = RhaiEngine::new();
        // Engine created successfully
    }

    #[test]
    fn test_eval_simple() {
        let engine = RhaiEngine::new();
        let result: i64 = engine.eval("2 + 2").unwrap();
        assert_eq!(result, 4);
    }

    #[test]
    fn test_eval_with_graph_count() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        let count: i64 = engine
            .eval_with_graph(
                &graph,
                r#"
                let g = graph.traversal();
                g.v().count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 3);
    }

    #[test]
    fn test_eval_with_graph_filter() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        let count: i64 = engine
            .eval_with_graph(
                &graph,
                r#"
                let g = graph.traversal();
                g.v().has_label("person").count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 3);
    }

    #[test]
    fn test_eval_with_graph_predicate() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        let count: i64 = engine
            .eval_with_graph(
                &graph,
                r#"
                let g = graph.traversal();
                g.v().has_where("age", gte(30)).count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 2); // Alice (30) and Carol (35)
    }

    #[test]
    fn test_eval_with_graph_navigation() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        let count: i64 = engine
            .eval_with_graph(
                &graph,
                r#"
                let g = graph.traversal();
                g.v().has_value("name", "Alice").out().count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 2); // Alice knows Bob and Carol
    }

    #[test]
    fn test_compile_and_eval() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        let ast = engine
            .compile(
                r#"
                let g = graph.traversal();
                g.v().count()
            "#,
            )
            .unwrap();

        let count: i64 = engine.eval_ast_with_graph(&graph, &ast).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_anonymous_factory_in_script() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        // Test that the A factory is available
        let result: rhai::Array = engine
            .eval_with_graph(
                &graph,
                r#"
                let anon = A.out().has_label("person");
                []  // Return empty array for now - just testing factory creation
            "#,
            )
            .unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_run_with_graph() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        // Should not error
        engine
            .run_with_graph(
                &graph,
                r#"
                let g = graph.traversal();
                let count = g.v().count();
            "#,
            )
            .unwrap();
    }

    #[test]
    fn test_eval_dynamic() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        let result = engine
            .eval_with_graph_dynamic(
                &graph,
                r#"
                let g = graph.traversal();
                g.v().count()
            "#,
            )
            .unwrap();

        assert_eq!(result.as_int().unwrap(), 3);
    }

    #[test]
    fn test_compile_error() {
        let engine = RhaiEngine::new();

        let result = engine.compile("this is invalid syntax {{{");
        assert!(result.is_err());

        if let Err(RhaiError::Compile(msg)) = result {
            assert!(!msg.is_empty());
        } else {
            panic!("Expected compile error");
        }
    }

    #[test]
    fn test_runtime_error() {
        let engine = RhaiEngine::new();

        let result: Result<i64, _> = engine.eval("undefined_function()");
        assert!(result.is_err());
    }
}
