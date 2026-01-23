//! The main Rhai engine for script execution.
//!
//! This module provides `RhaiEngine`, the primary interface for executing Rhai scripts
//! with Interstellar's graph traversal API.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use interstellar::prelude::*;
//! use interstellar::rhai::RhaiEngine;
//! use std::sync::Arc;
//!
//! // Create an engine
//! let engine = RhaiEngine::new();
//!
//! // Create and populate a graph (wrapped in Arc for sharing)
//! let graph = Arc::new(Graph::new());
//! graph.add_vertex("person", [("name", "Alice".into())].into());
//!
//! // Execute a script
//! let script = r#"
//!     let g = graph.gremlin();
//!     g.v().has_label("person").values("name").to_list()
//! "#;
//!
//! let result = engine.eval_with_graph(graph.clone(), script)?;
//! ```

use rhai::{Dynamic, Engine, Scope, AST};
use std::sync::Arc;

use super::anonymous::{create_anonymous_factory, register_anonymous};
use super::error::{RhaiError, RhaiResult};
use super::predicates::register_predicates;
use super::traversal::{register_traversal, RhaiGraph};
use super::types::register_types;
#[cfg(feature = "mmap")]
use crate::storage::CowMmapGraph;
use crate::storage::Graph;

/// The main Rhai engine for executing scripts with Interstellar graph support.
///
/// `RhaiEngine` wraps a Rhai `Engine` with all Interstellar types and functions
/// pre-registered, making it easy to execute graph traversal scripts.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let engine = RhaiEngine::new();
///
/// // Execute a simple expression
/// let result: i64 = engine.eval("1 + 2").unwrap();
/// assert_eq!(result, 3);
///
/// // Execute with a graph (wrapped in Arc)
/// let graph = Arc::new(Graph::new());
/// let result = engine.eval_with_graph(graph, r#"
///     let g = graph.gremlin();
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
    /// Create a new RhaiEngine with all Interstellar bindings registered.
    ///
    /// The engine is configured with safety limits to prevent resource exhaustion:
    /// - Max operations: 1,000,000 (prevents infinite loops)
    /// - Max expression depth: 64 (prevents deep recursion)
    /// - Max call stack levels: 64 (prevents call stack overflow)
    /// - Max array size: 10,000 elements
    /// - Max map size: 10,000 entries
    /// - Max string size: 1MB
    ///
    /// The `eval` function is disabled for security.
    pub fn new() -> Self {
        let mut engine = Engine::new();

        // Safety limits to prevent resource exhaustion
        engine.set_max_operations(1_000_000);
        engine.set_max_expr_depths(64, 64); // global depth, function depth
        engine.set_max_call_levels(64);
        engine.set_max_array_size(10_000);
        engine.set_max_map_size(10_000);
        engine.set_max_string_size(1_000_000); // 1MB

        // Disable eval for security
        engine.disable_symbol("eval");

        // Register all Interstellar types and functions
        register_types(&mut engine);
        register_predicates(&mut engine);
        register_traversal(&mut engine);
        register_anonymous(&mut engine);

        RhaiEngine { engine }
    }

    /// Create a RhaiEngine from an existing Rhai Engine.
    ///
    /// This adds Interstellar bindings to an existing engine that may have
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
    ///     let g = graph.gremlin();
    ///     g.v().count()
    /// "#)?;
    ///
    /// // Execute multiple times with different graphs
    /// let count1 = engine.eval_ast_with_graph(graph1, &ast)?;
    /// let count2 = engine.eval_ast_with_graph(graph2, &ast)?;
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
    /// The graph must be wrapped in an `Arc` for sharing with the script scope.
    ///
    /// The script has access to:
    /// - `graph` - A `RhaiGraph` wrapper around the provided graph
    /// - `A` - The anonymous traversal factory (similar to Gremlin's `__`)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    ///
    /// let engine = RhaiEngine::new();
    /// let graph = Arc::new(Graph::new());
    /// // ... populate graph ...
    ///
    /// let result: i64 = engine.eval_with_graph(graph, r#"
    ///     let g = graph.gremlin();
    ///     g.v().count()
    /// "#)?;
    /// ```
    pub fn eval_with_graph<T>(&self, graph: Arc<Graph>, script: &str) -> RhaiResult<T>
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
    pub fn eval_with_graph_dynamic(&self, graph: Arc<Graph>, script: &str) -> RhaiResult<Dynamic> {
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
    /// use std::sync::Arc;
    ///
    /// let engine = RhaiEngine::new();
    /// let ast = engine.compile("graph.gremlin().v().count()")?;
    /// let graph = Arc::new(Graph::new());
    ///
    /// let count: i64 = engine.eval_ast_with_graph(graph, &ast)?;
    /// ```
    pub fn eval_ast_with_graph<T>(&self, graph: Arc<Graph>, ast: &AST) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .eval_ast_with_scope(&mut scope, ast)
            .map_err(RhaiError::from)
    }

    /// Evaluate a pre-compiled AST with a graph and return a Dynamic result.
    pub fn eval_ast_with_graph_dynamic(&self, graph: Arc<Graph>, ast: &AST) -> RhaiResult<Dynamic> {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .eval_ast_with_scope(&mut scope, ast)
            .map_err(RhaiError::from)
    }

    /// Run a script with a graph, ignoring the return value.
    ///
    /// This is useful for scripts that perform side effects without
    /// returning a meaningful value.
    pub fn run_with_graph(&self, graph: Arc<Graph>, script: &str) -> RhaiResult<()> {
        let mut scope = self.create_graph_scope(graph);
        self.engine
            .run_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Create a scope with the graph and anonymous factory pre-bound.
    fn create_graph_scope(&self, graph: Arc<Graph>) -> Scope<'static> {
        let mut scope = Scope::new();

        // Create a RhaiGraph from the Arc<Graph>
        let rhai_graph = RhaiGraph::from_arc(graph);
        scope.push("graph", rhai_graph);

        // Add the anonymous traversal factory
        scope.push("A", create_anonymous_factory());

        scope
    }

    // =========================================================================
    // Mmap Graph Methods
    // =========================================================================

    /// Evaluate a script with a persistent mmap-backed graph.
    ///
    /// The graph must be wrapped in an `Arc` for sharing with the script scope.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use interstellar::storage::CowMmapGraph;
    ///
    /// let engine = RhaiEngine::new();
    /// let graph = Arc::new(CowMmapGraph::open("data.db")?);
    ///
    /// let result: i64 = engine.eval_with_mmap_graph(graph, r#"
    ///     let g = graph.gremlin();
    ///     g.v().count()
    /// "#)?;
    /// ```
    #[cfg(feature = "mmap")]
    pub fn eval_with_mmap_graph<T>(&self, graph: Arc<CowMmapGraph>, script: &str) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = self.create_mmap_graph_scope(graph);
        self.engine
            .eval_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Evaluate a script with a persistent mmap graph and return a Dynamic result.
    ///
    /// This is useful when the result type is not known at compile time.
    #[cfg(feature = "mmap")]
    pub fn eval_with_mmap_graph_dynamic(
        &self,
        graph: Arc<CowMmapGraph>,
        script: &str,
    ) -> RhaiResult<Dynamic> {
        let mut scope = self.create_mmap_graph_scope(graph);
        self.engine
            .eval_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Evaluate a pre-compiled AST with a persistent mmap graph context.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use interstellar::storage::CowMmapGraph;
    ///
    /// let engine = RhaiEngine::new();
    /// let ast = engine.compile("graph.gremlin().v().count()")?;
    /// let graph = Arc::new(CowMmapGraph::open("data.db")?);
    ///
    /// let count: i64 = engine.eval_ast_with_mmap_graph(graph, &ast)?;
    /// ```
    #[cfg(feature = "mmap")]
    pub fn eval_ast_with_mmap_graph<T>(&self, graph: Arc<CowMmapGraph>, ast: &AST) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = self.create_mmap_graph_scope(graph);
        self.engine
            .eval_ast_with_scope(&mut scope, ast)
            .map_err(RhaiError::from)
    }

    /// Evaluate a pre-compiled AST with a persistent mmap graph and return a Dynamic result.
    #[cfg(feature = "mmap")]
    pub fn eval_ast_with_mmap_graph_dynamic(
        &self,
        graph: Arc<CowMmapGraph>,
        ast: &AST,
    ) -> RhaiResult<Dynamic> {
        let mut scope = self.create_mmap_graph_scope(graph);
        self.engine
            .eval_ast_with_scope(&mut scope, ast)
            .map_err(RhaiError::from)
    }

    /// Run a script with a persistent mmap graph, ignoring the return value.
    ///
    /// This is useful for scripts that perform side effects without
    /// returning a meaningful value.
    #[cfg(feature = "mmap")]
    pub fn run_with_mmap_graph(&self, graph: Arc<CowMmapGraph>, script: &str) -> RhaiResult<()> {
        let mut scope = self.create_mmap_graph_scope(graph);
        self.engine
            .run_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }

    /// Create a scope with the mmap graph and anonymous factory pre-bound.
    #[cfg(feature = "mmap")]
    fn create_mmap_graph_scope(&self, graph: Arc<CowMmapGraph>) -> Scope<'static> {
        let mut scope = Scope::new();

        // Create a RhaiGraph from the Arc<CowMmapGraph>
        let rhai_graph = RhaiGraph::from_mmap_graph(graph);
        scope.push("graph", rhai_graph);

        // Add the anonymous traversal factory
        scope.push("A", create_anonymous_factory());

        scope
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use std::collections::HashMap;

    fn create_test_graph() -> Arc<Graph> {
        let graph = Graph::new();

        let alice = graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]),
        );

        let bob = graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::Int(25)),
            ]),
        );

        let carol = graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::Int(35)),
            ]),
        );

        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(alice, carol, "knows", HashMap::new())
            .unwrap();
        graph.add_edge(bob, carol, "knows", HashMap::new()).unwrap();

        Arc::new(graph)
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
                graph,
                r#"
                let g = graph.gremlin();
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
                graph,
                r#"
                let g = graph.gremlin();
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
                graph,
                r#"
                let g = graph.gremlin();
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
                graph,
                r#"
                let g = graph.gremlin();
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
                let g = graph.gremlin();
                g.v().count()
            "#,
            )
            .unwrap();

        let count: i64 = engine.eval_ast_with_graph(graph, &ast).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_anonymous_factory_in_script() {
        let engine = RhaiEngine::new();
        let graph = create_test_graph();

        // Test that the A factory is available
        let result: rhai::Array = engine
            .eval_with_graph(
                graph,
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
                graph,
                r#"
                let g = graph.gremlin();
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
                graph,
                r#"
                let g = graph.gremlin();
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
