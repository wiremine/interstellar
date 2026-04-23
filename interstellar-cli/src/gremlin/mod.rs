//! Gremlin-style traversal API powered by Interstellar's native Gremlin parser/compiler.
//!
//! This module provides a TinkerPop-compatible Gremlin query interface using the native
//! Gremlin parser and compiler introduced in the core `interstellar` package.
//!
//! # Example
//!
//! ```text
//! gremlin> g.V().hasLabel('person').values('name').toList()
//!
//! gremlin> alice = g.addV('person').property('name', 'Alice').next()
//! gremlin> bob = g.addV('person').property('name', 'Bob').next()
//! gremlin> g.addE('knows').from(alice).to(bob).next()
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use interstellar::gremlin::{
    CompileError, ExecutionResult, GremlinError, ParseError, ScriptResult, VariableContext,
};
use interstellar::storage::{Graph, PersistentGraph};
use interstellar::Value;

use crate::error::{CliError, Result};

// Re-export for use in REPL
pub use interstellar::gremlin::VariableContext as GremlinVariableContext;

/// Gremlin execution engine using native parser/compiler.
///
/// This engine supports both single-statement queries and multi-statement scripts
/// with variable assignment. For REPL workflows, use `execute_with_context()` to
/// maintain variable state across commands.
///
/// # Example
///
/// ```text
/// // Single statement
/// engine.execute("g.V().toList()")
///
/// // Multi-statement script with variables
/// engine.execute_script(r#"
///     alice = g.addV('person').property('name', 'Alice').next()
///     bob = g.addV('person').property('name', 'Bob').next()
///     g.addE('knows').from(alice).to(bob).next()
///     g.V().values('name').toList()
/// "#)
/// ```
pub struct GremlinEngine {
    graph: Arc<Graph>,
}

impl GremlinEngine {
    /// Create a new Gremlin engine for the given graph.
    pub fn new(graph: Graph) -> Self {
        Self::with_arc(Arc::new(graph))
    }

    /// Create a new Gremlin engine with a shared graph reference.
    pub fn with_arc(graph: Arc<Graph>) -> Self {
        Self { graph }
    }

    /// Get a reference to the underlying graph.
    ///
    /// Returns `&Arc<Graph>` so callers can invoke methods that require
    /// `self: &Arc<Self>` (e.g. `gql`, `execute_script`). Auto-deref also
    /// makes plain `&Graph` methods available transparently.
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }

    /// Get a clone of the Arc-wrapped graph.
    #[allow(dead_code)]
    pub fn graph_arc(&self) -> Arc<Graph> {
        Arc::clone(&self.graph)
    }

    /// Execute a single Gremlin query string and return the result.
    ///
    /// This uses `Graph::mutate()` which handles both reads and mutations.
    /// Mutations (addV, addE, property, drop) are persisted to the graph.
    ///
    /// For multi-statement scripts with variables, use `execute_script()` instead.
    pub fn execute(&self, query: &str) -> Result<ExecutionResult> {
        self.graph.mutate(query).map_err(map_gremlin_error)
    }

    /// Execute a multi-statement Gremlin script with variable support.
    ///
    /// This enables scripts like:
    /// ```text
    /// alice = g.addV('person').property('name', 'Alice').next()
    /// bob = g.addV('person').property('name', 'Bob').next()
    /// g.addE('knows').from(alice).to(bob).next()
    /// g.V().values('name').toList()
    /// ```
    ///
    /// Returns the result of the last statement and all bound variables.
    #[allow(dead_code)]
    pub fn execute_script(&self, script: &str) -> Result<ScriptResult> {
        self.graph.execute_script(script).map_err(map_gremlin_error)
    }

    /// Execute a Gremlin script with an existing variable context.
    ///
    /// This enables REPL-style workflows where variables persist across commands:
    /// ```text
    /// > alice = g.addV('person').property('name', 'Alice').next()
    /// > bob = g.addV('person').property('name', 'Bob').next()
    /// > g.addE('knows').from(alice).to(bob).next()
    /// ```
    ///
    /// The returned `ScriptResult` contains the updated variable context.
    pub fn execute_with_context(
        &self,
        script: &str,
        context: VariableContext,
    ) -> Result<ScriptResult> {
        self.graph
            .execute_script_with_context(script, context)
            .map_err(map_gremlin_error)
    }
}

/// Gremlin execution engine for persistent (mmap-backed) graphs.
///
/// This engine uses `PersistentGraph::mutate()` which supports both read and write operations.
/// Mutations are automatically persisted to disk.
///
/// Supports multi-statement scripts with variable assignment via `execute_with_context()`.
pub struct PersistentGremlinEngine {
    graph: Arc<PersistentGraph>,
}

impl PersistentGremlinEngine {
    /// Create a new Gremlin engine for the given persistent graph.
    pub fn new(graph: PersistentGraph) -> Self {
        Self::with_arc(Arc::new(graph))
    }

    /// Create a new Gremlin engine with a shared graph reference.
    pub fn with_arc(graph: Arc<PersistentGraph>) -> Self {
        Self { graph }
    }

    /// Get a reference to the underlying graph.
    pub fn graph(&self) -> &PersistentGraph {
        &self.graph
    }

    /// Get a clone of the Arc-wrapped graph.
    #[allow(dead_code)]
    pub fn graph_arc(&self) -> Arc<PersistentGraph> {
        Arc::clone(&self.graph)
    }

    /// Execute a Gremlin query string and return the result.
    ///
    /// This uses `PersistentGraph::mutate()` which handles both reads and mutations.
    /// Mutations (addV, addE, property, drop) are persisted to disk.
    pub fn execute(&self, query: &str) -> Result<ExecutionResult> {
        self.graph.mutate(query).map_err(map_gremlin_error)
    }

    /// Execute a Gremlin script with an existing variable context.
    ///
    /// This enables REPL-style workflows where variables persist across commands:
    /// ```text
    /// > alice = g.addV('person').property('name', 'Alice').next()
    /// > bob = g.addV('person').property('name', 'Bob').next()
    /// > g.addE('knows').from(alice).to(bob).next()
    /// ```
    ///
    /// The returned `ScriptResult` contains the updated variable context.
    pub fn execute_with_context(
        &self,
        script: &str,
        context: VariableContext,
    ) -> Result<ScriptResult> {
        self.graph
            .execute_script_with_context(script, context)
            .map_err(map_gremlin_error)
    }
}

/// Map a parse error to a CLI error.
fn map_parse_error(error: ParseError) -> CliError {
    CliError::query_syntax(error.to_string())
}

/// Map a compile error to a CLI error.
fn map_compile_error(error: CompileError) -> CliError {
    match &error {
        CompileError::UnsupportedStep { .. } => CliError::query_execution(error.to_string()),
        _ => CliError::query_syntax(error.to_string()),
    }
}

/// Map a Gremlin error (parse, compile, or execution) to a CLI error.
fn map_gremlin_error(error: GremlinError) -> CliError {
    match error {
        GremlinError::Parse(e) => map_parse_error(e),
        GremlinError::Compile(e) => map_compile_error(e),
        GremlinError::Execution(msg) => CliError::query_execution(msg),
    }
}

/// Format an ExecutionResult for human-readable output.
pub fn format_result(result: &ExecutionResult) -> String {
    match result {
        ExecutionResult::List(values) => format_value_list(values),
        ExecutionResult::Single(Some(value)) => format_value(value),
        ExecutionResult::Single(None) => "(no result)".to_string(),
        ExecutionResult::Set(values) => format_value_set(values),
        ExecutionResult::Bool(b) => b.to_string(),
        ExecutionResult::Unit => "(executed)".to_string(),
    }
}

/// Format a list of values for display.
fn format_value_list(values: &[Value]) -> String {
    if values.is_empty() {
        return "[]".to_string();
    }
    let items: Vec<String> = values.iter().map(format_value).collect();
    format!("[\n  {}\n]", items.join(",\n  "))
}

/// Format a set of values for display.
fn format_value_set(values: &HashSet<Value>) -> String {
    if values.is_empty() {
        return "{}".to_string();
    }
    let items: Vec<String> = values.iter().map(format_value).collect();
    format!("{{{}}}", items.join(", "))
}

/// Format an Interstellar Value for display.
pub fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{:.2}", f),
        Value::String(s) => s.clone(),
        Value::List(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let formatted: Vec<String> = items.iter().map(format_value).collect();
                format!("[{}]", formatted.join(", "))
            }
        }
        Value::Map(map) => {
            if map.is_empty() {
                "{}".to_string()
            } else {
                let formatted: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                    .collect();
                format!("{{{}}}", formatted.join(", "))
            }
        }
        Value::Vertex(id) => format!("v[{}]", id.0),
        Value::Edge(id) => format!("e[{}]", id.0),
        Value::Point(p) => p.to_string(),
        Value::Polygon(p) => p.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interstellar::gremlin::ExecutionResult;

    fn create_test_graph() -> Graph {
        Graph::new()
    }

    #[test]
    fn test_engine_creation() {
        let graph = create_test_graph();
        let _engine = GremlinEngine::new(graph);
    }

    #[test]
    fn test_basic_query() {
        let graph = create_test_graph();
        let engine = GremlinEngine::new(graph);

        // Empty graph should return empty list
        let result = engine.execute("g.V().toList()").unwrap();
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 0);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_has_next_query() {
        let graph = create_test_graph();
        let engine = GremlinEngine::new(graph);

        // Empty graph should return hasNext() = false
        let result = engine.execute("g.V().hasNext()").unwrap();
        if let ExecutionResult::Bool(has_next) = result {
            assert!(!has_next);
        } else {
            panic!("Expected Bool result, got {:?}", result);
        }
    }

    #[test]
    fn test_parse_error() {
        let graph = create_test_graph();
        let engine = GremlinEngine::new(graph);

        let result = engine.execute("g.V(.invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_format_result_list() {
        let result = ExecutionResult::List(vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
        ]);
        let output = format_result(&result);
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_format_result_single() {
        let result = ExecutionResult::Single(Some(Value::Int(42)));
        assert_eq!(format_result(&result), "42");
    }

    #[test]
    fn test_format_result_unit() {
        let result = ExecutionResult::Unit;
        assert_eq!(format_result(&result), "(executed)");
    }

    #[test]
    fn test_format_result_bool() {
        let result = ExecutionResult::Bool(true);
        assert_eq!(format_result(&result), "true");
    }

    #[test]
    fn test_format_result_empty_list() {
        let result = ExecutionResult::List(vec![]);
        assert_eq!(format_result(&result), "[]");
    }

    #[test]
    fn test_format_result_no_result() {
        let result = ExecutionResult::Single(None);
        assert_eq!(format_result(&result), "(no result)");
    }

    #[test]
    fn test_format_value_primitives() {
        assert_eq!(format_value(&Value::Int(42)), "42");
        assert_eq!(format_value(&Value::String("hello".to_string())), "hello");
        assert_eq!(format_value(&Value::Bool(true)), "true");
        assert_eq!(format_value(&Value::Null), "null");
    }

    #[test]
    fn test_format_value_vertex() {
        use interstellar::value::VertexId;
        assert_eq!(format_value(&Value::Vertex(VertexId(123))), "v[123]");
    }

    #[test]
    fn test_add_vertex_mutation() {
        let graph = create_test_graph();
        let engine = GremlinEngine::new(graph);

        // Add a vertex via Gremlin mutation
        let result = engine
            .execute("g.addV('person').property('name', 'Alice')")
            .unwrap();

        // Should return the created vertex
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            assert!(matches!(values[0], Value::Vertex(_)));
        } else {
            panic!("Expected List result, got {:?}", result);
        }

        // Verify the vertex exists by querying
        let query_result = engine.execute("g.V().hasLabel('person').toList()").unwrap();
        if let ExecutionResult::List(values) = query_result {
            assert_eq!(values.len(), 1);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_add_vertex_with_properties() {
        let graph = create_test_graph();
        let engine = GremlinEngine::new(graph);

        // Add vertex with multiple properties
        engine
            .execute("g.addV('person').property('name', 'Bob').property('age', 25)")
            .unwrap();

        // Query the name
        let result = engine
            .execute("g.V().hasLabel('person').values('name').toList()")
            .unwrap();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Bob".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_format_value_edge() {
        use interstellar::value::EdgeId;
        assert_eq!(format_value(&Value::Edge(EdgeId(456))), "e[456]");
    }
}
