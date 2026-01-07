//! Compiler that transforms GQL AST to traversal execution.
//!
//! The compiler takes a parsed GQL query and executes it against
//! a graph snapshot, returning results as `Vec<Value>`.

use std::collections::HashMap;

use crate::gql::ast::*;
use crate::gql::error::CompileError;
use crate::graph::GraphSnapshot;
use crate::value::Value;

/// Compile and execute a GQL query against a graph snapshot.
///
/// # Example
///
/// ```ignore
/// let query = parse("MATCH (n:Person) RETURN n")?;
/// let results = compile(&query, &snapshot)?;
/// ```
pub fn compile<'g>(
    query: &Query,
    snapshot: &'g GraphSnapshot<'g>,
) -> Result<Vec<Value>, CompileError> {
    let mut compiler = Compiler::new(snapshot);
    compiler.compile(query)
}

struct Compiler<'a, 'g> {
    snapshot: &'a GraphSnapshot<'g>,
    bindings: HashMap<String, BindingInfo>,
}

#[derive(Debug, Clone)]
struct BindingInfo {
    /// Index in the pattern where this variable was bound
    #[allow(dead_code)]
    pattern_index: usize,
    /// Whether this is a node or edge binding
    #[allow(dead_code)]
    is_node: bool,
}

impl<'a, 'g> Compiler<'a, 'g> {
    fn new(snapshot: &'a GraphSnapshot<'g>) -> Self {
        Self {
            snapshot,
            bindings: HashMap::new(),
        }
    }

    fn compile(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        // For spike: support only single pattern with single node
        if query.match_clause.patterns.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        let pattern = &query.match_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        // Get the first node pattern
        let node = match &pattern.elements[0] {
            PatternElement::Node(n) => n,
            PatternElement::Edge(_) => return Err(CompileError::PatternMustStartWithNode),
        };

        // Build traversal
        let g = self.snapshot.traversal();
        let mut traversal = g.v();

        // Apply label filter
        if !node.labels.is_empty() {
            let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
            traversal = traversal.has_label_any(labels);
        }

        // Register binding
        if let Some(var) = &node.variable {
            self.bindings.insert(
                var.clone(),
                BindingInfo {
                    pattern_index: 0,
                    is_node: true,
                },
            );
        }

        // Execute and collect results based on RETURN clause
        self.execute_return(&query.return_clause, traversal)
    }

    fn execute_return(
        &self,
        return_clause: &ReturnClause,
        traversal: crate::traversal::BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // For spike: support only returning the matched node
        // Verify all referenced variables are bound
        for item in &return_clause.items {
            if let Expression::Variable(var) = &item.expression {
                if !self.bindings.contains_key(var) {
                    return Err(CompileError::UndefinedVariable(var.clone()));
                }
            }
        }

        // Collect results - the traversal yields Value::Vertex for each match
        let results: Vec<Value> = traversal.to_list();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parser::parse;
    use crate::storage::InMemoryGraph;
    use crate::Graph;
    use std::sync::Arc;

    #[test]
    fn test_compile_simple_match() {
        let mut storage = InMemoryGraph::new();

        // Add test data
        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        storage.add_vertex("Person", props.clone());

        let mut props2 = std::collections::HashMap::new();
        props2.insert("name".to_string(), Value::from("Bob"));
        storage.add_vertex("Person", props2);

        let mut props3 = std::collections::HashMap::new();
        props3.insert("name".to_string(), Value::from("Acme"));
        storage.add_vertex("Company", props3);

        let graph = Graph::new(Arc::new(storage));
        let snapshot = graph.snapshot();
        let query = parse("MATCH (n:Person) RETURN n").unwrap();
        let results = compile(&query, &snapshot).unwrap();

        // Should find 2 Person vertices
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_compile_no_label() {
        let mut storage = InMemoryGraph::new();

        // Add test data
        let props1 = std::collections::HashMap::new();
        storage.add_vertex("Person", props1);

        let props2 = std::collections::HashMap::new();
        storage.add_vertex("Company", props2);

        let graph = Graph::new(Arc::new(storage));
        let snapshot = graph.snapshot();
        let query = parse("MATCH (n) RETURN n").unwrap();
        let results = compile(&query, &snapshot).unwrap();

        // Should find all 2 vertices
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_compile_undefined_variable() {
        let graph = Graph::in_memory();
        let snapshot = graph.snapshot();
        let query = parse("MATCH (n:Person) RETURN x").unwrap();
        let result = compile(&query, &snapshot);

        assert!(matches!(result, Err(CompileError::UndefinedVariable(_))));
    }
}
