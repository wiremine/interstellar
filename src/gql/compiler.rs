//! Compiler that transforms GQL AST to traversal execution.
//!
//! The compiler takes a parsed GQL query and executes it against
//! a graph snapshot, returning results as `Vec<Value>`.

use std::collections::HashMap;

use crate::gql::ast::*;
use crate::gql::error::CompileError;
use crate::graph::GraphSnapshot;
use crate::traversal::BoundTraversal;
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

impl<'a: 'g, 'g> Compiler<'a, 'g> {
    fn new(snapshot: &'a GraphSnapshot<'g>) -> Self {
        Self {
            snapshot,
            bindings: HashMap::new(),
        }
    }

    fn compile(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        if query.match_clause.patterns.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        let pattern = &query.match_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        // Build traversal starting from v()
        let g = self.snapshot.traversal();
        let traversal = g.v();

        // Compile the full pattern (nodes and edges)
        let traversal = self.compile_pattern(pattern, traversal)?;

        // Execute and collect results based on RETURN clause
        self.execute_return(&query.return_clause, traversal)
    }

    /// Compile a pattern into traversal steps.
    ///
    /// A pattern consists of alternating node and edge elements:
    /// (a)-[:KNOWS]->(b)-[:WORKS_WITH]->(c)
    ///
    /// The first element must be a node. Each edge is followed by a node.
    fn compile_pattern(
        &mut self,
        pattern: &Pattern,
        mut traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        for (element_index, element) in pattern.elements.iter().enumerate() {
            match element {
                PatternElement::Node(node) => {
                    traversal = self.compile_node(node, traversal, element_index)?;
                }
                PatternElement::Edge(edge) => {
                    traversal = self.compile_edge(edge, traversal)?;
                }
            }
        }

        Ok(traversal)
    }

    /// Compile a node pattern into filter steps.
    ///
    /// Applies label filters and property filters to the traversal.
    fn compile_node(
        &mut self,
        node: &NodePattern,
        mut traversal: BoundTraversal<'g, (), Value>,
        index: usize,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        // Apply label filter
        if !node.labels.is_empty() {
            let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
            traversal = traversal.has_label_any(labels);
        }

        // Apply property filters
        for (key, value) in &node.properties {
            let val: Value = value.clone().into();
            traversal = traversal.has_value(key.as_str(), val);
        }

        // Register binding
        if let Some(var) = &node.variable {
            if self.bindings.contains_key(var) {
                return Err(CompileError::DuplicateVariable(var.clone()));
            }
            self.bindings.insert(
                var.clone(),
                BindingInfo {
                    pattern_index: index,
                    is_node: true,
                },
            );
        }

        Ok(traversal)
    }

    /// Compile an edge pattern into navigation steps.
    ///
    /// Translates edge direction and labels into out()/in_()/both() calls.
    fn compile_edge(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();

        // Navigate based on direction
        let traversal = match edge.direction {
            EdgeDirection::Outgoing => {
                if labels.is_empty() {
                    traversal.out()
                } else {
                    traversal.out_labels(&labels)
                }
            }
            EdgeDirection::Incoming => {
                if labels.is_empty() {
                    traversal.in_()
                } else {
                    traversal.in_labels(&labels)
                }
            }
            EdgeDirection::Both => {
                if labels.is_empty() {
                    traversal.both()
                } else {
                    traversal.both_labels(&labels)
                }
            }
        };

        // TODO: Handle edge variable binding (requires outE/inE approach)
        // TODO: Handle property filters on edges
        // TODO: Handle quantifiers (variable-length paths)

        Ok(traversal)
    }

    fn execute_return(
        &self,
        return_clause: &ReturnClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // Verify all referenced variables are bound
        for item in &return_clause.items {
            self.validate_expression_variables(&item.expression)?;
        }

        // Collect the matched elements first
        let matched_elements: Vec<Value> = traversal.to_list();

        // Process each matched element according to the RETURN clause
        let results: Vec<Value> = matched_elements
            .into_iter()
            .filter_map(|element| self.evaluate_return_for_element(&return_clause.items, &element))
            .collect();

        Ok(results)
    }

    /// Evaluate the RETURN clause for a single matched element.
    ///
    /// Returns None if a required property is missing.
    fn evaluate_return_for_element(&self, items: &[ReturnItem], element: &Value) -> Option<Value> {
        if items.len() == 1 {
            // Single return item - return the value directly
            self.evaluate_expression(&items[0].expression, element)
        } else {
            // Multiple return items - return a map
            let mut map = std::collections::HashMap::new();
            for item in items {
                let key = self.get_return_item_key(item);
                let value = self.evaluate_expression(&item.expression, element)?;
                map.insert(key, value);
            }
            Some(Value::Map(map))
        }
    }

    /// Get the key name for a return item (alias or derived name).
    fn get_return_item_key(&self, item: &ReturnItem) -> String {
        if let Some(alias) = &item.alias {
            alias.clone()
        } else {
            match &item.expression {
                Expression::Variable(var) => var.clone(),
                Expression::Property { variable, property } => {
                    format!("{}.{}", variable, property)
                }
                _ => "value".to_string(),
            }
        }
    }

    /// Evaluate a single expression against an element.
    ///
    /// Returns None if the expression cannot be evaluated (e.g., missing property).
    fn evaluate_expression(&self, expr: &Expression, element: &Value) -> Option<Value> {
        match expr {
            Expression::Variable(_) => {
                // Return the element itself
                Some(element.clone())
            }
            Expression::Property { property, .. } => {
                // Extract property value from the element
                self.extract_property(element, property)
            }
            Expression::Literal(lit) => {
                // Convert literal to value
                Some(lit.clone().into())
            }
            _ => {
                // Other expressions not yet implemented
                Some(element.clone())
            }
        }
    }

    /// Extract a property value from a vertex or edge.
    fn extract_property(&self, element: &Value, property: &str) -> Option<Value> {
        match element {
            Value::Vertex(id) => {
                let vertex = self.snapshot.storage().get_vertex(*id)?;
                vertex.properties.get(property).cloned()
            }
            Value::Edge(id) => {
                let edge = self.snapshot.storage().get_edge(*id)?;
                edge.properties.get(property).cloned()
            }
            _ => None,
        }
    }

    /// Validate that all variables referenced in an expression are bound.
    fn validate_expression_variables(&self, expr: &Expression) -> Result<(), CompileError> {
        match expr {
            Expression::Variable(var) => {
                if !self.bindings.contains_key(var) && var != "*" {
                    return Err(CompileError::UndefinedVariable(var.clone()));
                }
            }
            Expression::Property { variable, .. } => {
                if !self.bindings.contains_key(variable) {
                    return Err(CompileError::UndefinedVariable(variable.clone()));
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.validate_expression_variables(left)?;
                self.validate_expression_variables(right)?;
            }
            Expression::UnaryOp { expr, .. } => {
                self.validate_expression_variables(expr)?;
            }
            Expression::IsNull { expr, .. } => {
                self.validate_expression_variables(expr)?;
            }
            Expression::InList { expr, list, .. } => {
                self.validate_expression_variables(expr)?;
                for item in list {
                    self.validate_expression_variables(item)?;
                }
            }
            Expression::List(items) => {
                for item in items {
                    self.validate_expression_variables(item)?;
                }
            }
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    self.validate_expression_variables(arg)?;
                }
            }
            Expression::Aggregate { expr, .. } => {
                self.validate_expression_variables(expr)?;
            }
            Expression::Literal(_) => {
                // Literals don't reference variables
            }
        }
        Ok(())
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
