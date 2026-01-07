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
        // Apply WHERE filter if present
        self.execute_return(&query.return_clause, &query.where_clause, traversal)
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
        where_clause: &Option<WhereClause>,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // Verify all referenced variables are bound
        for item in &return_clause.items {
            self.validate_expression_variables(&item.expression)?;
        }

        // Validate WHERE clause variables if present
        if let Some(where_cl) = where_clause {
            self.validate_expression_variables(&where_cl.expression)?;
        }

        // Collect the matched elements first
        let matched_elements: Vec<Value> = traversal.to_list();

        // Apply WHERE filter if present
        let filtered_elements = if let Some(where_cl) = where_clause {
            matched_elements
                .into_iter()
                .filter(|element| self.evaluate_predicate(&where_cl.expression, element))
                .collect()
        } else {
            matched_elements
        };

        // Process each matched element according to the RETURN clause
        let results: Vec<Value> = filtered_elements
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

    // =========================================================================
    // WHERE Clause Evaluation
    // =========================================================================

    /// Evaluate a predicate expression against an element, returning true if it matches.
    fn evaluate_predicate(&self, expr: &Expression, element: &Value) -> bool {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    // Logical operators
                    BinaryOperator::And => {
                        self.evaluate_predicate(left, element)
                            && self.evaluate_predicate(right, element)
                    }
                    BinaryOperator::Or => {
                        self.evaluate_predicate(left, element)
                            || self.evaluate_predicate(right, element)
                    }
                    // Comparison and other operators
                    _ => {
                        let left_val = self.evaluate_value(left, element);
                        let right_val = self.evaluate_value(right, element);
                        apply_comparison(*op, &left_val, &right_val)
                    }
                }
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => !self.evaluate_predicate(expr, element),
                UnaryOperator::Neg => {
                    // Negation of a value - treat non-zero as true
                    match self.evaluate_value(expr, element) {
                        Value::Int(n) => n == 0,
                        Value::Float(f) => f == 0.0,
                        Value::Bool(b) => !b,
                        Value::Null => true,
                        _ => false,
                    }
                }
            },
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_value(expr, element);
                let is_null = matches!(val, Value::Null);
                if *negated {
                    !is_null
                } else {
                    is_null
                }
            }
            Expression::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate_value(expr, element);
                let in_list = list.iter().any(|item| {
                    let item_val = self.evaluate_value(item, element);
                    val == item_val
                });
                if *negated {
                    !in_list
                } else {
                    in_list
                }
            }
            // For other expressions, evaluate and check truthiness
            _ => {
                let val = self.evaluate_value(expr, element);
                match val {
                    Value::Bool(b) => b,
                    Value::Null => false,
                    Value::Int(n) => n != 0,
                    Value::Float(f) => f != 0.0,
                    Value::String(s) => !s.is_empty(),
                    _ => true, // Non-null values are truthy
                }
            }
        }
    }

    /// Evaluate an expression to a Value against an element.
    fn evaluate_value(&self, expr: &Expression, element: &Value) -> Value {
        match expr {
            Expression::Literal(lit) => lit.clone().into(),
            Expression::Variable(_) => {
                // Return the element itself when referencing a variable
                element.clone()
            }
            Expression::Property { property, .. } => {
                // Extract property from the element
                self.extract_property(element, property)
                    .unwrap_or(Value::Null)
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_value(left, element);
                let right_val = self.evaluate_value(right, element);
                apply_binary_op(*op, left_val, right_val)
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let val = self.evaluate_value(expr, element);
                    match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    }
                }
                UnaryOperator::Neg => {
                    let val = self.evaluate_value(expr, element);
                    match val {
                        Value::Int(n) => Value::Int(-n),
                        Value::Float(f) => Value::Float(-f),
                        _ => Value::Null,
                    }
                }
            },
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_value(expr, element);
                let is_null = matches!(val, Value::Null);
                Value::Bool(if *negated { !is_null } else { is_null })
            }
            Expression::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate_value(expr, element);
                let in_list = list.iter().any(|item| {
                    let item_val = self.evaluate_value(item, element);
                    val == item_val
                });
                Value::Bool(if *negated { !in_list } else { in_list })
            }
            Expression::List(items) => {
                let values: Vec<Value> = items
                    .iter()
                    .map(|item| self.evaluate_value(item, element))
                    .collect();
                Value::List(values)
            }
            _ => Value::Null, // Unsupported expressions
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

// =============================================================================
// Helper Functions for Expression Evaluation
// =============================================================================

/// Apply a comparison operator to two values.
fn apply_comparison(op: BinaryOperator, left: &Value, right: &Value) -> bool {
    match op {
        BinaryOperator::Eq => left == right,
        BinaryOperator::Neq => left != right,
        BinaryOperator::Lt => compare_values(left, right) == std::cmp::Ordering::Less,
        BinaryOperator::Lte => {
            matches!(
                compare_values(left, right),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
        BinaryOperator::Gt => compare_values(left, right) == std::cmp::Ordering::Greater,
        BinaryOperator::Gte => {
            matches!(
                compare_values(left, right),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            )
        }
        BinaryOperator::And => value_to_bool(left) && value_to_bool(right),
        BinaryOperator::Or => value_to_bool(left) || value_to_bool(right),
        BinaryOperator::Contains => match (left, right) {
            (Value::String(s), Value::String(sub)) => s.contains(sub.as_str()),
            _ => false,
        },
        BinaryOperator::StartsWith => match (left, right) {
            (Value::String(s), Value::String(prefix)) => s.starts_with(prefix.as_str()),
            _ => false,
        },
        BinaryOperator::EndsWith => match (left, right) {
            (Value::String(s), Value::String(suffix)) => s.ends_with(suffix.as_str()),
            _ => false,
        },
        // Arithmetic operators don't return bool, but we handle them for completeness
        _ => false,
    }
}

/// Apply a binary operator and return the result as a Value.
fn apply_binary_op(op: BinaryOperator, left: Value, right: Value) -> Value {
    match op {
        BinaryOperator::Add => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 + b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a + b as f64),
            (Value::String(a), Value::String(b)) => Value::String(a + &b),
            _ => Value::Null,
        },
        BinaryOperator::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a - b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 - b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a - b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a * b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 * b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a * b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Div => match (left, right) {
            (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a / b),
            (Value::Float(a), Value::Float(b)) if b != 0.0 => Value::Float(a / b),
            (Value::Int(a), Value::Float(b)) if b != 0.0 => Value::Float(a as f64 / b),
            (Value::Float(a), Value::Int(b)) if b != 0 => Value::Float(a / b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mod => match (left, right) {
            (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a % b),
            _ => Value::Null,
        },
        // Comparison operators return Bool
        op => Value::Bool(apply_comparison(op, &left, &right)),
    }
}

/// Compare two values, returning Ordering.
fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        // Null is less than everything except Null
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        // Incompatible types - default to equal
        _ => std::cmp::Ordering::Equal,
    }
}

/// Convert a Value to a boolean for truthiness checks.
fn value_to_bool(val: &Value) -> bool {
    match val {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Int(n) => *n != 0,
        Value::Float(f) => *f != 0.0,
        Value::String(s) => !s.is_empty(),
        _ => true,
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
