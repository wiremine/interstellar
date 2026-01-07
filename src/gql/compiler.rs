//! Compiler that transforms GQL AST to traversal execution.
//!
//! The compiler takes a parsed GQL query and executes it against
//! a graph snapshot, returning results as `Vec<Value>`.

use std::collections::HashMap;

use crate::gql::ast::*;
use crate::gql::error::CompileError;
use crate::graph::GraphSnapshot;
use crate::traversal::{BoundTraversal, Traversal, __};
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
        let results = self.execute_return(&query.return_clause, &query.where_clause, traversal)?;

        // Apply ORDER BY if present
        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

        // Apply LIMIT/OFFSET if present
        let results = self.apply_limit(&query.limit_clause, results);

        Ok(results)
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
                return Err(CompileError::duplicate_variable(var));
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
    /// Handles variable-length paths when a quantifier is present.
    fn compile_edge(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        // Check if this edge has a quantifier (variable-length path)
        if let Some(quantifier) = &edge.quantifier {
            return self.compile_edge_with_quantifier(edge, quantifier, traversal);
        }

        // Simple single-hop edge traversal
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

        Ok(traversal)
    }

    /// Build an anonymous traversal for edge navigation based on direction and labels.
    ///
    /// Returns a `Traversal<Value, Value>` suitable for use with `repeat()`.
    fn build_edge_sub_traversal(
        &self,
        direction: EdgeDirection,
        labels: &[String],
    ) -> Traversal<Value, Value> {
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();

        match direction {
            EdgeDirection::Outgoing => {
                if label_refs.is_empty() {
                    __::out()
                } else {
                    __::out_labels(&label_refs)
                }
            }
            EdgeDirection::Incoming => {
                if label_refs.is_empty() {
                    __::in_()
                } else {
                    __::in_labels(&label_refs)
                }
            }
            EdgeDirection::Both => {
                if label_refs.is_empty() {
                    __::both()
                } else {
                    __::both_labels(&label_refs)
                }
            }
        }
    }

    /// Compile an edge with a path quantifier (variable-length path).
    ///
    /// Handles different quantifier patterns:
    /// - `*` (unbounded) → repeat with default max (10) and emit
    /// - `*n` (exact) → repeat n times
    /// - `*m..n` (range) → repeat up to n times with emit, filter for min
    /// - `*..n` (max only) → repeat up to n times with emit and emit_first (min=0)
    /// - `*m..` (min only) → repeat with default max and emit, filter for min
    fn compile_edge_with_quantifier(
        &mut self,
        edge: &EdgePattern,
        quantifier: &PathQuantifier,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        // Build the sub-traversal for the edge navigation
        let sub = self.build_edge_sub_traversal(edge.direction, &edge.labels);

        // Default max iterations to prevent infinite loops
        const DEFAULT_MAX: usize = 10;

        let min = quantifier.min.map(|v| v as usize);
        let max = quantifier.max.map(|v| v as usize);

        // Determine the traversal configuration based on quantifier
        let traversal = match (min, max) {
            // Exact count: *n (where min == max)
            (Some(m), Some(n)) if m == n => {
                // Execute exactly n iterations, no emit needed
                traversal.repeat(sub).times(n).dedup()
            }

            // Range with both bounds: *m..n
            (Some(m), Some(n)) => {
                if m == 0 {
                    // *0..n means 0 to n hops, include starting vertex
                    traversal.repeat(sub).times(n).emit().emit_first().dedup()
                } else {
                    // *m..n where m > 0: emit all depths 1..n, filter later
                    // Since we emit after each hop, min is achieved by filtering
                    // The repeat().emit() gives us all intermediate results
                    // We'd need to filter by path depth, but currently we emit all
                    // For now, emit all depths 1..n (best effort)
                    traversal.repeat(sub).times(n).emit().dedup()
                }
            }

            // Max only: *..n (implicitly *0..n)
            (None, Some(n)) => {
                // 0 to n hops, include starting vertex
                traversal.repeat(sub).times(n).emit().emit_first().dedup()
            }

            // Min only: *m.. (unbounded max)
            (Some(m), None) => {
                if m == 0 {
                    // *0.. means all reachable vertices including start
                    traversal
                        .repeat(sub)
                        .times(DEFAULT_MAX)
                        .emit()
                        .emit_first()
                        .dedup()
                } else {
                    // *m.. where m > 0: all reachable from depth m
                    traversal.repeat(sub).times(DEFAULT_MAX).emit().dedup()
                }
            }

            // Unbounded: * (no min or max)
            (None, None) => {
                // All reachable vertices including start
                traversal
                    .repeat(sub)
                    .times(DEFAULT_MAX)
                    .emit()
                    .emit_first()
                    .dedup()
            }
        };

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

        // Check if this is an aggregated query
        if self.has_aggregates(return_clause) {
            return self.execute_aggregated_return(return_clause, where_clause, traversal);
        }

        // Non-aggregated path: process each element individually

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

        // Apply DISTINCT if requested
        let results = if return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        Ok(results)
    }

    /// Deduplicate results using ComparableValue for equality checking.
    fn deduplicate_results(&self, results: Vec<Value>) -> Vec<Value> {
        let mut seen: Vec<ComparableValue> = Vec::new();
        let mut deduped = Vec::new();

        for value in results {
            let comparable = ComparableValue::from(value.clone());
            if !seen.contains(&comparable) {
                seen.push(comparable);
                deduped.push(value);
            }
        }

        deduped
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
                    return Err(CompileError::undefined_variable(var));
                }
            }
            Expression::Property { variable, .. } => {
                if !self.bindings.contains_key(variable) {
                    return Err(CompileError::undefined_variable(variable));
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

    // =========================================================================
    // Aggregation Support
    // =========================================================================

    /// Check if the RETURN clause contains any aggregate expressions.
    fn has_aggregates(&self, return_clause: &ReturnClause) -> bool {
        return_clause
            .items
            .iter()
            .any(|item| self.expression_has_aggregate(&item.expression))
    }

    /// Recursively check if an expression contains an aggregate function.
    fn expression_has_aggregate(&self, expr: &Expression) -> bool {
        Self::expr_has_aggregate(expr)
    }

    /// Static helper to check if an expression contains an aggregate function.
    fn expr_has_aggregate(expr: &Expression) -> bool {
        match expr {
            Expression::Aggregate { .. } => true,
            Expression::BinaryOp { left, right, .. } => {
                Self::expr_has_aggregate(left) || Self::expr_has_aggregate(right)
            }
            Expression::UnaryOp { expr, .. } => Self::expr_has_aggregate(expr),
            Expression::IsNull { expr, .. } => Self::expr_has_aggregate(expr),
            Expression::InList { expr, list, .. } => {
                Self::expr_has_aggregate(expr) || list.iter().any(Self::expr_has_aggregate)
            }
            Expression::List(items) => items.iter().any(Self::expr_has_aggregate),
            Expression::FunctionCall { args, .. } => args.iter().any(Self::expr_has_aggregate),
            _ => false,
        }
    }

    /// Execute a RETURN clause that contains aggregate functions.
    ///
    /// Separates group-by expressions from aggregates and processes accordingly.
    fn execute_aggregated_return(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // Collect the matched elements first
        let matched_elements: Vec<Value> = traversal.to_list();

        // Apply WHERE filter if present
        let filtered_elements: Vec<Value> = if let Some(where_cl) = where_clause {
            matched_elements
                .into_iter()
                .filter(|element| self.evaluate_predicate(&where_cl.expression, element))
                .collect()
        } else {
            matched_elements
        };

        // Separate group-by expressions from aggregates
        let mut group_by_items: Vec<(&ReturnItem, &Expression)> = Vec::new();
        let mut aggregate_items: Vec<(&ReturnItem, AggregateFunc, bool, &Expression)> = Vec::new();

        for item in &return_clause.items {
            if let Expression::Aggregate {
                func,
                distinct,
                expr,
            } = &item.expression
            {
                aggregate_items.push((item, *func, *distinct, expr.as_ref()));
            } else {
                group_by_items.push((item, &item.expression));
            }
        }

        if group_by_items.is_empty() {
            // No grouping - aggregate over all results (global aggregates)
            self.execute_global_aggregates(&aggregate_items, &filtered_elements)
        } else {
            // Group by non-aggregate expressions, then aggregate per group
            self.execute_grouped_aggregates(&group_by_items, &aggregate_items, &filtered_elements)
        }
    }

    /// Execute global aggregates (no GROUP BY).
    ///
    /// Aggregates over all matched elements and returns a single result.
    fn execute_global_aggregates(
        &self,
        aggregates: &[(&ReturnItem, AggregateFunc, bool, &Expression)],
        elements: &[Value],
    ) -> Result<Vec<Value>, CompileError> {
        if aggregates.len() == 1 {
            // Single aggregate - return just the value
            let (item, func, distinct, expr) = &aggregates[0];
            let value = self.compute_aggregate(*func, *distinct, expr, elements)?;

            // If there's an alias, we might want to return a map, but for simplicity
            // return the value directly for single aggregates (like SQL behavior)
            if item.alias.is_some() {
                let mut map = HashMap::new();
                map.insert(self.get_return_item_key(item), value);
                Ok(vec![Value::Map(map)])
            } else {
                Ok(vec![value])
            }
        } else {
            // Multiple aggregates - return a map
            let mut map = HashMap::new();

            for (item, func, distinct, expr) in aggregates {
                let key = self.get_return_item_key(item);
                let value = self.compute_aggregate(*func, *distinct, expr, elements)?;
                map.insert(key, value);
            }

            Ok(vec![Value::Map(map)])
        }
    }

    /// Execute grouped aggregates (with GROUP BY expressions).
    ///
    /// Groups elements by non-aggregate expressions, then computes aggregates per group.
    fn execute_grouped_aggregates(
        &self,
        group_by_items: &[(&ReturnItem, &Expression)],
        aggregates: &[(&ReturnItem, AggregateFunc, bool, &Expression)],
        elements: &[Value],
    ) -> Result<Vec<Value>, CompileError> {
        // Group elements by their group-by values
        let mut groups: HashMap<Vec<ComparableValue>, Vec<&Value>> = HashMap::new();

        for element in elements {
            let group_key: Vec<ComparableValue> = group_by_items
                .iter()
                .map(|(_, expr)| {
                    let val = self.evaluate_value(expr, element);
                    ComparableValue::from(val)
                })
                .collect();

            groups.entry(group_key).or_default().push(element);
        }

        // For each group, compute the aggregates
        let mut results = Vec::new();

        for (group_key, group_elements) in groups {
            let mut map = HashMap::new();

            // Add group-by values to result
            for (i, (item, _expr)) in group_by_items.iter().enumerate() {
                let key = self.get_return_item_key(item);
                let value = group_key[i].clone().into();
                map.insert(key, value);
            }

            // Compute aggregates for this group
            let group_values: Vec<Value> = group_elements.into_iter().cloned().collect();
            for (item, func, distinct, expr) in aggregates {
                let key = self.get_return_item_key(item);
                let value = self.compute_aggregate(*func, *distinct, expr, &group_values)?;
                map.insert(key, value);
            }

            results.push(Value::Map(map));
        }

        Ok(results)
    }

    /// Compute a single aggregate function over a set of values.
    fn compute_aggregate(
        &self,
        func: AggregateFunc,
        distinct: bool,
        expr: &Expression,
        elements: &[Value],
    ) -> Result<Value, CompileError> {
        // Handle COUNT(*) specially - count all elements
        let is_count_star = matches!(func, AggregateFunc::Count)
            && matches!(expr, Expression::Variable(v) if v == "*");

        if is_count_star {
            return Ok(Value::Int(elements.len() as i64));
        }

        // Extract values to aggregate
        let mut values: Vec<Value> = elements
            .iter()
            .filter_map(|element| self.evaluate_expression(expr, element))
            .collect();

        // Apply DISTINCT if requested
        if distinct {
            let mut seen: Vec<ComparableValue> = Vec::new();
            values.retain(|v| {
                let comparable = ComparableValue::from(v.clone());
                if seen.contains(&comparable) {
                    false
                } else {
                    seen.push(comparable);
                    true
                }
            });
        }

        match func {
            AggregateFunc::Count => Ok(Value::Int(values.len() as i64)),
            AggregateFunc::Sum => {
                let mut int_sum: i64 = 0;
                let mut float_sum: f64 = 0.0;
                let mut has_float = false;

                for v in &values {
                    match v {
                        Value::Int(n) => int_sum += n,
                        Value::Float(f) => {
                            has_float = true;
                            float_sum += f;
                        }
                        _ => {} // Skip non-numeric values
                    }
                }

                if has_float {
                    Ok(Value::Float(int_sum as f64 + float_sum))
                } else {
                    Ok(Value::Int(int_sum))
                }
            }
            AggregateFunc::Avg => {
                let mut sum: f64 = 0.0;
                let mut count: usize = 0;

                for v in &values {
                    match v {
                        Value::Int(n) => {
                            sum += *n as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += f;
                            count += 1;
                        }
                        _ => {} // Skip non-numeric values
                    }
                }

                if count > 0 {
                    Ok(Value::Float(sum / count as f64))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunc::Min => values
                .into_iter()
                .filter(|v| !matches!(v, Value::Null))
                .min_by(compare_values)
                .map(Ok)
                .unwrap_or(Ok(Value::Null)),
            AggregateFunc::Max => values
                .into_iter()
                .filter(|v| !matches!(v, Value::Null))
                .max_by(compare_values)
                .map(Ok)
                .unwrap_or(Ok(Value::Null)),
            AggregateFunc::Collect => Ok(Value::List(values)),
        }
    }

    // =========================================================================
    // ORDER BY and LIMIT Clause Application
    // =========================================================================

    /// Apply ORDER BY clause to results.
    ///
    /// Sorts the results based on the order items. Each order item specifies
    /// an expression to sort by and whether to sort descending.
    ///
    /// The tricky part: we need to extract the sort key from each result.
    /// Results are Values that came from execute_return, so they might be:
    /// - A Vertex/Edge if RETURN n
    /// - A property value if RETURN n.name  
    /// - A Map if RETURN n.name, n.age
    ///
    /// ORDER BY expressions reference the original bindings (e.g., ORDER BY n.age),
    /// but we only have the RETURN results. We need to either:
    /// 1. Keep the original elements alongside results, or
    /// 2. Extract from the result if it contains the needed data
    ///
    /// For now, we'll handle the common case where ORDER BY references
    /// properties that are also in the RETURN clause (directly or in a map).
    fn apply_order_by(
        &self,
        order_clause: &Option<OrderClause>,
        return_clause: &ReturnClause,
        mut results: Vec<Value>,
    ) -> Result<Vec<Value>, CompileError> {
        let order = match order_clause {
            Some(o) => o,
            None => return Ok(results),
        };

        if order.items.is_empty() {
            return Ok(results);
        }

        // Sort by each order item (multi-key sort)
        // We sort in reverse order of priority so that the first item has highest priority
        for order_item in order.items.iter().rev() {
            let descending = order_item.descending;
            let expr = &order_item.expression;

            // Create a key extractor for this order item
            results.sort_by(|a, b| {
                let key_a = self.extract_order_key(expr, a, return_clause);
                let key_b = self.extract_order_key(expr, b, return_clause);

                let cmp = compare_values(&key_a, &key_b);
                if descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }

        Ok(results)
    }

    /// Extract the sort key from a result value based on the order expression.
    ///
    /// This handles several cases:
    /// - If result is a Map and expression is Property, look up in map
    /// - If result is a Vertex/Edge and expression is Property, extract property
    /// - If expression is Variable, use the result directly
    fn extract_order_key(
        &self,
        expr: &Expression,
        result: &Value,
        return_clause: &ReturnClause,
    ) -> Value {
        match expr {
            Expression::Property { variable, property } => {
                // First, try to extract from the result itself
                match result {
                    Value::Map(map) => {
                        // Look for the property in the map
                        // Could be keyed as "n.age" or just "age" if aliased
                        let full_key = format!("{}.{}", variable, property);
                        if let Some(val) = map.get(&full_key) {
                            return val.clone();
                        }
                        if let Some(val) = map.get(property) {
                            return val.clone();
                        }
                        // Check aliases in return clause
                        for item in &return_clause.items {
                            if let Some(alias) = &item.alias {
                                if let Expression::Property {
                                    variable: v,
                                    property: p,
                                } = &item.expression
                                {
                                    if v == variable && p == property {
                                        if let Some(val) = map.get(alias) {
                                            return val.clone();
                                        }
                                    }
                                }
                            }
                        }
                        Value::Null
                    }
                    Value::Vertex(id) => {
                        // Extract property from vertex
                        if let Some(vertex) = self.snapshot.storage().get_vertex(*id) {
                            vertex
                                .properties
                                .get(property)
                                .cloned()
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    Value::Edge(id) => {
                        // Extract property from edge
                        if let Some(edge) = self.snapshot.storage().get_edge(*id) {
                            edge.properties
                                .get(property)
                                .cloned()
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    // If result is the property value itself (single return item),
                    // use it directly
                    _ => result.clone(),
                }
            }
            Expression::Variable(_) => {
                // Use the result directly
                result.clone()
            }
            Expression::Literal(lit) => lit.clone().into(),
            _ => Value::Null,
        }
    }

    /// Apply LIMIT and OFFSET to results.
    ///
    /// OFFSET skips the first N results, LIMIT takes at most N results.
    fn apply_limit(&self, limit_clause: &Option<LimitClause>, results: Vec<Value>) -> Vec<Value> {
        let limit = match limit_clause {
            Some(l) => l,
            None => return results,
        };

        let offset = limit.offset.unwrap_or(0) as usize;
        let count = limit.limit as usize;

        results.into_iter().skip(offset).take(count).collect()
    }
}

// =============================================================================
// Helper Types for Aggregation
// =============================================================================

/// A comparable wrapper for Value that implements Eq and Hash for grouping.
#[derive(Debug, Clone)]
struct ComparableValue(Value);

impl PartialEq for ComparableValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                // Handle NaN and compare floats bitwise
                a.to_bits() == b.to_bits()
            }
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Vertex(a), Value::Vertex(b)) => a == b,
            (Value::Edge(a), Value::Edge(b)) => a == b,
            (Value::List(a), Value::List(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| ComparableValue(x.clone()) == ComparableValue(y.clone()))
            }
            (Value::Map(a), Value::Map(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                a.iter().all(|(k, v)| {
                    b.get(k)
                        .map(|bv| ComparableValue(v.clone()) == ComparableValue(bv.clone()))
                        .unwrap_or(false)
                })
            }
            _ => false,
        }
    }
}

impl Eq for ComparableValue {}

impl std::hash::Hash for ComparableValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(n) => n.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Vertex(id) => id.0.hash(state),
            Value::Edge(id) => id.0.hash(state),
            Value::List(items) => {
                items.len().hash(state);
                for item in items {
                    ComparableValue(item.clone()).hash(state);
                }
            }
            Value::Map(map) => {
                map.len().hash(state);
                // Note: HashMap order is not deterministic, but we still hash for consistency
                for (k, v) in map {
                    k.hash(state);
                    ComparableValue(v.clone()).hash(state);
                }
            }
        }
    }
}

impl From<Value> for ComparableValue {
    fn from(v: Value) -> Self {
        ComparableValue(v)
    }
}

impl From<ComparableValue> for Value {
    fn from(cv: ComparableValue) -> Self {
        cv.0
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

        assert!(matches!(
            result,
            Err(CompileError::UndefinedVariable { .. })
        ));
    }
}
