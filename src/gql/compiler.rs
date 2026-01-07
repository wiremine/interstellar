//! Compiler that transforms GQL AST to traversal execution.
//!
//! The compiler takes a parsed GQL [`Query`] and executes it against
//! a [`GraphSnapshot`], returning results as `Vec<Value>`.
//!
//! # Overview
//!
//! The compilation process transforms a GQL query into a series of graph
//! traversal operations:
//!
//! 1. **Pattern matching**: MATCH clause patterns are compiled into `v()`,
//!    `out()`, `in_()`, `has_label()`, and `has_value()` traversal steps
//! 2. **Filtering**: WHERE clause predicates are evaluated against matched elements
//! 3. **Projection**: RETURN clause expressions extract values from matched elements
//! 4. **Ordering**: ORDER BY clause sorts results
//! 5. **Pagination**: LIMIT/OFFSET restrict the result set
//!
//! # Usage
//!
//! ```
//! use rustgremlin::gql::{parse, compile};
//! use rustgremlin::Graph;
//!
//! let graph = Graph::in_memory();
//! // ... populate graph ...
//!
//! let snapshot = graph.snapshot();
//! let query = parse("MATCH (n:Person) RETURN n.name").unwrap();
//! let results = compile(&query, &snapshot).unwrap();
//! ```
//!
//! # Supported Features
//!
//! ## Pattern Compilation
//!
//! - **Node patterns**: `(n)`, `(n:Label)`, `(n {prop: value})`
//! - **Edge patterns**: `-[e]->`, `-[:TYPE]->`, `<-[e]-`, `-[e]-`
//! - **Variable-length paths**: `-[*]->`, `-[*2..5]->`
//!
//! ## Expression Evaluation
//!
//! - **Comparisons**: `=`, `<>`, `<`, `<=`, `>`, `>=`
//! - **Logical**: `AND`, `OR`, `NOT`
//! - **Arithmetic**: `+`, `-`, `*`, `/`, `%`
//! - **String**: `CONTAINS`, `STARTS WITH`, `ENDS WITH`
//! - **Null checks**: `IS NULL`, `IS NOT NULL`
//! - **List membership**: `IN`, `NOT IN`
//!
//! ## Aggregation
//!
//! - `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)`
//! - `SUM(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)`
//! - `COLLECT(expr)` - collects values into a list
//!
//! # Error Handling
//!
//! The compiler returns [`CompileError`] for:
//! - Undefined variables referenced in WHERE or RETURN
//! - Duplicate variable bindings in MATCH
//! - Empty patterns
//! - Type mismatches in expressions
//!
//! [`Query`]: crate::gql::ast::Query
//! [`GraphSnapshot`]: crate::graph::GraphSnapshot
//! [`CompileError`]: crate::gql::error::CompileError

use std::collections::HashMap;
use std::collections::HashSet;

use crate::gql::ast::*;
use crate::gql::error::CompileError;
use crate::graph::GraphSnapshot;
use crate::traversal::{BoundTraversal, Traversal, __};
use crate::value::Value;

/// Compile and execute a GQL query against a graph snapshot.
///
/// This is the main entry point for executing GQL queries. It takes a parsed
/// [`Query`] AST and a [`GraphSnapshot`], executing the query and returning
/// matching results.
///
/// # Arguments
///
/// * `query` - A parsed GQL query from [`parse()`]
/// * `snapshot` - An immutable snapshot of the graph to query
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing the query results on success.
/// Results are [`Value`] instances that can be:
/// - `Value::Vertex` - when returning node variables
/// - `Value::Edge` - when returning edge variables  
/// - Primitive values (String, Int, Float, etc.) - when returning properties
/// - `Value::Map` - when returning multiple expressions
/// - `Value::List` - when using `COLLECT()` aggregation
///
/// # Errors
///
/// Returns [`CompileError`] if:
/// - A variable in RETURN or WHERE is not bound in MATCH
/// - A variable is bound multiple times in MATCH
/// - The MATCH pattern is empty
///
/// # Examples
///
/// ## Simple node query
///
/// ```
/// use rustgremlin::gql::{parse, compile};
/// use rustgremlin::Graph;
/// use rustgremlin::storage::InMemoryGraph;
/// use rustgremlin::value::Value;
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// let mut storage = InMemoryGraph::new();
/// let mut props = HashMap::new();
/// props.insert("name".to_string(), Value::from("Alice"));
/// storage.add_vertex("Person", props);
///
/// let graph = Graph::new(Arc::new(storage));
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) RETURN n.name").unwrap();
/// let results = compile(&query, &snapshot).unwrap();
/// assert_eq!(results.len(), 1);
/// ```
///
/// ## Query with filtering
///
/// ```
/// use rustgremlin::gql::{parse, compile};
/// use rustgremlin::Graph;
/// use rustgremlin::storage::InMemoryGraph;
/// use rustgremlin::value::Value;
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// let mut storage = InMemoryGraph::new();
/// let mut props = HashMap::new();
/// props.insert("name".to_string(), Value::from("Alice"));
/// props.insert("age".to_string(), Value::from(30));
/// storage.add_vertex("Person", props);
///
/// let graph = Graph::new(Arc::new(storage));
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) WHERE n.age > 25 RETURN n.name").unwrap();
/// let results = compile(&query, &snapshot).unwrap();
/// ```
///
/// ## Aggregation query
///
/// ```
/// use rustgremlin::gql::{parse, compile};
/// use rustgremlin::Graph;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) RETURN COUNT(*)").unwrap();
/// let results = compile(&query, &snapshot).unwrap();
/// ```
///
/// [`Query`]: crate::gql::ast::Query
/// [`GraphSnapshot`]: crate::graph::GraphSnapshot
/// [`Value`]: crate::value::Value
/// [`CompileError`]: crate::gql::error::CompileError
/// [`parse()`]: crate::gql::parse
pub fn compile<'g>(
    query: &Query,
    snapshot: &'g GraphSnapshot<'g>,
) -> Result<Vec<Value>, CompileError> {
    let mut compiler = Compiler::new(snapshot);
    compiler.compile(query)
}

/// Compile and execute a GQL statement (query or UNION) against a graph snapshot.
///
/// This function handles both single queries and UNION of multiple queries.
/// For UNION statements, it executes each query and combines the results,
/// deduplicating them unless UNION ALL is used.
///
/// # Arguments
///
/// * `stmt` - A parsed GQL statement from [`parse_statement()`]
/// * `snapshot` - An immutable snapshot of the graph to query
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing the statement results on success.
///
/// # Examples
///
/// ```
/// use rustgremlin::gql::{parse_statement, compile_statement};
/// use rustgremlin::Graph;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
///
/// // Single query
/// let stmt = parse_statement("MATCH (n:Person) RETURN n.name").unwrap();
/// let results = compile_statement(&stmt, &snapshot).unwrap();
///
/// // UNION query
/// let stmt = parse_statement(r#"
///     MATCH (a:TypeA) RETURN a.name
///     UNION
///     MATCH (b:TypeB) RETURN b.name
/// "#).unwrap();
/// let results = compile_statement(&stmt, &snapshot).unwrap();
/// ```
///
/// [`parse_statement()`]: crate::gql::parse_statement
pub fn compile_statement<'g>(
    stmt: &Statement,
    snapshot: &'g GraphSnapshot<'g>,
) -> Result<Vec<Value>, CompileError> {
    match stmt {
        Statement::Query(query) => compile(query, snapshot),
        Statement::Union { queries, all } => compile_union(queries, *all, snapshot),
    }
}

/// Execute a UNION of multiple queries.
fn compile_union<'g>(
    queries: &[Query],
    keep_duplicates: bool,
    snapshot: &'g GraphSnapshot<'g>,
) -> Result<Vec<Value>, CompileError> {
    let mut all_results = Vec::new();

    for query in queries {
        let results = compile(query, snapshot)?;
        all_results.extend(results);
    }

    if keep_duplicates {
        // UNION ALL - keep all results
        Ok(all_results)
    } else {
        // UNION - deduplicate results
        let mut seen: HashSet<ComparableValue> = HashSet::new();
        let deduped: Vec<Value> = all_results
            .into_iter()
            .filter(|v| {
                let key = ComparableValue::from(v.clone());
                seen.insert(key)
            })
            .collect();
        Ok(deduped)
    }
}

struct Compiler<'a, 'g> {
    snapshot: &'a GraphSnapshot<'g>,
    bindings: HashMap<String, BindingInfo>,
    /// Whether the current query has multiple bound variables (requires path tracking)
    has_multi_vars: bool,
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
            has_multi_vars: false,
        }
    }

    /// Count the number of variables in a pattern.
    fn count_pattern_variables(pattern: &Pattern) -> usize {
        pattern
            .elements
            .iter()
            .filter(|e| match e {
                PatternElement::Node(n) => n.variable.is_some(),
                PatternElement::Edge(e) => e.variable.is_some(),
            })
            .count()
    }

    /// Check if a pattern has any edge variables.
    fn has_edge_variable(pattern: &Pattern) -> bool {
        pattern
            .elements
            .iter()
            .any(|e| matches!(e, PatternElement::Edge(edge) if edge.variable.is_some()))
    }

    fn compile(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        if query.match_clause.patterns.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        let pattern = &query.match_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        // Check if we need multi-variable support (requires path tracking)
        // This is needed when:
        // 1. Multiple variables are bound (nodes or edges)
        // 2. Any edge variable is bound (needs path to access edge properties)
        let var_count = Self::count_pattern_variables(pattern);
        let has_edge_var = Self::has_edge_variable(pattern);
        self.has_multi_vars = var_count > 1 || has_edge_var;

        // Build traversal starting from v()
        let g = self.snapshot.traversal();
        let traversal = g.v();

        // Enable path tracking for multi-variable patterns or edge variable access
        let traversal = if self.has_multi_vars {
            traversal.with_path()
        } else {
            traversal
        };

        // Compile the full pattern (nodes and edges)
        let traversal = self.compile_pattern(pattern, traversal)?;

        // Verify all referenced variables are bound before proceeding
        for item in &query.return_clause.items {
            self.validate_expression_variables(&item.expression)?;
        }
        if let Some(where_cl) = &query.where_clause {
            self.validate_expression_variables(&where_cl.expression)?;
        }
        if let Some(group_by) = &query.group_by_clause {
            for expr in &group_by.expressions {
                self.validate_expression_variables(expr)?;
            }
        }

        // Check if this is a GROUP BY query
        if let Some(group_by) = &query.group_by_clause {
            let results = self.execute_group_by_query(
                &query.return_clause,
                &query.where_clause,
                group_by,
                traversal,
            )?;

            // Apply ORDER BY if present
            let results =
                self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

            // Apply LIMIT/OFFSET if present
            let results = self.apply_limit(&query.limit_clause, results);

            return Ok(results);
        }

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

        // Register binding and add as_() step for multi-variable patterns
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

            // Add as_() step to label this position in the path
            // This enables later retrieval via select() or path lookup
            if self.has_multi_vars {
                traversal = traversal.as_(var);
            }
        }

        Ok(traversal)
    }

    /// Compile an edge pattern into navigation steps.
    ///
    /// Translates edge direction and labels into out()/in_()/both() calls.
    /// Handles variable-length paths when a quantifier is present.
    /// Handles edge variable binding and edge property filters.
    fn compile_edge(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        // Check if this edge has a quantifier (variable-length path)
        if let Some(quantifier) = &edge.quantifier {
            return self.compile_edge_with_quantifier(edge, quantifier, traversal);
        }

        // Check if we need edge-level access (variable or properties)
        let needs_edge_access = edge.variable.is_some() || !edge.properties.is_empty();

        if needs_edge_access {
            return self.compile_edge_with_variable(edge, traversal);
        }

        // Simple single-hop edge traversal (no variable, no properties)
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

        Ok(traversal)
    }

    /// Compile an edge pattern that needs variable binding or property filtering.
    ///
    /// When an edge has a variable or properties, we need to:
    /// 1. Navigate to the edge (out_e/in_e/both_e)
    /// 2. Apply edge property filters
    /// 3. Bind the edge variable with as_() if present
    /// 4. Navigate to the target vertex (in_v/out_v/other_v)
    fn compile_edge_with_variable(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();

        // Step 1: Navigate to edge
        let mut traversal = match edge.direction {
            EdgeDirection::Outgoing => {
                if labels.is_empty() {
                    traversal.out_e()
                } else {
                    traversal.out_e_labels(&labels)
                }
            }
            EdgeDirection::Incoming => {
                if labels.is_empty() {
                    traversal.in_e()
                } else {
                    traversal.in_e_labels(&labels)
                }
            }
            EdgeDirection::Both => {
                if labels.is_empty() {
                    traversal.both_e()
                } else {
                    traversal.both_e_labels(&labels)
                }
            }
        };

        // Step 2: Apply edge property filters
        for (key, value) in &edge.properties {
            let val: Value = value.clone().into();
            traversal = traversal.has_value(key.as_str(), val);
        }

        // Step 3: Register and bind edge variable
        if let Some(var) = &edge.variable {
            if self.bindings.contains_key(var) {
                return Err(CompileError::duplicate_variable(var));
            }
            self.bindings.insert(
                var.clone(),
                BindingInfo {
                    pattern_index: 0, // Edge index not tracked precisely
                    is_node: false,
                },
            );

            // Add as_() step to label this edge position in the path
            if self.has_multi_vars {
                traversal = traversal.as_(var);
            }
        }

        // Step 4: Navigate to target vertex
        let traversal = match edge.direction {
            EdgeDirection::Outgoing => traversal.in_v(),
            EdgeDirection::Incoming => traversal.out_v(),
            EdgeDirection::Both => traversal.other_v(),
        };

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
    ///
    /// Note: When `has_multi_vars` is true, we skip the dedup() step because
    /// deduplicating by target value would lose different source->target paths.
    /// RETURN DISTINCT can handle deduplication at the result level if needed.
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

        // For multi-var patterns, skip dedup() to preserve (source, target) pairs.
        // Deduplication can be done at the RETURN level with DISTINCT.
        let skip_dedup = self.has_multi_vars;

        // Determine the traversal configuration based on quantifier
        let traversal = match (min, max) {
            // Exact count: *n (where min == max)
            (Some(m), Some(n)) if m == n => {
                // Execute exactly n iterations, no emit needed
                let t = traversal.repeat(sub).times(n);
                if skip_dedup {
                    t.identity()
                } else {
                    t.dedup()
                }
            }

            // Range with both bounds: *m..n
            (Some(m), Some(n)) => {
                if m == 0 {
                    // *0..n means 0 to n hops, include starting vertex
                    let t = traversal.repeat(sub).times(n).emit().emit_first();
                    if skip_dedup {
                        t.identity()
                    } else {
                        t.dedup()
                    }
                } else {
                    // *m..n where m > 0: emit all depths 1..n, filter later
                    // Since we emit after each hop, min is achieved by filtering
                    // The repeat().emit() gives us all intermediate results
                    // We'd need to filter by path depth, but currently we emit all
                    // For now, emit all depths 1..n (best effort)
                    let t = traversal.repeat(sub).times(n).emit();
                    if skip_dedup {
                        t.identity()
                    } else {
                        t.dedup()
                    }
                }
            }

            // Max only: *..n (implicitly *0..n)
            (None, Some(n)) => {
                // 0 to n hops, include starting vertex
                let t = traversal.repeat(sub).times(n).emit().emit_first();
                if skip_dedup {
                    t.identity()
                } else {
                    t.dedup()
                }
            }

            // Min only: *m.. (unbounded max)
            (Some(m), None) => {
                if m == 0 {
                    // *0.. means all reachable vertices including start
                    let t = traversal.repeat(sub).times(DEFAULT_MAX).emit().emit_first();
                    if skip_dedup {
                        t.identity()
                    } else {
                        t.dedup()
                    }
                } else {
                    // *m.. where m > 0: all reachable from depth m
                    let t = traversal.repeat(sub).times(DEFAULT_MAX).emit();
                    if skip_dedup {
                        t.identity()
                    } else {
                        t.dedup()
                    }
                }
            }

            // Unbounded: * (no min or max)
            (None, None) => {
                // All reachable vertices including start
                let t = traversal.repeat(sub).times(DEFAULT_MAX).emit().emit_first();
                if skip_dedup {
                    t.identity()
                } else {
                    t.dedup()
                }
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

        // For multi-variable patterns, we need to work with traversers to access the path
        if self.has_multi_vars {
            return self.execute_multi_var_return(return_clause, where_clause, traversal);
        }

        // Non-aggregated, single-variable path: process each element individually

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

    /// Execute RETURN for multi-variable patterns using traverser paths.
    fn execute_multi_var_return(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Execute and collect traversers (not just values) to access paths
        let traversers: Vec<Traverser> = traversal.execute().collect();

        // Apply WHERE filter if present
        let filtered_traversers: Vec<Traverser> = if let Some(where_cl) = where_clause {
            traversers
                .into_iter()
                .filter(|t| self.evaluate_predicate_from_path(&where_cl.expression, t))
                .collect()
        } else {
            traversers
        };

        // Process each traverser according to the RETURN clause
        let results: Vec<Value> = filtered_traversers
            .into_iter()
            .filter_map(|t| self.evaluate_return_for_traverser(&return_clause.items, &t))
            .collect();

        // Apply DISTINCT if requested
        let results = if return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        Ok(results)
    }

    /// Evaluate a predicate expression using the traverser's path for variable lookup.
    fn evaluate_predicate_from_path(
        &self,
        expr: &Expression,
        traverser: &crate::traversal::Traverser,
    ) -> bool {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    // Logical operators
                    BinaryOperator::And => {
                        self.evaluate_predicate_from_path(left, traverser)
                            && self.evaluate_predicate_from_path(right, traverser)
                    }
                    BinaryOperator::Or => {
                        self.evaluate_predicate_from_path(left, traverser)
                            || self.evaluate_predicate_from_path(right, traverser)
                    }
                    // Comparison and other operators
                    _ => {
                        let left_val = self.evaluate_value_from_path(left, traverser);
                        let right_val = self.evaluate_value_from_path(right, traverser);
                        apply_comparison(*op, &left_val, &right_val)
                    }
                }
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => !self.evaluate_predicate_from_path(expr, traverser),
                UnaryOperator::Neg => match self.evaluate_value_from_path(expr, traverser) {
                    Value::Int(n) => n == 0,
                    Value::Float(f) => f == 0.0,
                    Value::Bool(b) => !b,
                    Value::Null => true,
                    _ => false,
                },
            },
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_value_from_path(expr, traverser);
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
                let val = self.evaluate_value_from_path(expr, traverser);
                let in_list = list.iter().any(|item| {
                    let item_val = self.evaluate_value_from_path(item, traverser);
                    val == item_val
                });
                if *negated {
                    !in_list
                } else {
                    in_list
                }
            }
            Expression::Exists { pattern, negated } => {
                // For EXISTS in multi-var context, use the current element
                let exists = self.evaluate_exists_pattern(pattern, &traverser.value);
                if *negated {
                    !exists
                } else {
                    exists
                }
            }
            _ => {
                let val = self.evaluate_value_from_path(expr, traverser);
                match val {
                    Value::Bool(b) => b,
                    Value::Null => false,
                    Value::Int(n) => n != 0,
                    Value::Float(f) => f != 0.0,
                    Value::String(s) => !s.is_empty(),
                    _ => true,
                }
            }
        }
    }

    /// Evaluate an expression to a Value using the traverser's path for variable lookup.
    fn evaluate_value_from_path(
        &self,
        expr: &Expression,
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        match expr {
            Expression::Literal(lit) => lit.clone().into(),
            Expression::Variable(var) => {
                // Look up variable in the path
                self.get_variable_value_from_path(var, traverser)
            }
            Expression::Property { variable, property } => {
                // Get the element for this variable from the path, then extract property
                let element = self.get_variable_value_from_path(variable, traverser);
                self.extract_property(&element, property)
                    .unwrap_or(Value::Null)
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_value_from_path(left, traverser);
                let right_val = self.evaluate_value_from_path(right, traverser);
                apply_binary_op(*op, left_val, right_val)
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let val = self.evaluate_value_from_path(expr, traverser);
                    match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    }
                }
                UnaryOperator::Neg => {
                    let val = self.evaluate_value_from_path(expr, traverser);
                    match val {
                        Value::Int(n) => Value::Int(-n),
                        Value::Float(f) => Value::Float(-f),
                        _ => Value::Null,
                    }
                }
            },
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_value_from_path(expr, traverser);
                let is_null = matches!(val, Value::Null);
                Value::Bool(if *negated { !is_null } else { is_null })
            }
            Expression::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate_value_from_path(expr, traverser);
                let in_list = list.iter().any(|item| {
                    let item_val = self.evaluate_value_from_path(item, traverser);
                    val == item_val
                });
                Value::Bool(if *negated { !in_list } else { in_list })
            }
            Expression::List(items) => {
                let values: Vec<Value> = items
                    .iter()
                    .map(|item| self.evaluate_value_from_path(item, traverser))
                    .collect();
                Value::List(values)
            }
            Expression::Exists { pattern, negated } => {
                let exists = self.evaluate_exists_pattern(pattern, &traverser.value);
                Value::Bool(if *negated { !exists } else { exists })
            }
            Expression::FunctionCall { name, args } => {
                self.evaluate_function_call_from_path(name, args, traverser)
            }
            Expression::Case(case_expr) => self.evaluate_case_from_path(case_expr, traverser),
            _ => Value::Null,
        }
    }

    /// Evaluate a function call using path-based variable lookup.
    fn evaluate_function_call_from_path(
        &self,
        name: &str,
        args: &[Expression],
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        match name.to_uppercase().as_str() {
            // COALESCE: return first non-null argument
            "COALESCE" => {
                for arg in args {
                    let val = self.evaluate_value_from_path(arg, traverser);
                    if !matches!(val, Value::Null) {
                        return val;
                    }
                }
                Value::Null
            }

            // String functions
            "TOUPPER" | "UPPER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value_from_path(arg, traverser) {
                        return Value::String(s.to_uppercase());
                    }
                }
                Value::Null
            }
            "TOLOWER" | "LOWER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value_from_path(arg, traverser) {
                        return Value::String(s.to_lowercase());
                    }
                }
                Value::Null
            }
            "SIZE" | "LENGTH" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::String(s) => Value::Int(s.len() as i64),
                        Value::List(l) => Value::Int(l.len() as i64),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TRIM" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value_from_path(arg, traverser) {
                        return Value::String(s.trim().to_string());
                    }
                }
                Value::Null
            }
            "LTRIM" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value_from_path(arg, traverser) {
                        return Value::String(s.trim_start().to_string());
                    }
                }
                Value::Null
            }
            "RTRIM" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value_from_path(arg, traverser) {
                        return Value::String(s.trim_end().to_string());
                    }
                }
                Value::Null
            }
            "SUBSTRING" => {
                if args.len() >= 2 {
                    let val = self.evaluate_value_from_path(&args[0], traverser);
                    let start = self.evaluate_value_from_path(&args[1], traverser);
                    let length = args
                        .get(2)
                        .map(|a| self.evaluate_value_from_path(a, traverser));

                    if let (Value::String(s), Value::Int(start_idx)) = (val, start) {
                        let start_idx = start_idx.max(0) as usize;
                        if start_idx >= s.len() {
                            return Value::String(String::new());
                        }
                        let result = if let Some(Value::Int(len)) = length {
                            let len = len.max(0) as usize;
                            s.chars().skip(start_idx).take(len).collect()
                        } else {
                            s.chars().skip(start_idx).collect()
                        };
                        return Value::String(result);
                    }
                }
                Value::Null
            }
            "REPLACE" => {
                if args.len() >= 3 {
                    let val = self.evaluate_value_from_path(&args[0], traverser);
                    let search = self.evaluate_value_from_path(&args[1], traverser);
                    let replacement = self.evaluate_value_from_path(&args[2], traverser);

                    if let (Value::String(s), Value::String(search), Value::String(replacement)) =
                        (val, search, replacement)
                    {
                        return Value::String(s.replace(&search, &replacement));
                    }
                }
                Value::Null
            }

            // Numeric functions
            "ABS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Int(n.abs()),
                        Value::Float(f) => Value::Float(f.abs()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "CEIL" | "CEILING" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Float(f) => Value::Float(f.ceil()),
                        Value::Int(n) => Value::Int(n),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "FLOOR" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Float(f) => Value::Float(f.floor()),
                        Value::Int(n) => Value::Int(n),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "ROUND" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Float(f) => Value::Float(f.round()),
                        Value::Int(n) => Value::Int(n),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "SIGN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Int(n.signum()),
                        Value::Float(f) => {
                            if f > 0.0 {
                                Value::Int(1)
                            } else if f < 0.0 {
                                Value::Int(-1)
                            } else {
                                Value::Int(0)
                            }
                        }
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Type conversion functions
            "TOSTRING" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value_from_path(arg, traverser);
                    match val {
                        Value::String(s) => Value::String(s),
                        Value::Int(n) => Value::String(n.to_string()),
                        Value::Float(f) => Value::String(f.to_string()),
                        Value::Bool(b) => Value::String(b.to_string()),
                        Value::Null => Value::Null,
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOINTEGER" | "TOINT" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value_from_path(arg, traverser);
                    match val {
                        Value::Int(n) => Value::Int(n),
                        Value::Float(f) => Value::Int(f as i64),
                        Value::String(s) => {
                            s.parse::<i64>().ok().map(Value::Int).unwrap_or(Value::Null)
                        }
                        Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOFLOAT" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value_from_path(arg, traverser);
                    match val {
                        Value::Float(f) => Value::Float(f),
                        Value::Int(n) => Value::Float(n as f64),
                        Value::String(s) => s
                            .parse::<f64>()
                            .ok()
                            .map(Value::Float)
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOBOOLEAN" | "TOBOOL" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value_from_path(arg, traverser);
                    match val {
                        Value::Bool(b) => Value::Bool(b),
                        Value::String(s) => match s.to_lowercase().as_str() {
                            "true" | "yes" | "1" => Value::Bool(true),
                            "false" | "no" | "0" => Value::Bool(false),
                            _ => Value::Null,
                        },
                        Value::Int(n) => Value::Bool(n != 0),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Unknown function
            _ => Value::Null,
        }
    }

    /// Evaluate a CASE expression using path-based variable lookup.
    fn evaluate_case_from_path(
        &self,
        case_expr: &CaseExpression,
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        // Evaluate each WHEN clause in order
        for (condition, result) in &case_expr.when_clauses {
            if self.evaluate_predicate_from_path(condition, traverser) {
                return self.evaluate_value_from_path(result, traverser);
            }
        }

        // No WHEN matched, evaluate ELSE or return null
        if let Some(else_expr) = &case_expr.else_clause {
            self.evaluate_value_from_path(else_expr, traverser)
        } else {
            Value::Null
        }
    }

    /// Get the value for a variable from the traverser's path.
    fn get_variable_value_from_path(
        &self,
        variable: &str,
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        // Look up the variable in the path labels
        if let Some(values) = traverser.path.get(variable) {
            // Return the last value for this label (most recent)
            if let Some(path_value) = values.last() {
                return path_value.to_value();
            }
        }
        // Fallback to current element if variable is the "current" one
        // (this handles single-variable edge case)
        traverser.value.clone()
    }

    /// Evaluate the RETURN clause for a traverser with path-based variable lookup.
    fn evaluate_return_for_traverser(
        &self,
        items: &[ReturnItem],
        traverser: &crate::traversal::Traverser,
    ) -> Option<Value> {
        if items.len() == 1 {
            // Single return item - return the value directly
            self.evaluate_expression_from_path(&items[0].expression, traverser)
        } else {
            // Multiple return items - return a map
            let mut map = std::collections::HashMap::new();
            for item in items {
                let key = self.get_return_item_key(item);
                let value = self.evaluate_expression_from_path(&item.expression, traverser)?;
                map.insert(key, value);
            }
            Some(Value::Map(map))
        }
    }

    /// Evaluate a single expression using the traverser's path.
    fn evaluate_expression_from_path(
        &self,
        expr: &Expression,
        traverser: &crate::traversal::Traverser,
    ) -> Option<Value> {
        match expr {
            Expression::Variable(var) => {
                // Look up variable in path
                Some(self.get_variable_value_from_path(var, traverser))
            }
            Expression::Property { variable, property } => {
                // Look up variable in path, then extract property
                let element = self.get_variable_value_from_path(variable, traverser);
                self.extract_property(&element, property)
            }
            Expression::Literal(lit) => Some(lit.clone().into()),
            _ => {
                // For other expressions, use the value-based evaluation
                Some(self.evaluate_value_from_path(expr, traverser))
            }
        }
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
                // Delegate to evaluate_value for FunctionCall, Case, and other expressions
                Some(self.evaluate_value(expr, element))
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
            Expression::Exists { pattern, negated } => {
                let exists = self.evaluate_exists_pattern(pattern, element);
                if *negated {
                    !exists
                } else {
                    exists
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
            Expression::Exists { pattern, negated } => {
                let exists = self.evaluate_exists_pattern(pattern, element);
                Value::Bool(if *negated { !exists } else { exists })
            }
            Expression::FunctionCall { name, args } => {
                self.evaluate_function_call(name, args, element)
            }
            Expression::Case(case_expr) => self.evaluate_case(case_expr, element),
            _ => Value::Null, // Unsupported expressions
        }
    }

    /// Evaluate a function call expression.
    ///
    /// Supported functions:
    /// - `COALESCE(expr, ...)` - returns first non-null value
    /// - `TOUPPER(str)` / `UPPER(str)` - converts string to uppercase
    /// - `TOLOWER(str)` / `LOWER(str)` - converts string to lowercase
    /// - `SIZE(str|list)` / `LENGTH(str|list)` - returns length
    /// - `ABS(num)` - returns absolute value
    /// - `TOSTRING(val)` - converts value to string
    /// - `TOINTEGER(val)` / `TOINT(val)` - converts value to integer
    /// - `TOFLOAT(val)` - converts value to float
    /// - `TOBOOLEAN(val)` / `TOBOOL(val)` - converts value to boolean
    fn evaluate_function_call(&self, name: &str, args: &[Expression], element: &Value) -> Value {
        match name.to_uppercase().as_str() {
            // COALESCE: return first non-null argument
            "COALESCE" => {
                for arg in args {
                    let val = self.evaluate_value(arg, element);
                    if !matches!(val, Value::Null) {
                        return val;
                    }
                }
                Value::Null
            }

            // String functions
            "TOUPPER" | "UPPER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.to_uppercase());
                    }
                }
                Value::Null
            }
            "TOLOWER" | "LOWER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.to_lowercase());
                    }
                }
                Value::Null
            }
            "SIZE" | "LENGTH" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::String(s) => Value::Int(s.len() as i64),
                        Value::List(l) => Value::Int(l.len() as i64),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TRIM" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.trim().to_string());
                    }
                }
                Value::Null
            }
            "LTRIM" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.trim_start().to_string());
                    }
                }
                Value::Null
            }
            "RTRIM" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.trim_end().to_string());
                    }
                }
                Value::Null
            }
            "SUBSTRING" => {
                if args.len() >= 2 {
                    let val = self.evaluate_value(&args[0], element);
                    let start = self.evaluate_value(&args[1], element);
                    let length = args.get(2).map(|a| self.evaluate_value(a, element));

                    if let (Value::String(s), Value::Int(start_idx)) = (val, start) {
                        let start_idx = start_idx.max(0) as usize;
                        if start_idx >= s.len() {
                            return Value::String(String::new());
                        }
                        let result = if let Some(Value::Int(len)) = length {
                            let len = len.max(0) as usize;
                            s.chars().skip(start_idx).take(len).collect()
                        } else {
                            s.chars().skip(start_idx).collect()
                        };
                        return Value::String(result);
                    }
                }
                Value::Null
            }
            "REPLACE" => {
                if args.len() >= 3 {
                    let val = self.evaluate_value(&args[0], element);
                    let search = self.evaluate_value(&args[1], element);
                    let replacement = self.evaluate_value(&args[2], element);

                    if let (Value::String(s), Value::String(search), Value::String(replacement)) =
                        (val, search, replacement)
                    {
                        return Value::String(s.replace(&search, &replacement));
                    }
                }
                Value::Null
            }

            // Numeric functions
            "ABS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Int(n.abs()),
                        Value::Float(f) => Value::Float(f.abs()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "CEIL" | "CEILING" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Float(f) => Value::Float(f.ceil()),
                        Value::Int(n) => Value::Int(n),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "FLOOR" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Float(f) => Value::Float(f.floor()),
                        Value::Int(n) => Value::Int(n),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "ROUND" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Float(f) => Value::Float(f.round()),
                        Value::Int(n) => Value::Int(n),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "SIGN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Int(n.signum()),
                        Value::Float(f) => {
                            if f > 0.0 {
                                Value::Int(1)
                            } else if f < 0.0 {
                                Value::Int(-1)
                            } else {
                                Value::Int(0)
                            }
                        }
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Type conversion functions
            "TOSTRING" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::String(s) => Value::String(s),
                        Value::Int(n) => Value::String(n.to_string()),
                        Value::Float(f) => Value::String(f.to_string()),
                        Value::Bool(b) => Value::String(b.to_string()),
                        Value::Null => Value::Null,
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOINTEGER" | "TOINT" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::Int(n) => Value::Int(n),
                        Value::Float(f) => Value::Int(f as i64),
                        Value::String(s) => {
                            s.parse::<i64>().ok().map(Value::Int).unwrap_or(Value::Null)
                        }
                        Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOFLOAT" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::Float(f) => Value::Float(f),
                        Value::Int(n) => Value::Float(n as f64),
                        Value::String(s) => s
                            .parse::<f64>()
                            .ok()
                            .map(Value::Float)
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOBOOLEAN" | "TOBOOL" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::Bool(b) => Value::Bool(b),
                        Value::String(s) => match s.to_lowercase().as_str() {
                            "true" | "yes" | "1" => Value::Bool(true),
                            "false" | "no" | "0" => Value::Bool(false),
                            _ => Value::Null,
                        },
                        Value::Int(n) => Value::Bool(n != 0),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Unknown function
            _ => Value::Null,
        }
    }

    /// Evaluate a CASE expression.
    ///
    /// CASE expressions have the form:
    /// ```text
    /// CASE
    ///     WHEN condition1 THEN result1
    ///     WHEN condition2 THEN result2
    ///     ELSE defaultResult
    /// END
    /// ```
    fn evaluate_case(&self, case_expr: &CaseExpression, element: &Value) -> Value {
        // Evaluate each WHEN clause in order
        for (condition, result) in &case_expr.when_clauses {
            if self.evaluate_predicate(condition, element) {
                return self.evaluate_value(result, element);
            }
        }

        // No WHEN matched, evaluate ELSE or return null
        if let Some(else_expr) = &case_expr.else_clause {
            self.evaluate_value(else_expr, element)
        } else {
            Value::Null
        }
    }

    // =========================================================================
    // EXISTS Expression Evaluation
    // =========================================================================

    /// Evaluate an EXISTS expression against an element.
    ///
    /// EXISTS { (p)-[:KNOWS]->(friend) } checks if there's at least one
    /// path matching the pattern starting from the current element.
    ///
    /// The first node in the pattern is the "anchor" - it should match the
    /// current element. Subsequent edges and nodes form the pattern to check.
    fn evaluate_exists_pattern(&self, pattern: &Pattern, element: &Value) -> bool {
        // Get vertex ID from element - EXISTS only makes sense for vertices
        let vid = match element {
            Value::Vertex(id) => *id,
            _ => return false,
        };

        // Start traversal from this specific vertex
        let g = self.snapshot.traversal();
        let mut traversal = g.v_ids([vid]);

        // Process the pattern elements
        // The first node is the anchor (current element) - apply its filters
        // Subsequent edges navigate, and subsequent nodes filter
        let mut is_first_node = true;

        for element in &pattern.elements {
            match element {
                PatternElement::Node(node) => {
                    if is_first_node {
                        // First node - apply label and property filters to current vertex
                        is_first_node = false;
                        traversal = self.apply_node_filters(node, traversal);
                    } else {
                        // Subsequent node after an edge - apply filters
                        traversal = self.apply_node_filters(node, traversal);
                    }
                }
                PatternElement::Edge(edge) => {
                    // Navigate along the edge
                    traversal = self.apply_edge_navigation(edge, traversal);
                }
            }
        }

        // Check if any results exist
        !traversal.to_list().is_empty()
    }

    /// Apply node filters (labels and properties) to a traversal.
    fn apply_node_filters(
        &self,
        node: &NodePattern,
        mut traversal: BoundTraversal<'g, (), Value>,
    ) -> BoundTraversal<'g, (), Value> {
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

        traversal
    }

    /// Apply edge navigation based on direction and labels.
    fn apply_edge_navigation(
        &self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> BoundTraversal<'g, (), Value> {
        let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();

        match edge.direction {
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
            Expression::Exists { pattern, .. } => {
                // Validate variables referenced in the EXISTS pattern.
                // The first node variable (if present) should reference a bound variable
                // from the outer scope - it's the "anchor" for the sub-pattern.
                if let Some(PatternElement::Node(node)) = pattern.elements.first() {
                    if let Some(var) = &node.variable {
                        if !self.bindings.contains_key(var) {
                            return Err(CompileError::undefined_variable(var));
                        }
                    }
                }
                // Other variables in the EXISTS pattern are local to the sub-pattern
                // and don't need validation against outer scope bindings
            }
            Expression::Case(case_expr) => {
                // Validate all expressions in the CASE expression
                for (condition, result) in &case_expr.when_clauses {
                    self.validate_expression_variables(condition)?;
                    self.validate_expression_variables(result)?;
                }
                if let Some(else_expr) = &case_expr.else_clause {
                    self.validate_expression_variables(else_expr)?;
                }
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
            Expression::Case(case_expr) => {
                case_expr
                    .when_clauses
                    .iter()
                    .any(|(c, r)| Self::expr_has_aggregate(c) || Self::expr_has_aggregate(r))
                    || case_expr
                        .else_clause
                        .as_ref()
                        .map(|e| Self::expr_has_aggregate(e))
                        .unwrap_or(false)
            }
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

    /// Execute a query with an explicit GROUP BY clause.
    ///
    /// This is used when the query has a GROUP BY clause, which explicitly specifies
    /// which expressions to group by. Non-aggregate expressions in RETURN must appear
    /// in the GROUP BY clause.
    ///
    /// # Example
    ///
    /// ```text
    /// MATCH (p:player)
    /// RETURN p.position, count(*), avg(p.points_per_game)
    /// GROUP BY p.position
    /// ```
    fn execute_group_by_query(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        group_by: &GroupByClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // For multi-variable patterns, we need to work with traversers to access paths
        if self.has_multi_vars {
            return self.execute_group_by_query_multi_var(
                return_clause,
                where_clause,
                group_by,
                traversal,
            );
        }

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

        // Validate: non-aggregate expressions in RETURN must appear in GROUP BY
        for item in &return_clause.items {
            if !Self::expr_has_aggregate(&item.expression) {
                // This non-aggregate expression must match a GROUP BY expression
                if !self.expression_in_group_by(&item.expression, group_by) {
                    let expr_str = self.expression_to_string(&item.expression);
                    return Err(CompileError::expression_not_in_group_by(expr_str));
                }
            }
        }

        // Group elements by GROUP BY expressions
        let mut groups: HashMap<Vec<ComparableValue>, Vec<Value>> = HashMap::new();

        for element in filtered_elements {
            let group_key: Vec<ComparableValue> = group_by
                .expressions
                .iter()
                .map(|expr| {
                    let val = self.evaluate_value(expr, &element);
                    ComparableValue::from(val)
                })
                .collect();

            groups.entry(group_key).or_default().push(element);
        }

        // For each group, compute the RETURN clause
        let mut results = Vec::new();

        for (group_key, group_elements) in groups {
            let result =
                self.compute_group_result(return_clause, group_by, &group_key, &group_elements)?;
            results.push(result);
        }

        // Apply DISTINCT if requested
        let results = if return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        Ok(results)
    }

    /// Execute a GROUP BY query with multi-variable path support.
    ///
    /// This version uses traversers to access path-based variable lookups,
    /// which is needed for edge property access in aggregations.
    fn execute_group_by_query_multi_var(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        group_by: &GroupByClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Collect traversers to access paths
        let traversers: Vec<Traverser> = traversal.execute().collect();

        // Apply WHERE filter if present
        let filtered_traversers: Vec<Traverser> = if let Some(where_cl) = where_clause {
            traversers
                .into_iter()
                .filter(|t| self.evaluate_predicate_from_path(&where_cl.expression, t))
                .collect()
        } else {
            traversers
        };

        // Validate: non-aggregate expressions in RETURN must appear in GROUP BY
        for item in &return_clause.items {
            if !Self::expr_has_aggregate(&item.expression) {
                if !self.expression_in_group_by(&item.expression, group_by) {
                    let expr_str = self.expression_to_string(&item.expression);
                    return Err(CompileError::expression_not_in_group_by(expr_str));
                }
            }
        }

        // Group traversers by GROUP BY expressions
        let mut groups: HashMap<Vec<ComparableValue>, Vec<Traverser>> = HashMap::new();

        for t in filtered_traversers {
            let group_key: Vec<ComparableValue> = group_by
                .expressions
                .iter()
                .map(|expr| {
                    let val = self.evaluate_value_from_path(expr, &t);
                    ComparableValue::from(val)
                })
                .collect();

            groups.entry(group_key).or_default().push(t);
        }

        // For each group, compute the RETURN clause
        let mut results = Vec::new();

        for (group_key, group_traversers) in groups {
            let result = self.compute_group_result_multi_var(
                return_clause,
                group_by,
                &group_key,
                &group_traversers,
            )?;
            results.push(result);
        }

        // Apply DISTINCT if requested
        let results = if return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        Ok(results)
    }

    /// Compute the result for a single group with multi-variable path support.
    fn compute_group_result_multi_var(
        &self,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_traversers: &[crate::traversal::Traverser],
    ) -> Result<Value, CompileError> {
        if return_clause.items.len() == 1 {
            // Single return item
            let item = &return_clause.items[0];
            let value = self.evaluate_group_expression_multi_var(
                &item.expression,
                group_by,
                group_key,
                group_traversers,
            )?;

            if item.alias.is_some() {
                let mut map = HashMap::new();
                map.insert(self.get_return_item_key(item), value);
                Ok(Value::Map(map))
            } else {
                Ok(value)
            }
        } else {
            // Multiple return items - return a map
            let mut map = HashMap::new();

            for item in &return_clause.items {
                let key = self.get_return_item_key(item);
                let value = self.evaluate_group_expression_multi_var(
                    &item.expression,
                    group_by,
                    group_key,
                    group_traversers,
                )?;
                map.insert(key, value);
            }

            Ok(Value::Map(map))
        }
    }

    /// Evaluate an expression in the context of a group with multi-var support.
    fn evaluate_group_expression_multi_var(
        &self,
        expr: &Expression,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_traversers: &[crate::traversal::Traverser],
    ) -> Result<Value, CompileError> {
        if let Expression::Aggregate {
            func,
            distinct,
            expr: inner,
        } = expr
        {
            // Compute aggregate over group using path-based evaluation
            self.compute_aggregate_multi_var(*func, *distinct, inner, group_traversers)
        } else {
            // Non-aggregate: should be a GROUP BY expression
            for (i, group_expr) in group_by.expressions.iter().enumerate() {
                if self.expressions_match(expr, group_expr) {
                    return Ok(group_key[i].clone().into());
                }
            }

            // If not found in GROUP BY, try to evaluate using the first traverser
            group_traversers
                .first()
                .map(|t| self.evaluate_value_from_path(expr, t))
                .ok_or(CompileError::EmptyPattern)
        }
    }

    /// Compute an aggregate function over group traversers with path support.
    fn compute_aggregate_multi_var(
        &self,
        func: AggregateFunc,
        distinct: bool,
        expr: &Expression,
        traversers: &[crate::traversal::Traverser],
    ) -> Result<Value, CompileError> {
        // Handle COUNT(*) specially
        let is_count_star = matches!(func, AggregateFunc::Count)
            && matches!(expr, Expression::Variable(v) if v == "*");

        if is_count_star {
            return Ok(Value::Int(traversers.len() as i64));
        }

        // Extract values to aggregate using path-based evaluation
        let mut values: Vec<Value> = traversers
            .iter()
            .filter_map(|t| {
                let val = self.evaluate_value_from_path(expr, t);
                if matches!(val, Value::Null) {
                    None
                } else {
                    Some(val)
                }
            })
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

        // Compute the aggregate
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
                        _ => {}
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
                        _ => {}
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

    /// Check if an expression matches any expression in the GROUP BY clause.
    fn expression_in_group_by(&self, expr: &Expression, group_by: &GroupByClause) -> bool {
        group_by
            .expressions
            .iter()
            .any(|group_expr| self.expressions_match(expr, group_expr))
    }

    /// Check if two expressions are structurally equivalent.
    ///
    /// This is a simple structural comparison for common cases.
    fn expressions_match(&self, a: &Expression, b: &Expression) -> bool {
        match (a, b) {
            (Expression::Variable(va), Expression::Variable(vb)) => va == vb,
            (
                Expression::Property {
                    variable: va,
                    property: pa,
                },
                Expression::Property {
                    variable: vb,
                    property: pb,
                },
            ) => va == vb && pa == pb,
            (Expression::Literal(la), Expression::Literal(lb)) => la == lb,
            _ => false,
        }
    }

    /// Convert an expression to a string for error messages.
    fn expression_to_string(&self, expr: &Expression) -> String {
        match expr {
            Expression::Variable(v) => v.clone(),
            Expression::Property { variable, property } => format!("{}.{}", variable, property),
            Expression::Literal(lit) => match lit {
                Literal::Null => "null".to_string(),
                Literal::Bool(b) => b.to_string(),
                Literal::Int(n) => n.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => format!("'{}'", s),
            },
            Expression::Aggregate { func, .. } => {
                let func_name = match func {
                    AggregateFunc::Count => "count",
                    AggregateFunc::Sum => "sum",
                    AggregateFunc::Avg => "avg",
                    AggregateFunc::Min => "min",
                    AggregateFunc::Max => "max",
                    AggregateFunc::Collect => "collect",
                };
                format!("{}(...)", func_name)
            }
            _ => "<expression>".to_string(),
        }
    }

    /// Compute the result for a single group.
    fn compute_group_result(
        &self,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_elements: &[Value],
    ) -> Result<Value, CompileError> {
        if return_clause.items.len() == 1 {
            // Single return item
            let item = &return_clause.items[0];
            let value = self.evaluate_group_expression(
                &item.expression,
                group_by,
                group_key,
                group_elements,
            )?;

            if item.alias.is_some() {
                let mut map = HashMap::new();
                map.insert(self.get_return_item_key(item), value);
                Ok(Value::Map(map))
            } else {
                Ok(value)
            }
        } else {
            // Multiple return items - return a map
            let mut map = HashMap::new();

            for item in &return_clause.items {
                let key = self.get_return_item_key(item);
                let value = self.evaluate_group_expression(
                    &item.expression,
                    group_by,
                    group_key,
                    group_elements,
                )?;
                map.insert(key, value);
            }

            Ok(Value::Map(map))
        }
    }

    /// Evaluate an expression in the context of a group.
    ///
    /// If the expression is an aggregate, compute it over the group elements.
    /// If it's a GROUP BY expression, return the corresponding key value.
    fn evaluate_group_expression(
        &self,
        expr: &Expression,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_elements: &[Value],
    ) -> Result<Value, CompileError> {
        if let Expression::Aggregate {
            func,
            distinct,
            expr: inner,
        } = expr
        {
            // Compute aggregate over group
            self.compute_aggregate(*func, *distinct, inner, group_elements)
        } else {
            // Non-aggregate: should be a GROUP BY expression
            // Find which group_by expression matches and return the corresponding key value
            for (i, group_expr) in group_by.expressions.iter().enumerate() {
                if self.expressions_match(expr, group_expr) {
                    return Ok(group_key[i].clone().into());
                }
            }

            // If not found in GROUP BY, try to evaluate using the first element
            // This handles edge cases like literals
            group_elements
                .first()
                .map(|e| self.evaluate_value(expr, e))
                .ok_or_else(|| CompileError::EmptyPattern)
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
            Expression::Variable(var) => {
                // Check if result is a Map and the variable is a key in it (e.g., an alias)
                if let Value::Map(map) = result {
                    if let Some(val) = map.get(var) {
                        return val.clone();
                    }
                }
                // Otherwise, use the result directly
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
