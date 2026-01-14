//! GQL mutation compiler and executor.
//!
//! This module provides compilation and execution of GQL mutation statements
//! (CREATE, SET, REMOVE, DELETE, DETACH DELETE, MERGE).
//!
//! # Architecture
//!
//! Mutation execution follows this pipeline:
//!
//! ```text
//! MutationQuery AST → MutationCompiler → MutationPlan → Execution → Results
//! ```
//!
//! Unlike read-only queries that work with immutable [`GraphSnapshot`]s,
//! mutations require mutable access via [`GraphStorageMut`].
//!
//! # Example
//!
//! ```ignore
//! use intersteller::gql::{parse_statement, execute_mutation};
//! use intersteller::storage::InMemoryGraph;
//!
//! let mut storage = InMemoryGraph::new();
//!
//! // Parse and execute a CREATE statement
//! let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30}) RETURN n").unwrap();
//! let results = execute_mutation(&stmt, &mut storage).unwrap();
//! ```
//!
//! [`GraphSnapshot`]: crate::graph::GraphSnapshot
//! [`GraphStorageMut`]: crate::storage::GraphStorageMut

use std::collections::HashMap;

use crate::error::StorageError;
use crate::gql::ast::{
    CreateClause, DeleteClause, DetachDeleteClause, EdgeDirection, Expression, ForeachClause,
    ForeachMutation, MatchClause, MergeClause, MutationClause, MutationQuery, Pattern,
    PatternElement, RemoveClause, ReturnClause, ReturnItem, SetClause, SetItem, Statement,
    WhereClause,
};
use crate::gql::error::CompileError;
use crate::schema::{
    validate_edge, validate_property_update, validate_vertex, GraphSchema, ValidationMode,
    ValidationResult,
};
use crate::storage::{GraphStorage, GraphStorageMut};
use crate::value::{EdgeId, Value, VertexId};

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during mutation execution.
#[derive(Debug, thiserror::Error)]
pub enum MutationError {
    /// Compilation error (variable binding, pattern issues)
    #[error("Compilation error: {0}")]
    Compile(#[from] CompileError),

    /// Storage error during mutation
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Variable not bound during execution
    #[error("Unbound variable '{0}': variable was not bound by MATCH or CREATE")]
    UnboundVariable(String),

    /// Cannot delete vertex with edges (use DETACH DELETE)
    #[error("Cannot delete vertex {0:?}: vertex has connected edges. Use DETACH DELETE to remove edges automatically.")]
    VertexHasEdges(VertexId),

    /// Invalid element type for operation
    #[error("Invalid element type for {operation}: expected {expected}, got {actual}")]
    InvalidElementType {
        operation: &'static str,
        expected: &'static str,
        actual: String,
    },

    /// Pattern requires a label for CREATE
    #[error("CREATE requires a label for new vertices")]
    MissingLabel,

    /// Edge requires source and target
    #[error("CREATE edge requires both source and target vertices")]
    IncompleteEdge,

    /// Schema validation error
    #[error("Schema validation error: {0}")]
    Schema(#[from] crate::schema::SchemaError),
}

// =============================================================================
// Execution Context
// =============================================================================

/// Context for tracking variable bindings during mutation execution.
///
/// The `MutationContext` maintains:
/// - Variables bound from MATCH clauses
/// - Variables for newly created elements
/// - Access to the underlying storage
/// - Optional schema for validation
#[derive(Debug)]
pub struct MutationContext<'s, S: GraphStorage + GraphStorageMut> {
    /// Mutable reference to storage
    storage: &'s mut S,
    /// Variables bound to vertex IDs
    vertex_bindings: HashMap<String, VertexId>,
    /// Variables bound to edge IDs
    edge_bindings: HashMap<String, EdgeId>,
    /// Variables bound to primitive values (for FOREACH iteration)
    value_bindings: HashMap<String, Value>,
    /// Optional schema for validation
    schema: Option<&'s GraphSchema>,
}

impl<'s, S: GraphStorage + GraphStorageMut> MutationContext<'s, S> {
    /// Create a new mutation context with the given storage.
    pub fn new(storage: &'s mut S) -> Self {
        Self {
            storage,
            vertex_bindings: HashMap::new(),
            edge_bindings: HashMap::new(),
            value_bindings: HashMap::new(),
            schema: None,
        }
    }

    /// Create a new mutation context with optional schema validation.
    pub fn with_schema(storage: &'s mut S, schema: Option<&'s GraphSchema>) -> Self {
        Self {
            storage,
            vertex_bindings: HashMap::new(),
            edge_bindings: HashMap::new(),
            value_bindings: HashMap::new(),
            schema,
        }
    }

    /// Get the optional schema reference.
    pub fn schema(&self) -> Option<&GraphSchema> {
        self.schema
    }

    /// Bind a variable to a vertex ID.
    pub fn bind_vertex(&mut self, variable: &str, id: VertexId) {
        self.vertex_bindings.insert(variable.to_string(), id);
    }

    /// Bind a variable to an edge ID.
    pub fn bind_edge(&mut self, variable: &str, id: EdgeId) {
        self.edge_bindings.insert(variable.to_string(), id);
    }

    /// Bind a variable to a primitive value (for FOREACH iteration).
    pub fn bind_value(&mut self, variable: &str, value: Value) {
        self.value_bindings.insert(variable.to_string(), value);
    }

    /// Get the vertex ID for a variable.
    pub fn get_vertex(&self, variable: &str) -> Option<VertexId> {
        self.vertex_bindings.get(variable).copied()
    }

    /// Get the edge ID for a variable.
    pub fn get_edge(&self, variable: &str) -> Option<EdgeId> {
        self.edge_bindings.get(variable).copied()
    }

    /// Get a primitive value for a variable (for FOREACH iteration).
    pub fn get_value(&self, variable: &str) -> Option<&Value> {
        self.value_bindings.get(variable)
    }

    /// Get the element (vertex or edge) for a variable.
    pub fn get_element(&self, variable: &str) -> Option<Element> {
        if let Some(vid) = self.vertex_bindings.get(variable) {
            return Some(Element::Vertex(*vid));
        }
        if let Some(eid) = self.edge_bindings.get(variable) {
            return Some(Element::Edge(*eid));
        }
        None
    }

    /// Check if a variable is bound.
    pub fn is_bound(&self, variable: &str) -> bool {
        self.vertex_bindings.contains_key(variable)
            || self.edge_bindings.contains_key(variable)
            || self.value_bindings.contains_key(variable)
    }

    /// Get mutable storage reference.
    pub fn storage_mut(&mut self) -> &mut S {
        self.storage
    }

    /// Get immutable storage reference.
    pub fn storage(&self) -> &S {
        self.storage
    }

    /// Clear all bindings (for processing next match result).
    pub fn clear_bindings(&mut self) {
        self.vertex_bindings.clear();
        self.edge_bindings.clear();
        self.value_bindings.clear();
    }

    /// Clone current bindings (for nested operations).
    pub fn clone_bindings(&self) -> (HashMap<String, VertexId>, HashMap<String, EdgeId>) {
        (self.vertex_bindings.clone(), self.edge_bindings.clone())
    }

    /// Clone current bindings including value bindings (for nested operations).
    pub fn clone_all_bindings(
        &self,
    ) -> (
        HashMap<String, VertexId>,
        HashMap<String, EdgeId>,
        HashMap<String, Value>,
    ) {
        (
            self.vertex_bindings.clone(),
            self.edge_bindings.clone(),
            self.value_bindings.clone(),
        )
    }

    /// Restore bindings from a previous state.
    pub fn restore_bindings(
        &mut self,
        vertex_bindings: HashMap<String, VertexId>,
        edge_bindings: HashMap<String, EdgeId>,
    ) {
        self.vertex_bindings = vertex_bindings;
        self.edge_bindings = edge_bindings;
    }

    /// Remove a value binding.
    pub fn remove_value_binding(&mut self, variable: &str) {
        self.value_bindings.remove(variable);
    }
}

/// An element reference (vertex or edge).
#[derive(Debug, Clone, Copy)]
pub enum Element {
    Vertex(VertexId),
    Edge(EdgeId),
}

// =============================================================================
// Mutation Execution
// =============================================================================

/// Execute a GQL mutation statement against mutable storage.
///
/// This is the main entry point for executing GQL mutations.
///
/// # Arguments
///
/// * `stmt` - A parsed GQL statement (must be a Mutation variant)
/// * `storage` - Mutable reference to graph storage
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing RETURN clause results (empty if no RETURN).
///
/// # Errors
///
/// Returns [`MutationError`] if:
/// - The statement is not a mutation
/// - Variable binding fails
/// - Storage operations fail
/// - DELETE on vertex with edges (use DETACH DELETE)
///
/// # Example
///
/// ```ignore
/// use intersteller::gql::{parse_statement, execute_mutation};
/// use intersteller::storage::InMemoryGraph;
///
/// let mut storage = InMemoryGraph::new();
/// let stmt = parse_statement("CREATE (n:Person {name: 'Alice'})").unwrap();
/// execute_mutation(&stmt, &mut storage).unwrap();
///
/// assert_eq!(storage.vertex_count(), 1);
/// ```
pub fn execute_mutation<S: GraphStorage + GraphStorageMut>(
    stmt: &Statement,
    storage: &mut S,
) -> Result<Vec<Value>, MutationError> {
    match stmt {
        Statement::Mutation(mutation) => execute_mutation_query(mutation.as_ref(), storage),
        Statement::Query(_) | Statement::Union { .. } => Err(MutationError::Compile(
            CompileError::UnsupportedFeature("Expected mutation statement, got read query".into()),
        )),
        Statement::Ddl(_) => Err(MutationError::Compile(CompileError::UnsupportedFeature(
            "Expected mutation statement, got DDL statement. Use execute_ddl() instead.".into(),
        ))),
    }
}

/// Execute a GQL mutation statement with optional schema validation.
///
/// This variant of [`execute_mutation`] accepts an optional schema for validating
/// CREATE, SET, and MERGE operations. Validation behavior depends on the schema's
/// [`ValidationMode`].
///
/// # Arguments
///
/// * `stmt` - A parsed GQL statement (must be a Mutation variant)
/// * `storage` - Mutable reference to graph storage
/// * `schema` - Optional schema for validation
///
/// # Validation Behavior
///
/// When a schema is provided:
/// - CREATE vertex: Validates label is known (in CLOSED mode), required properties
///   are present, and property types match
/// - CREATE edge: Validates source/target labels and properties
/// - SET: Validates property updates against schema definitions
/// - MERGE: When creating new elements, validates like CREATE
///
/// # Example
///
/// ```ignore
/// use intersteller::gql::{parse_statement, execute_mutation_with_schema};
/// use intersteller::schema::{SchemaBuilder, PropertyType, ValidationMode};
/// use intersteller::storage::InMemoryGraph;
///
/// let mut storage = InMemoryGraph::new();
/// let schema = SchemaBuilder::new()
///     .mode(ValidationMode::Strict)
///     .vertex("Person")
///         .property("name", PropertyType::String)
///         .done()
///     .build();
///
/// let stmt = parse_statement("CREATE (n:Person {name: 'Alice'})").unwrap();
/// execute_mutation_with_schema(&stmt, &mut storage, Some(&schema)).unwrap();
/// ```
pub fn execute_mutation_with_schema<S: GraphStorage + GraphStorageMut>(
    stmt: &Statement,
    storage: &mut S,
    schema: Option<&GraphSchema>,
) -> Result<Vec<Value>, MutationError> {
    match stmt {
        Statement::Mutation(mutation) => {
            execute_mutation_query_with_schema(mutation.as_ref(), storage, schema)
        }
        Statement::Query(_) | Statement::Union { .. } => Err(MutationError::Compile(
            CompileError::UnsupportedFeature("Expected mutation statement, got read query".into()),
        )),
        Statement::Ddl(_) => Err(MutationError::Compile(CompileError::UnsupportedFeature(
            "Expected mutation statement, got DDL statement. Use execute_ddl() instead.".into(),
        ))),
    }
}

/// Execute a mutation query against mutable storage.
pub fn execute_mutation_query<S: GraphStorage + GraphStorageMut>(
    query: &MutationQuery,
    storage: &mut S,
) -> Result<Vec<Value>, MutationError> {
    execute_mutation_query_with_schema(query, storage, None)
}

/// Execute a mutation query with optional schema validation.
pub fn execute_mutation_query_with_schema<S: GraphStorage + GraphStorageMut>(
    query: &MutationQuery,
    storage: &mut S,
    schema: Option<&GraphSchema>,
) -> Result<Vec<Value>, MutationError> {
    let mut ctx = MutationContext::with_schema(storage, schema);
    let mut results = Vec::new();

    // If there's a MATCH clause, execute mutations for each match result
    if let Some(match_clause) = &query.match_clause {
        // Execute the MATCH pattern to get bindings
        let match_results = execute_match(&ctx, match_clause, &query.where_clause)?;

        if match_results.is_empty() {
            // No matches - no mutations to perform
            return Ok(vec![]);
        }

        // For each match result, execute the mutation clauses
        for bindings in match_results {
            // Restore bindings for this match
            ctx.vertex_bindings = bindings.vertices;
            ctx.edge_bindings = bindings.edges;

            // Execute each mutation clause
            for mutation in &query.mutations {
                execute_mutation_clause(&mut ctx, mutation)?;
            }

            // Execute each FOREACH clause
            for foreach_clause in &query.foreach_clauses {
                execute_foreach(&mut ctx, foreach_clause)?;
            }

            // Collect RETURN results if present
            if let Some(return_clause) = &query.return_clause {
                if let Some(result) = evaluate_return(&ctx, return_clause)? {
                    results.push(result);
                }
            }
        }
    } else {
        // No MATCH clause - direct mutations (CREATE, MERGE)
        for mutation in &query.mutations {
            execute_mutation_clause(&mut ctx, mutation)?;
        }

        // Execute each FOREACH clause
        for foreach_clause in &query.foreach_clauses {
            execute_foreach(&mut ctx, foreach_clause)?;
        }

        // Collect RETURN results if present
        if let Some(return_clause) = &query.return_clause {
            if let Some(result) = evaluate_return(&ctx, return_clause)? {
                results.push(result);
            }
        }
    }

    Ok(results)
}

/// Bindings from a single MATCH result.
#[derive(Debug, Clone, Default)]
struct MatchBindings {
    vertices: HashMap<String, VertexId>,
    edges: HashMap<String, EdgeId>,
}

/// Execute a MATCH clause and return variable bindings.
fn execute_match<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    match_clause: &MatchClause,
    where_clause: &Option<WhereClause>,
) -> Result<Vec<MatchBindings>, MutationError> {
    let mut all_bindings = Vec::new();

    // For each pattern in the MATCH clause
    for pattern in &match_clause.patterns {
        let pattern_bindings = match_pattern(ctx, pattern)?;

        // Apply WHERE filter if present
        let filtered = if let Some(where_cl) = where_clause {
            pattern_bindings
                .into_iter()
                .filter(|b| evaluate_where_filter(ctx, where_cl, b))
                .collect()
        } else {
            pattern_bindings
        };

        all_bindings.extend(filtered);
    }

    Ok(all_bindings)
}

/// Match a pattern against the graph and return bindings.
fn match_pattern<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    pattern: &Pattern,
) -> Result<Vec<MatchBindings>, MutationError> {
    if pattern.elements.is_empty() {
        return Ok(vec![]);
    }

    // Start with all vertices that match the first node pattern
    let first_element = &pattern.elements[0];
    let PatternElement::Node(first_node) = first_element else {
        return Err(MutationError::Compile(
            CompileError::PatternMustStartWithNode,
        ));
    };

    // Get initial candidates
    let mut current_bindings: Vec<MatchBindings> = Vec::new();

    for vertex in ctx.storage().all_vertices() {
        // Check label filter
        if !first_node.labels.is_empty() && !first_node.labels.contains(&vertex.label) {
            continue;
        }

        // Check property filters
        let mut matches = true;
        for (key, lit) in &first_node.properties {
            let expected: Value = lit.clone().into();
            match vertex.properties.get(key) {
                Some(actual) if *actual == expected => {}
                _ => {
                    matches = false;
                    break;
                }
            }
        }

        if matches {
            let mut bindings = MatchBindings::default();
            if let Some(var) = &first_node.variable {
                bindings.vertices.insert(var.clone(), vertex.id);
            }
            current_bindings.push(bindings);
        }
    }

    // Process remaining pattern elements (edges and nodes)
    let mut element_iter = pattern.elements.iter().skip(1).peekable();
    while let Some(element) = element_iter.next() {
        match element {
            PatternElement::Edge(edge) => {
                // Must be followed by a node
                let Some(PatternElement::Node(target_node)) = element_iter.next() else {
                    continue; // Skip malformed patterns
                };

                let mut new_bindings = Vec::new();

                for binding in &current_bindings {
                    // Get the last bound vertex as the source
                    let source_id = binding
                        .vertices
                        .values()
                        .last()
                        .copied()
                        .ok_or_else(|| MutationError::UnboundVariable("source".to_string()))?;

                    // Get edges from the source vertex based on direction
                    let edges: Vec<_> = match edge.direction {
                        EdgeDirection::Outgoing => ctx.storage().out_edges(source_id).collect(),
                        EdgeDirection::Incoming => ctx.storage().in_edges(source_id).collect(),
                        EdgeDirection::Both => {
                            let mut all = ctx.storage().out_edges(source_id).collect::<Vec<_>>();
                            all.extend(ctx.storage().in_edges(source_id));
                            all
                        }
                    };

                    for edge_ref in edges {
                        // Check edge label filter
                        if !edge.labels.is_empty() && !edge.labels.contains(&edge_ref.label) {
                            continue;
                        }

                        // Check edge property filters
                        let mut edge_matches = true;
                        for (key, lit) in &edge.properties {
                            let expected: Value = lit.clone().into();
                            match edge_ref.properties.get(key) {
                                Some(actual) if *actual == expected => {}
                                _ => {
                                    edge_matches = false;
                                    break;
                                }
                            }
                        }
                        if !edge_matches {
                            continue;
                        }

                        // Get the target vertex
                        let target_id = match edge.direction {
                            EdgeDirection::Outgoing => edge_ref.dst,
                            EdgeDirection::Incoming => edge_ref.src,
                            EdgeDirection::Both => {
                                if edge_ref.src == source_id {
                                    edge_ref.dst
                                } else {
                                    edge_ref.src
                                }
                            }
                        };

                        // Check target vertex constraints
                        let Some(target_vertex) = ctx.storage().get_vertex(target_id) else {
                            continue;
                        };

                        // Check target label filter
                        if !target_node.labels.is_empty()
                            && !target_node.labels.contains(&target_vertex.label)
                        {
                            continue;
                        }

                        // Check target property filters
                        let mut target_matches = true;
                        for (key, lit) in &target_node.properties {
                            let expected: Value = lit.clone().into();
                            match target_vertex.properties.get(key) {
                                Some(actual) if *actual == expected => {}
                                _ => {
                                    target_matches = false;
                                    break;
                                }
                            }
                        }
                        if !target_matches {
                            continue;
                        }

                        // Create new binding with edge and target vertex
                        let mut new_binding = binding.clone();
                        if let Some(var) = &edge.variable {
                            new_binding.edges.insert(var.clone(), edge_ref.id);
                        }
                        if let Some(var) = &target_node.variable {
                            new_binding.vertices.insert(var.clone(), target_id);
                        }
                        new_bindings.push(new_binding);
                    }
                }

                current_bindings = new_bindings;
            }
            PatternElement::Node(_) => {
                // Standalone nodes after the first should be handled differently
                // (comma-separated patterns in MATCH)
                // For now, skip
            }
        }
    }

    Ok(current_bindings)
}

/// Evaluate a WHERE clause filter against bindings.
fn evaluate_where_filter<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    where_clause: &WhereClause,
    bindings: &MatchBindings,
) -> bool {
    evaluate_predicate(ctx, &where_clause.expression, bindings)
}

/// Evaluate a predicate expression.
fn evaluate_predicate<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    expr: &Expression,
    bindings: &MatchBindings,
) -> bool {
    match evaluate_expression(ctx, expr, bindings) {
        Value::Bool(b) => b,
        Value::Null => false,
        _ => true, // Truthy for non-null non-bool values
    }
}

/// Evaluate an expression using bindings for variable lookup.
fn evaluate_expression<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    expr: &Expression,
    bindings: &MatchBindings,
) -> Value {
    use crate::gql::ast::BinaryOperator;

    match expr {
        Expression::Literal(lit) => lit.clone().into(),
        Expression::Variable(var) => {
            // Look up variable in bindings and return as Value
            // Check vertex bindings first
            if let Some(vid) = bindings.vertices.get(var) {
                Value::Vertex(*vid)
            } else if let Some(eid) = bindings.edges.get(var) {
                Value::Edge(*eid)
            } else if let Some(value) = ctx.get_value(var) {
                // Check value_bindings for FOREACH iteration variables
                value.clone()
            } else {
                Value::Null
            }
        }
        Expression::Property { variable, property } => {
            // Get the element and extract property
            if let Some(vid) = bindings.vertices.get(variable) {
                if let Some(vertex) = ctx.storage().get_vertex(*vid) {
                    return vertex
                        .properties
                        .get(property)
                        .cloned()
                        .unwrap_or(Value::Null);
                }
            } else if let Some(eid) = bindings.edges.get(variable) {
                if let Some(edge) = ctx.storage().get_edge(*eid) {
                    return edge
                        .properties
                        .get(property)
                        .cloned()
                        .unwrap_or(Value::Null);
                }
            }
            Value::Null
        }
        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_expression(ctx, left, bindings);
            let right_val = evaluate_expression(ctx, right, bindings);

            match op {
                BinaryOperator::Eq => Value::Bool(left_val == right_val),
                BinaryOperator::Neq => Value::Bool(left_val != right_val),
                BinaryOperator::Lt => Value::Bool(
                    compare_values(&left_val, &right_val) == Some(std::cmp::Ordering::Less),
                ),
                BinaryOperator::Lte => Value::Bool(matches!(
                    compare_values(&left_val, &right_val),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )),
                BinaryOperator::Gt => Value::Bool(
                    compare_values(&left_val, &right_val) == Some(std::cmp::Ordering::Greater),
                ),
                BinaryOperator::Gte => Value::Bool(matches!(
                    compare_values(&left_val, &right_val),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )),
                BinaryOperator::And => Value::Bool(
                    left_val.as_bool().unwrap_or(false) && right_val.as_bool().unwrap_or(false),
                ),
                BinaryOperator::Or => Value::Bool(
                    left_val.as_bool().unwrap_or(false) || right_val.as_bool().unwrap_or(false),
                ),
                BinaryOperator::Add => {
                    apply_arithmetic(&left_val, &right_val, |a, b| a + b, |a, b| a + b)
                }
                BinaryOperator::Sub => {
                    apply_arithmetic(&left_val, &right_val, |a, b| a - b, |a, b| a - b)
                }
                BinaryOperator::Mul => {
                    apply_arithmetic(&left_val, &right_val, |a, b| a * b, |a, b| a * b)
                }
                BinaryOperator::Div => apply_arithmetic(
                    &left_val,
                    &right_val,
                    |a, b| if b != 0 { a / b } else { 0 },
                    |a, b| if b != 0.0 { a / b } else { f64::NAN },
                ),
                BinaryOperator::Mod => apply_arithmetic(
                    &left_val,
                    &right_val,
                    |a, b| if b != 0 { a % b } else { 0 },
                    |a, b| if b != 0.0 { a % b } else { f64::NAN },
                ),
                _ => Value::Null, // Other operators not supported in mutations
            }
        }
        Expression::UnaryOp { op, expr } => {
            use crate::gql::ast::UnaryOperator;
            let val = evaluate_expression(ctx, expr, bindings);
            match op {
                UnaryOperator::Not => match val {
                    Value::Bool(b) => Value::Bool(!b),
                    Value::Null => Value::Null,
                    _ => Value::Bool(false),
                },
                UnaryOperator::Neg => match val {
                    Value::Int(n) => Value::Int(-n),
                    Value::Float(f) => Value::Float(-f),
                    _ => Value::Null,
                },
            }
        }
        Expression::IsNull { expr, negated } => {
            let val = evaluate_expression(ctx, expr, bindings);
            let is_null = matches!(val, Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }
        Expression::InList {
            expr,
            list,
            negated,
        } => {
            let val = evaluate_expression(ctx, expr, bindings);
            let in_list = list.iter().any(|item| {
                let item_val = evaluate_expression(ctx, item, bindings);
                val == item_val
            });
            Value::Bool(if *negated { !in_list } else { in_list })
        }
        Expression::List(items) => {
            // Evaluate each item in the list and return a Value::List
            let evaluated: Vec<Value> = items
                .iter()
                .map(|item| evaluate_expression(ctx, item, bindings))
                .collect();
            Value::List(evaluated)
        }
        _ => Value::Null,
    }
}

/// Compare two values for ordering.
fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Apply arithmetic operation.
fn apply_arithmetic<F1, F2>(left: &Value, right: &Value, int_op: F1, float_op: F2) -> Value
where
    F1: Fn(i64, i64) -> i64,
    F2: Fn(f64, f64) -> f64,
{
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Value::Int(int_op(*a, *b)),
        (Value::Float(a), Value::Float(b)) => Value::Float(float_op(*a, *b)),
        (Value::Int(a), Value::Float(b)) => Value::Float(float_op(*a as f64, *b)),
        (Value::Float(a), Value::Int(b)) => Value::Float(float_op(*a, *b as f64)),
        _ => Value::Null,
    }
}

// =============================================================================
// Mutation Clause Execution
// =============================================================================

/// Check validation results and return error if any failed.
///
/// In WARN mode, validation failures are logged but don't block the operation.
/// In STRICT/CLOSED mode, any error result will be returned.
fn check_validation_results(
    results: &[ValidationResult],
    mode: ValidationMode,
) -> Result<(), MutationError> {
    for result in results {
        if let ValidationResult::Error(err) = result {
            // In Warn mode, errors were already converted to warnings during validation
            // So if we see an Error here, we're in Strict/Closed mode
            if mode != ValidationMode::Warn {
                return Err(MutationError::Schema(err.clone()));
            }
        }
    }
    Ok(())
}

/// Execute a single mutation clause.
fn execute_mutation_clause<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    mutation: &MutationClause,
) -> Result<(), MutationError> {
    match mutation {
        MutationClause::Create(create) => execute_create(ctx, create),
        MutationClause::Set(set) => execute_set(ctx, set),
        MutationClause::Remove(remove) => execute_remove(ctx, remove),
        MutationClause::Delete(delete) => execute_delete(ctx, delete),
        MutationClause::DetachDelete(detach) => execute_detach_delete(ctx, detach),
        MutationClause::Merge(merge) => execute_merge(ctx, merge),
    }
}

/// Execute a CREATE clause.
fn execute_create<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    create: &CreateClause,
) -> Result<(), MutationError> {
    for pattern in &create.patterns {
        create_pattern(ctx, pattern)?;
    }
    Ok(())
}

/// Create elements from a pattern.
fn create_pattern<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    pattern: &Pattern,
) -> Result<(), MutationError> {
    let mut prev_vertex_id: Option<VertexId> = None;
    let mut prev_vertex_label: Option<String> = None;
    let mut pending_edge: Option<(&crate::gql::ast::EdgePattern, VertexId, String)> = None;

    for element in &pattern.elements {
        match element {
            PatternElement::Node(node) => {
                // Check if this variable is already bound (reference to existing vertex)
                let (vertex_id, vertex_label) = if let Some(var) = &node.variable {
                    if let Some(existing_id) = ctx.get_vertex(var) {
                        // Variable already bound - use existing vertex
                        // Get the label from storage
                        let label = ctx
                            .storage()
                            .get_vertex(existing_id)
                            .map(|v| v.label.clone())
                            .unwrap_or_default();
                        (existing_id, label)
                    } else {
                        // Create new vertex
                        let label = node.labels.first().ok_or(MutationError::MissingLabel)?;

                        let properties: HashMap<String, Value> = node
                            .properties
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone().into()))
                            .collect();

                        // Validate vertex against schema before creating
                        if let Some(schema) = ctx.schema() {
                            let results = validate_vertex(schema, label, &properties)?;
                            check_validation_results(&results, schema.mode)?;
                        }

                        let id = ctx.storage_mut().add_vertex(label, properties);
                        ctx.bind_vertex(var, id);
                        (id, label.to_string())
                    }
                } else {
                    // Anonymous vertex - always create new
                    let label = node.labels.first().ok_or(MutationError::MissingLabel)?;

                    let properties: HashMap<String, Value> = node
                        .properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone().into()))
                        .collect();

                    // Validate vertex against schema before creating
                    if let Some(schema) = ctx.schema() {
                        let results = validate_vertex(schema, label, &properties)?;
                        check_validation_results(&results, schema.mode)?;
                    }

                    let id = ctx.storage_mut().add_vertex(label, properties);
                    (id, label.to_string())
                };

                // If we have a pending edge, create it now
                if let Some((edge_pattern, from_id, from_label)) = pending_edge.take() {
                    let to_id = vertex_id;
                    let to_label = &vertex_label;

                    let (src, dst, src_label, dst_label) = match edge_pattern.direction {
                        EdgeDirection::Outgoing => (from_id, to_id, &from_label, to_label),
                        EdgeDirection::Incoming => (to_id, from_id, to_label, &from_label),
                        EdgeDirection::Both => (from_id, to_id, &from_label, to_label), // Default to outgoing for bidirectional in CREATE
                    };

                    let edge_label = edge_pattern
                        .labels
                        .first()
                        .map(|s| s.as_str())
                        .unwrap_or("edge");

                    let edge_properties: HashMap<String, Value> = edge_pattern
                        .properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone().into()))
                        .collect();

                    // Validate edge against schema before creating
                    if let Some(schema) = ctx.schema() {
                        let results = validate_edge(
                            schema,
                            edge_label,
                            src_label,
                            dst_label,
                            &edge_properties,
                        )?;
                        check_validation_results(&results, schema.mode)?;
                    }

                    let edge_id =
                        ctx.storage_mut()
                            .add_edge(src, dst, edge_label, edge_properties)?;

                    if let Some(var) = &edge_pattern.variable {
                        ctx.bind_edge(var, edge_id);
                    }
                }

                prev_vertex_id = Some(vertex_id);
                prev_vertex_label = Some(vertex_label);
            }
            PatternElement::Edge(edge) => {
                // Store edge to create after the next node
                let from_id = prev_vertex_id.ok_or(MutationError::IncompleteEdge)?;
                let from_label = prev_vertex_label
                    .clone()
                    .ok_or(MutationError::IncompleteEdge)?;
                pending_edge = Some((edge, from_id, from_label));
            }
        }
    }

    // Check for dangling edge
    if pending_edge.is_some() {
        return Err(MutationError::IncompleteEdge);
    }

    Ok(())
}

/// Execute a SET clause.
fn execute_set<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    set: &SetClause,
) -> Result<(), MutationError> {
    for item in &set.items {
        execute_set_item(ctx, item)?;
    }
    Ok(())
}

/// Execute a single SET item.
fn execute_set_item<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    item: &SetItem,
) -> Result<(), MutationError> {
    let element = ctx
        .get_element(&item.target.variable)
        .ok_or_else(|| MutationError::UnboundVariable(item.target.variable.clone()))?;

    // Create temporary bindings for expression evaluation
    let (vertices, edges) = ctx.clone_bindings();
    let bindings = MatchBindings { vertices, edges };

    let value = evaluate_expression(ctx, &item.value, &bindings);

    match element {
        Element::Vertex(vid) => {
            // Validate property update against schema
            if let Some(schema) = ctx.schema() {
                if let Some(vertex) = ctx.storage().get_vertex(vid) {
                    let results = validate_property_update(
                        schema,
                        &vertex.label,
                        &item.target.property,
                        &value,
                        true, // is_vertex
                    )?;
                    check_validation_results(&results, schema.mode)?;
                }
            }

            ctx.storage_mut()
                .set_vertex_property(vid, &item.target.property, value)?;
        }
        Element::Edge(eid) => {
            // Validate property update against schema
            if let Some(schema) = ctx.schema() {
                if let Some(edge) = ctx.storage().get_edge(eid) {
                    let results = validate_property_update(
                        schema,
                        &edge.label,
                        &item.target.property,
                        &value,
                        false, // is_vertex
                    )?;
                    check_validation_results(&results, schema.mode)?;
                }
            }

            ctx.storage_mut()
                .set_edge_property(eid, &item.target.property, value)?;
        }
    }

    Ok(())
}

/// Execute a REMOVE clause.
fn execute_remove<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    remove: &RemoveClause,
) -> Result<(), MutationError> {
    for prop_ref in &remove.properties {
        let element = ctx
            .get_element(&prop_ref.variable)
            .ok_or_else(|| MutationError::UnboundVariable(prop_ref.variable.clone()))?;

        match element {
            Element::Vertex(vid) => {
                // Set property to Null to effectively remove it
                ctx.storage_mut()
                    .set_vertex_property(vid, &prop_ref.property, Value::Null)?;
            }
            Element::Edge(eid) => {
                ctx.storage_mut()
                    .set_edge_property(eid, &prop_ref.property, Value::Null)?;
            }
        }
    }
    Ok(())
}

/// Execute a DELETE clause.
fn execute_delete<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    delete: &DeleteClause,
) -> Result<(), MutationError> {
    for var in &delete.variables {
        let element = ctx
            .get_element(var)
            .ok_or_else(|| MutationError::UnboundVariable(var.clone()))?;

        match element {
            Element::Vertex(vid) => {
                // Check if vertex has edges
                let has_edges = ctx.storage().out_edges(vid).next().is_some()
                    || ctx.storage().in_edges(vid).next().is_some();

                if has_edges {
                    return Err(MutationError::VertexHasEdges(vid));
                }

                ctx.storage_mut().remove_vertex(vid)?;
            }
            Element::Edge(eid) => {
                ctx.storage_mut().remove_edge(eid)?;
            }
        }
    }
    Ok(())
}

/// Execute a DETACH DELETE clause.
fn execute_detach_delete<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    detach: &DetachDeleteClause,
) -> Result<(), MutationError> {
    for var in &detach.variables {
        let element = ctx
            .get_element(var)
            .ok_or_else(|| MutationError::UnboundVariable(var.clone()))?;

        match element {
            Element::Vertex(vid) => {
                // remove_vertex in InMemoryGraph already removes incident edges
                ctx.storage_mut().remove_vertex(vid)?;
            }
            Element::Edge(eid) => {
                ctx.storage_mut().remove_edge(eid)?;
            }
        }
    }
    Ok(())
}

/// Execute a MERGE clause.
fn execute_merge<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    merge: &MergeClause,
) -> Result<(), MutationError> {
    // Try to match the pattern
    let match_results = match_pattern(ctx, &merge.pattern)?;

    if match_results.is_empty() {
        // Pattern not found - CREATE it
        create_pattern(ctx, &merge.pattern)?;

        // Execute ON CREATE SET actions
        if let Some(on_create) = &merge.on_create {
            for item in on_create {
                execute_set_item(ctx, item)?;
            }
        }
    } else {
        // Pattern found - use the first match and execute ON MATCH SET
        let first_match = &match_results[0];

        // Bind the matched variables
        for (var, vid) in &first_match.vertices {
            ctx.bind_vertex(var, *vid);
        }
        for (var, eid) in &first_match.edges {
            ctx.bind_edge(var, *eid);
        }

        // Execute ON MATCH SET actions
        if let Some(on_match) = &merge.on_match {
            for item in on_match {
                execute_set_item(ctx, item)?;
            }
        }
    }

    Ok(())
}

/// Execute a FOREACH clause.
///
/// FOREACH iterates over a list expression and applies mutations to each element.
/// The iteration variable shadows any outer variable with the same name.
///
/// # Arguments
///
/// * `ctx` - The mutation context with storage and bindings
/// * `foreach_clause` - The FOREACH clause to execute
///
/// # Example
///
/// ```text
/// FOREACH (n IN nodes(p) | SET n.visited = true)
/// FOREACH (i IN items | SET i.done = true REMOVE i.pending)
/// ```
fn execute_foreach<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    foreach_clause: &ForeachClause,
) -> Result<(), MutationError> {
    // Create bindings for expression evaluation (include value_bindings)
    let bindings = MatchBindings {
        vertices: ctx.vertex_bindings.clone(),
        edges: ctx.edge_bindings.clone(),
    };

    // Evaluate the list expression using extended evaluation that supports value_bindings
    let list_value = evaluate_expression(ctx, &foreach_clause.list, &bindings);

    // Ensure it's a list
    let items = match list_value {
        Value::List(items) => items,
        Value::Null => {
            // Empty iteration - nothing to do
            return Ok(());
        }
        other => {
            return Err(MutationError::Compile(CompileError::foreach_not_list(
                &foreach_clause.variable,
                value_type_name(&other),
            )));
        }
    };

    // Save current bindings so we can restore after FOREACH
    let saved_vertex = ctx.vertex_bindings.get(&foreach_clause.variable).copied();
    let saved_edge = ctx.edge_bindings.get(&foreach_clause.variable).copied();
    let saved_value = ctx.get_value(&foreach_clause.variable).cloned();

    // Iterate over each item and apply mutations
    for item in items {
        // Bind the iteration variable based on item type
        match &item {
            Value::Vertex(vid) => {
                ctx.bind_vertex(&foreach_clause.variable, *vid);
                // Clear any value binding with same name
                ctx.remove_value_binding(&foreach_clause.variable);
            }
            Value::Edge(eid) => {
                ctx.bind_edge(&foreach_clause.variable, *eid);
                // Clear any value binding with same name
                ctx.remove_value_binding(&foreach_clause.variable);
            }
            _ => {
                // For primitive values (Int, String, Bool, etc.), bind as value
                ctx.bind_value(&foreach_clause.variable, item.clone());
            }
        }

        // Execute each mutation in the FOREACH body
        for mutation in &foreach_clause.mutations {
            execute_foreach_mutation(ctx, mutation)?;
        }
    }

    // Restore original bindings or remove the iteration variable binding
    if let Some(vid) = saved_vertex {
        ctx.bind_vertex(&foreach_clause.variable, vid);
    } else {
        ctx.vertex_bindings.remove(&foreach_clause.variable);
    }

    if let Some(eid) = saved_edge {
        ctx.bind_edge(&foreach_clause.variable, eid);
    } else {
        ctx.edge_bindings.remove(&foreach_clause.variable);
    }

    if let Some(val) = saved_value {
        ctx.bind_value(&foreach_clause.variable, val);
    } else {
        ctx.remove_value_binding(&foreach_clause.variable);
    }

    Ok(())
}

/// Execute a mutation inside a FOREACH clause.
fn execute_foreach_mutation<S: GraphStorage + GraphStorageMut>(
    ctx: &mut MutationContext<S>,
    mutation: &ForeachMutation,
) -> Result<(), MutationError> {
    match mutation {
        ForeachMutation::Set(set) => execute_set(ctx, set),
        ForeachMutation::Remove(remove) => execute_remove(ctx, remove),
        ForeachMutation::Delete(delete) => execute_delete(ctx, delete),
        ForeachMutation::DetachDelete(detach) => execute_detach_delete(ctx, detach),
        ForeachMutation::Create(create) => execute_create(ctx, create),
        ForeachMutation::Foreach(nested) => execute_foreach(ctx, nested),
    }
}

/// Get a human-readable type name for a Value.
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Int(_) => "integer",
        Value::Float(_) => "float",
        Value::String(_) => "string",
        Value::List(_) => "list",
        Value::Map(_) => "map",
        Value::Vertex(_) => "vertex",
        Value::Edge(_) => "edge",
    }
}

// =============================================================================
// RETURN Clause Evaluation
// =============================================================================

/// Evaluate a RETURN clause and produce a result value.
fn evaluate_return<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    return_clause: &ReturnClause,
) -> Result<Option<Value>, MutationError> {
    // Create bindings from context
    let bindings = MatchBindings {
        vertices: ctx.vertex_bindings.clone(),
        edges: ctx.edge_bindings.clone(),
    };

    if return_clause.items.is_empty() {
        return Ok(None);
    }

    if return_clause.items.len() == 1 {
        let item = &return_clause.items[0];
        let value = evaluate_return_item(ctx, item, &bindings)?;
        Ok(Some(value))
    } else {
        // Multiple items - return as map
        let mut map = HashMap::new();
        for item in &return_clause.items {
            let key = get_return_item_key(item);
            let value = evaluate_return_item(ctx, item, &bindings)?;
            map.insert(key, value);
        }
        Ok(Some(Value::Map(map)))
    }
}

/// Get the key (name) for a return item.
fn get_return_item_key(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        alias.clone()
    } else {
        match &item.expression {
            Expression::Variable(var) => var.clone(),
            Expression::Property { variable, property } => format!("{}.{}", variable, property),
            _ => "expr".to_string(),
        }
    }
}

/// Evaluate a single RETURN item.
fn evaluate_return_item<S: GraphStorage + GraphStorageMut>(
    ctx: &MutationContext<S>,
    item: &ReturnItem,
    bindings: &MatchBindings,
) -> Result<Value, MutationError> {
    Ok(evaluate_expression(ctx, &item.expression, bindings))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parse_statement;
    use crate::storage::InMemoryGraph;

    #[test]
    fn test_create_single_vertex() {
        let mut storage = InMemoryGraph::new();

        let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 1);

        let vertex = storage.all_vertices().next().unwrap();
        assert_eq!(vertex.label, "Person");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_create_multiple_vertices() {
        let mut storage = InMemoryGraph::new();

        let stmt =
            parse_statement("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 2);
    }

    #[test]
    fn test_create_edge() {
        let mut storage = InMemoryGraph::new();

        let stmt = parse_statement(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
        )
        .unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 2);
        assert_eq!(storage.edge_count(), 1);

        let edge = storage.all_edges().next().unwrap();
        assert_eq!(edge.label, "KNOWS");
        assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
    }

    #[test]
    fn test_create_with_return() {
        let mut storage = InMemoryGraph::new();

        let stmt = parse_statement("CREATE (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let results = execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Value::Vertex(_)));
    }

    #[test]
    fn test_match_set() {
        let mut storage = InMemoryGraph::new();

        // Create a vertex first
        let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Update the vertex
        let stmt = parse_statement("MATCH (n:Person {name: 'Alice'}) SET n.age = 31").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        let vertex = storage.all_vertices().next().unwrap();
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(31)));
    }

    #[test]
    fn test_match_delete_edge() {
        let mut storage = InMemoryGraph::new();

        // Create vertices and edge
        let stmt =
            parse_statement("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                .unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.edge_count(), 1);

        // Delete the edge
        let stmt = parse_statement("MATCH (a:Person)-[r:KNOWS]->() DELETE r").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.edge_count(), 0);
        assert_eq!(storage.vertex_count(), 2); // Vertices still exist
    }

    #[test]
    fn test_delete_vertex_with_edges_fails() {
        let mut storage = InMemoryGraph::new();

        // Create vertices and edge
        let stmt =
            parse_statement("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                .unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Try to delete vertex with edge - should fail
        let stmt = parse_statement("MATCH (a:Person {name: 'Alice'}) DELETE a").unwrap();
        let result = execute_mutation(&stmt, &mut storage);

        assert!(matches!(result, Err(MutationError::VertexHasEdges(_))));
    }

    #[test]
    fn test_detach_delete_vertex() {
        let mut storage = InMemoryGraph::new();

        // Create vertices and edge
        let stmt =
            parse_statement("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                .unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 2);
        assert_eq!(storage.edge_count(), 1);

        // Detach delete vertex - should succeed
        let stmt = parse_statement("MATCH (a:Person {name: 'Alice'}) DETACH DELETE a").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 1); // Only Bob remains
        assert_eq!(storage.edge_count(), 0); // Edge removed
    }

    #[test]
    fn test_merge_creates_when_not_exists() {
        let mut storage = InMemoryGraph::new();

        let stmt =
            parse_statement("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true")
                .unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 1);

        let vertex = storage.all_vertices().next().unwrap();
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(vertex.properties.get("created"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_merge_matches_when_exists() {
        let mut storage = InMemoryGraph::new();

        // Create existing vertex
        let stmt = parse_statement("CREATE (n:Person {name: 'Alice', version: 1})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Merge should match and update
        let stmt =
            parse_statement("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.version = 2").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        assert_eq!(storage.vertex_count(), 1); // Still just one vertex

        let vertex = storage.all_vertices().next().unwrap();
        assert_eq!(vertex.properties.get("version"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_remove_property() {
        let mut storage = InMemoryGraph::new();

        // Create vertex with property
        let stmt = parse_statement("CREATE (n:Person {name: 'Alice', temp: 'value'})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Remove the temp property
        let stmt = parse_statement("MATCH (n:Person) REMOVE n.temp").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        let vertex = storage.all_vertices().next().unwrap();
        // Property should be set to Null (our remove implementation)
        assert_eq!(vertex.properties.get("temp"), Some(&Value::Null));
    }

    #[test]
    fn test_match_with_where() {
        let mut storage = InMemoryGraph::new();

        // Create multiple vertices
        let stmt = parse_statement("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();
        let stmt = parse_statement("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Update only matching vertex
        let stmt = parse_statement("MATCH (n:Person) WHERE n.age > 26 SET n.adult = true").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Verify only Alice was updated
        let mut updated_count = 0;
        for vertex in storage.all_vertices() {
            if vertex.properties.get("adult") == Some(&Value::Bool(true)) {
                assert_eq!(
                    vertex.properties.get("name"),
                    Some(&Value::String("Alice".into()))
                );
                updated_count += 1;
            }
        }
        assert_eq!(updated_count, 1);
    }

    // =========================================================================
    // FOREACH Tests
    // =========================================================================

    #[test]
    fn test_foreach_basic_set() {
        let mut storage = InMemoryGraph::new();

        // Create some vertices
        let stmt = parse_statement("CREATE (a:Person {name: 'Alice'})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();
        let stmt = parse_statement("CREATE (b:Person {name: 'Bob'})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Get vertex IDs
        let vertex_ids: Vec<_> = storage.all_vertices().map(|v| v.id).collect();
        assert_eq!(vertex_ids.len(), 2);

        // Use FOREACH - test with an actual list value using Expression::List
        let foreach_clause = ForeachClause {
            variable: "n".to_string(),
            list: Expression::List(vec![
                Expression::Literal(crate::gql::ast::Literal::Int(1)),
                Expression::Literal(crate::gql::ast::Literal::Int(2)),
            ]),
            mutations: vec![],
        };

        let mut ctx = MutationContext::new(&mut storage);
        // This should succeed (empty mutations, iterating over integers)
        execute_foreach(&mut ctx, &foreach_clause).unwrap();
    }

    #[test]
    fn test_foreach_with_vertex_list() {
        let mut storage = InMemoryGraph::new();

        // Create vertices
        let stmt = parse_statement("CREATE (a:Person {name: 'Alice', visited: false})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();
        let stmt = parse_statement("CREATE (b:Person {name: 'Bob', visited: false})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        // Get vertex IDs
        let vertex_ids: Vec<_> = storage.all_vertices().map(|v| v.id).collect();

        // Create a FOREACH clause with a list of integer literals (representing IDs)
        let foreach_clause = ForeachClause {
            variable: "n".to_string(),
            list: Expression::List(
                vertex_ids
                    .iter()
                    .map(|id| Expression::Literal(crate::gql::ast::Literal::Int(id.0 as i64)))
                    .collect(),
            ),
            mutations: vec![],
        };

        let mut ctx = MutationContext::new(&mut storage);
        execute_foreach(&mut ctx, &foreach_clause).unwrap();

        // Verify execution completed (no direct mutations since we're iterating over integers)
        assert_eq!(storage.vertex_count(), 2);
    }

    #[test]
    fn test_foreach_not_list_error() {
        let mut storage = InMemoryGraph::new();

        // Create a FOREACH clause with a non-list expression
        let foreach_clause = ForeachClause {
            variable: "n".to_string(),
            list: Expression::Literal(crate::gql::ast::Literal::Int(42)),
            mutations: vec![],
        };

        let mut ctx = MutationContext::new(&mut storage);
        let result = execute_foreach(&mut ctx, &foreach_clause);

        assert!(matches!(
            result,
            Err(MutationError::Compile(CompileError::ForeachNotList { .. }))
        ));
    }

    #[test]
    fn test_foreach_null_list_is_noop() {
        let mut storage = InMemoryGraph::new();

        // Create a FOREACH clause with a null expression (should be a no-op)
        let foreach_clause = ForeachClause {
            variable: "n".to_string(),
            list: Expression::Literal(crate::gql::ast::Literal::Null),
            mutations: vec![],
        };

        let mut ctx = MutationContext::new(&mut storage);
        // Should succeed with no effect
        execute_foreach(&mut ctx, &foreach_clause).unwrap();
    }

    #[test]
    fn test_foreach_empty_list() {
        let mut storage = InMemoryGraph::new();

        // Create a FOREACH clause with an empty list
        let foreach_clause = ForeachClause {
            variable: "n".to_string(),
            list: Expression::List(vec![]),
            mutations: vec![],
        };

        let mut ctx = MutationContext::new(&mut storage);
        // Should succeed with no iterations
        execute_foreach(&mut ctx, &foreach_clause).unwrap();
    }

    #[test]
    fn test_foreach_nested() {
        let mut storage = InMemoryGraph::new();

        // Create a nested FOREACH clause
        let inner_foreach = ForeachClause {
            variable: "y".to_string(),
            list: Expression::List(vec![
                Expression::Literal(crate::gql::ast::Literal::Int(1)),
                Expression::Literal(crate::gql::ast::Literal::Int(2)),
            ]),
            mutations: vec![],
        };

        let outer_foreach = ForeachClause {
            variable: "x".to_string(),
            list: Expression::List(vec![
                Expression::Literal(crate::gql::ast::Literal::Int(10)),
                Expression::Literal(crate::gql::ast::Literal::Int(20)),
            ]),
            mutations: vec![ForeachMutation::Foreach(Box::new(inner_foreach))],
        };

        let mut ctx = MutationContext::new(&mut storage);
        // Should execute without error (4 iterations total: 2 outer * 2 inner)
        execute_foreach(&mut ctx, &outer_foreach).unwrap();
    }

    #[test]
    fn test_foreach_set_with_iteration_variable() {
        let mut storage = InMemoryGraph::new();

        // Create a vertex
        let stmt = parse_statement("CREATE (a:Person {name: 'Alice'})").unwrap();
        execute_mutation(&stmt, &mut storage).unwrap();

        let vertex_id = storage.all_vertices().next().unwrap().id;

        // Create a FOREACH clause that sets a property using the iteration variable
        let foreach_clause = ForeachClause {
            variable: "i".to_string(),
            list: Expression::List(vec![
                Expression::Literal(crate::gql::ast::Literal::Int(1)),
                Expression::Literal(crate::gql::ast::Literal::Int(2)),
                Expression::Literal(crate::gql::ast::Literal::Int(3)),
            ]),
            mutations: vec![ForeachMutation::Set(SetClause {
                items: vec![SetItem {
                    target: crate::gql::ast::PropertyRef {
                        variable: "p".to_string(),
                        property: "counter".to_string(),
                    },
                    value: Expression::Variable("i".to_string()),
                }],
            })],
        };

        let mut ctx = MutationContext::new(&mut storage);
        // Bind the vertex to variable 'p'
        ctx.bind_vertex("p", vertex_id);

        execute_foreach(&mut ctx, &foreach_clause).unwrap();

        // Verify the property was set to the last iteration value (3)
        let vertex = storage.get_vertex(vertex_id).unwrap();
        assert_eq!(
            vertex.properties.get("counter"),
            Some(&Value::Int(3)),
            "Counter should be 3 (last value)"
        );
    }
}
