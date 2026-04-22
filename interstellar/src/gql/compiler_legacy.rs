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
//! use interstellar::gql::{parse, compile};
//! use interstellar::Graph;
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
//! [`GraphSnapshot`]: crate::storage::GraphSnapshot
//! [`CompileError`]: crate::gql::error::CompileError

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::gql::ast::{
    AggregateFunc, BinaryOperator, CallBody, CallClause, CallProcedureClause, CallQuery,
    CaseExpression, EdgeDirection, EdgePattern, Expression, GroupByClause, HavingClause, LetClause,
    LimitClause, Literal, MatchClause, NodePattern, OptionalMatchClause, OrderClause,
    PathQuantifier, Pattern, PatternElement, Query, ReturnClause, ReturnItem, Statement,
    UnaryOperator, UnwindClause, WhereClause, WithClause, YieldItem,
};
use crate::gql::error::CompileError;
use crate::storage::cow::Graph;
use crate::traversal::{BoundTraversal, SnapshotLike, Traversal, __};
use crate::value::{IntoValueMap, Value, ValueMap, VertexId};

/// Parameters passed to query execution.
///
/// A map of parameter names to their values. Parameter names should not include
/// the leading `$` - for example, to provide a value for `$personId`, use
/// `"personId"` as the key.
///
/// # Example
///
/// ```
/// use interstellar::gql::Parameters;
/// use interstellar::Value;
///
/// let mut params = Parameters::new();
/// params.insert("personId".to_string(), Value::Int(123));
/// params.insert("minAge".to_string(), Value::Int(18));
/// ```
pub type Parameters = HashMap<String, Value>;

/// Configuration for query complexity limits.
///
/// These limits help prevent denial-of-service attacks and resource exhaustion
/// from overly complex queries. When a limit is exceeded, the compiler returns
/// a [`CompileError::ComplexityLimitExceeded`] error.
///
/// # Default Limits
///
/// The default configuration provides generous limits suitable for most use cases:
/// - `max_pattern_length`: 100 elements per pattern
/// - `max_optional_matches`: 50 OPTIONAL MATCH clauses
/// - `max_subquery_depth`: 10 levels of nested subqueries
/// - `max_union_clauses`: 50 UNION clauses
///
/// # Example
///
/// ```
/// use interstellar::gql::CompilerConfig;
///
/// // Use default limits
/// let config = CompilerConfig::default();
///
/// // Create stricter limits for untrusted queries
/// let strict_config = CompilerConfig {
///     max_pattern_length: 20,
///     max_optional_matches: 10,
///     max_subquery_depth: 3,
///     max_union_clauses: 5,
/// };
///
/// // Disable all limits (use with caution!)
/// let unlimited = CompilerConfig::unlimited();
/// ```
#[derive(Debug, Clone)]
pub struct CompilerConfig {
    /// Maximum number of elements (nodes + edges) in a single pattern.
    /// Default: 100
    pub max_pattern_length: usize,

    /// Maximum number of OPTIONAL MATCH clauses in a query.
    /// Default: 50
    pub max_optional_matches: usize,

    /// Maximum depth of nested subqueries (CALL, EXISTS subqueries).
    /// Default: 10
    pub max_subquery_depth: usize,

    /// Maximum number of UNION clauses in a statement.
    /// Default: 50
    pub max_union_clauses: usize,
}

impl Default for CompilerConfig {
    fn default() -> Self {
        Self {
            max_pattern_length: 100,
            max_optional_matches: 50,
            max_subquery_depth: 10,
            max_union_clauses: 50,
        }
    }
}

impl CompilerConfig {
    /// Create a configuration with no limits.
    ///
    /// **Warning**: This should only be used for trusted queries, as it allows
    /// arbitrarily complex queries that may exhaust system resources.
    pub fn unlimited() -> Self {
        Self {
            max_pattern_length: usize::MAX,
            max_optional_matches: usize::MAX,
            max_subquery_depth: usize::MAX,
            max_union_clauses: usize::MAX,
        }
    }

    /// Create a strict configuration suitable for untrusted queries.
    ///
    /// Uses conservative limits to prevent resource exhaustion:
    /// - `max_pattern_length`: 20
    /// - `max_optional_matches`: 10
    /// - `max_subquery_depth`: 3
    /// - `max_union_clauses`: 10
    pub fn strict() -> Self {
        Self {
            max_pattern_length: 20,
            max_optional_matches: 10,
            max_subquery_depth: 3,
            max_union_clauses: 10,
        }
    }
}

/// Convert a u64 ID to a Value, handling IDs that exceed i64::MAX.
///
/// For IDs that fit in i64, returns Value::Int for backward compatibility.
/// For IDs >= 2^63, returns Value::String to avoid negative number representation.
#[inline]
fn id_to_value(id: u64) -> Value {
    if id > i64::MAX as u64 {
        Value::String(id.to_string())
    } else {
        Value::Int(id as i64)
    }
}

/// Convert a path-tracked traverser into a row keyed by `as_()` labels.
/// `__path__` and `__current__` mirror the conventions used by other
/// row-based execution paths (UNWIND/LET/WITH).
fn traverser_to_row(t: &crate::traversal::Traverser) -> std::collections::HashMap<String, Value> {
    let mut row = std::collections::HashMap::new();
    for label in t.path.all_labels() {
        if let Some(values) = t.path.get(label) {
            if let Some(pv) = values.last() {
                row.insert(label.clone(), pv.to_value());
            }
        }
    }
    let path_values: Vec<Value> = t.path.objects().map(|pv| pv.to_value()).collect();
    row.insert("__path__".to_string(), Value::List(path_values));
    row.insert("__current__".to_string(), t.value.clone());
    row
}

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
/// ```ignore
/// use interstellar::gql::{parse, compile};
/// use interstellar::storage::Graph;
/// use interstellar::value::Value;
///
/// let graph = Graph::new();
/// graph.mutate(|g| {
///     g.add_v("Person").property("name", "Alice").exec()
/// }).unwrap();
///
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) RETURN n.name").unwrap();
/// let results = compile(&query, &snapshot).unwrap();
/// assert_eq!(results.len(), 1);
/// ```
///
/// ## Query with filtering
///
/// ```ignore
/// use interstellar::gql::{parse, compile};
/// use interstellar::storage::Graph;
/// use interstellar::value::Value;
///
/// let graph = Graph::new();
/// graph.mutate(|g| {
///     g.add_v("Person").property("name", "Alice").property("age", 30).exec()
/// }).unwrap();
///
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) WHERE n.age > 25 RETURN n.name").unwrap();
/// let results = compile(&query, &snapshot).unwrap();
/// ```
///
/// ## Aggregation query
///
/// ```ignore
/// use interstellar::gql::{parse, compile};
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) RETURN COUNT(*)").unwrap();
/// let results = compile(&query, &snapshot).unwrap();
/// ```
///
/// [`Query`]: crate::gql::ast::Query
/// [`GraphSnapshot`]: crate::storage::GraphSnapshot
/// [`Value`]: crate::value::Value
/// [`CompileError`]: crate::gql::error::CompileError
/// [`parse()`]: crate::gql::parse
pub fn compile<S: SnapshotLike + ?Sized>(
    query: &Query,
    snapshot: &S,
) -> Result<Vec<Value>, CompileError> {
    compile_with_config(
        query,
        snapshot,
        &Parameters::new(),
        &CompilerConfig::default(),
    )
}

/// Compile and execute a parameterized GQL query against a graph snapshot.
///
/// This function allows passing query parameters that can be referenced in the
/// query using `$paramName` syntax. Parameters provide a safe way to inject
/// values into queries without string concatenation, preventing injection attacks
/// and enabling query reuse with different values.
///
/// # Arguments
///
/// * `query` - A parsed GQL query from [`parse()`]
/// * `snapshot` - An immutable snapshot of the graph to query
/// * `params` - A map of parameter names to their values
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing the query results on success.
///
/// # Errors
///
/// Returns [`CompileError`] if:
/// - A parameter referenced in the query is not provided in `params`
/// - A variable in RETURN or WHERE is not bound in MATCH
/// - A variable is bound multiple times in MATCH
///
/// # Examples
///
/// ```ignore
/// use interstellar::gql::{parse, compile_with_params, Parameters};
/// use interstellar::storage::Graph;
/// use interstellar::value::Value;
///
/// let graph = Graph::new();
/// graph.mutate(|g| {
///     g.add_v("Person").property("name", "Alice").property("age", 30).exec()
/// }).unwrap();
///
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name").unwrap();
///
/// let mut params = Parameters::new();
/// params.insert("minAge".to_string(), Value::Int(25));
///
/// let results = compile_with_params(&query, &snapshot, &params).unwrap();
/// assert_eq!(results.len(), 1);
/// ```
///
/// [`parse()`]: crate::gql::parse
/// [`CompileError`]: crate::gql::error::CompileError
pub fn compile_with_params<S: SnapshotLike + ?Sized>(
    query: &Query,
    snapshot: &S,
    params: &Parameters,
) -> Result<Vec<Value>, CompileError> {
    compile_with_config(query, snapshot, params, &CompilerConfig::default())
}

/// Compile and execute a GQL query with custom complexity limits.
///
/// This function allows configuring complexity limits to prevent denial-of-service
/// attacks from overly complex queries. Use this when processing queries from
/// untrusted sources.
///
/// # Arguments
///
/// * `query` - A parsed GQL query from [`parse()`]
/// * `snapshot` - An immutable snapshot of the graph to query
/// * `params` - A map of parameter names to their values
/// * `config` - Configuration for query complexity limits
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing the query results on success.
///
/// # Errors
///
/// Returns [`CompileError::ComplexityLimitExceeded`] if any configured limit is exceeded.
/// Also returns other [`CompileError`] variants for standard compilation errors.
///
/// # Examples
///
/// ```
/// use interstellar::gql::{parse, compile_with_config, Parameters, CompilerConfig};
/// use interstellar::Graph;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let query = parse("MATCH (n:Person) RETURN n.name").unwrap();
///
/// // Use strict limits for untrusted queries
/// let config = CompilerConfig::strict();
/// let results = compile_with_config(&query, &snapshot, &Parameters::new(), &config).unwrap();
/// ```
///
/// [`parse()`]: crate::gql::parse
/// [`CompileError`]: crate::gql::error::CompileError
/// [`CompileError::ComplexityLimitExceeded`]: crate::gql::error::CompileError::ComplexityLimitExceeded
pub fn compile_with_config<S: SnapshotLike + ?Sized>(
    query: &Query,
    snapshot: &S,
    params: &Parameters,
    config: &CompilerConfig,
) -> Result<Vec<Value>, CompileError> {
    compile_with_config_inner(query, snapshot, params, config, None)
}

/// Internal: compile_with_config that accepts an optional `Arc<Graph>` handle
/// for full-text search CALL procedures.
fn compile_with_config_inner<S: SnapshotLike + ?Sized>(
    query: &Query,
    snapshot: &S,
    params: &Parameters,
    config: &CompilerConfig,
    graph_handle: Option<Arc<Graph>>,
) -> Result<Vec<Value>, CompileError> {
    let mut compiler = Compiler::new_with_graph(snapshot, params, config, graph_handle);
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
/// use interstellar::gql::{parse_statement, compile_statement};
/// use interstellar::Graph;
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
pub fn compile_statement<S: SnapshotLike + ?Sized>(
    stmt: &Statement,
    snapshot: &S,
) -> Result<Vec<Value>, CompileError> {
    compile_statement_with_config(
        stmt,
        snapshot,
        &Parameters::new(),
        &CompilerConfig::default(),
    )
}

/// Compile and execute a parameterized GQL statement against a graph snapshot.
///
/// This function handles both single queries and UNION of multiple queries,
/// with support for query parameters. For UNION statements, all queries share
/// the same parameter set.
///
/// # Arguments
///
/// * `stmt` - A parsed GQL statement from [`parse_statement()`]
/// * `snapshot` - An immutable snapshot of the graph to query
/// * `params` - A map of parameter names to their values
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing the statement results on success.
///
/// # Errors
///
/// Returns [`CompileError`] if:
/// - A parameter referenced in the query is not provided in `params`
/// - Any other compilation error occurs
///
/// # Examples
///
/// ```
/// use interstellar::gql::{parse_statement, compile_statement_with_params, Parameters};
/// use interstellar::Graph;
/// use interstellar::value::Value;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
///
/// let stmt = parse_statement("MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name").unwrap();
///
/// let mut params = Parameters::new();
/// params.insert("minAge".to_string(), Value::Int(25));
///
/// let results = compile_statement_with_params(&stmt, &snapshot, &params).unwrap();
/// ```
///
/// [`parse_statement()`]: crate::gql::parse_statement
/// [`CompileError`]: crate::gql::error::CompileError
pub fn compile_statement_with_params<S: SnapshotLike + ?Sized>(
    stmt: &Statement,
    snapshot: &S,
    params: &Parameters,
) -> Result<Vec<Value>, CompileError> {
    compile_statement_with_config(stmt, snapshot, params, &CompilerConfig::default())
}

/// Compile and execute a GQL statement with custom complexity limits.
///
/// This function handles both single queries and UNION of multiple queries,
/// with support for query parameters and configurable complexity limits.
///
/// # Arguments
///
/// * `stmt` - A parsed GQL statement from [`parse_statement()`]
/// * `snapshot` - An immutable snapshot of the graph to query
/// * `params` - A map of parameter names to their values
/// * `config` - Configuration for query complexity limits
///
/// # Returns
///
/// Returns `Ok(Vec<Value>)` containing the statement results on success.
///
/// # Errors
///
/// Returns [`CompileError::ComplexityLimitExceeded`] if any configured limit is exceeded,
/// including the `max_union_clauses` limit for UNION statements.
///
/// [`parse_statement()`]: crate::gql::parse_statement
/// [`CompileError::ComplexityLimitExceeded`]: crate::gql::error::CompileError::ComplexityLimitExceeded
pub fn compile_statement_with_config<S: SnapshotLike + ?Sized>(
    stmt: &Statement,
    snapshot: &S,
    params: &Parameters,
    config: &CompilerConfig,
) -> Result<Vec<Value>, CompileError> {
    compile_statement_with_config_inner(stmt, snapshot, params, config, None)
}

/// Compile a GQL statement with an optional live `Graph` handle for full-text
/// search CALL procedures (`interstellar.searchTextV`, etc.).
///
/// This is the entry point used by [`Graph::gql`] so that FTS CALL procedures
/// can reach the live text-index registry on the originating `Graph`.
///
/// [`Graph::gql`]: crate::storage::cow::Graph::gql
pub fn compile_statement_with_graph<S: SnapshotLike + ?Sized>(
    stmt: &Statement,
    snapshot: &S,
    graph_handle: Option<Arc<Graph>>,
) -> Result<Vec<Value>, CompileError> {
    compile_statement_with_config_inner(
        stmt,
        snapshot,
        &Parameters::new(),
        &CompilerConfig::default(),
        graph_handle,
    )
}

/// Parameterized variant of [`compile_statement_with_graph`].
///
/// Used by [`Graph::gql_with_params`] so that FTS CALL procedures can reach
/// the live text-index registry on the originating `Graph`.
///
/// [`Graph::gql_with_params`]: crate::storage::cow::Graph::gql_with_params
pub fn compile_statement_with_params_and_graph<S: SnapshotLike + ?Sized>(
    stmt: &Statement,
    snapshot: &S,
    params: &Parameters,
    graph_handle: Option<Arc<Graph>>,
) -> Result<Vec<Value>, CompileError> {
    compile_statement_with_config_inner(stmt, snapshot, params, &CompilerConfig::default(), graph_handle)
}

/// Internal: compile_statement_with_config that accepts an optional `Arc<Graph>`
/// handle for full-text search CALL procedures.
fn compile_statement_with_config_inner<S: SnapshotLike + ?Sized>(
    stmt: &Statement,
    snapshot: &S,
    params: &Parameters,
    config: &CompilerConfig,
    graph_handle: Option<Arc<Graph>>,
) -> Result<Vec<Value>, CompileError> {
    match stmt {
        Statement::Query(query) => {
            compile_with_config_inner(query.as_ref(), snapshot, params, config, graph_handle)
        }
        Statement::Union { queries, all } => {
            // Check UNION clause limit
            if queries.len() > config.max_union_clauses {
                return Err(CompileError::complexity_limit_exceeded(format!(
                    "UNION has {} queries, maximum allowed is {}",
                    queries.len(),
                    config.max_union_clauses
                )));
            }
            compile_union_with_config_inner(queries, *all, snapshot, params, config, graph_handle)
        }
        Statement::Mutation(_) => {
            // Mutation compilation requires mutable access to the graph
            // Use compile_mutation() with a GraphMut instead
            Err(CompileError::UnsupportedFeature(
                "Mutation statements require mutable graph access. Use compile_mutation() with GraphMut.".to_string(),
            ))
        }
        Statement::Ddl(_) => {
            // DDL statements modify the schema, not query data
            // Use execute_ddl() instead
            Err(CompileError::UnsupportedFeature(
                "DDL statements modify schema, not query data. Use execute_ddl() instead."
                    .to_string(),
            ))
        }
    }
}

/// Execute a UNION of multiple queries.
#[allow(dead_code)]
fn compile_union<S: SnapshotLike + ?Sized>(
    queries: &[Query],
    keep_duplicates: bool,
    snapshot: &S,
) -> Result<Vec<Value>, CompileError> {
    compile_union_with_config(
        queries,
        keep_duplicates,
        snapshot,
        &Parameters::new(),
        &CompilerConfig::default(),
    )
}

/// Execute a UNION of multiple queries with parameters.
#[allow(dead_code)]
fn compile_union_with_params<S: SnapshotLike + ?Sized>(
    queries: &[Query],
    keep_duplicates: bool,
    snapshot: &S,
    params: &Parameters,
) -> Result<Vec<Value>, CompileError> {
    compile_union_with_config(
        queries,
        keep_duplicates,
        snapshot,
        params,
        &CompilerConfig::default(),
    )
}

/// Execute a UNION of multiple queries with parameters and config.
fn compile_union_with_config<S: SnapshotLike + ?Sized>(
    queries: &[Query],
    keep_duplicates: bool,
    snapshot: &S,
    params: &Parameters,
    config: &CompilerConfig,
) -> Result<Vec<Value>, CompileError> {
    compile_union_with_config_inner(queries, keep_duplicates, snapshot, params, config, None)
}

/// Internal: compile_union_with_config that accepts an optional `Arc<Graph>`
/// handle for full-text search CALL procedures.
fn compile_union_with_config_inner<S: SnapshotLike + ?Sized>(
    queries: &[Query],
    keep_duplicates: bool,
    snapshot: &S,
    params: &Parameters,
    config: &CompilerConfig,
    graph_handle: Option<Arc<Graph>>,
) -> Result<Vec<Value>, CompileError> {
    let mut all_results = Vec::new();

    for query in queries {
        let results =
            compile_with_config_inner(query, snapshot, params, config, graph_handle.clone())?;
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

struct Compiler<'a, S: SnapshotLike + ?Sized> {
    snapshot: &'a S,
    bindings: HashMap<String, BindingInfo>,
    /// Query parameters for parameterized queries
    parameters: &'a Parameters,
    /// Whether the current query has multiple bound variables (requires path tracking)
    has_multi_vars: bool,
    /// Query complexity limits configuration
    config: &'a CompilerConfig,
    /// Current subquery nesting depth (for limit checking)
    subquery_depth: usize,
    /// Optional handle to the live `Graph`, required for full-text search
    /// CALL procedures (`interstellar.searchTextV`, etc.). `None` when the
    /// compiler was invoked via a snapshot-only entry point.
    #[allow(dead_code)] // used by FTS CALL dispatch (spec-55c Layer 5)
    graph_handle: Option<Arc<Graph>>,
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

/// Kind of list predicate for evaluation (ALL, ANY, NONE, SINGLE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListPredicateKind {
    /// ALL(x IN list WHERE condition) - true if all elements satisfy condition
    All,
    /// ANY(x IN list WHERE condition) - true if at least one element satisfies
    Any,
    /// NONE(x IN list WHERE condition) - true if no elements satisfy
    None,
    /// SINGLE(x IN list WHERE condition) - true if exactly one element satisfies
    Single,
}

impl<'a, S: SnapshotLike + ?Sized> Compiler<'a, S> {
    fn new_with_graph(
        snapshot: &'a S,
        parameters: &'a Parameters,
        config: &'a CompilerConfig,
        graph_handle: Option<Arc<Graph>>,
    ) -> Self {
        Self {
            snapshot,
            bindings: HashMap::new(),
            parameters,
            has_multi_vars: false,
            config,
            subquery_depth: 0,
            graph_handle,
        }
    }

    /// Validate query complexity against configured limits.
    fn validate_query_complexity(&self, query: &Query) -> Result<(), CompileError> {
        // Check pattern length in MATCH clause
        for pattern in &query.match_clause.patterns {
            if pattern.elements.len() > self.config.max_pattern_length {
                return Err(CompileError::complexity_limit_exceeded(format!(
                    "Pattern has {} elements, maximum allowed is {}",
                    pattern.elements.len(),
                    self.config.max_pattern_length
                )));
            }
        }

        // Check number of OPTIONAL MATCH clauses
        if query.optional_match_clauses.len() > self.config.max_optional_matches {
            return Err(CompileError::complexity_limit_exceeded(format!(
                "Query has {} OPTIONAL MATCH clauses, maximum allowed is {}",
                query.optional_match_clauses.len(),
                self.config.max_optional_matches
            )));
        }

        // Check pattern length in OPTIONAL MATCH clauses
        for opt_match in &query.optional_match_clauses {
            for pattern in &opt_match.patterns {
                if pattern.elements.len() > self.config.max_pattern_length {
                    return Err(CompileError::complexity_limit_exceeded(format!(
                        "OPTIONAL MATCH pattern has {} elements, maximum allowed is {}",
                        pattern.elements.len(),
                        self.config.max_pattern_length
                    )));
                }
            }
        }

        // Check subquery depth
        if self.subquery_depth > self.config.max_subquery_depth {
            return Err(CompileError::complexity_limit_exceeded(format!(
                "Subquery nesting depth is {}, maximum allowed is {}",
                self.subquery_depth, self.config.max_subquery_depth
            )));
        }

        Ok(())
    }

    /// Resolve a parameter by name, returning an error if not found.
    #[allow(dead_code)]
    fn resolve_parameter(&self, name: &str) -> Result<Value, CompileError> {
        self.parameters
            .get(name)
            .cloned()
            .ok_or_else(|| CompileError::unbound_parameter(name))
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

    /// Check if the RETURN clause uses the path() function.
    fn return_uses_path_function(&self, return_clause: &ReturnClause) -> bool {
        return_clause
            .items
            .iter()
            .any(|item| Self::expression_uses_path_function(&item.expression))
    }

    /// Check if an expression contains a path() function call.
    fn expression_uses_path_function(expr: &Expression) -> bool {
        match expr {
            Expression::FunctionCall { name, .. } if name.eq_ignore_ascii_case("path") => true,
            Expression::BinaryOp { left, right, .. } => {
                Self::expression_uses_path_function(left)
                    || Self::expression_uses_path_function(right)
            }
            Expression::UnaryOp { expr, .. } => Self::expression_uses_path_function(expr),
            Expression::List(items) => items.iter().any(Self::expression_uses_path_function),
            Expression::Map(entries) => entries
                .iter()
                .any(|(_, value)| Self::expression_uses_path_function(value)),
            Expression::FunctionCall { args, .. } => {
                args.iter().any(Self::expression_uses_path_function)
            }
            Expression::Aggregate { expr, .. } => Self::expression_uses_path_function(expr),
            Expression::Case(case_expr) => {
                case_expr.when_clauses.iter().any(|(c, r)| {
                    Self::expression_uses_path_function(c) || Self::expression_uses_path_function(r)
                }) || case_expr
                    .else_clause
                    .as_ref()
                    .map(|e| Self::expression_uses_path_function(e))
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    /// Check if a pattern has any edge variables.
    fn has_edge_variable(pattern: &Pattern) -> bool {
        pattern
            .elements
            .iter()
            .any(|e| matches!(e, PatternElement::Edge(edge) if edge.variable.is_some()))
    }

    fn compile(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        // Validate query complexity against configured limits
        self.validate_query_complexity(query)?;

        if query.match_clause.patterns.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        // Multi-pattern MATCH (`MATCH (a)-[…]->(b), (a)-[…]->(c)`) is handled
        // by a dedicated row-based pipeline. We only take this path when the
        // query has features that the row-based pipeline currently supports
        // (RETURN/WHERE/ORDER/LIMIT/DISTINCT). Combinations with OPTIONAL
        // MATCH, WITH, UNWIND, CALL, LET, or GROUP BY still fall through to
        // the linear single-pattern compiler and will surface as
        // "undefined variable" errors for variables in the second pattern.
        if query.match_clause.patterns.len() > 1
            && query.optional_match_clauses.is_empty()
            && query.with_clauses.is_empty()
            && query.unwind_clauses.is_empty()
            && query.call_clauses.is_empty()
            && query.let_clauses.is_empty()
            && query.group_by_clause.is_none()
        {
            return self.execute_multi_pattern_query(query);
        }

        let pattern = &query.match_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Err(CompileError::EmptyPattern);
        }

        // Check if we need multi-variable support (requires path tracking)
        // This is needed when:
        // 1. Multiple variables are bound (nodes or edges)
        // 2. Any edge variable is bound (needs path to access edge properties)
        // 3. There are OPTIONAL MATCH clauses (always need path tracking)
        // 4. WITH PATH clause is present (explicit path tracking)
        // 5. path() function is used in RETURN clause
        // 6. UNWIND clause is present (needs variable access for expression evaluation)
        let var_count = Self::count_pattern_variables(pattern);
        let has_edge_var = Self::has_edge_variable(pattern);
        let has_optional = !query.optional_match_clauses.is_empty();
        let has_with_path = query.with_path_clause.is_some();
        let uses_path_func = self.return_uses_path_function(&query.return_clause);
        let has_unwind = !query.unwind_clauses.is_empty();
        let has_let = !query.let_clauses.is_empty();
        let has_with = !query.with_clauses.is_empty();
        let has_call = !query.call_clauses.is_empty();
        let has_procedure_calls = !query.call_procedure_clauses.is_empty();
        self.has_multi_vars = var_count > 1
            || has_edge_var
            || has_optional
            || has_with_path
            || uses_path_func
            || has_unwind
            || has_let
            || has_with
            || has_call
            || has_procedure_calls;

        // Build traversal starting from v()
        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let traversal = g.v();

        // Enable path tracking for multi-variable patterns or edge variable access
        let traversal = if self.has_multi_vars {
            traversal.with_path()
        } else {
            traversal
        };

        // Compile the full pattern (nodes and edges)
        let traversal = self.compile_pattern(pattern, traversal)?;

        // Register variables from OPTIONAL MATCH clauses (they may be null)
        // This is needed for validation - OPTIONAL MATCH variables are valid to reference
        for opt_clause in &query.optional_match_clauses {
            for opt_pattern in &opt_clause.patterns {
                self.register_optional_pattern_variables(opt_pattern);
            }
        }

        // Register UNWIND aliases as valid variable bindings
        for unwind in &query.unwind_clauses {
            self.bindings.insert(
                unwind.alias.clone(),
                BindingInfo {
                    pattern_index: 0,
                    is_node: false,
                },
            );
        }

        // Register LET clause variables as valid bindings
        for let_clause in &query.let_clauses {
            self.bindings.insert(
                let_clause.variable.clone(),
                BindingInfo {
                    pattern_index: 0,
                    is_node: false,
                },
            );
        }

        // Register WITH clause output variables as valid bindings
        // Each WITH clause outputs variables that can be referenced afterward
        for with_clause in &query.with_clauses {
            for item in &with_clause.items {
                let var_name = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                self.bindings.insert(
                    var_name,
                    BindingInfo {
                        pattern_index: 0,
                        is_node: false,
                    },
                );
            }
        }

        // Validate and register CALL clause variables
        for call_clause in &query.call_clauses {
            self.validate_call_clause(call_clause)?;
            self.register_call_clause_variables(call_clause);
        }

        // Register CALL procedure YIELD variables
        for proc_clause in &query.call_procedure_clauses {
            for item in &proc_clause.yield_items {
                let var_name = item
                    .alias
                    .as_ref()
                    .unwrap_or(&item.field)
                    .clone();
                self.bindings.insert(
                    var_name,
                    BindingInfo {
                        pattern_index: 0,
                        is_node: false,
                    },
                );
            }
        }

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
                &query.having_clause,
                traversal,
            )?;

            // Apply ORDER BY if present
            let results =
                self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

            // Apply LIMIT/OFFSET if present
            let results = self.apply_limit(&query.limit_clause, results);

            return Ok(results);
        }

        // Handle OPTIONAL MATCH if present
        if has_optional {
            let results = self.execute_with_optional_match(
                &query.return_clause,
                &query.where_clause,
                &query.let_clauses,
                &query.optional_match_clauses,
                traversal,
            )?;

            // Apply ORDER BY if present
            let results =
                self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

            // Apply LIMIT/OFFSET if present
            let results = self.apply_limit(&query.limit_clause, results);

            return Ok(results);
        }

        // Check if we have UNWIND clauses to apply
        if !query.unwind_clauses.is_empty() {
            return self.execute_with_unwind(query, traversal);
        }

        // Check if we have CALL clauses or procedure calls - process them with row-based execution
        if has_call || has_procedure_calls {
            return self.execute_with_call_clauses(query, traversal);
        }

        // Check if we have WITH clauses - process them with row-based execution
        if !query.with_clauses.is_empty() {
            return self.execute_with_with_clauses(query, traversal);
        }

        // Check if we have LET clauses - if so, use row-based processing
        if !query.let_clauses.is_empty() {
            return self.execute_with_let(query, traversal);
        }

        // Execute and collect results based on RETURN clause
        // Apply WHERE filter if present
        let results = self.execute_return(
            &query.return_clause,
            &query.where_clause,
            &query.having_clause,
            traversal,
        )?;

        // Apply ORDER BY if present
        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

        // Apply LIMIT/OFFSET if present
        let results = self.apply_limit(&query.limit_clause, results);

        Ok(results)
    }

    /// Execute a multi-pattern MATCH query
    /// (`MATCH (a)-[…]->(b), (c)-[…]->(d) [WHERE …] RETURN …`).
    ///
    /// The first pattern is compiled and executed normally; each subsequent
    /// pattern is then "joined" against the running rows. The join strategy is:
    ///
    /// * If the additional pattern's first node references a variable that is
    ///   already bound in the row, the inner traversal is anchored at that
    ///   vertex (this is the common case — e.g. `(parent)-[:PARENT_OF]->(p),
    ///   (parent)-[:PARENT_OF]->(s)`).
    /// * Otherwise the inner traversal walks the entire vertex set, producing a
    ///   Cartesian product (e.g. `(p1:Person)-[…], (p2:Person)-[…]`).
    ///
    /// Pattern variables introduced by the additional pattern are bound via
    /// `as_(var)` so they are visible to subsequent patterns, the WHERE clause,
    /// and the RETURN clause. A duplicate binding (variable already bound and
    /// not used as the anchor) is treated as an equality constraint —
    /// implemented by filtering rows whose anchor vertex doesn't match.
    fn execute_multi_pattern_query(
        &mut self,
        query: &Query,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Multi-pattern always needs path tracking (we read variable bindings
        // off the path) and falls under the multi-vars regime.
        self.has_multi_vars = true;

        // Compile pattern 0 with the standard pipeline so its variables get
        // registered in self.bindings and the traversal is built end-to-end.
        let first_pattern = &query.match_clause.patterns[0];
        if first_pattern.elements.is_empty() {
            return Err(CompileError::EmptyPattern);
        }
        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let traversal = g.v().with_path();
        let traversal = self.compile_pattern(first_pattern, traversal)?;

        // Pre-register variables introduced in subsequent patterns so that
        // WHERE/RETURN validation succeeds. We don't error on duplicates here
        // because in the multi-pattern context a repeated variable is a join
        // constraint, not a duplicate binding.
        for pattern in query.match_clause.patterns.iter().skip(1) {
            for element in &pattern.elements {
                match element {
                    PatternElement::Node(node) => {
                        if let Some(var) = &node.variable {
                            self.bindings.entry(var.clone()).or_insert(BindingInfo {
                                pattern_index: 0,
                                is_node: true,
                            });
                        }
                    }
                    PatternElement::Edge(edge) => {
                        if let Some(var) = &edge.variable {
                            self.bindings.entry(var.clone()).or_insert(BindingInfo {
                                pattern_index: 0,
                                is_node: false,
                            });
                        }
                    }
                }
            }
        }

        // Register CALL procedure YIELD variables
        for proc_clause in &query.call_procedure_clauses {
            for item in &proc_clause.yield_items {
                let var_name = item.alias.as_ref().unwrap_or(&item.field).clone();
                self.bindings.entry(var_name).or_insert(BindingInfo {
                    pattern_index: 0,
                    is_node: false,
                });
            }
        }

        // Validate variables now that all are registered.
        for item in &query.return_clause.items {
            self.validate_expression_variables(&item.expression)?;
        }
        if let Some(where_cl) = &query.where_clause {
            self.validate_expression_variables(&where_cl.expression)?;
        }

        // Materialise rows from pattern 0.
        let mut rows: Vec<HashMap<String, Value>> = traversal
            .execute()
            .map(|t: Traverser| traverser_to_row(&t))
            .collect();

        // Join each subsequent pattern.
        for pattern in query.match_clause.patterns.iter().skip(1) {
            rows = self.expand_rows_with_pattern(rows, pattern)?;
            if rows.is_empty() {
                break;
            }
        }

        // Apply WHERE filter.
        if let Some(where_cl) = &query.where_clause {
            rows.retain(|row| self.evaluate_predicate_from_row(&where_cl.expression, row));
        }

        // Process CALL procedure clauses
        for proc_clause in &query.call_procedure_clauses {
            rows = self.execute_call_procedure(rows, proc_clause)?;
        }

        // Build RETURN values.
        let mut results: Vec<Value> = rows
            .into_iter()
            .filter_map(|row| self.evaluate_return_for_row(&query.return_clause.items, &row))
            .collect();

        if query.return_clause.distinct {
            results = self.deduplicate_results(results);
        }

        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;
        let results = self.apply_limit(&query.limit_clause, results);
        Ok(results)
    }

    /// Expand each input row by joining it with traversers produced by
    /// `pattern`. See `execute_multi_pattern_query` for the join semantics.
    fn expand_rows_with_pattern(
        &self,
        rows: Vec<HashMap<String, Value>>,
        pattern: &Pattern,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        if pattern.elements.is_empty() {
            return Ok(rows);
        }

        // Determine anchor: the first node's variable, if it's already bound
        // in the row (we check the first row — variable presence is uniform).
        let anchor_var = match &pattern.elements[0] {
            PatternElement::Node(node) => node.variable.clone(),
            PatternElement::Edge(_) => None,
        };
        let anchored = anchor_var
            .as_ref()
            .and_then(|v| rows.first().map(|row| row.contains_key(v)))
            .unwrap_or(false);

        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let mut out: Vec<HashMap<String, Value>> = Vec::new();

        for row in rows {
            // Build a fresh traversal for this row.
            let mut traversal = if anchored {
                let var = anchor_var.as_ref().expect("anchored => anchor_var is Some");
                let vid = match row.get(var) {
                    Some(Value::Vertex(v)) => *v,
                    // Anchor variable resolves to a non-vertex (or null) — no
                    // matches for this row.
                    _ => continue,
                };
                g.v_ids([vid]).with_path()
            } else {
                g.v().with_path()
            };

            // Walk pattern elements. When anchored, we skip the first node's
            // filters (it's already pinned to a specific vertex), but we still
            // apply `as_(var)` so the binding is propagated.
            let mut is_first = true;
            for element in &pattern.elements {
                match element {
                    PatternElement::Node(node) => {
                        let skip_filters = is_first && anchored;
                        is_first = false;

                        if !skip_filters {
                            traversal = self.apply_node_filters(node, traversal);
                        }

                        if let Some(var) = &node.variable {
                            // If the variable is already bound and this is not
                            // the anchor position, enforce equality by
                            // filtering on the bound vertex id.
                            if let Some(existing) = row.get(var) {
                                if !(skip_filters) {
                                    if let Value::Vertex(target) = existing {
                                        let target_id = *target;
                                        traversal = traversal.filter(move |_ctx, v| {
                                            matches!(v, Value::Vertex(id) if *id == target_id)
                                        });
                                    } else {
                                        // Non-vertex binding can't match a node
                                        // pattern; drop this row.
                                        traversal = traversal.filter(|_ctx, _v| false);
                                    }
                                }
                                // Re-label so it's recoverable from the path.
                                traversal = traversal.as_(var.as_str());
                            } else {
                                traversal = traversal.as_(var.as_str());
                            }
                        }
                    }
                    PatternElement::Edge(edge) => {
                        is_first = false;
                        traversal = self.apply_edge_navigation(edge, traversal);
                        if let Some(var) = &edge.variable {
                            traversal = traversal.as_(var.as_str());
                        }
                    }
                }
            }

            // Execute and produce one combined row per matching traverser.
            for traverser in traversal.execute() {
                let mut new_row = row.clone();
                for label in traverser.path.all_labels() {
                    if let Some(values) = traverser.path.get(label) {
                        if let Some(pv) = values.last() {
                            new_row.insert(label.clone(), pv.to_value());
                        }
                    }
                }
                out.push(new_row);
            }
        }

        Ok(out)
    }

    /// Execute a query with UNWIND clauses.
    ///
    /// UNWIND expands a list expression into individual rows. Each element
    /// from the list becomes a separate row bound to the alias variable.
    fn execute_with_unwind(
        &mut self,
        query: &Query,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Execute the base match first
        let base_traversers: Vec<Traverser> = if self.has_multi_vars {
            traversal.execute().collect()
        } else {
            // Convert values to traversers for consistent handling
            traversal
                .to_list()
                .into_iter()
                .map(Traverser::new)
                .collect()
        };

        // Apply each UNWIND clause in sequence
        let mut current_rows: Vec<HashMap<String, Value>> = base_traversers
            .into_iter()
            .map(|t| {
                let mut row = HashMap::new();
                // Copy bound variables from path to row
                for label in t.path.all_labels() {
                    if let Some(values) = t.path.get(label) {
                        if let Some(path_value) = values.last() {
                            row.insert(label.clone(), path_value.to_value());
                        }
                    }
                }
                // Store the full path as __path__ for path() function
                let path_values: Vec<Value> = t.path.objects().map(|pv| pv.to_value()).collect();
                row.insert("__path__".to_string(), Value::List(path_values));
                // Also store the current traverser value if we have bindings
                row.insert("__current__".to_string(), t.value);
                row
            })
            .collect();

        // Apply each UNWIND clause
        for unwind in &query.unwind_clauses {
            current_rows = self.apply_unwind(current_rows, unwind)?;
        }

        // Apply WHERE filter if present
        let filtered_rows: Vec<HashMap<String, Value>> = if let Some(where_cl) = &query.where_clause
        {
            current_rows
                .into_iter()
                .filter(|row| self.evaluate_predicate_from_row(&where_cl.expression, row))
                .collect()
        } else {
            current_rows
        };

        // Apply LET clauses if present
        let filtered_rows = self.apply_let_clauses(filtered_rows, &query.let_clauses);

        // Process RETURN clause
        let results: Vec<Value> = filtered_rows
            .into_iter()
            .filter_map(|row| self.evaluate_return_for_row(&query.return_clause.items, &row))
            .collect();

        // Apply DISTINCT if requested
        let results = if query.return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        // Apply ORDER BY if present
        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

        // Apply LIMIT/OFFSET if present
        let results = self.apply_limit(&query.limit_clause, results);

        Ok(results)
    }

    /// Execute a query with LET clauses.
    ///
    /// LET binds computed values to variables for use in RETURN.
    /// This function converts the traversal results to row-based processing
    /// to support LET variable bindings.
    fn execute_with_let(
        &self,
        query: &Query,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Execute the base match first
        let base_traversers: Vec<Traverser> = if self.has_multi_vars {
            traversal.execute().collect()
        } else {
            // Convert values to traversers for consistent handling
            traversal
                .to_list()
                .into_iter()
                .map(Traverser::new)
                .collect()
        };

        // Convert traversers to rows
        let current_rows: Vec<HashMap<String, Value>> = base_traversers
            .into_iter()
            .map(|t| {
                let mut row = HashMap::new();
                // Copy bound variables from path to row
                for label in t.path.all_labels() {
                    if let Some(values) = t.path.get(label) {
                        if let Some(path_value) = values.last() {
                            row.insert(label.clone(), path_value.to_value());
                        }
                    }
                }
                // Store the full path as __path__ for path() function
                let path_values: Vec<Value> = t.path.objects().map(|pv| pv.to_value()).collect();
                row.insert("__path__".to_string(), Value::List(path_values));
                // Also store the current traverser value
                row.insert("__current__".to_string(), t.value);
                row
            })
            .collect();

        // Apply WHERE filter if present
        let filtered_rows: Vec<HashMap<String, Value>> = if let Some(where_cl) = &query.where_clause
        {
            current_rows
                .into_iter()
                .filter(|row| self.evaluate_predicate_from_row(&where_cl.expression, row))
                .collect()
        } else {
            current_rows
        };

        // Apply LET clauses
        let filtered_rows = self.apply_let_clauses(filtered_rows, &query.let_clauses);

        // Process RETURN clause
        let results: Vec<Value> = filtered_rows
            .into_iter()
            .filter_map(|row| self.evaluate_return_for_row(&query.return_clause.items, &row))
            .collect();

        // Apply DISTINCT if requested
        let results = if query.return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        // Apply ORDER BY if present
        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

        // Apply LIMIT/OFFSET if present
        let results = self.apply_limit(&query.limit_clause, results);

        Ok(results)
    }

    /// Execute a query with WITH clauses.
    ///
    /// WITH clauses act as query pipes, transforming and filtering results between
    /// query stages. Each WITH clause:
    /// 1. Projects specified expressions to a new scope (only projected variables are available afterward)
    /// 2. Can contain aggregations (GROUP BY semantics when mixing aggregates with non-aggregates)
    /// 3. Can filter with WHERE after projection
    /// 4. Can apply ORDER BY and LIMIT within the clause
    /// 5. Can use DISTINCT to deduplicate
    fn execute_with_with_clauses(
        &self,
        query: &Query,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Execute the base match first
        let base_traversers: Vec<Traverser> = if self.has_multi_vars {
            traversal.execute().collect()
        } else {
            traversal
                .to_list()
                .into_iter()
                .map(Traverser::new)
                .collect()
        };

        // Convert traversers to rows
        let mut current_rows: Vec<HashMap<String, Value>> = base_traversers
            .into_iter()
            .map(|t| {
                let mut row = HashMap::new();
                // Copy bound variables from path to row
                for label in t.path.all_labels() {
                    if let Some(values) = t.path.get(label) {
                        if let Some(path_value) = values.last() {
                            row.insert(label.clone(), path_value.to_value());
                        }
                    }
                }
                // Store the full path as __path__ for path() function
                let path_values: Vec<Value> = t.path.objects().map(|pv| pv.to_value()).collect();
                row.insert("__path__".to_string(), Value::List(path_values));
                // Also store the current traverser value
                row.insert("__current__".to_string(), t.value);
                row
            })
            .collect();

        // Apply main WHERE filter if present (before any WITH clause)
        if let Some(where_cl) = &query.where_clause {
            current_rows.retain(|row| self.evaluate_predicate_from_row(&where_cl.expression, row));
        }

        // Process each WITH clause sequentially
        for with_clause in &query.with_clauses {
            current_rows = self.apply_with_clause(current_rows, with_clause)?;
        }

        // Apply LET clauses if present
        let current_rows = self.apply_let_clauses(current_rows, &query.let_clauses);

        // Process RETURN clause
        let results: Vec<Value> = current_rows
            .into_iter()
            .filter_map(|row| self.evaluate_return_for_row(&query.return_clause.items, &row))
            .collect();

        // Apply DISTINCT if requested in RETURN
        let results = if query.return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        // Apply ORDER BY if present
        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

        // Apply LIMIT/OFFSET if present
        let results = self.apply_limit(&query.limit_clause, results);

        Ok(results)
    }

    /// Apply a single WITH clause to transform rows.
    ///
    /// WITH clause semantics:
    /// - Projects only the specified expressions (creates a new scope)
    /// - If aggregates are present, groups by non-aggregate expressions
    /// - Applies WHERE filter after projection
    /// - Applies ORDER BY and LIMIT within the clause
    /// - DISTINCT deduplicates the projected rows
    fn apply_with_clause(
        &self,
        rows: Vec<HashMap<String, Value>>,
        with_clause: &WithClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        if rows.is_empty() {
            return Ok(rows);
        }

        // Check if any WITH item contains aggregates
        let has_aggregates = with_clause
            .items
            .iter()
            .any(|item| Self::expr_has_aggregate(&item.expression));

        let mut new_rows = if has_aggregates {
            // WITH with aggregates: group by non-aggregate expressions
            self.apply_with_aggregation(&rows, with_clause)?
        } else {
            // Simple projection: evaluate expressions per-row
            self.apply_with_projection(&rows, with_clause)?
        };

        // Apply DISTINCT if requested
        if with_clause.distinct {
            new_rows = self.deduplicate_rows(new_rows);
        }

        // Apply WHERE filter if present (after projection)
        if let Some(where_cl) = &with_clause.where_clause {
            new_rows.retain(|row| self.evaluate_predicate_from_row(&where_cl.expression, row));
        }

        // Apply ORDER BY if present within WITH clause
        if let Some(order_clause) = &with_clause.order_clause {
            new_rows = self.apply_order_by_to_rows(new_rows, order_clause)?;
        }

        // Apply LIMIT/OFFSET if present within WITH clause
        if let Some(limit_clause) = &with_clause.limit_clause {
            new_rows = self.apply_limit_to_rows(new_rows, limit_clause);
        }

        Ok(new_rows)
    }

    /// Apply simple WITH projection without aggregation.
    fn apply_with_projection(
        &self,
        rows: &[HashMap<String, Value>],
        with_clause: &WithClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        let mut new_rows = Vec::with_capacity(rows.len());

        for row in rows {
            let mut new_row = HashMap::new();
            for item in &with_clause.items {
                let key = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                let value = self.evaluate_expression_from_row(&item.expression, row);
                new_row.insert(key, value);
            }
            new_rows.push(new_row);
        }

        Ok(new_rows)
    }

    /// Apply WITH clause with aggregation (GROUP BY semantics).
    ///
    /// When WITH contains aggregates, non-aggregate expressions become the group key.
    fn apply_with_aggregation(
        &self,
        rows: &[HashMap<String, Value>],
        with_clause: &WithClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        // Separate aggregate from non-aggregate items
        let mut group_items: Vec<&ReturnItem> = Vec::new();
        let mut aggregate_items: Vec<&ReturnItem> = Vec::new();

        for item in &with_clause.items {
            if Self::expr_has_aggregate(&item.expression) {
                aggregate_items.push(item);
            } else {
                group_items.push(item);
            }
        }

        // If no group items, aggregate over all rows (global aggregation)
        if group_items.is_empty() {
            let mut result_row = HashMap::new();
            for item in &aggregate_items {
                let key = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                let value = self.compute_aggregate_over_rows(rows, &item.expression);
                result_row.insert(key, value);
            }
            return Ok(vec![result_row]);
        }

        // Group rows by non-aggregate expressions
        let mut groups: HashMap<Vec<ComparableValue>, Vec<&HashMap<String, Value>>> =
            HashMap::new();

        for row in rows {
            let group_key: Vec<ComparableValue> = group_items
                .iter()
                .map(|item| {
                    let val = self.evaluate_expression_from_row(&item.expression, row);
                    ComparableValue::from(val)
                })
                .collect();

            groups.entry(group_key).or_default().push(row);
        }

        // Compute result for each group
        let mut new_rows = Vec::with_capacity(groups.len());

        for (group_key, group_rows) in groups {
            let mut new_row = HashMap::new();

            // Add group-by values
            for (i, item) in group_items.iter().enumerate() {
                let key = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                let value = Value::from(group_key[i].clone());
                new_row.insert(key, value);
            }

            // Compute aggregates over the group
            let owned_rows: Vec<HashMap<String, Value>> = group_rows.into_iter().cloned().collect();
            for item in &aggregate_items {
                let key = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                let value = self.compute_aggregate_over_rows(&owned_rows, &item.expression);
                new_row.insert(key, value);
            }

            new_rows.push(new_row);
        }

        Ok(new_rows)
    }

    /// Compute an aggregate expression over a collection of rows.
    fn compute_aggregate_over_rows(
        &self,
        rows: &[HashMap<String, Value>],
        expr: &Expression,
    ) -> Value {
        match expr {
            Expression::Aggregate {
                func,
                distinct,
                expr: inner_expr,
            } => {
                // Collect values from all rows
                let mut values: Vec<Value> = rows
                    .iter()
                    .map(|row| self.evaluate_expression_from_row(inner_expr, row))
                    .filter(|v| !matches!(v, Value::Null))
                    .collect();

                // Apply DISTINCT if requested
                if *distinct {
                    let mut seen = HashSet::new();
                    values.retain(|v| {
                        let key = format!("{:?}", v);
                        seen.insert(key)
                    });
                }

                // Compute the aggregate
                match func {
                    AggregateFunc::Count => Value::Int(values.len() as i64),
                    AggregateFunc::Sum => self.compute_sum(&values),
                    AggregateFunc::Avg => self.compute_avg(&values),
                    AggregateFunc::Min => self.compute_min(&values),
                    AggregateFunc::Max => self.compute_max(&values),
                    AggregateFunc::Collect => Value::List(values),
                }
            }
            Expression::BinaryOp { left, right, op } => {
                // Handle expressions containing aggregates
                let left_val = self.compute_aggregate_over_rows(rows, left);
                let right_val = self.compute_aggregate_over_rows(rows, right);
                apply_binary_op(*op, left_val, right_val)
            }
            _ => {
                // Non-aggregate expression - should not happen in aggregate context
                // but handle gracefully by evaluating against first row
                if let Some(row) = rows.first() {
                    self.evaluate_expression_from_row(expr, row)
                } else {
                    Value::Null
                }
            }
        }
    }

    /// Helper to compute sum from values.
    ///
    /// Uses checked arithmetic for integer sums, falling back to float
    /// representation if overflow occurs.
    fn compute_sum(&self, values: &[Value]) -> Value {
        let mut float_sum = 0.0;
        let mut is_int = true;
        let mut int_sum: i64 = 0;
        let mut overflow = false;

        for v in values {
            match v {
                Value::Int(i) => {
                    if is_int && !overflow {
                        match int_sum.checked_add(*i) {
                            Some(s) => int_sum = s,
                            None => overflow = true, // Switch to float on overflow
                        }
                    }
                    float_sum += *i as f64;
                }
                Value::Float(f) => {
                    is_int = false;
                    float_sum += f;
                }
                _ => {}
            }
        }

        if is_int && !overflow {
            Value::Int(int_sum)
        } else {
            Value::Float(float_sum)
        }
    }

    /// Helper to compute average from values.
    fn compute_avg(&self, values: &[Value]) -> Value {
        if values.is_empty() {
            return Value::Null;
        }

        let mut sum = 0.0;
        let mut count = 0;

        for v in values {
            match v {
                Value::Int(i) => {
                    sum += *i as f64;
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
            Value::Float(sum / count as f64)
        } else {
            Value::Null
        }
    }

    /// Helper to compute minimum from values.
    fn compute_min(&self, values: &[Value]) -> Value {
        values
            .iter()
            .filter(|v| !matches!(v, Value::Null))
            .min_by(|a, b| compare_values(a, b))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Helper to compute maximum from values.
    fn compute_max(&self, values: &[Value]) -> Value {
        values
            .iter()
            .filter(|v| !matches!(v, Value::Null))
            .max_by(|a, b| compare_values(a, b))
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Deduplicate rows based on all columns.
    fn deduplicate_rows(&self, rows: Vec<HashMap<String, Value>>) -> Vec<HashMap<String, Value>> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for row in rows {
            // Create a comparable key from all row values
            let key = self.row_to_comparable_key(&row);
            if seen.insert(key) {
                result.push(row);
            }
        }

        result
    }

    /// Convert a row to a comparable string key for deduplication.
    fn row_to_comparable_key(&self, row: &HashMap<String, Value>) -> String {
        let mut pairs: Vec<_> = row.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        format!("{:?}", pairs)
    }

    /// Apply ORDER BY to rows.
    fn apply_order_by_to_rows(
        &self,
        mut rows: Vec<HashMap<String, Value>>,
        order_clause: &OrderClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        rows.sort_by(|a, b| {
            for order_item in &order_clause.items {
                let val_a = self.evaluate_expression_from_row(&order_item.expression, a);
                let val_b = self.evaluate_expression_from_row(&order_item.expression, b);

                let cmp = compare_values(&val_a, &val_b);
                if cmp != std::cmp::Ordering::Equal {
                    return if order_item.descending {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Apply LIMIT/OFFSET to rows.
    fn apply_limit_to_rows(
        &self,
        rows: Vec<HashMap<String, Value>>,
        limit_clause: &LimitClause,
    ) -> Vec<HashMap<String, Value>> {
        let offset = limit_clause.offset.unwrap_or(0) as usize;
        let limit = limit_clause.limit as usize;

        rows.into_iter().skip(offset).take(limit).collect()
    }

    /// Convert expression to a key string for WITH clause output naming.
    fn expression_to_key(expr: &Expression) -> String {
        match expr {
            Expression::Variable(name) => name.clone(),
            Expression::Property { variable, property } => format!("{}.{}", variable, property),
            Expression::Aggregate { func, expr, .. } => {
                let inner = Self::expression_to_key(expr);
                match func {
                    AggregateFunc::Count => format!("count({})", inner),
                    AggregateFunc::Sum => format!("sum({})", inner),
                    AggregateFunc::Avg => format!("avg({})", inner),
                    AggregateFunc::Min => format!("min({})", inner),
                    AggregateFunc::Max => format!("max({})", inner),
                    AggregateFunc::Collect => format!("collect({})", inner),
                }
            }
            Expression::FunctionCall { name, args } => {
                let args_str: Vec<String> = args.iter().map(Self::expression_to_key).collect();
                format!("{}({})", name, args_str.join(", "))
            }
            Expression::Literal(lit) => match lit {
                Literal::Int(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => format!("\"{}\"", s),
                Literal::Bool(b) => b.to_string(),
                Literal::Null => "null".to_string(),
            },
            _ => format!("{:?}", expr),
        }
    }

    /// Apply an UNWIND clause to expand lists into rows.
    fn apply_unwind(
        &self,
        rows: Vec<HashMap<String, Value>>,
        unwind: &UnwindClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        let mut result = Vec::new();

        for row in rows {
            // Evaluate the expression to get a list
            let list_value = self.evaluate_expression_from_row(&unwind.expression, &row);

            match list_value {
                Value::List(items) => {
                    // Create a new row for each item
                    for item in items {
                        let mut new_row = row.clone();
                        new_row.insert(unwind.alias.clone(), item);
                        result.push(new_row);
                    }
                }
                Value::Null => {
                    // UNWIND null produces no rows
                }
                other => {
                    // UNWIND non-list wraps in single-element list
                    let mut new_row = row.clone();
                    new_row.insert(unwind.alias.clone(), other);
                    result.push(new_row);
                }
            }
        }

        Ok(result)
    }

    /// Apply LET clauses to transform rows by binding new variables.
    ///
    /// LET binds the result of an expression to a new variable. If the expression
    /// contains aggregates, the aggregate is computed over all rows and bound to
    /// each row. Non-aggregate expressions are evaluated per-row.
    fn apply_let_clauses(
        &self,
        rows: Vec<HashMap<String, Value>>,
        let_clauses: &[LetClause],
    ) -> Vec<HashMap<String, Value>> {
        if let_clauses.is_empty() {
            return rows;
        }

        let mut current_rows = rows;

        for let_clause in let_clauses {
            current_rows = self.apply_single_let_clause(current_rows, let_clause);
        }

        current_rows
    }

    /// Apply a single LET clause to all rows.
    fn apply_single_let_clause(
        &self,
        rows: Vec<HashMap<String, Value>>,
        let_clause: &LetClause,
    ) -> Vec<HashMap<String, Value>> {
        if rows.is_empty() {
            return rows;
        }

        // Check if the expression contains aggregates
        if Self::expr_has_aggregate(&let_clause.expression) {
            // Aggregate LET: compute once over all rows, bind to all
            let aggregate_value = self.compute_let_aggregate(&rows, &let_clause.expression);

            rows.into_iter()
                .map(|mut row| {
                    row.insert(let_clause.variable.clone(), aggregate_value.clone());
                    row
                })
                .collect()
        } else {
            // Non-aggregate LET: evaluate per-row
            rows.into_iter()
                .map(|mut row| {
                    let value = self.evaluate_expression_from_row(&let_clause.expression, &row);
                    row.insert(let_clause.variable.clone(), value);
                    row
                })
                .collect()
        }
    }

    /// Compute an aggregate expression for LET over all rows.
    fn compute_let_aggregate(&self, rows: &[HashMap<String, Value>], expr: &Expression) -> Value {
        match expr {
            Expression::Aggregate {
                func,
                distinct,
                expr: inner_expr,
            } => {
                // Collect values from all rows
                let mut values: Vec<Value> = rows
                    .iter()
                    .map(|row| self.evaluate_expression_from_row(inner_expr, row))
                    .filter(|v| !matches!(v, Value::Null))
                    .collect();

                // Apply DISTINCT if requested
                if *distinct {
                    let mut seen = HashSet::new();
                    values.retain(|v| {
                        let key = format!("{:?}", v);
                        seen.insert(key)
                    });
                }

                // Compute the aggregate
                match func {
                    AggregateFunc::Count => Value::Int(values.len() as i64),
                    AggregateFunc::Sum => {
                        let mut float_sum = 0.0;
                        let mut is_int = true;
                        let mut int_sum: i64 = 0;
                        let mut overflow = false;

                        for val in &values {
                            match val {
                                Value::Int(n) => {
                                    if is_int && !overflow {
                                        match int_sum.checked_add(*n) {
                                            Some(s) => int_sum = s,
                                            None => overflow = true, // Switch to float on overflow
                                        }
                                    }
                                    float_sum += *n as f64;
                                }
                                Value::Float(f) => {
                                    is_int = false;
                                    float_sum += f;
                                }
                                _ => {}
                            }
                        }

                        if is_int && !overflow {
                            Value::Int(int_sum)
                        } else {
                            Value::Float(float_sum)
                        }
                    }
                    AggregateFunc::Avg => {
                        if values.is_empty() {
                            return Value::Null;
                        }
                        let mut sum = 0.0;
                        let mut count = 0;
                        for val in &values {
                            match val {
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
                            Value::Float(sum / count as f64)
                        } else {
                            Value::Null
                        }
                    }
                    AggregateFunc::Min => values
                        .into_iter()
                        .min_by(compare_values)
                        .unwrap_or(Value::Null),
                    AggregateFunc::Max => values
                        .into_iter()
                        .max_by(compare_values)
                        .unwrap_or(Value::Null),
                    AggregateFunc::Collect => Value::List(values),
                }
            }
            // For non-aggregate expressions at the top level (but may contain nested aggregates),
            // we evaluate the binary ops etc. but with aggregate handling
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.compute_let_aggregate(rows, left);
                let right_val = self.compute_let_aggregate(rows, right);
                apply_binary_op(*op, left_val, right_val)
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.compute_let_aggregate(rows, expr);
                match op {
                    UnaryOperator::Not => match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    },
                    UnaryOperator::Neg => match val {
                        Value::Int(n) => Value::Int(-n),
                        Value::Float(f) => Value::Float(-f),
                        _ => Value::Null,
                    },
                }
            }
            Expression::FunctionCall { name, args } => {
                // Handle SIZE function specially for aggregated lists
                if name.to_uppercase() == "SIZE" {
                    if let Some(arg) = args.first() {
                        let val = self.compute_let_aggregate(rows, arg);
                        return match val {
                            Value::List(l) => Value::Int(l.len() as i64),
                            Value::String(s) => Value::Int(s.len() as i64),
                            _ => Value::Null,
                        };
                    }
                }
                // For other functions, use the first row as context
                if !rows.is_empty() {
                    self.evaluate_function_call_from_row(name, args, &rows[0])
                } else {
                    Value::Null
                }
            }
            // For non-aggregate expressions, evaluate from the first row (they should be the same)
            _ => {
                if !rows.is_empty() {
                    self.evaluate_expression_from_row(expr, &rows[0])
                } else {
                    Value::Null
                }
            }
        }
    }

    // =========================================================================
    // CALL Subquery Support
    // =========================================================================

    /// Validate a CALL clause.
    ///
    /// Semantic validation rules:
    /// 1. Variables in importing WITH must exist in outer scope
    /// 2. Variables returned by CALL must not shadow existing outer scope variables
    fn validate_call_clause(&self, call_clause: &CallClause) -> Result<(), CompileError> {
        match &call_clause.body {
            CallBody::Single(query) => self.validate_call_query(query)?,
            CallBody::Union { queries, .. } => {
                for query in queries {
                    self.validate_call_query(query)?;
                }
            }
        }
        Ok(())
    }

    /// Validate a single CallQuery.
    fn validate_call_query(&self, query: &CallQuery) -> Result<(), CompileError> {
        // 1. Validate importing WITH variables exist in outer scope
        if let Some(importing_with) = &query.importing_with {
            for item in &importing_with.items {
                let var_name = match &item.expression {
                    Expression::Variable(name) => name.clone(),
                    Expression::Property { variable, .. } => variable.clone(),
                    _ => continue, // Non-variable expressions are allowed
                };
                if !self.bindings.contains_key(&var_name) {
                    return Err(CompileError::undefined_variable(&var_name));
                }
            }
        }

        // 2. Validate returned variables don't shadow outer scope
        for item in &query.return_clause.items {
            let returned_var = item
                .alias
                .clone()
                .unwrap_or_else(|| Self::expression_to_key(&item.expression));

            // Check if this variable already exists in outer scope
            // Exception: if the variable was imported and is being passed through
            let is_imported = query
                .importing_with
                .as_ref()
                .map(|iw| {
                    iw.items.iter().any(|imp_item| {
                        let imp_var = imp_item
                            .alias
                            .clone()
                            .unwrap_or_else(|| Self::expression_to_key(&imp_item.expression));
                        imp_var == returned_var
                    })
                })
                .unwrap_or(false);

            if self.bindings.contains_key(&returned_var) && !is_imported {
                return Err(CompileError::duplicate_variable(&returned_var));
            }
        }

        // Recursively validate nested CALL clauses
        for nested_call in &query.call_clauses {
            self.validate_call_clause(nested_call)?;
        }

        Ok(())
    }

    /// Register variables returned by a CALL clause as bindings.
    fn register_call_clause_variables(&mut self, call_clause: &CallClause) {
        let return_items = match &call_clause.body {
            CallBody::Single(query) => &query.return_clause.items,
            CallBody::Union { queries, .. } => {
                // For UNION, all branches must return the same columns
                // Use the first query's return items
                if let Some(first) = queries.first() {
                    &first.return_clause.items
                } else {
                    return;
                }
            }
        };

        for item in return_items {
            let var_name = item
                .alias
                .clone()
                .unwrap_or_else(|| Self::expression_to_key(&item.expression));
            self.bindings.insert(
                var_name,
                BindingInfo {
                    pattern_index: 0,
                    is_node: false,
                },
            );
        }
    }

    /// Execute a query with CALL clauses.
    ///
    /// CALL subqueries execute nested queries that can reference outer scope
    /// variables (correlated) or run independently (uncorrelated).
    fn execute_with_call_clauses(
        &self,
        query: &Query,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Execute the base match first
        let base_traversers: Vec<Traverser> = if self.has_multi_vars {
            traversal.execute().collect()
        } else {
            traversal
                .to_list()
                .into_iter()
                .map(Traverser::new)
                .collect()
        };

        // Convert traversers to rows
        let mut current_rows: Vec<HashMap<String, Value>> = base_traversers
            .into_iter()
            .map(|t| {
                let mut row = HashMap::new();
                // Copy bound variables from path to row
                for label in t.path.all_labels() {
                    if let Some(values) = t.path.get(label) {
                        if let Some(path_value) = values.last() {
                            row.insert(label.clone(), path_value.to_value());
                        }
                    }
                }
                // Store the full path as __path__ for path() function
                let path_values: Vec<Value> = t.path.objects().map(|pv| pv.to_value()).collect();
                row.insert("__path__".to_string(), Value::List(path_values));
                // Also store the current traverser value
                row.insert("__current__".to_string(), t.value);
                row
            })
            .collect();

        // Apply WHERE filter if present
        if let Some(where_cl) = &query.where_clause {
            current_rows.retain(|row| self.evaluate_predicate_from_row(&where_cl.expression, row));
        }

        // Process each CALL clause sequentially
        for call_clause in &query.call_clauses {
            current_rows = self.execute_call_clause(current_rows, call_clause)?;
        }

        // Process each CALL procedure clause sequentially
        for proc_clause in &query.call_procedure_clauses {
            current_rows = self.execute_call_procedure(current_rows, proc_clause)?;
        }

        // Apply LET clauses if present
        let current_rows = self.apply_let_clauses(current_rows, &query.let_clauses);

        // Apply WITH clauses if present
        let mut current_rows = current_rows;
        for with_clause in &query.with_clauses {
            current_rows = self.apply_with_clause(current_rows, with_clause)?;
        }

        // Process RETURN clause
        let results: Vec<Value> = current_rows
            .into_iter()
            .filter_map(|row| self.evaluate_return_for_row(&query.return_clause.items, &row))
            .collect();

        // Apply DISTINCT if requested
        let results = if query.return_clause.distinct {
            self.deduplicate_results(results)
        } else {
            results
        };

        // Apply ORDER BY if present
        let results = self.apply_order_by(&query.order_clause, &query.return_clause, results)?;

        // Apply LIMIT/OFFSET if present
        let results = self.apply_limit(&query.limit_clause, results);

        Ok(results)
    }

    /// Execute a single CALL clause against a set of rows.
    fn execute_call_clause(
        &self,
        rows: Vec<HashMap<String, Value>>,
        call_clause: &CallClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        if rows.is_empty() {
            return Ok(rows);
        }

        if call_clause.is_correlated() {
            self.execute_correlated_call(rows, call_clause)
        } else {
            self.execute_uncorrelated_call(rows, call_clause)
        }
    }

    /// Execute a CALL procedure clause against a set of rows.
    ///
    /// Evaluates procedure arguments from each row, dispatches to the appropriate
    /// algorithm implementation, and merges YIELD results back into the row.
    fn execute_call_procedure(
        &self,
        rows: Vec<HashMap<String, Value>>,
        proc_clause: &CallProcedureClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        if rows.is_empty() {
            return Ok(rows);
        }

        let mut result_rows = Vec::new();

        for outer_row in rows {
            // Evaluate arguments from the row context
            let args: Vec<Value> = proc_clause
                .arguments
                .iter()
                .map(|expr| self.evaluate_expression_from_row(expr, &outer_row))
                .collect();

            // Dispatch to the appropriate procedure
            let proc_results = self.dispatch_procedure(
                &proc_clause.procedure_name,
                &args,
                &proc_clause.yield_items,
            )?;

            // Merge each procedure result with the outer row
            for proc_row in proc_results {
                let mut combined = outer_row.clone();
                combined.extend(proc_row);
                result_rows.push(combined);
            }
        }

        Ok(result_rows)
    }

    /// Dispatch a procedure call to the appropriate algorithm implementation.
    ///
    /// Supported procedures:
    /// - `interstellar.shortestPath(source, target)` → YIELD path, distance
    /// - `interstellar.dijkstra(source, target, weightProperty)` → YIELD path, distance
    /// - `interstellar.kShortestPaths(source, target, k, weightProperty)` → YIELD path, distance, index
    /// - `interstellar.bfs(source)` → YIELD node, depth
    fn dispatch_procedure(
        &self,
        name: &str,
        args: &[Value],
        yield_items: &[YieldItem],
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        use crate::traversal::algorithm_steps::{
            bfs_shortest_path, dijkstra_on_storage, expand_from_storage, StepDirection,
        };

        let storage = self.snapshot.storage();

        match name {
            "interstellar.shortestPath" => {
                // Args: source (VertexId), target (VertexId)
                let source = self.extract_vertex_id_arg(name, args, 0, "source")?;
                let target = self.extract_vertex_id_arg(name, args, 1, "target")?;

                match bfs_shortest_path(storage, source, target, StepDirection::Out) {
                    Some(path_value) => {
                        let distance = match &path_value {
                            Value::List(v) => Value::Int(v.len() as i64 - 1),
                            _ => Value::Int(0),
                        };
                        let mut row = HashMap::new();
                        self.bind_yield(&mut row, yield_items, "path", path_value);
                        self.bind_yield(&mut row, yield_items, "distance", distance);
                        Ok(vec![row])
                    }
                    None => Ok(vec![]), // No path found → no rows
                }
            }

            "interstellar.dijkstra" => {
                // Args: source (VertexId), target (VertexId), weightProperty (String)
                let source = self.extract_vertex_id_arg(name, args, 0, "source")?;
                let target = self.extract_vertex_id_arg(name, args, 1, "target")?;
                let weight_prop = self.extract_string_arg(name, args, 2, "weightProperty")?;

                match dijkstra_on_storage(storage, source, target, &weight_prop, StepDirection::Out)
                {
                    Some(Value::Map(map)) => {
                        let path = map
                            .get("path")
                            .cloned()
                            .unwrap_or(Value::List(Vec::new()));
                        let weight = map.get("weight").cloned().unwrap_or(Value::Float(0.0));
                        let mut row = HashMap::new();
                        self.bind_yield(&mut row, yield_items, "path", path);
                        self.bind_yield(&mut row, yield_items, "distance", weight);
                        Ok(vec![row])
                    }
                    _ => Ok(vec![]), // No path found → no rows
                }
            }

            "interstellar.kShortestPaths" => {
                // Args: source (VertexId), target (VertexId), k (Int), weightProperty (String)
                let source = self.extract_vertex_id_arg(name, args, 0, "source")?;
                let target = self.extract_vertex_id_arg(name, args, 1, "target")?;
                let _k = self.extract_int_arg(name, args, 2, "k")? as usize;
                let weight_prop = self.extract_string_arg(name, args, 3, "weightProperty")?;

                // Use Yen's k-shortest-paths via repeated Dijkstra
                // For now, find up to k paths by running Dijkstra repeatedly
                // with path exclusion (simplified: just return Dijkstra result as single path)
                let mut results = Vec::new();

                // First path: standard Dijkstra
                if let Some(Value::Map(map)) = dijkstra_on_storage(
                    storage,
                    source,
                    target,
                    &weight_prop,
                    StepDirection::Out,
                ) {
                    let path = map
                        .get("path")
                        .cloned()
                        .unwrap_or(Value::List(Vec::new()));
                    let weight = map.get("weight").cloned().unwrap_or(Value::Float(0.0));
                    let mut row = HashMap::new();
                    self.bind_yield(&mut row, yield_items, "path", path);
                    self.bind_yield(&mut row, yield_items, "distance", weight);
                    self.bind_yield(&mut row, yield_items, "index", Value::Int(0));
                    results.push(row);
                }

                // TODO: Implement full Yen's k-shortest-paths via GraphStorage
                // For now, only the single shortest path is returned.
                // Full implementation requires k_shortest_paths from algorithms module
                // to be adapted for GraphStorage (currently requires GraphAccess).

                Ok(results)
            }

            "interstellar.bfs" => {
                // Args: source (VertexId)
                let source = self.extract_vertex_id_arg(name, args, 0, "source")?;

                // BFS traversal yielding (node, depth)
                let mut results = Vec::new();
                let mut visited = std::collections::HashSet::new();
                let mut queue = std::collections::VecDeque::new();

                visited.insert(source);
                queue.push_back((source, 0i64));

                while let Some((vid, depth)) = queue.pop_front() {
                    let mut row = HashMap::new();
                    self.bind_yield(&mut row, yield_items, "node", Value::Vertex(vid));
                    self.bind_yield(&mut row, yield_items, "depth", Value::Int(depth));
                    results.push(row);

                    let neighbors = expand_from_storage(storage, vid, StepDirection::Out);
                    for (neighbor, _, _) in neighbors {
                        if visited.insert(neighbor) {
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                }

                Ok(results)
            }

            _ => Err(CompileError::UnknownProcedure {
                name: name.to_string(),
            }),
        }
    }

    /// Extract a VertexId from procedure arguments.
    fn extract_vertex_id_arg(
        &self,
        proc_name: &str,
        args: &[Value],
        index: usize,
        param_name: &str,
    ) -> Result<VertexId, CompileError> {
        let value = args.get(index).ok_or_else(|| CompileError::ProcedureArgumentError {
            procedure: proc_name.to_string(),
            message: format!("missing argument '{param_name}' at position {index}"),
        })?;
        match value {
            Value::Vertex(id) => Ok(*id),
            Value::Int(n) => Ok(VertexId(*n as u64)),
            _ => Err(CompileError::ProcedureArgumentError {
                procedure: proc_name.to_string(),
                message: format!(
                    "argument '{param_name}' must be a vertex ID, got {value:?}"
                ),
            }),
        }
    }

    /// Extract a String from procedure arguments.
    fn extract_string_arg(
        &self,
        proc_name: &str,
        args: &[Value],
        index: usize,
        param_name: &str,
    ) -> Result<String, CompileError> {
        let value = args.get(index).ok_or_else(|| CompileError::ProcedureArgumentError {
            procedure: proc_name.to_string(),
            message: format!("missing argument '{param_name}' at position {index}"),
        })?;
        match value {
            Value::String(s) => Ok(s.clone()),
            _ => Err(CompileError::ProcedureArgumentError {
                procedure: proc_name.to_string(),
                message: format!(
                    "argument '{param_name}' must be a string, got {value:?}"
                ),
            }),
        }
    }

    /// Extract an integer from procedure arguments.
    fn extract_int_arg(
        &self,
        proc_name: &str,
        args: &[Value],
        index: usize,
        param_name: &str,
    ) -> Result<i64, CompileError> {
        let value = args.get(index).ok_or_else(|| CompileError::ProcedureArgumentError {
            procedure: proc_name.to_string(),
            message: format!("missing argument '{param_name}' at position {index}"),
        })?;
        match value {
            Value::Int(n) => Ok(*n),
            _ => Err(CompileError::ProcedureArgumentError {
                procedure: proc_name.to_string(),
                message: format!(
                    "argument '{param_name}' must be an integer, got {value:?}"
                ),
            }),
        }
    }

    /// Bind a procedure result field to a YIELD variable in the row.
    ///
    /// If yield_items is empty, binds the field with its default name.
    /// Otherwise, only binds fields that appear in the YIELD clause,
    /// using the alias if specified.
    fn bind_yield(
        &self,
        row: &mut HashMap<String, Value>,
        yield_items: &[YieldItem],
        field_name: &str,
        value: Value,
    ) {
        if yield_items.is_empty() {
            // No YIELD clause → bind all fields with default names
            row.insert(field_name.to_string(), value);
        } else {
            // Only bind fields listed in YIELD
            for item in yield_items {
                if item.field == field_name {
                    let key = item
                        .alias
                        .as_ref()
                        .unwrap_or(&item.field)
                        .clone();
                    row.insert(key, value);
                    return;
                }
            }
        }
    }

    /// Execute a correlated CALL subquery.
    ///
    /// Correlated subqueries execute once per outer row, with imported variables
    /// available in the subquery scope. Results are merged with outer rows.
    fn execute_correlated_call(
        &self,
        outer_rows: Vec<HashMap<String, Value>>,
        call_clause: &CallClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        let mut result_rows = Vec::new();

        for outer_row in outer_rows {
            // Execute subquery with this outer row's context
            let sub_results = self.execute_call_body_with_context(&call_clause.body, &outer_row)?;

            // Merge each subquery result with the outer row
            for sub_row in sub_results {
                let mut combined = outer_row.clone();
                combined.extend(sub_row);
                result_rows.push(combined);
            }
        }

        Ok(result_rows)
    }

    /// Execute an uncorrelated CALL subquery.
    ///
    /// Uncorrelated subqueries execute once and their results are cross-joined
    /// with all outer rows.
    fn execute_uncorrelated_call(
        &self,
        outer_rows: Vec<HashMap<String, Value>>,
        call_clause: &CallClause,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        // Execute subquery once with empty context
        let empty_context = HashMap::new();
        let sub_results = self.execute_call_body_with_context(&call_clause.body, &empty_context)?;

        if sub_results.is_empty() {
            // No subquery results - outer rows are excluded
            return Ok(Vec::new());
        }

        // Cross-join: each outer row combines with each subquery result
        let mut result_rows = Vec::new();
        for outer_row in outer_rows {
            for sub_row in &sub_results {
                let mut combined = outer_row.clone();
                combined.extend(sub_row.clone());
                result_rows.push(combined);
            }
        }

        Ok(result_rows)
    }

    /// Execute a CallBody (Single or Union) with a given outer context.
    fn execute_call_body_with_context(
        &self,
        body: &CallBody,
        outer_context: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        match body {
            CallBody::Single(query) => self.execute_call_query_with_context(query, outer_context),
            CallBody::Union { queries, all } => {
                let mut all_results = Vec::new();
                for query in queries {
                    let results = self.execute_call_query_with_context(query, outer_context)?;
                    all_results.extend(results);
                }

                if *all {
                    // UNION ALL - keep all results
                    Ok(all_results)
                } else {
                    // UNION - deduplicate
                    Ok(self.deduplicate_rows(all_results))
                }
            }
        }
    }

    /// Execute a CallQuery with a given outer context.
    fn execute_call_query_with_context(
        &self,
        query: &CallQuery,
        outer_context: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        // Build initial scope from importing WITH
        let mut scope = HashMap::new();
        if let Some(importing_with) = &query.importing_with {
            for item in &importing_with.items {
                let var_name = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                let value = self.evaluate_expression_from_row(&item.expression, outer_context);
                scope.insert(var_name, value);
            }
        }

        // If there's a MATCH clause, execute it
        let mut current_rows: Vec<HashMap<String, Value>> =
            if let Some(match_clause) = &query.match_clause {
                self.execute_match_for_call(match_clause, &scope)?
            } else {
                // No MATCH - start with just the imported scope
                vec![scope]
            };

        // Apply WHERE filter if present
        if let Some(where_cl) = &query.where_clause {
            current_rows.retain(|row| self.evaluate_predicate_from_row(&where_cl.expression, row));
        }

        // Process nested CALL clauses
        for nested_call in &query.call_clauses {
            current_rows = self.execute_call_clause(current_rows, nested_call)?;
        }

        // Apply WITH clauses
        for with_clause in &query.with_clauses {
            current_rows = self.apply_with_clause(current_rows, with_clause)?;
        }

        // Apply ORDER BY if present
        if let Some(order_clause) = &query.order_clause {
            current_rows = self.apply_order_by_to_rows(current_rows, order_clause)?;
        }

        // Apply LIMIT/OFFSET if present
        if let Some(limit_clause) = &query.limit_clause {
            current_rows = self.apply_limit_to_rows(current_rows, limit_clause);
        }

        // Check if RETURN clause has aggregates
        let has_aggregates = query
            .return_clause
            .items
            .iter()
            .any(|item| Self::expr_has_aggregate(&item.expression));

        // Project RETURN clause items to result rows
        let result_rows: Vec<HashMap<String, Value>> = if has_aggregates {
            // Aggregated return - produce a single row with aggregated values
            if current_rows.is_empty() {
                // No rows to aggregate - return empty result (or could return row with NULL/0)
                Vec::new()
            } else {
                let mut result = HashMap::new();
                for item in &query.return_clause.items {
                    let key = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| Self::expression_to_key(&item.expression));

                    let value = if Self::expr_has_aggregate(&item.expression) {
                        // Compute aggregate over all rows
                        self.compute_aggregate_over_rows(&current_rows, &item.expression)
                    } else {
                        // Non-aggregate expression - use first row
                        self.evaluate_expression_from_row(&item.expression, &current_rows[0])
                    };
                    result.insert(key, value);
                }
                vec![result]
            }
        } else {
            // Non-aggregated return - process each row
            current_rows
                .into_iter()
                .map(|row| {
                    let mut result = HashMap::new();
                    for item in &query.return_clause.items {
                        let key = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| Self::expression_to_key(&item.expression));
                        let value = self.evaluate_expression_from_row(&item.expression, &row);
                        result.insert(key, value);
                    }
                    result
                })
                .collect()
        };

        Ok(result_rows)
    }

    /// Execute a MATCH clause for a CALL subquery, returning rows with matched elements.
    fn execute_match_for_call(
        &self,
        match_clause: &MatchClause,
        imported_scope: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
        use crate::traversal::Traverser;

        if match_clause.patterns.is_empty() {
            return Ok(vec![imported_scope.clone()]);
        }

        let pattern = &match_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Ok(vec![imported_scope.clone()]);
        }

        // Check if the first node has a variable that's in the imported scope
        // If so, we need to start from that specific vertex
        let first_node = pattern.elements.first();
        let start_vertex = match first_node {
            Some(PatternElement::Node(node)) => {
                if let Some(var) = &node.variable {
                    if let Some(Value::Vertex(vid)) = imported_scope.get(var) {
                        Some(*vid)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        };

        // Build traversal
        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let traversal = if let Some(vid) = start_vertex {
            g.v_ids([vid]).with_path()
        } else {
            g.v().with_path()
        };

        // Compile the pattern (simplified - just apply labels and properties from first node)
        let traversal = if let Some(PatternElement::Node(node)) = first_node {
            let mut t = traversal;

            // Apply label filter if present
            if !node.labels.is_empty() {
                let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
                t = t.has_label_any(labels);
            }

            // Register variable if present
            if let Some(var) = &node.variable {
                t = t.as_(var);
            }

            // Apply property filters
            for (key, value) in &node.properties {
                let val: Value = value.clone().into();
                t = t.has_value(key.as_str(), val);
            }

            t
        } else {
            traversal
        };

        // Continue with remaining pattern elements
        let traversal = self.compile_remaining_pattern_for_call(pattern, traversal, 1)?;

        // Execute and collect results
        let traversers: Vec<Traverser> = traversal.execute().collect();

        let result_rows: Vec<HashMap<String, Value>> = traversers
            .into_iter()
            .map(|t| {
                let mut row = imported_scope.clone();
                // Copy bound variables from path to row
                for label in t.path.all_labels() {
                    if let Some(values) = t.path.get(label) {
                        if let Some(path_value) = values.last() {
                            row.insert(label.clone(), path_value.to_value());
                        }
                    }
                }
                row.insert("__current__".to_string(), t.value);
                row
            })
            .collect();

        Ok(result_rows)
    }

    /// Compile remaining pattern elements for a CALL subquery match.
    fn compile_remaining_pattern_for_call(
        &self,
        pattern: &Pattern,
        mut traversal: BoundTraversal<'a, (), Value>,
        start_index: usize,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
        for element in pattern.elements.iter().skip(start_index) {
            match element {
                PatternElement::Edge(edge) => {
                    // Apply edge navigation
                    let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();

                    traversal = match edge.direction {
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

                    // Register edge variable if present
                    if let Some(var) = &edge.variable {
                        traversal = traversal.as_(var);
                    }

                    // Navigate to the target vertex
                    traversal = match edge.direction {
                        EdgeDirection::Outgoing => traversal.in_v(),
                        EdgeDirection::Incoming => traversal.out_v(),
                        EdgeDirection::Both => traversal.other_v(),
                    };
                }
                PatternElement::Node(node) => {
                    // Apply label filter
                    if !node.labels.is_empty() {
                        let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
                        traversal = traversal.has_label_any(labels);
                    }

                    // Register variable
                    if let Some(var) = &node.variable {
                        traversal = traversal.as_(var);
                    }

                    // Apply property filters
                    for (key, value) in &node.properties {
                        let val: Value = value.clone().into();
                        traversal = traversal.has_value(key.as_str(), val);
                    }
                }
            }
        }

        Ok(traversal)
    }

    /// Evaluate an expression using a row (HashMap) for variable lookup.
    fn evaluate_expression_from_row(
        &self,
        expr: &Expression,
        row: &HashMap<String, Value>,
    ) -> Value {
        match expr {
            Expression::Literal(lit) => lit.clone().into(),
            Expression::Variable(var) => row.get(var).cloned().unwrap_or(Value::Null),
            Expression::Parameter(name) => {
                // Resolve parameter - if not found, return Null (error handling happens at validation)
                self.parameters.get(name).cloned().unwrap_or(Value::Null)
            }
            Expression::Property { variable, property } => {
                let element = row.get(variable).cloned().unwrap_or(Value::Null);
                self.extract_property(&element, property)
                    .unwrap_or(Value::Null)
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression_from_row(left, row);
                let right_val = self.evaluate_expression_from_row(right, row);
                apply_binary_op(*op, left_val, right_val)
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let val = self.evaluate_expression_from_row(expr, row);
                    match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    }
                }
                UnaryOperator::Neg => {
                    let val = self.evaluate_expression_from_row(expr, row);
                    match val {
                        Value::Int(n) => Value::Int(-n),
                        Value::Float(f) => Value::Float(-f),
                        _ => Value::Null,
                    }
                }
            },
            Expression::List(items) => {
                let values: Vec<Value> = items
                    .iter()
                    .map(|item| self.evaluate_expression_from_row(item, row))
                    .collect();
                Value::List(values)
            }
            Expression::Map(entries) => {
                let map: ValueMap = entries
                    .iter()
                    .map(|(key, value_expr)| {
                        let value = self.evaluate_expression_from_row(value_expr, row);
                        (key.clone(), value)
                    })
                    .collect();
                Value::Map(map)
            }
            Expression::FunctionCall { name, args } => {
                self.evaluate_function_call_from_row(name, args, row)
            }
            Expression::ListComprehension {
                variable,
                list,
                filter,
                transform,
            } => self.evaluate_list_comprehension_from_row(variable, list, filter, transform, row),
            Expression::Reduce {
                accumulator,
                initial,
                variable,
                list,
                expression,
            } => {
                self.evaluate_reduce_from_row(accumulator, initial, variable, list, expression, row)
            }
            Expression::Case(case_expr) => self.evaluate_case_from_row(case_expr, row),
            Expression::All {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_row(
                ListPredicateKind::All,
                variable,
                list,
                condition,
                row,
            ),
            Expression::Any {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_row(
                ListPredicateKind::Any,
                variable,
                list,
                condition,
                row,
            ),
            Expression::None {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_row(
                ListPredicateKind::None,
                variable,
                list,
                condition,
                row,
            ),
            Expression::Single {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_row(
                ListPredicateKind::Single,
                variable,
                list,
                condition,
                row,
            ),
            Expression::PatternComprehension {
                pattern,
                filter,
                transform,
            } => self.evaluate_pattern_comprehension_from_row(
                pattern,
                filter.as_deref(),
                transform,
                row,
            ),
            _ => Value::Null,
        }
    }

    /// Evaluate a function call using a row for variable lookup.
    fn evaluate_function_call_from_row(
        &self,
        name: &str,
        args: &[Expression],
        row: &HashMap<String, Value>,
    ) -> Value {
        match name.to_uppercase().as_str() {
            "COALESCE" => {
                for arg in args {
                    let val = self.evaluate_expression_from_row(arg, row);
                    if !matches!(val, Value::Null) {
                        return val;
                    }
                }
                Value::Null
            }
            "TOUPPER" | "UPPER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_expression_from_row(arg, row) {
                        return Value::String(s.to_uppercase());
                    }
                }
                Value::Null
            }
            "TOLOWER" | "LOWER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_expression_from_row(arg, row) {
                        return Value::String(s.to_lowercase());
                    }
                }
                Value::Null
            }
            "SIZE" | "LENGTH" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::String(s) => Value::Int(s.len() as i64),
                        Value::List(l) => Value::Int(l.len() as i64),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "ABS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Int(n.abs()),
                        Value::Float(f) => Value::Float(f.abs()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Extended math functions (Phase 2: Math-GQL Integration)

            // Square root
            "SQRT" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) if n >= 0 => Value::Float((n as f64).sqrt()),
                        Value::Float(f) if f >= 0.0 => Value::Float(f.sqrt()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Power function (alternative to ^ operator)
            "POW" | "POWER" => {
                if args.len() >= 2 {
                    let base = self.evaluate_expression_from_row(&args[0], row);
                    let exp = self.evaluate_expression_from_row(&args[1], row);
                    apply_binary_op(BinaryOperator::Pow, base, exp)
                } else {
                    Value::Null
                }
            }

            // Natural logarithm
            "LOG" | "LN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) if n > 0 => Value::Float((n as f64).ln()),
                        Value::Float(f) if f > 0.0 => Value::Float(f.ln()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Log base 10
            "LOG10" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) if n > 0 => Value::Float((n as f64).log10()),
                        Value::Float(f) if f > 0.0 => Value::Float(f.log10()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Exponential (e^x)
            "EXP" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).exp()),
                        Value::Float(f) => Value::Float(f.exp()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Trigonometric functions (input in radians)
            "SIN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).sin()),
                        Value::Float(f) => Value::Float(f.sin()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "COS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).cos()),
                        Value::Float(f) => Value::Float(f.cos()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "TAN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).tan()),
                        Value::Float(f) => Value::Float(f.tan()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Inverse trigonometric functions
            "ASIN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => {
                            let f = n as f64;
                            if (-1.0..=1.0).contains(&f) {
                                Value::Float(f.asin())
                            } else {
                                Value::Null
                            }
                        }
                        Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.asin()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "ACOS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => {
                            let f = n as f64;
                            if (-1.0..=1.0).contains(&f) {
                                Value::Float(f.acos())
                            } else {
                                Value::Null
                            }
                        }
                        Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.acos()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "ATAN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).atan()),
                        Value::Float(f) => Value::Float(f.atan()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Two-argument arctangent (atan2)
            "ATAN2" => {
                if args.len() >= 2 {
                    let y = self.evaluate_expression_from_row(&args[0], row);
                    let x = self.evaluate_expression_from_row(&args[1], row);
                    match (y, x) {
                        (Value::Int(y), Value::Int(x)) => Value::Float((y as f64).atan2(x as f64)),
                        (Value::Float(y), Value::Float(x)) => Value::Float(y.atan2(x)),
                        (Value::Int(y), Value::Float(x)) => Value::Float((y as f64).atan2(x)),
                        (Value::Float(y), Value::Int(x)) => Value::Float(y.atan2(x as f64)),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Degree/radian conversion
            "RADIANS" | "TORADIANS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).to_radians()),
                        Value::Float(f) => Value::Float(f.to_radians()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "DEGREES" | "TODEGREES" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
                        Value::Int(n) => Value::Float((n as f64).to_degrees()),
                        Value::Float(f) => Value::Float(f.to_degrees()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Rounding functions (these might already exist, adding for completeness)
            "CEIL" | "CEILING" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_expression_from_row(arg, row) {
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
                    match self.evaluate_expression_from_row(arg, row) {
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
                    match self.evaluate_expression_from_row(arg, row) {
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
                    match self.evaluate_expression_from_row(arg, row) {
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

            // Mathematical constants
            "PI" => Value::Float(std::f64::consts::PI),
            "E" => Value::Float(std::f64::consts::E),

            // MATH() function - evaluate mathexpr expressions
            "MATH" => self.evaluate_math_from_row(args, row),

            // Path function - returns the full traversal path stored in row
            "PATH" => row.get("__path__").cloned().unwrap_or(Value::List(vec![])),

            // Introspection functions
            "PROPERTIES" => {
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_expression_from_row(arg, row);
                    match element_val {
                        Value::Vertex(vid) => {
                            if let Some(vertex) = self.snapshot.storage().get_vertex(vid) {
                                return Value::Map(vertex.properties.into_value_map());
                            }
                        }
                        Value::Edge(eid) => {
                            if let Some(edge) = self.snapshot.storage().get_edge(eid) {
                                return Value::Map(edge.properties.into_value_map());
                            }
                        }
                        _ => {}
                    }
                }
                Value::Null
            }
            "LABELS" => {
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_expression_from_row(arg, row);
                    if let Value::Vertex(vid) = element_val {
                        if let Some(vertex) = self.snapshot.storage().get_vertex(vid) {
                            return Value::List(vec![Value::String(vertex.label)]);
                        }
                    }
                }
                Value::Null
            }
            "TYPE" => {
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_expression_from_row(arg, row);
                    if let Value::Edge(eid) = element_val {
                        if let Some(edge) = self.snapshot.storage().get_edge(eid) {
                            return Value::String(edge.label);
                        }
                    }
                }
                Value::Null
            }
            "ID" => {
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_expression_from_row(arg, row);
                    match element_val {
                        Value::Vertex(vid) => return id_to_value(vid.0),
                        Value::Edge(eid) => return id_to_value(eid.0),
                        _ => {}
                    }
                }
                Value::Null
            }

            _ => Value::Null,
        }
    }

    /// Evaluate a list comprehension expression using a row for variable lookup.
    ///
    /// List comprehension syntax: `[variable IN list WHERE? filter | transform]`
    ///
    /// # Semantics
    /// - Evaluates the list expression
    /// - For each element in the list, binds the variable and:
    ///   - If filter is present, evaluates it; skips element if false
    ///   - Evaluates transform expression to produce output element
    /// - Returns a new list with transformed elements
    /// - If input is NULL or not a list, returns NULL
    /// - Empty list input returns empty list
    fn evaluate_list_comprehension_from_row(
        &self,
        variable: &str,
        list_expr: &Expression,
        filter: &Option<Box<Expression>>,
        transform: &Expression,
        row: &HashMap<String, Value>,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_expression_from_row(list_expr, row);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null, // Non-list, non-null returns null
        };

        // Process each element
        let mut results = Vec::new();
        for item in items {
            // Create a temporary row with the comprehension variable bound
            let mut comp_row = row.clone();
            comp_row.insert(variable.to_string(), item);

            // Apply filter if present
            if let Some(filter_expr) = filter {
                if !self.evaluate_predicate_from_row(filter_expr, &comp_row) {
                    continue; // Skip this element
                }
            }

            // Evaluate transform expression
            let transformed = self.evaluate_expression_from_row(transform, &comp_row);
            results.push(transformed);
        }

        Value::List(results)
    }

    /// Evaluate a REDUCE expression using a row for variable lookup.
    ///
    /// REDUCE(accumulator = initial, variable IN list | expression)
    ///
    /// # Behavior
    /// - Initializes accumulator to the initial value
    /// - For each element in the list:
    ///   - Binds both accumulator and variable to the current row
    ///   - Evaluates the expression to produce the next accumulator value
    /// - Returns the final accumulator value
    ///
    /// # Edge Cases
    /// - NULL list returns NULL
    /// - Empty list returns the initial value
    /// - Non-list input returns NULL
    fn evaluate_reduce_from_row(
        &self,
        accumulator: &str,
        initial: &Expression,
        variable: &str,
        list_expr: &Expression,
        expression: &Expression,
        row: &HashMap<String, Value>,
    ) -> Value {
        // Evaluate the initial value for the accumulator
        let mut acc_value = self.evaluate_expression_from_row(initial, row);

        // Evaluate the list expression
        let list_value = self.evaluate_expression_from_row(list_expr, row);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null, // Non-list, non-null returns null
        };

        // Iterate and accumulate
        for item in items {
            // Create a temporary row with both accumulator and loop variable bound
            let mut iter_row = row.clone();
            iter_row.insert(accumulator.to_string(), acc_value);
            iter_row.insert(variable.to_string(), item);

            // Evaluate the expression to get the next accumulator value
            acc_value = self.evaluate_expression_from_row(expression, &iter_row);
        }

        acc_value
    }

    /// Evaluate a list predicate (ALL/ANY/NONE/SINGLE) using a row for variable lookup.
    ///
    /// List predicates test conditions across all elements of a list:
    /// - ALL: true if all elements satisfy the condition
    /// - ANY: true if at least one element satisfies the condition
    /// - NONE: true if no elements satisfy the condition
    /// - SINGLE: true if exactly one element satisfies the condition
    ///
    /// # Semantics
    /// - Empty list: ALL returns true (vacuous truth), ANY returns false, NONE returns true, SINGLE returns false
    /// - Non-list input: returns NULL
    fn evaluate_list_predicate_from_row(
        &self,
        kind: ListPredicateKind,
        variable: &str,
        list_expr: &Expression,
        condition: &Expression,
        row: &HashMap<String, Value>,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_expression_from_row(list_expr, row);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Count matching elements
        let mut match_count = 0;

        for item in items {
            let mut iter_row = row.clone();
            iter_row.insert(variable.to_string(), item);

            // Evaluate condition
            if self.evaluate_predicate_from_row(condition, &iter_row) {
                match_count += 1;

                // Early exit optimization for ANY - found one match
                if kind == ListPredicateKind::Any {
                    return Value::Bool(true);
                }
                // Early exit for NONE - found a match so result is false
                if kind == ListPredicateKind::None {
                    return Value::Bool(false);
                }
                // Early exit for SINGLE - found more than one match
                if kind == ListPredicateKind::Single && match_count > 1 {
                    return Value::Bool(false);
                }
                // Early exit for ALL - can't exit early, need to check all
            } else {
                // Condition was false
                // Early exit for ALL - found one that doesn't match
                if kind == ListPredicateKind::All {
                    return Value::Bool(false);
                }
            }
        }

        // Return result based on predicate kind
        Value::Bool(match kind {
            ListPredicateKind::All => true, // All matched (or empty list - vacuous truth)
            ListPredicateKind::Any => false, // None matched (we would have returned early if any)
            ListPredicateKind::None => true, // None matched (we would have returned early if any)
            ListPredicateKind::Single => match_count == 1,
        })
    }

    /// Evaluate a pattern comprehension expression using a row for variable lookup.
    ///
    /// Pattern comprehension allows inline pattern matching within expressions:
    /// `[(p)-[:FRIEND]->(f) | f.name]` returns a list of friend names for person p.
    ///
    /// # Semantics
    /// - The pattern must reference at least one outer variable (correlation)
    /// - For each match of the pattern, the transform expression is evaluated
    /// - Results are collected into a list
    /// - If pattern matches nothing, returns empty list
    /// - Optional WHERE filter can eliminate matches before transformation
    ///
    /// # Example
    /// ```text
    /// MATCH (p:Person)
    /// RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames
    /// ```
    fn evaluate_pattern_comprehension_from_row(
        &self,
        pattern: &Pattern,
        filter: Option<&Expression>,
        transform: &Expression,
        row: &HashMap<String, Value>,
    ) -> Value {
        // Find the starting vertex from the outer context
        // The first node in the pattern should reference an outer variable
        let start_vertex_id = match pattern.elements.first() {
            Some(PatternElement::Node(node)) => {
                if let Some(var) = &node.variable {
                    // Get the vertex from the row
                    match row.get(var) {
                        Some(Value::Vertex(vid)) => *vid,
                        Some(Value::Map(map)) => {
                            // Handle case where variable is bound to a map containing id
                            match map.get("id") {
                                Some(Value::Vertex(vid)) => *vid,
                                Some(Value::Int(id)) => VertexId(*id as u64),
                                _ => return Value::List(vec![]),
                            }
                        }
                        _ => return Value::List(vec![]), // No correlation point
                    }
                } else {
                    return Value::List(vec![]); // Pattern must start with a variable
                }
            }
            _ => return Value::List(vec![]), // Invalid pattern structure
        };

        // Build a traversal starting from the correlated vertex
        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let traversal = g.v_ids([start_vertex_id]).with_path();

        // Apply the remaining pattern elements (skip the first node which is the correlation point)
        // We need to apply label/property filters from the first node, then traverse edges and nodes
        let traversal = match self.compile_pattern_elements_for_comprehension(pattern, traversal) {
            Ok(t) => t,
            Err(_) => return Value::List(vec![]),
        };

        // Collect all matches
        let matches: Vec<Value> = traversal.to_list();

        // For each match, build a context with pattern bindings and evaluate transform
        let mut results = Vec::new();

        for match_value in matches {
            // Build row with pattern variables bound
            let mut match_row = row.clone();

            // Extract bindings from the match based on pattern structure
            self.bind_pattern_variables_from_match(pattern, &match_value, &mut match_row);

            // Apply optional filter
            if let Some(filter_expr) = filter {
                if !self.evaluate_predicate_from_row(filter_expr, &match_row) {
                    continue; // Skip this match
                }
            }

            // Evaluate transform expression
            let transformed = self.evaluate_expression_from_row(transform, &match_row);
            results.push(transformed);
        }

        Value::List(results)
    }

    /// Compile pattern elements for pattern comprehension traversal.
    ///
    /// This is similar to compile_pattern but handles the correlation point specially:
    /// - The first node's variable is already bound from outer context
    /// - We still apply label/property filters from the first node
    /// - Then traverse the remaining edges and nodes
    fn compile_pattern_elements_for_comprehension(
        &self,
        pattern: &Pattern,
        mut traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
        for (idx, element) in pattern.elements.iter().enumerate() {
            match element {
                PatternElement::Node(node) => {
                    if idx == 0 {
                        // First node: apply filters but don't add as_() (already bound)
                        if !node.labels.is_empty() {
                            let labels: Vec<&str> =
                                node.labels.iter().map(|s| s.as_str()).collect();
                            traversal = traversal.has_label_any(labels);
                        }
                        for (key, value) in &node.properties {
                            let val: Value = value.clone().into();
                            traversal = traversal.has_value(key.as_str(), val);
                        }
                        // Note: first node's variable comes from outer scope
                    } else {
                        // Subsequent nodes: apply full node compilation
                        if !node.labels.is_empty() {
                            let labels: Vec<&str> =
                                node.labels.iter().map(|s| s.as_str()).collect();
                            traversal = traversal.has_label_any(labels);
                        }
                        for (key, value) in &node.properties {
                            let val: Value = value.clone().into();
                            traversal = traversal.has_value(key.as_str(), val);
                        }
                        // Add as_() step to label this position for later binding
                        if let Some(var) = &node.variable {
                            traversal = traversal.as_(var);
                        }
                    }
                }
                PatternElement::Edge(edge) => {
                    // Compile edge navigation
                    let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();

                    traversal = match edge.direction {
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
                }
            }
        }
        Ok(traversal)
    }

    /// Bind pattern variables from a match result into the row.
    ///
    /// Extracts variable bindings from a traversal result based on pattern structure.
    fn bind_pattern_variables_from_match(
        &self,
        pattern: &Pattern,
        match_value: &Value,
        row: &mut HashMap<String, Value>,
    ) {
        // The match_value is the final vertex in the traversal
        // For patterns like (p)-[:FRIEND]->(f), the match_value is the 'f' vertex

        // Find the last node variable in the pattern (this is what the traversal returns)
        if let Some(PatternElement::Node(last_node)) = pattern.elements.last() {
            if let Some(var) = &last_node.variable {
                row.insert(var.clone(), match_value.clone());
            }
        }

        // Note: For more complex patterns with intermediate variables bound via as_(),
        // we would need to extract from the path. For now, we handle the simple case
        // where the last node is the main variable of interest.
        // If the match_value is a Map with path information, we could extract more bindings.
        if let Value::Map(map) = match_value {
            // If the traversal produced a map (e.g., from select()), copy bindings
            for (key, value) in map {
                if !row.contains_key(key) {
                    row.insert(key.clone(), value.clone());
                }
            }
        }
    }

    /// Evaluate a CASE expression using a row for variable lookup.
    fn evaluate_case_from_row(
        &self,
        case_expr: &CaseExpression,
        row: &HashMap<String, Value>,
    ) -> Value {
        // Evaluate each WHEN clause in order
        for (condition, result) in &case_expr.when_clauses {
            if self.evaluate_predicate_from_row(condition, row) {
                return self.evaluate_expression_from_row(result, row);
            }
        }

        // No WHEN matched, evaluate ELSE or return null
        if let Some(else_expr) = &case_expr.else_clause {
            self.evaluate_expression_from_row(else_expr, row)
        } else {
            Value::Null
        }
    }

    /// Evaluate a predicate using a row for variable lookup.
    fn evaluate_predicate_from_row(&self, expr: &Expression, row: &HashMap<String, Value>) -> bool {
        match expr {
            Expression::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    self.evaluate_predicate_from_row(left, row)
                        && self.evaluate_predicate_from_row(right, row)
                }
                BinaryOperator::Or => {
                    self.evaluate_predicate_from_row(left, row)
                        || self.evaluate_predicate_from_row(right, row)
                }
                _ => {
                    let left_val = self.evaluate_expression_from_row(left, row);
                    let right_val = self.evaluate_expression_from_row(right, row);
                    apply_comparison(*op, &left_val, &right_val)
                }
            },
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => !self.evaluate_predicate_from_row(expr, row),
                UnaryOperator::Neg => match self.evaluate_expression_from_row(expr, row) {
                    Value::Int(n) => n == 0,
                    Value::Float(f) => f == 0.0,
                    Value::Bool(b) => !b,
                    Value::Null => true,
                    _ => false,
                },
            },
            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_expression_from_row(expr, row);
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
                let val = self.evaluate_expression_from_row(expr, row);
                let in_list = list.iter().any(|item| {
                    let item_val = self.evaluate_expression_from_row(item, row);
                    val == item_val
                });
                if *negated {
                    !in_list
                } else {
                    in_list
                }
            }
            _ => {
                let val = self.evaluate_expression_from_row(expr, row);
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

    /// Evaluate RETURN clause for a row.
    fn evaluate_return_for_row(
        &self,
        items: &[ReturnItem],
        row: &HashMap<String, Value>,
    ) -> Option<Value> {
        if items.len() == 1 {
            Some(self.evaluate_expression_from_row(&items[0].expression, row))
        } else {
            let mut map = ValueMap::new();
            for item in items {
                let key = self.get_return_item_key(item);
                let value = self.evaluate_expression_from_row(&item.expression, row);
                map.insert(key, value);
            }
            Some(Value::Map(map))
        }
    }

    /// Register variables from an OPTIONAL MATCH pattern.
    /// These variables are valid to reference but may be null if the pattern doesn't match.
    fn register_optional_pattern_variables(&mut self, pattern: &Pattern) {
        for (index, element) in pattern.elements.iter().enumerate() {
            match element {
                PatternElement::Node(node) => {
                    if let Some(var) = &node.variable {
                        // Don't overwrite if already bound from main MATCH
                        if !self.bindings.contains_key(var) {
                            self.bindings.insert(
                                var.clone(),
                                BindingInfo {
                                    pattern_index: index,
                                    is_node: true,
                                },
                            );
                        }
                    }
                }
                PatternElement::Edge(edge) => {
                    if let Some(var) = &edge.variable {
                        if !self.bindings.contains_key(var) {
                            self.bindings.insert(
                                var.clone(),
                                BindingInfo {
                                    pattern_index: index,
                                    is_node: false,
                                },
                            );
                        }
                    }
                }
            }
        }
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
        mut traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
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
    /// Applies label filters, property filters, and inline WHERE to the traversal.
    fn compile_node(
        &mut self,
        node: &NodePattern,
        mut traversal: BoundTraversal<'a, (), Value>,
        index: usize,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
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

        // Apply inline WHERE filter if present
        if let Some(where_expr) = &node.where_clause {
            let expr = where_expr.clone();
            let params = self.parameters.clone();
            traversal = traversal
                .filter(move |ctx, val| eval_inline_predicate(ctx.storage(), &expr, val, &params));
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
    /// Handles edge variable binding, edge property filters, and inline WHERE.
    fn compile_edge(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
        // Check if this edge has a quantifier (variable-length path)
        if let Some(quantifier) = &edge.quantifier {
            return self.compile_edge_with_quantifier(edge, quantifier, traversal);
        }

        // Check if we need edge-level access (variable, properties, or inline WHERE)
        let needs_edge_access =
            edge.variable.is_some() || !edge.properties.is_empty() || edge.where_clause.is_some();

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
    /// When an edge has a variable, properties, or inline WHERE, we need to:
    /// 1. Navigate to the edge (out_e/in_e/both_e)
    /// 2. Apply edge property filters
    /// 3. Apply inline WHERE filter if present
    /// 4. Bind the edge variable with as_() if present
    /// 5. Navigate to the target vertex (in_v/out_v/other_v)
    fn compile_edge_with_variable(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
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

        // Step 3: Apply inline WHERE filter if present
        if let Some(where_expr) = &edge.where_clause {
            let expr = where_expr.clone();
            let params = self.parameters.clone();
            traversal = traversal
                .filter(move |ctx, val| eval_inline_predicate(ctx.storage(), &expr, val, &params));
        }

        // Step 4: Register and bind edge variable
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

        // Step 5: Navigate to target vertex
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
                    __.out()
                } else {
                    __.out_labels(&label_refs)
                }
            }
            EdgeDirection::Incoming => {
                if label_refs.is_empty() {
                    __.in_()
                } else {
                    __.in_labels(&label_refs)
                }
            }
            EdgeDirection::Both => {
                if label_refs.is_empty() {
                    __.both()
                } else {
                    __.both_labels(&label_refs)
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
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<BoundTraversal<'a, (), Value>, CompileError> {
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
        having_clause: &Option<HavingClause>,
        traversal: BoundTraversal<'a, (), Value>,
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
            return self.execute_aggregated_return(
                return_clause,
                where_clause,
                having_clause,
                traversal,
            );
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
        traversal: BoundTraversal<'a, (), Value>,
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

    /// Execute a query with OPTIONAL MATCH clauses.
    ///
    /// For each result from the main MATCH, we try to execute each OPTIONAL MATCH.
    /// If the OPTIONAL MATCH succeeds, we merge the results. If it fails, we
    /// add null values for the variables introduced in that OPTIONAL MATCH.
    fn execute_with_optional_match(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        let_clauses: &[LetClause],
        optional_match_clauses: &[OptionalMatchClause],
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        use crate::traversal::Traverser;

        // Execute the main MATCH and collect traversers
        let base_traversers: Vec<Traverser> = traversal.execute().collect();

        // For each base traverser, try to execute each OPTIONAL MATCH
        let mut expanded_traversers: Vec<Traverser> = Vec::new();

        for base_traverser in base_traversers {
            // Start with the base traverser's path
            let mut current_traversers = vec![base_traverser];

            // Process each OPTIONAL MATCH clause
            for opt_clause in optional_match_clauses {
                let mut next_traversers = Vec::new();

                for traverser in current_traversers {
                    // Try to execute the optional pattern from the current state
                    let optional_results = self.try_optional_match(opt_clause, &traverser)?;

                    if optional_results.is_empty() {
                        // No match - add null values for optional variables and keep the row
                        let mut updated_traverser = traverser.clone();
                        self.add_null_optional_vars(&mut updated_traverser.path, opt_clause);
                        next_traversers.push(updated_traverser);
                    } else {
                        // Matches found - create expanded rows
                        for opt_result in optional_results {
                            let mut merged_traverser = traverser.clone();
                            // Merge the optional match path into the base path
                            self.merge_paths(&mut merged_traverser.path, &opt_result.path);
                            // Update the current value if the optional match produced one
                            merged_traverser.value = opt_result.value;
                            next_traversers.push(merged_traverser);
                        }
                    }
                }

                current_traversers = next_traversers;
            }

            expanded_traversers.extend(current_traversers);
        }

        // Apply WHERE filter if present
        let filtered_traversers: Vec<Traverser> = if let Some(where_cl) = where_clause {
            expanded_traversers
                .into_iter()
                .filter(|t| self.evaluate_predicate_from_path(&where_cl.expression, t))
                .collect()
        } else {
            expanded_traversers
        };

        // If we have LET clauses, convert to row-based processing
        if !let_clauses.is_empty() {
            // Convert traversers to rows
            let current_rows: Vec<HashMap<String, Value>> = filtered_traversers
                .into_iter()
                .map(|t| {
                    let mut row = HashMap::new();
                    // Copy bound variables from path to row
                    for label in t.path.all_labels() {
                        if let Some(values) = t.path.get(label) {
                            if let Some(path_value) = values.last() {
                                row.insert(label.clone(), path_value.to_value());
                            }
                        }
                    }
                    // Store the full path as __path__ for path() function
                    let path_values: Vec<Value> =
                        t.path.objects().map(|pv| pv.to_value()).collect();
                    row.insert("__path__".to_string(), Value::List(path_values));
                    row.insert("__current__".to_string(), t.value);
                    row
                })
                .collect();

            // Apply LET clauses
            let rows_with_let = self.apply_let_clauses(current_rows, let_clauses);

            // Process RETURN clause
            let results: Vec<Value> = rows_with_let
                .into_iter()
                .filter_map(|row| self.evaluate_return_for_row(&return_clause.items, &row))
                .collect();

            // Apply DISTINCT if requested
            let results = if return_clause.distinct {
                self.deduplicate_results(results)
            } else {
                results
            };

            return Ok(results);
        }

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

    /// Try to execute an OPTIONAL MATCH pattern from a base traverser.
    ///
    /// Returns the matching traversers, or an empty vec if no matches.
    fn try_optional_match(
        &self,
        opt_clause: &OptionalMatchClause,
        base_traverser: &crate::traversal::Traverser,
    ) -> Result<Vec<crate::traversal::Traverser>, CompileError> {
        use crate::traversal::Traverser;

        // For now, we only support the first pattern in the optional match
        if opt_clause.patterns.is_empty() {
            return Ok(vec![]);
        }
        let pattern = &opt_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Ok(vec![]);
        }

        // The first node in the pattern should reference a variable from the base match
        // Get the anchor variable and look it up in the base traverser's path
        let anchor_var = match &pattern.elements[0] {
            PatternElement::Node(node) => node.variable.as_ref(),
            PatternElement::Edge(_) => None,
        };

        let anchor_vertex_id = if let Some(var) = anchor_var {
            // Look up the variable in the base traverser's path
            if let Some(values) = base_traverser.path.get(var) {
                if let Some(path_value) = values.last() {
                    match path_value.to_value() {
                        Value::Vertex(vid) => Some(vid),
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                // Variable not in path - might be the current value
                match &base_traverser.value {
                    Value::Vertex(vid) => Some(*vid),
                    _ => None,
                }
            }
        } else {
            // No anchor variable - use current value
            match &base_traverser.value {
                Value::Vertex(vid) => Some(*vid),
                _ => None,
            }
        };

        let anchor_id = match anchor_vertex_id {
            Some(id) => id,
            None => return Ok(vec![]), // Can't anchor the optional match
        };

        // Start a new traversal from the anchor vertex
        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let mut traversal = g.v_ids([anchor_id]).with_path();

        // Apply the pattern starting from the first element
        // Skip the first node if it's just the anchor reference
        let mut skip_first_node = anchor_var.is_some();

        for element in &pattern.elements {
            match element {
                PatternElement::Node(node) => {
                    if skip_first_node {
                        // Still need to apply label and property filters to anchor
                        if !node.labels.is_empty() {
                            let labels: Vec<&str> =
                                node.labels.iter().map(|s| s.as_str()).collect();
                            traversal = traversal.has_label_any(labels);
                        }
                        for (key, value) in &node.properties {
                            let val: Value = value.clone().into();
                            traversal = traversal.has_value(key.as_str(), val);
                        }
                        // Add as_() step for the anchor if it has a variable
                        if let Some(var) = &node.variable {
                            traversal = traversal.as_(var);
                        }
                        skip_first_node = false;
                    } else {
                        // Apply label filter
                        if !node.labels.is_empty() {
                            let labels: Vec<&str> =
                                node.labels.iter().map(|s| s.as_str()).collect();
                            traversal = traversal.has_label_any(labels);
                        }
                        // Apply property filters
                        for (key, value) in &node.properties {
                            let val: Value = value.clone().into();
                            traversal = traversal.has_value(key.as_str(), val);
                        }
                        // Add as_() step for variable binding
                        if let Some(var) = &node.variable {
                            traversal = traversal.as_(var);
                        }
                    }
                }
                PatternElement::Edge(edge) => {
                    // Navigate based on direction and labels
                    let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();
                    traversal = match edge.direction {
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
                }
            }
        }

        // Execute and collect the optional match results
        let results: Vec<Traverser> = traversal.execute().collect();
        Ok(results)
    }

    /// Add null values for all variables in an OPTIONAL MATCH clause to a path.
    fn add_null_optional_vars(
        &self,
        path: &mut crate::traversal::Path,
        opt_clause: &OptionalMatchClause,
    ) {
        use crate::traversal::PathValue;

        for pattern in &opt_clause.patterns {
            for element in &pattern.elements {
                match element {
                    PatternElement::Node(node) => {
                        if let Some(var) = &node.variable {
                            // Only add if not already present (don't overwrite anchor vars)
                            if path.get(var).is_none() {
                                path.push_labeled(PathValue::Property(Value::Null), var);
                            }
                        }
                    }
                    PatternElement::Edge(edge) => {
                        if let Some(var) = &edge.variable {
                            if path.get(var).is_none() {
                                path.push_labeled(PathValue::Property(Value::Null), var);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Merge paths from an optional match into the base path.
    fn merge_paths(
        &self,
        base_path: &mut crate::traversal::Path,
        optional_path: &crate::traversal::Path,
    ) {
        // Get all labeled values from the optional path and add them to base
        for label in optional_path.all_labels() {
            if let Some(values) = optional_path.get(label) {
                for value in values {
                    base_path.push_labeled(value.clone(), label);
                }
            }
        }
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
            Expression::Exists {
                pattern,
                negated,
                where_expr,
            } => {
                // For EXISTS in multi-var context, use the current element
                let exists = self.evaluate_exists_pattern_with_where(
                    pattern,
                    where_expr.as_deref(),
                    &traverser.value,
                );
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
            Expression::Parameter(name) => {
                // Resolve parameter value
                self.parameters.get(name).cloned().unwrap_or(Value::Null)
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
            Expression::Map(entries) => {
                let map: ValueMap = entries
                    .iter()
                    .map(|(key, value_expr)| {
                        let value = self.evaluate_value_from_path(value_expr, traverser);
                        (key.clone(), value)
                    })
                    .collect();
                Value::Map(map)
            }
            Expression::Exists {
                pattern,
                negated,
                where_expr,
            } => {
                let exists = self.evaluate_exists_pattern_with_where(
                    pattern,
                    where_expr.as_deref(),
                    &traverser.value,
                );
                Value::Bool(if *negated { !exists } else { exists })
            }
            Expression::FunctionCall { name, args } => {
                self.evaluate_function_call_from_path(name, args, traverser)
            }
            Expression::Case(case_expr) => self.evaluate_case_from_path(case_expr, traverser),
            Expression::Reduce {
                accumulator,
                initial,
                variable,
                list,
                expression,
            } => self.evaluate_reduce_from_path(
                accumulator,
                initial,
                variable,
                list,
                expression,
                traverser,
            ),
            Expression::ListComprehension {
                variable,
                list,
                filter,
                transform,
            } => self.evaluate_list_comprehension_from_path(
                variable, list, filter, transform, traverser,
            ),
            Expression::All {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_path(
                ListPredicateKind::All,
                variable,
                list,
                condition,
                traverser,
            ),
            Expression::Any {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_path(
                ListPredicateKind::Any,
                variable,
                list,
                condition,
                traverser,
            ),
            Expression::None {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_path(
                ListPredicateKind::None,
                variable,
                list,
                condition,
                traverser,
            ),
            Expression::Single {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate_from_path(
                ListPredicateKind::Single,
                variable,
                list,
                condition,
                traverser,
            ),
            _ => Value::Null,
        }
    }

    /// Evaluate a REDUCE expression using path-based variable lookup.
    fn evaluate_reduce_from_path(
        &self,
        accumulator: &str,
        initial: &Expression,
        variable: &str,
        list_expr: &Expression,
        expression: &Expression,
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        // Evaluate the initial value for the accumulator
        let mut acc_value = self.evaluate_value_from_path(initial, traverser);

        // Evaluate the list expression
        let list_value = self.evaluate_value_from_path(list_expr, traverser);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Iterate and accumulate
        for item in items {
            // Create a temporary map with both accumulator and loop variable bound
            let mut iter_map = std::collections::HashMap::new();
            iter_map.insert(accumulator.to_string(), acc_value);
            iter_map.insert(variable.to_string(), item);

            // Evaluate the expression to get the next accumulator value
            acc_value = self.evaluate_expression_from_row(expression, &iter_map);
        }

        acc_value
    }

    /// Evaluate a list predicate (ALL/ANY/NONE/SINGLE) using path-based variable lookup.
    fn evaluate_list_predicate_from_path(
        &self,
        kind: ListPredicateKind,
        variable: &str,
        list_expr: &Expression,
        condition: &Expression,
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_value_from_path(list_expr, traverser);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Count matching elements
        let mut match_count = 0;

        for item in items {
            let mut iter_map = std::collections::HashMap::new();
            iter_map.insert(variable.to_string(), item);

            // Evaluate condition
            if self.evaluate_predicate_from_row(condition, &iter_map) {
                match_count += 1;

                // Early exit optimizations
                if kind == ListPredicateKind::Any {
                    return Value::Bool(true);
                }
                if kind == ListPredicateKind::None {
                    return Value::Bool(false);
                }
                if kind == ListPredicateKind::Single && match_count > 1 {
                    return Value::Bool(false);
                }
            } else if kind == ListPredicateKind::All {
                return Value::Bool(false);
            }
        }

        // Return result based on predicate kind
        Value::Bool(match kind {
            ListPredicateKind::All => true,
            ListPredicateKind::Any => false,
            ListPredicateKind::None => true,
            ListPredicateKind::Single => match_count == 1,
        })
    }

    /// Evaluate a list comprehension using path-based variable lookup.
    fn evaluate_list_comprehension_from_path(
        &self,
        variable: &str,
        list_expr: &Expression,
        filter: &Option<Box<Expression>>,
        transform: &Expression,
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_value_from_path(list_expr, traverser);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Process each item
        let mut results = Vec::new();
        for item in items {
            // Create a temporary map with the loop variable bound
            let mut iter_map = std::collections::HashMap::new();
            iter_map.insert(variable.to_string(), item);

            // Apply filter if present
            if let Some(filter_expr) = filter {
                if !self.evaluate_predicate_from_row(filter_expr, &iter_map) {
                    continue;
                }
            }

            // Apply transform
            let transformed = self.evaluate_expression_from_row(transform, &iter_map);
            results.push(transformed);
        }

        Value::List(results)
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

            // Extended math functions (Phase 2: Math-GQL Integration)

            // Square root
            "SQRT" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) if n >= 0 => Value::Float((n as f64).sqrt()),
                        Value::Float(f) if f >= 0.0 => Value::Float(f.sqrt()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Power function (alternative to ^ operator)
            "POW" | "POWER" => {
                if args.len() >= 2 {
                    let base = self.evaluate_value_from_path(&args[0], traverser);
                    let exp = self.evaluate_value_from_path(&args[1], traverser);
                    apply_binary_op(BinaryOperator::Pow, base, exp)
                } else {
                    Value::Null
                }
            }

            // Natural logarithm
            "LOG" | "LN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) if n > 0 => Value::Float((n as f64).ln()),
                        Value::Float(f) if f > 0.0 => Value::Float(f.ln()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Log base 10
            "LOG10" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) if n > 0 => Value::Float((n as f64).log10()),
                        Value::Float(f) if f > 0.0 => Value::Float(f.log10()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Exponential (e^x)
            "EXP" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).exp()),
                        Value::Float(f) => Value::Float(f.exp()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Trigonometric functions (input in radians)
            "SIN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).sin()),
                        Value::Float(f) => Value::Float(f.sin()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "COS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).cos()),
                        Value::Float(f) => Value::Float(f.cos()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "TAN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).tan()),
                        Value::Float(f) => Value::Float(f.tan()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Inverse trigonometric functions
            "ASIN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => {
                            let f = n as f64;
                            if (-1.0..=1.0).contains(&f) {
                                Value::Float(f.asin())
                            } else {
                                Value::Null
                            }
                        }
                        Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.asin()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "ACOS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => {
                            let f = n as f64;
                            if (-1.0..=1.0).contains(&f) {
                                Value::Float(f.acos())
                            } else {
                                Value::Null
                            }
                        }
                        Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.acos()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "ATAN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).atan()),
                        Value::Float(f) => Value::Float(f.atan()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Two-argument arctangent (atan2)
            "ATAN2" => {
                if args.len() >= 2 {
                    let y = self.evaluate_value_from_path(&args[0], traverser);
                    let x = self.evaluate_value_from_path(&args[1], traverser);
                    match (y, x) {
                        (Value::Int(y), Value::Int(x)) => Value::Float((y as f64).atan2(x as f64)),
                        (Value::Float(y), Value::Float(x)) => Value::Float(y.atan2(x)),
                        (Value::Int(y), Value::Float(x)) => Value::Float((y as f64).atan2(x)),
                        (Value::Float(y), Value::Int(x)) => Value::Float(y.atan2(x as f64)),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Degree/radian conversion
            "RADIANS" | "TORADIANS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).to_radians()),
                        Value::Float(f) => Value::Float(f.to_radians()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "DEGREES" | "TODEGREES" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value_from_path(arg, traverser) {
                        Value::Int(n) => Value::Float((n as f64).to_degrees()),
                        Value::Float(f) => Value::Float(f.to_degrees()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Mathematical constants
            "PI" => Value::Float(std::f64::consts::PI),
            "E" => Value::Float(std::f64::consts::E),

            // MATH() function - evaluate mathexpr expressions
            "MATH" => self.evaluate_math_from_path(args, traverser),

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

            // Path function - returns the full traversal path as a list
            "PATH" => {
                // Convert all path objects to a Value::List
                let path_values: Vec<Value> =
                    traverser.path.objects().map(|pv| pv.to_value()).collect();
                Value::List(path_values)
            }

            // Introspection functions
            "PROPERTIES" => {
                // Return all properties of a vertex or edge as a map
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value_from_path(arg, traverser);
                    match element_val {
                        Value::Vertex(vid) => {
                            if let Some(vertex) = self.snapshot.storage().get_vertex(vid) {
                                return Value::Map(vertex.properties.into_value_map());
                            }
                        }
                        Value::Edge(eid) => {
                            if let Some(edge) = self.snapshot.storage().get_edge(eid) {
                                return Value::Map(edge.properties.into_value_map());
                            }
                        }
                        _ => {}
                    }
                }
                Value::Null
            }
            "LABELS" => {
                // Return vertex label(s) as a list
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value_from_path(arg, traverser);
                    if let Value::Vertex(vid) = element_val {
                        if let Some(vertex) = self.snapshot.storage().get_vertex(vid) {
                            return Value::List(vec![Value::String(vertex.label)]);
                        }
                    }
                }
                Value::Null
            }
            "TYPE" => {
                // Return edge type/label as a string
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value_from_path(arg, traverser);
                    if let Value::Edge(eid) = element_val {
                        if let Some(edge) = self.snapshot.storage().get_edge(eid) {
                            return Value::String(edge.label);
                        }
                    }
                }
                Value::Null
            }
            "ID" => {
                // Return internal ID of a vertex or edge
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value_from_path(arg, traverser);
                    match element_val {
                        Value::Vertex(vid) => return id_to_value(vid.0),
                        Value::Edge(eid) => return id_to_value(eid.0),
                        _ => {}
                    }
                }
                Value::Null
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
            let mut map = ValueMap::new();
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
            let mut map = ValueMap::new();
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
    ///
    /// Returns `Some(Value::Null)` for null elements (e.g., from OPTIONAL MATCH that didn't match).
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
            Value::Map(map) => {
                // Support property access on map values (e.g., in list comprehensions)
                Some(map.get(property).cloned().unwrap_or(Value::Null))
            }
            Value::Null => Some(Value::Null), // OPTIONAL MATCH variable that didn't match
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
            Expression::Exists {
                pattern,
                negated,
                where_expr,
            } => {
                let exists = self.evaluate_exists_pattern_with_where(
                    pattern,
                    where_expr.as_deref(),
                    element,
                );
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
            Expression::Parameter(name) => {
                // Resolve parameter value
                self.parameters.get(name).cloned().unwrap_or(Value::Null)
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
            Expression::Map(entries) => {
                let map: ValueMap = entries
                    .iter()
                    .map(|(key, value_expr)| {
                        let value = self.evaluate_value(value_expr, element);
                        (key.clone(), value)
                    })
                    .collect();
                Value::Map(map)
            }
            Expression::Exists {
                pattern,
                negated,
                where_expr,
            } => {
                let exists = self.evaluate_exists_pattern_with_where(
                    pattern,
                    where_expr.as_deref(),
                    element,
                );
                Value::Bool(if *negated { !exists } else { exists })
            }
            Expression::FunctionCall { name, args } => {
                self.evaluate_function_call(name, args, element)
            }
            Expression::Case(case_expr) => self.evaluate_case(case_expr, element),
            Expression::Reduce {
                accumulator,
                initial,
                variable,
                list,
                expression,
            } => self.evaluate_reduce(accumulator, initial, variable, list, expression, element),
            Expression::ListComprehension {
                variable,
                list,
                filter,
                transform,
            } => self.evaluate_list_comprehension(variable, list, filter, transform, element),
            Expression::All {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate(
                ListPredicateKind::All,
                variable,
                list,
                condition,
                element,
            ),
            Expression::Any {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate(
                ListPredicateKind::Any,
                variable,
                list,
                condition,
                element,
            ),
            Expression::None {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate(
                ListPredicateKind::None,
                variable,
                list,
                condition,
                element,
            ),
            Expression::Single {
                variable,
                list,
                condition,
            } => self.evaluate_list_predicate(
                ListPredicateKind::Single,
                variable,
                list,
                condition,
                element,
            ),
            Expression::Index { list, index } => self.evaluate_index(list, index, element),
            Expression::Slice { list, start, end } => {
                self.evaluate_slice(list, start.as_deref(), end.as_deref(), element)
            }
            Expression::PatternComprehension {
                pattern,
                filter,
                transform,
            } => {
                // For pattern comprehension in single-variable context,
                // we need to build a row with the element bound to the start variable
                if let Some(PatternElement::Node(node)) = pattern.elements.first() {
                    if let Some(var) = &node.variable {
                        let mut row = HashMap::new();
                        row.insert(var.clone(), element.clone());
                        self.evaluate_pattern_comprehension_from_row(
                            pattern,
                            filter.as_deref(),
                            transform,
                            &row,
                        )
                    } else {
                        Value::List(vec![])
                    }
                } else {
                    Value::List(vec![])
                }
            }
            _ => Value::Null, // Unsupported expressions
        }
    }

    /// Evaluate a REDUCE expression against an element.
    ///
    /// REDUCE(accumulator = initial, variable IN list | expression)
    fn evaluate_reduce(
        &self,
        accumulator: &str,
        initial: &Expression,
        variable: &str,
        list_expr: &Expression,
        expression: &Expression,
        element: &Value,
    ) -> Value {
        // Evaluate the initial value for the accumulator
        let mut acc_value = self.evaluate_value(initial, element);

        // Evaluate the list expression
        let list_value = self.evaluate_value(list_expr, element);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Iterate and accumulate
        for item in items {
            // Create a temporary map with both accumulator and loop variable bound
            let mut iter_map = std::collections::HashMap::new();
            iter_map.insert(accumulator.to_string(), acc_value);
            iter_map.insert(variable.to_string(), item);

            // Evaluate the expression to get the next accumulator value
            // Use evaluate_expression_from_row since we have local bindings
            acc_value = self.evaluate_expression_from_row(expression, &iter_map);
        }

        acc_value
    }

    /// Evaluate a list predicate (ALL/ANY/NONE/SINGLE) against an element.
    fn evaluate_list_predicate(
        &self,
        kind: ListPredicateKind,
        variable: &str,
        list_expr: &Expression,
        condition: &Expression,
        element: &Value,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_value(list_expr, element);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Count matching elements
        let mut match_count = 0;

        for item in items {
            let mut iter_map = std::collections::HashMap::new();
            iter_map.insert(variable.to_string(), item);

            // Add bound variables from the element context
            // In single-variable queries, all bindings refer to the same element
            for bound_var in self.bindings.keys() {
                iter_map.insert(bound_var.clone(), element.clone());
            }

            // Evaluate condition
            if self.evaluate_predicate_from_row(condition, &iter_map) {
                match_count += 1;

                // Early exit optimizations
                if kind == ListPredicateKind::Any {
                    return Value::Bool(true);
                }
                if kind == ListPredicateKind::None {
                    return Value::Bool(false);
                }
                if kind == ListPredicateKind::Single && match_count > 1 {
                    return Value::Bool(false);
                }
            } else if kind == ListPredicateKind::All {
                return Value::Bool(false);
            }
        }

        // Return result based on predicate kind
        Value::Bool(match kind {
            ListPredicateKind::All => true,
            ListPredicateKind::Any => false,
            ListPredicateKind::None => true,
            ListPredicateKind::Single => match_count == 1,
        })
    }

    /// Evaluate a list comprehension against an element.
    ///
    /// [transform(variable) FOR variable IN list WHERE filter(variable)]
    fn evaluate_list_comprehension(
        &self,
        variable: &str,
        list_expr: &Expression,
        filter: &Option<Box<Expression>>,
        transform: &Expression,
        element: &Value,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_value(list_expr, element);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Process each item
        let mut results = Vec::new();
        for item in items {
            // Create a temporary map with the loop variable bound
            let mut iter_map = std::collections::HashMap::new();
            iter_map.insert(variable.to_string(), item);

            // Apply filter if present
            if let Some(filter_expr) = filter {
                if !self.evaluate_predicate_from_row(filter_expr, &iter_map) {
                    continue;
                }
            }

            // Apply transform
            let transformed = self.evaluate_expression_from_row(transform, &iter_map);
            results.push(transformed);
        }

        Value::List(results)
    }

    /// Evaluate an index access expression: `list[index]`
    ///
    /// Accesses a single element from a list by index.
    /// - Indices are 0-based.
    /// - Negative indices count from the end: `-1` is last, `-2` is second-to-last, etc.
    /// - Out-of-bounds indices return NULL.
    /// - Non-list inputs return NULL.
    /// - Non-integer indices return NULL.
    ///
    /// # Examples
    ///
    /// ```text
    /// [1, 2, 3][0]  => 1
    /// [1, 2, 3][-1] => 3
    /// [1, 2, 3][5]  => NULL
    /// ```
    fn evaluate_index(
        &self,
        list_expr: &Expression,
        index_expr: &Expression,
        element: &Value,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_value(list_expr, element);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        // Evaluate the index expression
        let index_value = self.evaluate_value(index_expr, element);

        // Index must be an integer
        let index = match index_value {
            Value::Int(i) => i,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        let len = items.len() as i64;

        // Handle negative indices (count from end)
        let resolved_index = if index < 0 {
            len + index // -1 becomes len-1, etc.
        } else {
            index
        };

        // Bounds check
        if resolved_index < 0 || resolved_index >= len {
            return Value::Null;
        }

        items
            .into_iter()
            .nth(resolved_index as usize)
            .unwrap_or(Value::Null)
    }

    /// Evaluate a slice access expression: `list[start..end]`
    ///
    /// Extracts a sublist from start (inclusive) to end (exclusive).
    /// - Omitted bounds default to start of list or end of list.
    /// - Negative indices are supported.
    /// - Out-of-bounds indices are clamped (no error).
    /// - Non-list inputs return NULL.
    /// - Non-integer bounds return NULL.
    ///
    /// # Examples
    ///
    /// ```text
    /// [1, 2, 3, 4][1..3]  => [2, 3]
    /// [1, 2, 3, 4][..2]   => [1, 2]
    /// [1, 2, 3, 4][2..]   => [3, 4]
    /// [1, 2, 3, 4][-2..]  => [3, 4]
    /// [1, 2, 3, 4][..-1]  => [1, 2, 3]
    /// [1, 2, 3, 4][10..20] => []
    /// ```
    fn evaluate_slice(
        &self,
        list_expr: &Expression,
        start_expr: Option<&Expression>,
        end_expr: Option<&Expression>,
        element: &Value,
    ) -> Value {
        // Evaluate the list expression
        let list_value = self.evaluate_value(list_expr, element);

        // Handle non-list inputs
        let items = match list_value {
            Value::List(items) => items,
            Value::Null => return Value::Null,
            _ => return Value::Null,
        };

        let len = items.len() as i64;

        // Evaluate start bound (default to 0)
        let start = if let Some(expr) = start_expr {
            match self.evaluate_value(expr, element) {
                Value::Int(i) => i,
                Value::Null => return Value::Null,
                _ => return Value::Null,
            }
        } else {
            0
        };

        // Evaluate end bound (default to len)
        let end = if let Some(expr) = end_expr {
            match self.evaluate_value(expr, element) {
                Value::Int(i) => i,
                Value::Null => return Value::Null,
                _ => return Value::Null,
            }
        } else {
            len
        };

        // Resolve negative indices
        let resolved_start = if start < 0 { len + start } else { start };
        let resolved_end = if end < 0 { len + end } else { end };

        // Clamp to bounds
        let clamped_start = resolved_start.clamp(0, len) as usize;
        let clamped_end = resolved_end.clamp(0, len) as usize;

        // Handle invalid range (start > end after clamping)
        if clamped_start >= clamped_end {
            return Value::List(vec![]);
        }

        // Extract the slice
        let slice: Vec<Value> = items
            .into_iter()
            .skip(clamped_start)
            .take(clamped_end - clamped_start)
            .collect();

        Value::List(slice)
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
    /// - `PROPERTIES(node|edge)` - returns all properties as a map
    /// - `LABELS(node)` - returns vertex label(s) as a list
    /// - `TYPE(edge)` - returns edge type/label as a string
    /// - `ID(node|edge)` - returns internal element ID
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

            // Extended math functions (Phase 2: Math-GQL Integration)

            // Square root
            "SQRT" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) if n >= 0 => Value::Float((n as f64).sqrt()),
                        Value::Float(f) if f >= 0.0 => Value::Float(f.sqrt()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Power function (alternative to ^ operator)
            "POW" | "POWER" => {
                if args.len() >= 2 {
                    let base = self.evaluate_value(&args[0], element);
                    let exp = self.evaluate_value(&args[1], element);
                    apply_binary_op(BinaryOperator::Pow, base, exp)
                } else {
                    Value::Null
                }
            }

            // Natural logarithm
            "LOG" | "LN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) if n > 0 => Value::Float((n as f64).ln()),
                        Value::Float(f) if f > 0.0 => Value::Float(f.ln()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Log base 10
            "LOG10" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) if n > 0 => Value::Float((n as f64).log10()),
                        Value::Float(f) if f > 0.0 => Value::Float(f.log10()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Exponential (e^x)
            "EXP" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).exp()),
                        Value::Float(f) => Value::Float(f.exp()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Trigonometric functions (input in radians)
            "SIN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).sin()),
                        Value::Float(f) => Value::Float(f.sin()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "COS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).cos()),
                        Value::Float(f) => Value::Float(f.cos()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "TAN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).tan()),
                        Value::Float(f) => Value::Float(f.tan()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Inverse trigonometric functions
            "ASIN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => {
                            let f = n as f64;
                            if (-1.0..=1.0).contains(&f) {
                                Value::Float(f.asin())
                            } else {
                                Value::Null
                            }
                        }
                        Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.asin()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "ACOS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => {
                            let f = n as f64;
                            if (-1.0..=1.0).contains(&f) {
                                Value::Float(f.acos())
                            } else {
                                Value::Null
                            }
                        }
                        Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.acos()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "ATAN" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).atan()),
                        Value::Float(f) => Value::Float(f.atan()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Two-argument arctangent (atan2)
            "ATAN2" => {
                if args.len() >= 2 {
                    let y = self.evaluate_value(&args[0], element);
                    let x = self.evaluate_value(&args[1], element);
                    match (y, x) {
                        (Value::Int(y), Value::Int(x)) => Value::Float((y as f64).atan2(x as f64)),
                        (Value::Float(y), Value::Float(x)) => Value::Float(y.atan2(x)),
                        (Value::Int(y), Value::Float(x)) => Value::Float((y as f64).atan2(x)),
                        (Value::Float(y), Value::Int(x)) => Value::Float(y.atan2(x as f64)),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Degree/radian conversion
            "RADIANS" | "TORADIANS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).to_radians()),
                        Value::Float(f) => Value::Float(f.to_radians()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            "DEGREES" | "TODEGREES" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Float((n as f64).to_degrees()),
                        Value::Float(f) => Value::Float(f.to_degrees()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }

            // Mathematical constants
            "PI" => Value::Float(std::f64::consts::PI),
            "E" => Value::Float(std::f64::consts::E),

            // MATH() function - evaluate mathexpr expressions
            "MATH" => self.evaluate_math(args, element),

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

            // Introspection functions
            "PROPERTIES" => {
                // Return all properties of a vertex or edge as a map
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value(arg, element);
                    match element_val {
                        Value::Vertex(vid) => {
                            if let Some(vertex) = self.snapshot.storage().get_vertex(vid) {
                                return Value::Map(vertex.properties.into_value_map());
                            }
                        }
                        Value::Edge(eid) => {
                            if let Some(edge) = self.snapshot.storage().get_edge(eid) {
                                return Value::Map(edge.properties.into_value_map());
                            }
                        }
                        _ => {}
                    }
                }
                Value::Null
            }
            "LABELS" => {
                // Return vertex label(s) as a list
                // Note: In this implementation vertices have a single label
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value(arg, element);
                    if let Value::Vertex(vid) = element_val {
                        if let Some(vertex) = self.snapshot.storage().get_vertex(vid) {
                            return Value::List(vec![Value::String(vertex.label)]);
                        }
                    }
                }
                Value::Null
            }
            "TYPE" => {
                // Return edge type/label as a string
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value(arg, element);
                    if let Value::Edge(eid) = element_val {
                        if let Some(edge) = self.snapshot.storage().get_edge(eid) {
                            return Value::String(edge.label);
                        }
                    }
                }
                Value::Null
            }
            "ID" => {
                // Return internal ID of a vertex or edge
                if let Some(arg) = args.first() {
                    let element_val = self.evaluate_value(arg, element);
                    match element_val {
                        Value::Vertex(vid) => return id_to_value(vid.0),
                        Value::Edge(eid) => return id_to_value(eid.0),
                        _ => {}
                    }
                }
                Value::Null
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

    /// Evaluate an EXISTS expression with an optional WHERE filter inside the
    /// subquery, e.g. `EXISTS { MATCH (p)-[:KNOWS]->(x) WHERE x.age > 30 }`.
    ///
    /// `EXISTS { (p)-[:KNOWS]->(friend) }` (no WHERE) checks if there's at
    /// least one path matching the pattern starting from the current element.
    ///
    /// The first node in the pattern is the "anchor" - it should match the
    /// current element. Subsequent edges and nodes form the pattern to check.
    ///
    /// When `where_expr` is `Some`, path tracking is enabled and each pattern
    /// variable is bound via `as_(varname)` so the predicate can reference
    /// variables introduced inside the subquery.
    fn evaluate_exists_pattern_with_where(
        &self,
        pattern: &Pattern,
        where_expr: Option<&Expression>,
        element: &Value,
    ) -> bool {
        // Get vertex ID from element - EXISTS only makes sense for vertices
        let vid = match element {
            Value::Vertex(id) => *id,
            _ => return false,
        };

        // Start traversal from this specific vertex
        let g = crate::traversal::GraphTraversalSource::from_snapshot(self.snapshot);
        let mut traversal = g.v_ids([vid]);

        // When a WHERE clause is present we need to track paths so we can look
        // up variable bindings (the pattern variables) when evaluating it.
        if where_expr.is_some() {
            traversal = traversal.with_path();
        }

        // Process the pattern elements
        // The first node is the anchor (current element) - apply its filters
        // Subsequent edges navigate, and subsequent nodes filter
        for elem in &pattern.elements {
            match elem {
                PatternElement::Node(node) => {
                    traversal = self.apply_node_filters(node, traversal);
                    if where_expr.is_some() {
                        if let Some(var) = &node.variable {
                            traversal = traversal.as_(var.as_str());
                        }
                    }
                }
                PatternElement::Edge(edge) => {
                    // Navigate along the edge
                    traversal = self.apply_edge_navigation(edge, traversal);
                    if where_expr.is_some() {
                        if let Some(var) = &edge.variable {
                            traversal = traversal.as_(var.as_str());
                        }
                    }
                }
            }
        }

        // Fast path: no WHERE clause, just check if any results exist
        let Some(predicate) = where_expr else {
            return !traversal.to_list().is_empty();
        };

        // Iterate matched paths and look for one that satisfies the predicate.
        for traverser in traversal.traversers() {
            if self.evaluate_predicate_from_path(predicate, &traverser) {
                return true;
            }
        }
        false
    }

    /// Apply node filters (labels and properties) to a traversal.
    fn apply_node_filters(
        &self,
        node: &NodePattern,
        mut traversal: BoundTraversal<'a, (), Value>,
    ) -> BoundTraversal<'a, (), Value> {
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
        traversal: BoundTraversal<'a, (), Value>,
    ) -> BoundTraversal<'a, (), Value> {
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
            Expression::Map(entries) => {
                for (_, value) in entries {
                    self.validate_expression_variables(value)?;
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
            Expression::Parameter(name) => {
                // Parameters are resolved from the parameters map, not bindings.
                // Check if the parameter is bound at validation time.
                if !self.parameters.contains_key(name) {
                    return Err(CompileError::unbound_parameter(name));
                }
            }
            Expression::ListComprehension {
                variable,
                list,
                filter,
                transform,
            } => {
                // Validate the list expression (references outer scope)
                self.validate_expression_variables(list)?;
                // The variable is locally scoped to the comprehension, so filter and transform
                // may reference it. We can't easily check this without modifying our bindings
                // temporarily, so we just validate that any other variables in filter/transform
                // are bound. For simplicity, we validate as if the variable exists.
                // Note: A more thorough validation would temporarily add the variable to bindings.

                // Create a temporary validator context with the comprehension variable bound
                // For now, we skip deep validation of filter/transform since they reference
                // the locally-scoped comprehension variable. The evaluation will handle
                // any undefined variable errors at runtime.
                let _ = variable; // Acknowledge the local variable
                let _ = filter;
                let _ = transform;
            }
            Expression::Reduce {
                accumulator,
                initial,
                variable,
                list,
                expression,
            } => {
                // Validate the initial value and list expressions (reference outer scope)
                self.validate_expression_variables(initial)?;
                self.validate_expression_variables(list)?;
                // The accumulator and variable are locally scoped to the reduce expression,
                // so the expression body may reference them. Similar to ListComprehension,
                // we skip deep validation since the evaluation will handle any undefined
                // variable errors at runtime.
                let _ = accumulator;
                let _ = variable;
                let _ = expression;
            }
            Expression::All {
                variable,
                list,
                condition,
            }
            | Expression::Any {
                variable,
                list,
                condition,
            }
            | Expression::None {
                variable,
                list,
                condition,
            }
            | Expression::Single {
                variable,
                list,
                condition,
            } => {
                // Validate the list expression (references outer scope)
                self.validate_expression_variables(list)?;
                // The variable is locally scoped to the predicate, so condition may
                // reference it. Similar to ListComprehension, we skip deep validation
                // since the evaluation will handle any undefined variable errors at runtime.
                let _ = variable;
                let _ = condition;
            }
            Expression::Index { list, index } => {
                // Validate both the list and index expressions
                self.validate_expression_variables(list)?;
                self.validate_expression_variables(index)?;
            }
            Expression::Slice { list, start, end } => {
                // Validate the list expression
                self.validate_expression_variables(list)?;
                // Validate optional start and end expressions
                if let Some(s) = start {
                    self.validate_expression_variables(s)?;
                }
                if let Some(e) = end {
                    self.validate_expression_variables(e)?;
                }
            }
            Expression::PatternComprehension {
                pattern,
                filter,
                transform,
            } => {
                // Validate variables referenced in the pattern comprehension.
                // At least one node variable in the pattern should reference a bound variable
                // from the outer scope - this is how the pattern is correlated.
                // We check if the first node has a variable that exists in bindings.
                if let Some(PatternElement::Node(node)) = pattern.elements.first() {
                    if let Some(var) = &node.variable {
                        if !self.bindings.contains_key(var) {
                            return Err(CompileError::undefined_variable(var));
                        }
                    }
                }
                // Other variables in the pattern are local to the comprehension.
                // The filter and transform expressions can reference both outer variables
                // and local pattern variables - skip deep validation similar to ListComprehension.
                let _ = filter;
                let _ = transform;
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
            Expression::Map(entries) => entries.iter().any(|(_, v)| Self::expr_has_aggregate(v)),
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
            Expression::Index { list, index } => {
                Self::expr_has_aggregate(list) || Self::expr_has_aggregate(index)
            }
            Expression::Slice { list, start, end } => {
                Self::expr_has_aggregate(list)
                    || start
                        .as_ref()
                        .map(|s| Self::expr_has_aggregate(s))
                        .unwrap_or(false)
                    || end
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
    /// If `having_clause` is provided, filters resulting rows after aggregation.
    fn execute_aggregated_return(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        having_clause: &Option<HavingClause>,
        traversal: BoundTraversal<'a, (), Value>,
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

        let results = if group_by_items.is_empty() {
            // No grouping - aggregate over all results (global aggregates)
            self.execute_global_aggregates(
                return_clause,
                &aggregate_items,
                &filtered_elements,
                having_clause,
            )?
        } else {
            // Group by non-aggregate expressions, then aggregate per group
            self.execute_grouped_aggregates(
                return_clause,
                &group_by_items,
                &aggregate_items,
                &filtered_elements,
                having_clause,
            )?
        };

        Ok(results)
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
    /// HAVING count(*) > 5
    /// ```
    fn execute_group_by_query(
        &self,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        group_by: &GroupByClause,
        having_clause: &Option<HavingClause>,
        traversal: BoundTraversal<'a, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // For multi-variable patterns, we need to work with traversers to access paths
        if self.has_multi_vars {
            return self.execute_group_by_query_multi_var(
                return_clause,
                where_clause,
                group_by,
                having_clause,
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

        // For each group, compute the RETURN clause and apply HAVING filter
        let mut results = Vec::new();

        for (group_key, group_elements) in groups {
            let result =
                self.compute_group_result(return_clause, group_by, &group_key, &group_elements)?;

            // Apply HAVING filter if present
            if let Some(having) = having_clause {
                if !self.evaluate_having_predicate(
                    &having.expression,
                    return_clause,
                    group_by,
                    &group_key,
                    &group_elements,
                    &result,
                ) {
                    continue; // Skip this group
                }
            }

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
        having_clause: &Option<HavingClause>,
        traversal: BoundTraversal<'a, (), Value>,
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
            if !Self::expr_has_aggregate(&item.expression)
                && !self.expression_in_group_by(&item.expression, group_by)
            {
                let expr_str = self.expression_to_string(&item.expression);
                return Err(CompileError::expression_not_in_group_by(expr_str));
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

        // For each group, compute the RETURN clause and apply HAVING filter
        let mut results = Vec::new();

        for (group_key, group_traversers) in groups {
            let result = self.compute_group_result_multi_var(
                return_clause,
                group_by,
                &group_key,
                &group_traversers,
            )?;

            // Apply HAVING filter if present
            if let Some(having) = having_clause {
                if !self.evaluate_having_predicate_multi_var(
                    &having.expression,
                    return_clause,
                    group_by,
                    &group_key,
                    &group_traversers,
                    &result,
                ) {
                    continue; // Skip this group
                }
            }

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
                let mut map = ValueMap::new();
                map.insert(self.get_return_item_key(item), value);
                Ok(Value::Map(map))
            } else {
                Ok(value)
            }
        } else {
            // Multiple return items - return a map
            let mut map = ValueMap::new();

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
                let mut map = ValueMap::new();
                map.insert(self.get_return_item_key(item), value);
                Ok(Value::Map(map))
            } else {
                Ok(value)
            }
        } else {
            // Multiple return items - return a map
            let mut map = ValueMap::new();

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
                .ok_or(CompileError::EmptyPattern)
        }
    }

    /// Evaluate a HAVING predicate expression in the context of a group.
    ///
    /// HAVING predicates can reference:
    /// - Aliases defined in RETURN clause (looked up from result map)
    /// - Aggregate expressions (computed over the group)
    /// - GROUP BY expressions (extracted from group key)
    fn evaluate_having_predicate(
        &self,
        expr: &Expression,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_elements: &[Value],
        result_map: &Value,
    ) -> bool {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    // Logical operators
                    BinaryOperator::And => {
                        self.evaluate_having_predicate(
                            left,
                            return_clause,
                            group_by,
                            group_key,
                            group_elements,
                            result_map,
                        ) && self.evaluate_having_predicate(
                            right,
                            return_clause,
                            group_by,
                            group_key,
                            group_elements,
                            result_map,
                        )
                    }
                    BinaryOperator::Or => {
                        self.evaluate_having_predicate(
                            left,
                            return_clause,
                            group_by,
                            group_key,
                            group_elements,
                            result_map,
                        ) || self.evaluate_having_predicate(
                            right,
                            return_clause,
                            group_by,
                            group_key,
                            group_elements,
                            result_map,
                        )
                    }
                    // Comparison operators
                    _ => {
                        let left_val = self.evaluate_having_value(
                            left,
                            return_clause,
                            group_by,
                            group_key,
                            group_elements,
                            result_map,
                        );
                        let right_val = self.evaluate_having_value(
                            right,
                            return_clause,
                            group_by,
                            group_key,
                            group_elements,
                            result_map,
                        );
                        apply_comparison(*op, &left_val, &right_val)
                    }
                }
            }
            Expression::UnaryOp { op, expr: inner } => match op {
                UnaryOperator::Not => !self.evaluate_having_predicate(
                    inner,
                    return_clause,
                    group_by,
                    group_key,
                    group_elements,
                    result_map,
                ),
                UnaryOperator::Neg => {
                    let val = self.evaluate_having_value(
                        inner,
                        return_clause,
                        group_by,
                        group_key,
                        group_elements,
                        result_map,
                    );
                    match val {
                        Value::Int(n) => n == 0,
                        Value::Float(f) => f == 0.0,
                        Value::Bool(b) => !b,
                        Value::Null => true,
                        _ => false,
                    }
                }
            },
            // For other expressions, evaluate and check truthiness
            _ => {
                let val = self.evaluate_having_value(
                    expr,
                    return_clause,
                    group_by,
                    group_key,
                    group_elements,
                    result_map,
                );
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

    /// Evaluate a HAVING expression to a Value in the context of a group.
    ///
    /// Handles alias lookups, aggregate expressions, and GROUP BY expressions.
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_having_value(
        &self,
        expr: &Expression,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_elements: &[Value],
        result_map: &Value,
    ) -> Value {
        match expr {
            Expression::Literal(lit) => lit.clone().into(),
            Expression::Variable(name) => {
                // Check if this is an alias in the RETURN clause
                if let Value::Map(map) = result_map {
                    if let Some(val) = map.get(name) {
                        return val.clone();
                    }
                }
                // Fallback: try group element
                group_elements.first().cloned().unwrap_or(Value::Null)
            }
            Expression::Aggregate {
                func,
                distinct,
                expr: inner,
            } => {
                // Compute aggregate over group
                self.compute_aggregate(*func, *distinct, inner, group_elements)
                    .unwrap_or(Value::Null)
            }
            Expression::Property { variable, property } => {
                // First check if this is an alias reference
                if let Value::Map(map) = result_map {
                    // Check direct property access on alias
                    if let Some(val) = map.get(variable) {
                        return self.extract_property(val, property).unwrap_or(Value::Null);
                    }
                }
                // Fallback: check GROUP BY keys
                for (i, group_expr) in group_by.expressions.iter().enumerate() {
                    if self.expressions_match(expr, group_expr) {
                        return group_key[i].clone().into();
                    }
                }
                // Last resort: evaluate against first element
                group_elements
                    .first()
                    .map(|e| self.evaluate_value(expr, e))
                    .unwrap_or(Value::Null)
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value(
                    left,
                    return_clause,
                    group_by,
                    group_key,
                    group_elements,
                    result_map,
                );
                let right_val = self.evaluate_having_value(
                    right,
                    return_clause,
                    group_by,
                    group_key,
                    group_elements,
                    result_map,
                );
                apply_binary_op(*op, left_val, right_val)
            }
            Expression::FunctionCall { name, args } => {
                // For function calls in HAVING, use the first element as context
                let element = group_elements.first().cloned().unwrap_or(Value::Null);
                self.evaluate_function_call(name, args, &element)
            }
            _ => {
                // Fallback: try to evaluate as group expression
                self.evaluate_group_expression(expr, group_by, group_key, group_elements)
                    .unwrap_or(Value::Null)
            }
        }
    }

    /// Evaluate a HAVING predicate expression in the context of a group (multi-var version).
    ///
    /// This version works with traversers instead of plain values.
    fn evaluate_having_predicate_multi_var(
        &self,
        expr: &Expression,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_traversers: &[crate::traversal::Traverser],
        result_map: &Value,
    ) -> bool {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    // Logical operators
                    BinaryOperator::And => {
                        self.evaluate_having_predicate_multi_var(
                            left,
                            return_clause,
                            group_by,
                            group_key,
                            group_traversers,
                            result_map,
                        ) && self.evaluate_having_predicate_multi_var(
                            right,
                            return_clause,
                            group_by,
                            group_key,
                            group_traversers,
                            result_map,
                        )
                    }
                    BinaryOperator::Or => {
                        self.evaluate_having_predicate_multi_var(
                            left,
                            return_clause,
                            group_by,
                            group_key,
                            group_traversers,
                            result_map,
                        ) || self.evaluate_having_predicate_multi_var(
                            right,
                            return_clause,
                            group_by,
                            group_key,
                            group_traversers,
                            result_map,
                        )
                    }
                    // Comparison operators
                    _ => {
                        let left_val = self.evaluate_having_value_multi_var(
                            left,
                            return_clause,
                            group_by,
                            group_key,
                            group_traversers,
                            result_map,
                        );
                        let right_val = self.evaluate_having_value_multi_var(
                            right,
                            return_clause,
                            group_by,
                            group_key,
                            group_traversers,
                            result_map,
                        );
                        apply_comparison(*op, &left_val, &right_val)
                    }
                }
            }
            Expression::UnaryOp { op, expr: inner } => match op {
                UnaryOperator::Not => !self.evaluate_having_predicate_multi_var(
                    inner,
                    return_clause,
                    group_by,
                    group_key,
                    group_traversers,
                    result_map,
                ),
                UnaryOperator::Neg => {
                    let val = self.evaluate_having_value_multi_var(
                        inner,
                        return_clause,
                        group_by,
                        group_key,
                        group_traversers,
                        result_map,
                    );
                    match val {
                        Value::Int(n) => n == 0,
                        Value::Float(f) => f == 0.0,
                        Value::Bool(b) => !b,
                        Value::Null => true,
                        _ => false,
                    }
                }
            },
            // For other expressions, evaluate and check truthiness
            _ => {
                let val = self.evaluate_having_value_multi_var(
                    expr,
                    return_clause,
                    group_by,
                    group_key,
                    group_traversers,
                    result_map,
                );
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

    /// Evaluate a HAVING expression to a Value in the context of a group (multi-var version).
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_having_value_multi_var(
        &self,
        expr: &Expression,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_traversers: &[crate::traversal::Traverser],
        result_map: &Value,
    ) -> Value {
        match expr {
            Expression::Literal(lit) => lit.clone().into(),
            Expression::Variable(name) => {
                // Check if this is an alias in the RETURN clause
                if let Value::Map(map) = result_map {
                    if let Some(val) = map.get(name) {
                        return val.clone();
                    }
                }
                // Fallback: try path-based lookup
                group_traversers
                    .first()
                    .map(|t| self.evaluate_value_from_path(expr, t))
                    .unwrap_or(Value::Null)
            }
            Expression::Aggregate {
                func,
                distinct,
                expr: inner,
            } => {
                // Compute aggregate over group using path-based evaluation
                self.compute_aggregate_multi_var(*func, *distinct, inner, group_traversers)
                    .unwrap_or(Value::Null)
            }
            Expression::Property { variable, property } => {
                // First check if this is an alias reference
                if let Value::Map(map) = result_map {
                    if let Some(val) = map.get(variable) {
                        return self.extract_property(val, property).unwrap_or(Value::Null);
                    }
                }
                // Check GROUP BY keys
                for (i, group_expr) in group_by.expressions.iter().enumerate() {
                    if self.expressions_match(expr, group_expr) {
                        return group_key[i].clone().into();
                    }
                }
                // Path-based lookup
                group_traversers
                    .first()
                    .map(|t| self.evaluate_value_from_path(expr, t))
                    .unwrap_or(Value::Null)
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value_multi_var(
                    left,
                    return_clause,
                    group_by,
                    group_key,
                    group_traversers,
                    result_map,
                );
                let right_val = self.evaluate_having_value_multi_var(
                    right,
                    return_clause,
                    group_by,
                    group_key,
                    group_traversers,
                    result_map,
                );
                apply_binary_op(*op, left_val, right_val)
            }
            Expression::FunctionCall { name, args } => {
                // For function calls in HAVING, use the first traverser's value as context
                let element = group_traversers
                    .first()
                    .map(|t| t.value.clone())
                    .unwrap_or(Value::Null);
                self.evaluate_function_call(name, args, &element)
            }
            _ => {
                // Fallback: try to evaluate as group expression
                self.evaluate_group_expression_multi_var(
                    expr,
                    group_by,
                    group_key,
                    group_traversers,
                )
                .unwrap_or(Value::Null)
            }
        }
    }

    /// Execute global aggregates (no GROUP BY).
    ///
    /// Aggregates over all matched elements and returns a single result.
    /// If `having_clause` is provided, the resulting row is filtered against it
    /// (returning an empty Vec if it fails).
    fn execute_global_aggregates(
        &self,
        return_clause: &ReturnClause,
        aggregates: &[(&ReturnItem, AggregateFunc, bool, &Expression)],
        elements: &[Value],
        having_clause: &Option<HavingClause>,
    ) -> Result<Vec<Value>, CompileError> {
        // Always build a result map first so HAVING can reference aliases by name.
        let mut row_map = ValueMap::new();
        for (item, func, distinct, expr) in aggregates {
            let key = self.get_return_item_key(item);
            let value = self.compute_aggregate(*func, *distinct, expr, elements)?;
            row_map.insert(key, value);
        }
        let row_value = Value::Map(row_map);

        // Apply HAVING (if any) before unwrapping.
        if let Some(having) = having_clause {
            let empty_group_by = GroupByClause {
                expressions: Vec::new(),
            };
            if !self.evaluate_having_predicate(
                &having.expression,
                return_clause,
                &empty_group_by,
                &[],
                elements,
                &row_value,
            ) {
                return Ok(Vec::new());
            }
        }

        // Mirror the previous unwrapping behavior for the single-aggregate case
        // so we don't break existing callers / tests.
        if aggregates.len() == 1 {
            let (item, _, _, _) = &aggregates[0];
            if item.alias.is_some() {
                Ok(vec![row_value])
            } else {
                // Unwrap the single value
                if let Value::Map(map) = row_value {
                    let key = self.get_return_item_key(item);
                    let value = map.get(&key).cloned().unwrap_or(Value::Null);
                    Ok(vec![value])
                } else {
                    Ok(vec![row_value])
                }
            }
        } else {
            Ok(vec![row_value])
        }
    }

    /// Execute grouped aggregates (with GROUP BY expressions).
    ///
    /// Groups elements by non-aggregate expressions, then computes aggregates per group.
    /// If `having_clause` is provided, each resulting row is filtered against it.
    fn execute_grouped_aggregates(
        &self,
        return_clause: &ReturnClause,
        group_by_items: &[(&ReturnItem, &Expression)],
        aggregates: &[(&ReturnItem, AggregateFunc, bool, &Expression)],
        elements: &[Value],
        having_clause: &Option<HavingClause>,
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

        // Synthesize a GroupByClause for HAVING evaluation (uses the implicit
        // group-by expressions taken from the non-aggregate RETURN items).
        let synthesized_group_by = having_clause.as_ref().map(|_| GroupByClause {
            expressions: group_by_items
                .iter()
                .map(|(_, expr)| (*expr).clone())
                .collect(),
        });

        // For each group, compute the aggregates
        let mut results = Vec::new();

        for (group_key, group_elements) in groups {
            let mut map = ValueMap::new();

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

            let row = Value::Map(map);

            // Apply HAVING if present
            if let (Some(having), Some(gb)) = (having_clause, &synthesized_group_by) {
                if !self.evaluate_having_predicate(
                    &having.expression,
                    return_clause,
                    gb,
                    &group_key,
                    &group_values,
                    &row,
                ) {
                    continue;
                }
            }

            results.push(row);
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

    // =========================================================================
    // MATH() Function Implementation
    // =========================================================================

    /// Evaluate a MATH() expression from row context.
    ///
    /// Syntax: MATH(expression_string, arg1, arg2, ...)
    fn evaluate_math_from_row(&self, args: &[Expression], row: &HashMap<String, Value>) -> Value {
        if args.is_empty() {
            return Value::Null;
        }

        // First argument must be the expression string
        let expr_string = match self.evaluate_expression_from_row(&args[0], row) {
            Value::String(s) => s,
            _ => return Value::Null,
        };

        // Evaluate remaining arguments as numeric values
        let mut var_values: Vec<f64> = Vec::new();
        for arg in args.iter().skip(1) {
            match self.evaluate_expression_from_row(arg, row) {
                Value::Int(n) => var_values.push(n as f64),
                Value::Float(f) => var_values.push(f),
                _ => return Value::Null,
            }
        }

        evaluate_math_expr_internal(&expr_string, &var_values)
    }

    /// Evaluate a MATH() expression from path/traverser context.
    ///
    /// Syntax: MATH(expression_string, arg1, arg2, ...)
    fn evaluate_math_from_path(
        &self,
        args: &[Expression],
        traverser: &crate::traversal::Traverser,
    ) -> Value {
        if args.is_empty() {
            return Value::Null;
        }

        // First argument must be the expression string
        let expr_string = match self.evaluate_value_from_path(&args[0], traverser) {
            Value::String(s) => s,
            _ => return Value::Null,
        };

        // Evaluate remaining arguments as numeric values
        let mut var_values: Vec<f64> = Vec::new();
        for arg in args.iter().skip(1) {
            match self.evaluate_value_from_path(arg, traverser) {
                Value::Int(n) => var_values.push(n as f64),
                Value::Float(f) => var_values.push(f),
                _ => return Value::Null,
            }
        }

        evaluate_math_expr_internal(&expr_string, &var_values)
    }

    /// Evaluate a MATH() expression from element context.
    ///
    /// Syntax: MATH(expression_string, arg1, arg2, ...)
    fn evaluate_math(&self, args: &[Expression], element: &Value) -> Value {
        if args.is_empty() {
            return Value::Null;
        }

        // First argument must be the expression string
        let expr_string = match self.evaluate_value(&args[0], element) {
            Value::String(s) => s,
            _ => return Value::Null,
        };

        // Evaluate remaining arguments as numeric values
        let mut var_values: Vec<f64> = Vec::new();
        for arg in args.iter().skip(1) {
            match self.evaluate_value(arg, element) {
                Value::Int(n) => var_values.push(n as f64),
                Value::Float(f) => var_values.push(f),
                _ => return Value::Null,
            }
        }

        evaluate_math_expr_internal(&expr_string, &var_values)
    }
}

// =============================================================================
// Helper Types and Functions - Re-exported from helpers module
// =============================================================================

use super::helpers::{
    apply_binary_op, apply_comparison, compare_values, eval_inline_predicate, ComparableValue,
};
use super::math::evaluate_math_expr_internal;

// Unit tests have been moved to tests/gql/compiler_unit.rs
