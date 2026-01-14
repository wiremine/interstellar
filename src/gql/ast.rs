//! AST types for GQL queries.
//!
//! This module defines the Abstract Syntax Tree (AST) types that represent
//! parsed GQL queries. These types are produced by the [`parser`] module
//! and consumed by the [`compiler`] module.
//!
//! # Structure
//!
//! A GQL query is represented by the [`Query`] struct, which contains:
//!
//! - [`MatchClause`] - The MATCH clause with graph patterns (required)
//! - [`WhereClause`] - Optional filtering conditions
//! - [`ReturnClause`] - The RETURN clause specifying output (required)
//! - [`OrderClause`] - Optional sorting specification
//! - [`LimitClause`] - Optional result pagination
//!
//! # Example AST Structure
//!
//! For the query: `MATCH (n:Person)-[:KNOWS]->(m) WHERE n.age > 30 RETURN n.name`
//!
//! ```text
//! Query {
//!     match_clause: MatchClause {
//!         patterns: [Pattern {
//!             elements: [
//!                 Node { variable: "n", labels: ["Person"], properties: [] },
//!                 Edge { direction: Outgoing, labels: ["KNOWS"], ... },
//!                 Node { variable: "m", labels: [], properties: [] }
//!             ]
//!         }]
//!     },
//!     where_clause: Some(WhereClause {
//!         expression: BinaryOp { left: Property("n", "age"), op: Gt, right: Literal(30) }
//!     }),
//!     return_clause: ReturnClause {
//!         items: [ReturnItem { expression: Property("n", "name"), alias: None }]
//!     },
//!     ...
//! }
//! ```
//!
//! [`parser`]: crate::gql::parser
//! [`compiler`]: crate::gql::compiler

use serde::Serialize;

// =============================================================================
// Statement Structure
// =============================================================================

/// A GQL statement which may be a single query or a UNION of queries.
///
/// The `Statement` type is the top-level AST node produced by the parser.
/// It can represent either a single query or multiple queries combined with
/// UNION / UNION ALL.
///
/// # Examples
///
/// Single query:
/// ```text
/// MATCH (n:Person) RETURN n.name
/// ```
///
/// UNION query (deduplicates results):
/// ```text
/// MATCH (p:Player)-[:played_for]->(t:Team) RETURN t.name
/// UNION
/// MATCH (p:Player)-[:won_championship_with]->(t:Team) RETURN t.name
/// ```
///
/// UNION ALL query (keeps duplicates):
/// ```text
/// MATCH (p:Player)-[:played_for]->(t:Team) RETURN t.name
/// UNION ALL
/// MATCH (p:Player)-[:won_championship_with]->(t:Team) RETURN t.name
/// ```
#[derive(Debug, Clone, Serialize)]
pub enum Statement {
    /// A single query (boxed to reduce enum variant size).
    Query(Box<Query>),
    /// A UNION of multiple queries.
    ///
    /// The `all` flag indicates whether duplicates should be kept:
    /// - `false` (UNION): Results are deduplicated
    /// - `true` (UNION ALL): All results are kept, including duplicates
    Union {
        /// The queries to union together.
        queries: Vec<Query>,
        /// True for UNION ALL (keep duplicates), false for UNION (deduplicate).
        all: bool,
    },
    /// A mutation statement (CREATE, SET, DELETE, MERGE, etc.)
    ///
    /// Represents a GQL statement that modifies the graph.
    ///
    /// # Examples
    ///
    /// ```text
    /// CREATE (n:Person {name: 'Alice'})
    ///
    /// MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n
    ///
    /// MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 123
    /// ```
    Mutation(Box<MutationQuery>),
    /// A DDL statement (schema definition/modification)
    ///
    /// Represents a GQL statement that defines or modifies the schema.
    ///
    /// # Examples
    ///
    /// ```text
    /// CREATE NODE TYPE Person (name STRING NOT NULL, age INT)
    ///
    /// CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person
    ///
    /// ALTER NODE TYPE Person ADD bio STRING
    ///
    /// DROP NODE TYPE Person
    ///
    /// SET SCHEMA VALIDATION STRICT
    /// ```
    Ddl(Box<DdlStatement>),
}

// =============================================================================
// DDL Statement Types (Schema Definition)
// =============================================================================

/// A DDL (Data Definition Language) statement for schema management.
///
/// DDL statements define the structure of the graph schema, including
/// vertex types, edge types, property constraints, and validation modes.
///
/// # Examples
///
/// ```text
/// -- Create a node type
/// CREATE NODE TYPE Person (
///     name STRING NOT NULL,
///     age INT,
///     active BOOL DEFAULT true
/// )
///
/// -- Create an edge type with endpoint constraints
/// CREATE EDGE TYPE KNOWS (
///     since INT,
///     weight FLOAT DEFAULT 1.0
/// ) FROM Person TO Person
///
/// -- Modify existing type
/// ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES
/// ALTER NODE TYPE Person ADD bio STRING
///
/// -- Remove type definition
/// DROP NODE TYPE Person
///
/// -- Set validation mode
/// SET SCHEMA VALIDATION STRICT
/// ```
#[derive(Debug, Clone, Serialize)]
pub enum DdlStatement {
    /// CREATE NODE TYPE statement
    CreateNodeType(CreateNodeType),
    /// CREATE EDGE TYPE statement
    CreateEdgeType(CreateEdgeType),
    /// ALTER NODE TYPE statement
    AlterNodeType(AlterNodeType),
    /// ALTER EDGE TYPE statement
    AlterEdgeType(AlterEdgeType),
    /// DROP NODE TYPE statement
    DropNodeType(DropType),
    /// DROP EDGE TYPE statement
    DropEdgeType(DropType),
    /// SET SCHEMA VALIDATION statement
    SetValidation(SetValidation),
}

/// CREATE NODE TYPE statement.
///
/// Defines a vertex type with optional property constraints.
///
/// # Example
///
/// ```text
/// CREATE NODE TYPE Person (
///     name STRING NOT NULL,
///     age INT,
///     email STRING DEFAULT 'unknown'
/// )
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CreateNodeType {
    /// The name of the node type (becomes the vertex label)
    pub name: String,
    /// Property definitions for this type
    pub properties: Vec<PropertyDefinition>,
}

/// CREATE EDGE TYPE statement.
///
/// Defines an edge type with endpoint constraints and optional property constraints.
///
/// # Example
///
/// ```text
/// CREATE EDGE TYPE KNOWS (
///     since INT,
///     weight FLOAT DEFAULT 1.0
/// ) FROM Person TO Person
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CreateEdgeType {
    /// The name of the edge type (becomes the edge label)
    pub name: String,
    /// Property definitions for this type
    pub properties: Vec<PropertyDefinition>,
    /// Allowed source vertex labels
    pub from_types: Vec<String>,
    /// Allowed target vertex labels
    pub to_types: Vec<String>,
}

/// ALTER NODE TYPE statement.
///
/// Modifies an existing node type definition.
///
/// # Examples
///
/// ```text
/// ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES
/// ALTER NODE TYPE Person ADD bio STRING
/// ALTER NODE TYPE Person DROP bio
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct AlterNodeType {
    /// The name of the node type to alter
    pub name: String,
    /// The alteration to apply
    pub action: AlterTypeAction,
}

/// ALTER EDGE TYPE statement.
///
/// Modifies an existing edge type definition.
///
/// # Examples
///
/// ```text
/// ALTER EDGE TYPE KNOWS ALLOW ADDITIONAL PROPERTIES
/// ALTER EDGE TYPE KNOWS ADD notes STRING
/// ALTER EDGE TYPE KNOWS DROP notes
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct AlterEdgeType {
    /// The name of the edge type to alter
    pub name: String,
    /// The alteration to apply
    pub action: AlterTypeAction,
}

/// Actions that can be performed in an ALTER TYPE statement.
#[derive(Debug, Clone, Serialize)]
pub enum AlterTypeAction {
    /// Allow properties not defined in the schema
    AllowAdditionalProperties,
    /// Add a new property definition
    AddProperty(PropertyDefinition),
    /// Drop a property definition (by name)
    DropProperty(String),
}

/// DROP TYPE statement (for both node and edge types).
///
/// Removes a type definition from the schema. Note that dropping a type
/// does NOT delete existing vertices/edges with that label - they simply
/// become "unschemaed" (no validation for that label).
///
/// # Examples
///
/// ```text
/// DROP NODE TYPE Person
/// DROP EDGE TYPE KNOWS
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct DropType {
    /// The name of the type to drop
    pub name: String,
}

/// SET SCHEMA VALIDATION statement.
///
/// Sets the validation mode for the graph schema.
///
/// # Examples
///
/// ```text
/// SET SCHEMA VALIDATION NONE    -- No validation (schema is documentation only)
/// SET SCHEMA VALIDATION WARN    -- Log warnings but allow invalid data
/// SET SCHEMA VALIDATION STRICT  -- Validate types with schemas, allow unknown types
/// SET SCHEMA VALIDATION CLOSED  -- All types must have schemas defined
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct SetValidation {
    /// The validation mode to set
    pub mode: ValidationModeAst,
}

/// Validation modes for schema enforcement.
///
/// | Mode | Unknown Label | Schema Violation | Additional Properties |
/// |------|---------------|------------------|----------------------|
/// | `None` | Allowed | Allowed | Allowed |
/// | `Warn` | Allowed (log) | Allowed (log) | Allowed (log) |
/// | `Strict` | Allowed | Rejected | Rejected (unless allowed) |
/// | `Closed` | Rejected | Rejected | Rejected (unless allowed) |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValidationModeAst {
    /// No validation (schema is documentation only)
    None,
    /// Log warnings but allow invalid data
    Warn,
    /// Validate types with schemas, allow unknown types
    Strict,
    /// All types must have schemas defined
    Closed,
}

/// A property definition in a type declaration.
///
/// # Example
///
/// ```text
/// name STRING NOT NULL DEFAULT 'unknown'
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct PropertyDefinition {
    /// The property key name
    pub name: String,
    /// The expected value type
    pub prop_type: PropertyTypeAst,
    /// Whether this property is required (NOT NULL)
    pub required: bool,
    /// Default value if not provided (query-time application)
    pub default: Option<Literal>,
}

/// Property types in DDL statements.
///
/// Maps to `PropertyType` in the schema module at execution time.
///
/// | GQL Type | PropertyTypeAst | Value Variant |
/// |----------|-----------------|---------------|
/// | `STRING` | `String` | `Value::String` |
/// | `INT` | `Int` | `Value::Int` |
/// | `FLOAT` | `Float` | `Value::Float` |
/// | `BOOL` | `Bool` | `Value::Bool` |
/// | `LIST` | `List(None)` | `Value::List` |
/// | `LIST<T>` | `List(Some(T))` | `Value::List` of T |
/// | `MAP` | `Map(None)` | `Value::Map` |
/// | `MAP<T>` | `Map(Some(T))` | `Value::Map` with T values |
/// | `ANY` | `Any` | Any variant |
#[derive(Debug, Clone, Serialize)]
pub enum PropertyTypeAst {
    /// STRING type
    String,
    /// INT type (i64)
    Int,
    /// FLOAT type (f64)
    Float,
    /// BOOL type
    Bool,
    /// LIST type with optional element type
    List(Option<Box<PropertyTypeAst>>),
    /// MAP type with optional value type
    Map(Option<Box<PropertyTypeAst>>),
    /// ANY type (accepts any value)
    Any,
}

// =============================================================================
// Mutation Statement Types
// =============================================================================

/// A mutation query (CREATE, SET, DELETE, MERGE, etc.)
///
/// Represents a GQL statement that modifies the graph. A mutation query
/// can optionally start with a MATCH clause (for pattern-based mutations),
/// followed by one or more mutation clauses, and optionally end with a RETURN.
///
/// # Variants
///
/// 1. **CREATE-only**: `CREATE (n:Person {name: 'Alice'}) RETURN n`
/// 2. **MATCH + mutations**: `MATCH (n:Person) SET n.age = 31 RETURN n`
/// 3. **MERGE**: `MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 123`
///
/// # Examples
///
/// ```text
/// // Create a new vertex
/// CREATE (n:Person {name: 'Alice', age: 30})
///
/// // Update properties on matched vertices
/// MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n
///
/// // Delete vertices (must have no edges)
/// MATCH (n:Person {status: 'inactive'}) DELETE n
///
/// // Delete vertices and their edges
/// MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
///
/// // Upsert: create if not exists, update if exists
/// MERGE (n:Person {name: 'Alice'})
/// ON CREATE SET n.created = 123
/// ON MATCH SET n.updated = 456
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MutationQuery {
    /// Optional MATCH clause for pattern-based mutations.
    pub match_clause: Option<MatchClause>,
    /// Optional MATCH clauses that produce nulls if not found.
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    /// Optional WHERE clause for filtering matched patterns.
    pub where_clause: Option<WhereClause>,
    /// List of mutation clauses (CREATE, SET, DELETE, etc.)
    pub mutations: Vec<MutationClause>,
    /// FOREACH clauses for list iteration with mutations.
    pub foreach_clauses: Vec<ForeachClause>,
    /// Optional RETURN clause for returning results.
    pub return_clause: Option<ReturnClause>,
}

/// A mutation clause (CREATE, SET, DELETE, etc.)
///
/// Represents a single mutation operation in a mutation query.
/// Multiple mutation clauses can be combined in a single query.
#[derive(Debug, Clone, Serialize)]
pub enum MutationClause {
    /// CREATE clause - creates new vertices and edges.
    Create(CreateClause),
    /// SET clause - updates properties on vertices or edges.
    Set(SetClause),
    /// REMOVE clause - removes properties from vertices or edges.
    Remove(RemoveClause),
    /// DELETE clause - deletes elements (fails if vertex has edges).
    Delete(DeleteClause),
    /// DETACH DELETE clause - deletes vertices with automatic edge removal.
    DetachDelete(DetachDeleteClause),
    /// MERGE clause - upsert operation (match or create).
    Merge(MergeClause),
}

/// CREATE clause - creates new vertices and edges.
///
/// Creates new graph elements from the specified patterns.
/// Each pattern describes the structure to create.
///
/// # Examples
///
/// ```text
/// // Create a single vertex
/// CREATE (n:Person {name: 'Alice', age: 30})
///
/// // Create multiple vertices
/// CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
///
/// // Create vertices with edges
/// CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CreateClause {
    /// Patterns to create.
    pub patterns: Vec<Pattern>,
}

/// SET clause - updates properties on vertices or edges.
///
/// Sets property values on elements bound to variables.
///
/// # Examples
///
/// ```text
/// SET n.age = 31
/// SET n.age = 31, n.status = 'active'
/// SET n.updated = n.count + 1
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct SetClause {
    /// Property assignments.
    pub items: Vec<SetItem>,
}

/// A single SET assignment (e.g., n.age = 31).
///
/// Assigns a value to a property on a variable.
#[derive(Debug, Clone, Serialize)]
pub struct SetItem {
    /// The property to set (variable.property).
    pub target: PropertyRef,
    /// The value expression to assign.
    pub value: Expression,
}

/// Reference to a property (variable.property).
///
/// Identifies a specific property on a variable.
#[derive(Debug, Clone, Serialize)]
pub struct PropertyRef {
    /// Variable name (must be bound in MATCH or CREATE).
    pub variable: String,
    /// Property name on the variable.
    pub property: String,
}

/// REMOVE clause - removes properties from vertices or edges.
///
/// Removes properties from elements bound to variables.
///
/// # Examples
///
/// ```text
/// REMOVE n.age
/// REMOVE n.age, n.status
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct RemoveClause {
    /// Properties to remove.
    pub properties: Vec<PropertyRef>,
}

/// DELETE clause - deletes elements.
///
/// Deletes vertices or edges. If deleting a vertex that has
/// connected edges, the operation will fail unless DETACH DELETE is used.
///
/// # Examples
///
/// ```text
/// DELETE n
/// DELETE n, m
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct DeleteClause {
    /// Variables referencing elements to delete.
    pub variables: Vec<String>,
}

/// DETACH DELETE clause - deletes vertices with automatic edge removal.
///
/// Similar to DELETE, but automatically removes any edges connected
/// to the deleted vertices before deletion.
///
/// # Examples
///
/// ```text
/// DETACH DELETE n
/// DETACH DELETE n, m
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct DetachDeleteClause {
    /// Variables referencing vertices to delete.
    pub variables: Vec<String>,
}

/// MERGE clause - upsert operation.
///
/// MERGE is an "upsert" operation: it matches an existing pattern if found,
/// or creates it if not. Optional ON CREATE and ON MATCH clauses specify
/// what properties to set in each case.
///
/// # Examples
///
/// ```text
/// // Simple merge (create if not exists)
/// MERGE (n:Person {name: 'Alice'})
///
/// // Merge with ON CREATE action
/// MERGE (n:Person {name: 'Alice'})
/// ON CREATE SET n.created = timestamp()
///
/// // Merge with ON MATCH action  
/// MERGE (n:Person {name: 'Alice'})
/// ON MATCH SET n.lastSeen = timestamp()
///
/// // Merge with both actions
/// MERGE (n:Person {name: 'Alice'})
/// ON CREATE SET n.created = 123
/// ON MATCH SET n.updated = 456
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MergeClause {
    /// Pattern to merge (match or create).
    pub pattern: Pattern,
    /// Actions to perform when creating (ON CREATE SET).
    pub on_create: Option<Vec<SetItem>>,
    /// Actions to perform when matching (ON MATCH SET).
    pub on_match: Option<Vec<SetItem>>,
}

/// FOREACH clause - iterates over a list and applies mutations.
///
/// FOREACH provides a way to iterate over a list and apply mutations to each element.
/// The iteration variable is scoped to the FOREACH body and shadows any outer variable
/// with the same name.
///
/// # Syntax
///
/// ```text
/// FOREACH (variable IN list_expression | mutations)
/// ```
///
/// # Examples
///
/// ```text
/// -- Mark all nodes in a path as visited
/// MATCH p = (start:Person {name: 'Alice'})-[*]->(end)
/// FOREACH (n IN nodes(p) | SET n.visited = true)
/// RETURN end.name
///
/// -- Multiple mutations per iteration
/// FOREACH (i IN items |
///     SET i.processed = true
///     SET i.processedAt = $timestamp
///     REMOVE i.pending
/// )
///
/// -- Nested FOREACH
/// MATCH (p:Parent)-[:HAS_CHILD]->(c:Child)
/// FOREACH (parent IN collect(p) |
///     FOREACH (task IN parent.tasks |
///         SET task.assigned = true
///     )
/// )
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ForeachClause {
    /// The variable name bound to each element in the list.
    pub variable: String,
    /// The list expression to iterate over.
    pub list: Expression,
    /// Mutations to apply for each element.
    pub mutations: Vec<ForeachMutation>,
}

/// A mutation that can appear inside a FOREACH clause.
///
/// FOREACH supports a subset of mutation operations that can be applied
/// to each element during iteration.
#[derive(Debug, Clone, Serialize)]
pub enum ForeachMutation {
    /// SET clause - updates properties.
    Set(SetClause),
    /// REMOVE clause - removes properties.
    Remove(RemoveClause),
    /// DELETE clause - deletes elements.
    Delete(DeleteClause),
    /// DETACH DELETE clause - deletes vertices with edges.
    DetachDelete(DetachDeleteClause),
    /// CREATE clause - creates new elements.
    Create(CreateClause),
    /// Nested FOREACH clause.
    Foreach(Box<ForeachClause>),
}

// =============================================================================
// Query Structure
// =============================================================================

/// A complete GQL query.
///
/// Represents a fully parsed GQL query with all its clauses. This is the
/// root type of the AST, produced by [`parse()`] and consumed by [`compile()`].
///
/// # Required Clauses
///
/// - `match_clause` - Specifies the graph pattern to match
/// - `return_clause` - Specifies what to return from matched patterns
///
/// # Optional Clauses
///
/// - `optional_match_clauses` - Optional pattern matches that produce nulls if not found
/// - `with_path_clause` - Enables path tracking for use with path() function
/// - `unwind_clauses` - Expands lists into rows
/// - `where_clause` - Filters matched patterns
/// - `call_clauses` - CALL subqueries for nested computations
/// - `let_clauses` - Binds computed values to variables
/// - `group_by_clause` - Groups results for aggregation
/// - `order_clause` - Sorts results
/// - `limit_clause` - Limits and offsets results
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// let query = parse("MATCH (n:Person) WHERE n.age > 21 RETURN n.name ORDER BY n.name LIMIT 10").unwrap();
///
/// assert!(!query.match_clause.patterns.is_empty());
/// assert!(query.where_clause.is_some());
/// assert!(!query.return_clause.items.is_empty());
/// assert!(query.order_clause.is_some());
/// assert!(query.limit_clause.is_some());
/// ```
///
/// [`parse()`]: crate::gql::parse
/// [`compile()`]: crate::gql::compile
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    /// The MATCH clause specifying graph patterns to find.
    pub match_clause: MatchClause,
    /// Optional MATCH clauses that produce nulls if patterns don't match.
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    /// Optional WITH PATH clause for enabling path tracking.
    pub with_path_clause: Option<WithPathClause>,
    /// UNWIND clauses for expanding lists into rows.
    pub unwind_clauses: Vec<UnwindClause>,
    /// Optional WHERE clause for filtering matched patterns.
    pub where_clause: Option<WhereClause>,
    /// CALL subqueries for nested query execution.
    pub call_clauses: Vec<CallClause>,
    /// LET clauses for binding computed values to variables.
    pub let_clauses: Vec<LetClause>,
    /// WITH clauses for piping results between query parts.
    pub with_clauses: Vec<WithClause>,
    /// The RETURN clause specifying what values to output.
    pub return_clause: ReturnClause,
    /// Optional GROUP BY clause for grouping aggregation results.
    pub group_by_clause: Option<GroupByClause>,
    /// Optional HAVING clause for filtering aggregated groups.
    pub having_clause: Option<HavingClause>,
    /// Optional ORDER BY clause for sorting results.
    pub order_clause: Option<OrderClause>,
    /// Optional LIMIT/OFFSET clause for pagination.
    pub limit_clause: Option<LimitClause>,
}

/// The LET clause for binding computed values to variables.
///
/// LET binds the result of an expression to a new variable that can be
/// used in subsequent LET clauses, RETURN, and ORDER BY. LET is evaluated
/// after MATCH/WHERE filtering but before RETURN.
///
/// # Example
///
/// ```text
/// MATCH (p:Person)-[:FRIEND]->(f)
/// LET friendCount = COUNT(f)
/// RETURN p.name, friendCount
/// ```
///
/// Multiple LET clauses can be chained, with later clauses able to
/// reference variables from earlier ones:
///
/// ```text
/// MATCH (person)-[:WORKS_AT]->(company)
/// LET colleagues = COLLECT(person)
/// LET companySize = SIZE(colleagues)
/// RETURN company.name, companySize
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct LetClause {
    /// The variable name to bind the expression result to.
    pub variable: String,
    /// The expression to evaluate and bind.
    pub expression: Expression,
}

/// The WITH clause for piping results between query parts.
///
/// WITH terminates the current query part and starts a new one.
/// Only variables explicitly listed in WITH are available in subsequent clauses.
/// WITH can include aggregations (like RETURN), DISTINCT, WHERE, ORDER BY, and LIMIT.
///
/// # Key Semantics
///
/// - WITH resets variable scope - only projected variables are available afterward
/// - WHERE after WITH filters on the WITH output (not the original MATCH)
/// - WITH can contain aggregates, requiring implicit grouping by non-aggregated expressions
///
/// # Examples
///
/// ```text
/// -- Basic WITH
/// MATCH (p:Person)-[:KNOWS]->(friend)
/// WITH p, COUNT(friend) AS friendCount
/// WHERE friendCount > 5
/// RETURN p.name, friendCount
///
/// -- Multiple WITH clauses
/// MATCH (p:Person)
/// WITH p, SIZE((p)-[:KNOWS]->()) AS degree
/// WHERE degree > 10
/// WITH p, degree, p.age AS age
/// WHERE age > 30
/// RETURN p.name, degree
///
/// -- WITH DISTINCT
/// MATCH (p:Person)-[:KNOWS]->(friend)
/// WITH DISTINCT friend.city AS city
/// RETURN city
///
/// -- WITH ORDER BY and LIMIT (pagination mid-query)
/// MATCH (p:Person)
/// WITH p ORDER BY p.score DESC LIMIT 10
/// RETURN p.name
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct WithClause {
    /// Whether to apply DISTINCT to projected values.
    pub distinct: bool,
    /// Items to project forward (same structure as RETURN items).
    pub items: Vec<ReturnItem>,
    /// Optional WHERE clause filtering WITH output.
    pub where_clause: Option<WhereClause>,
    /// Optional ORDER BY within WITH.
    pub order_clause: Option<OrderClause>,
    /// Optional LIMIT within WITH.
    pub limit_clause: Option<LimitClause>,
}

// =============================================================================
// CALL Subquery Types
// =============================================================================

/// A CALL subquery clause.
///
/// CALL executes a nested query for each row in the outer query.
/// Variables can be imported from the outer scope using WITH at the
/// start of the subquery.
///
/// # Semantics
///
/// - **Correlated**: If the subquery starts with `WITH var`, it runs once
///   per outer row with that variable in scope
/// - **Uncorrelated**: If no importing WITH, runs once and cross-joins
///   with outer results
/// - **Returning**: Subquery must end with RETURN; returned variables
///   are added to outer scope
///
/// # Examples
///
/// ```text
/// // Correlated - count friends per person
/// MATCH (p:Person)
/// CALL {
///     WITH p
///     MATCH (p)-[:FRIEND]->(f)
///     RETURN count(f) AS friendCount
/// }
/// RETURN p.name, friendCount
///
/// // Uncorrelated - global count cross-joined
/// MATCH (p:Person)
/// CALL {
///     MATCH (t:Team)
///     RETURN count(t) AS teamCount
/// }
/// RETURN p.name, teamCount
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CallClause {
    /// The subquery body - either a single query or UNION of queries.
    pub body: CallBody,
}

impl CallClause {
    /// Returns true if this CALL subquery is correlated (has importing WITH).
    ///
    /// Correlated subqueries execute once per outer row with imported variables
    /// in scope. Uncorrelated subqueries execute once and cross-join with outer rows.
    pub fn is_correlated(&self) -> bool {
        match &self.body {
            CallBody::Single(query) => query.importing_with.is_some(),
            CallBody::Union { queries, .. } => queries.iter().any(|q| q.importing_with.is_some()),
        }
    }
}

/// Body of a CALL subquery.
///
/// A CALL subquery can contain either a single query or a UNION of queries.
/// Each query inside CALL must end with a RETURN clause.
#[derive(Debug, Clone, Serialize)]
pub enum CallBody {
    /// A single subquery.
    Single(Box<CallQuery>),
    /// A UNION of subqueries.
    ///
    /// Multiple queries combined with UNION. If `all` is false, results
    /// are deduplicated (UNION). If `all` is true, all results are kept
    /// including duplicates (UNION ALL).
    Union {
        /// The queries to union together.
        queries: Vec<CallQuery>,
        /// True for UNION ALL (keep duplicates), false for UNION (deduplicate).
        all: bool,
    },
}

/// A query inside a CALL clause.
///
/// Similar to a regular Query but:
/// - May start with an importing WITH (to bring outer variables into scope)
/// - MATCH is optional (can just transform imported variables)
/// - Must have a RETURN clause
/// - Can contain nested CALL clauses
///
/// # Examples
///
/// ```text
/// // Full subquery with match
/// CALL {
///     WITH p
///     MATCH (p)-[:FRIEND]->(f)
///     WHERE f.age > 21
///     RETURN count(f) AS friendCount
/// }
///
/// // Simple transformation without match
/// CALL {
///     WITH p
///     RETURN p.name AS personName
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CallQuery {
    /// Optional WITH clause importing variables from outer scope.
    /// Must be first if present.
    pub importing_with: Option<ImportingWith>,
    /// Optional MATCH clause.
    pub match_clause: Option<MatchClause>,
    /// Optional MATCH clauses that produce nulls if not found.
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    /// Optional WHERE clause.
    pub where_clause: Option<WhereClause>,
    /// Nested CALL clauses (subqueries can contain subqueries).
    pub call_clauses: Vec<CallClause>,
    /// WITH clauses for intermediate transformations.
    pub with_clauses: Vec<WithClause>,
    /// Required RETURN clause.
    pub return_clause: ReturnClause,
    /// Optional ORDER BY clause for sorting results.
    pub order_clause: Option<OrderClause>,
    /// Optional LIMIT/OFFSET clause for pagination.
    pub limit_clause: Option<LimitClause>,
}

/// WITH clause that imports variables from outer scope into a CALL subquery.
///
/// Unlike regular WITH which projects forward, importing WITH brings
/// variables from the outer query into the subquery's scope.
///
/// # Example
///
/// ```text
/// MATCH (p:Person)
/// CALL {
///     WITH p           -- imports p from outer scope
///     MATCH (p)-[:FRIEND]->(f)
///     RETURN count(f) AS cnt
/// }
/// RETURN p.name, cnt
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ImportingWith {
    /// Variables to import from outer scope.
    pub items: Vec<ReturnItem>,
}

// =============================================================================
// MATCH Clause Types
// =============================================================================

/// The MATCH clause containing graph patterns to find.
///
/// Contains one or more patterns that describe the subgraph structure
/// to search for in the graph.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// // Single pattern
/// let query = parse("MATCH (n:Person) RETURN n").unwrap();
/// assert_eq!(query.match_clause.patterns.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MatchClause {
    /// List of patterns to match. Currently only the first pattern is used.
    pub patterns: Vec<Pattern>,
}

/// The OPTIONAL MATCH clause for optional pattern matching.
///
/// Similar to MATCH, but if the pattern doesn't find any matches,
/// the query continues with null values for the variables introduced
/// in this clause instead of filtering out the row.
///
/// OPTIONAL MATCH can reference variables from previous MATCH or
/// OPTIONAL MATCH clauses.
///
/// # Example
///
/// ```text
/// MATCH (p:Player)
/// OPTIONAL MATCH (p)-[:won_championship_with]->(t:Team)
/// RETURN p.name, t.name
/// ```
///
/// Players without championships will have `null` for `t.name`.
#[derive(Debug, Clone, Serialize)]
pub struct OptionalMatchClause {
    /// List of patterns to optionally match.
    pub patterns: Vec<Pattern>,
}

/// The WITH PATH clause for enabling path tracking.
///
/// When present, the query engine tracks the full traversal path,
/// which can be retrieved using the `path()` function in the RETURN clause.
///
/// # Example
///
/// ```text
/// MATCH (p1:Player)-[:played_for]->(t:Team)<-[:played_for]-(p2:Player)
/// WITH PATH
/// RETURN path(), p2.name
/// ```
///
/// The path() function returns a list containing all vertices and edges
/// traversed, in order.
#[derive(Debug, Clone, Serialize)]
pub struct WithPathClause {
    /// Optional alias for the path variable.
    /// If specified with AS, the path is bound to this variable name.
    pub alias: Option<String>,
}

/// The UNWIND clause for expanding lists into rows.
///
/// UNWIND takes a list expression and produces a row for each element
/// in the list, binding each element to the specified variable.
///
/// This is equivalent to the `unfold()` traversal step.
///
/// # Example
///
/// ```text
/// UNWIND [1, 2, 3] AS num
/// RETURN num * 2
/// -- Returns: 2, 4, 6
///
/// MATCH (p:Player)
/// UNWIND collect(p.name) AS name
/// RETURN name
/// ```
///
/// UNWIND null produces no rows.
/// UNWIND a non-list value wraps it in a single-element list.
#[derive(Debug, Clone, Serialize)]
pub struct UnwindClause {
    /// The expression that produces a list to unwind.
    pub expression: Expression,
    /// The variable name for each element.
    pub alias: String,
}

/// A graph pattern describing a path through the graph.
///
/// A pattern consists of alternating node and edge elements that describe
/// a path structure to match. Patterns always start with a node element.
///
/// # Structure
///
/// A pattern like `(a)-[:KNOWS]->(b)-[:WORKS_AT]->(c)` is represented as:
///
/// ```text
/// Pattern {
///     elements: [
///         Node("a"),
///         Edge(KNOWS, Outgoing),
///         Node("b"),
///         Edge(WORKS_AT, Outgoing),
///         Node("c")
///     ]
/// }
/// ```
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::PatternElement;
///
/// let query = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
/// let pattern = &query.match_clause.patterns[0];
///
/// assert_eq!(pattern.elements.len(), 3); // node, edge, node
/// assert!(matches!(&pattern.elements[0], PatternElement::Node(_)));
/// assert!(matches!(&pattern.elements[1], PatternElement::Edge(_)));
/// assert!(matches!(&pattern.elements[2], PatternElement::Node(_)));
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct Pattern {
    /// Alternating sequence of node and edge pattern elements.
    pub elements: Vec<PatternElement>,
}

/// An element in a pattern: either a node or an edge.
///
/// Patterns consist of alternating nodes and edges. A valid pattern
/// always starts and ends with a node.
#[derive(Debug, Clone, Serialize)]
pub enum PatternElement {
    /// A node pattern like `(n:Person {name: "Alice"})`.
    Node(NodePattern),
    /// An edge pattern like `-[:KNOWS]->`.
    Edge(EdgePattern),
}

/// A node pattern specifying constraints on matched vertices.
///
/// Node patterns can optionally specify:
/// - A variable name for binding the matched vertex
/// - One or more labels to filter by
/// - Property value constraints
/// - An inline WHERE clause for additional filtering
///
/// # Syntax
///
/// ```text
/// (variable:Label1:Label2 {prop1: value1, prop2: value2} WHERE condition)
/// ```
///
/// All parts are optional:
/// - `()` - matches any vertex
/// - `(n)` - matches any vertex, binds to `n`
/// - `(:Person)` - matches vertices with label "Person"
/// - `(n:Person {age: 30})` - matches Person vertices with age=30, binds to `n`
/// - `(n:Person WHERE n.age > 21)` - matches Person vertices where age > 21
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::{PatternElement, Literal};
///
/// let query = parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
/// if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
///     assert_eq!(node.variable, Some("n".to_string()));
///     assert_eq!(node.labels, vec!["Person".to_string()]);
///     assert_eq!(node.properties.len(), 1);
///     assert_eq!(node.properties[0].0, "name");
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct NodePattern {
    /// Optional variable name to bind the matched vertex.
    pub variable: Option<String>,
    /// Labels that the vertex must have (empty means any label).
    pub labels: Vec<String>,
    /// Property constraints as (key, value) pairs.
    pub properties: Vec<(String, Literal)>,
    /// Optional inline WHERE expression for additional filtering.
    ///
    /// Example: `(n:Person WHERE n.age > 21)`
    pub where_clause: Option<Expression>,
}

/// An edge pattern specifying constraints on matched edges.
///
/// Edge patterns can optionally specify:
/// - A variable name for binding the matched edge
/// - One or more relationship types (labels)
/// - Direction (outgoing, incoming, or both)
/// - A path quantifier for variable-length paths
/// - Property constraints
/// - An inline WHERE clause for additional filtering
///
/// # Syntax
///
/// ```text
/// -[variable:TYPE1|TYPE2 {prop: value} WHERE condition]->   // outgoing
/// <-[variable:TYPE WHERE condition]-                         // incoming  
/// -[variable:TYPE WHERE condition]-                          // either direction
/// -[*2..5]->                                                  // variable-length path
/// ```
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::{PatternElement, EdgeDirection};
///
/// let query = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
/// if let PatternElement::Edge(edge) = &query.match_clause.patterns[0].elements[1] {
///     assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
///     assert_eq!(edge.direction, EdgeDirection::Outgoing);
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct EdgePattern {
    /// Optional variable name to bind the matched edge.
    pub variable: Option<String>,
    /// Relationship types that the edge must have (empty means any type).
    pub labels: Vec<String>,
    /// Direction of the edge in the pattern.
    pub direction: EdgeDirection,
    /// Optional quantifier for variable-length paths.
    pub quantifier: Option<PathQuantifier>,
    /// Property constraints as (key, value) pairs.
    pub properties: Vec<(String, Literal)>,
    /// Optional inline WHERE expression for additional filtering.
    ///
    /// Example: `-[r:KNOWS WHERE r.since > 2020]->`
    pub where_clause: Option<Expression>,
}

/// Direction of an edge in a pattern.
///
/// Determines which direction to traverse when matching edges.
///
/// # Syntax Mapping
///
/// | Direction | Syntax | Description |
/// |-----------|--------|-------------|
/// | `Outgoing` | `-->` | Follow edges from source to target |
/// | `Incoming` | `<--` | Follow edges from target to source |
/// | `Both` | `--` | Follow edges in either direction |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EdgeDirection {
    /// Outgoing edge: `-->`
    Outgoing,
    /// Incoming edge: `<--`
    Incoming,
    /// Either direction: `--`
    Both,
}

/// Quantifier for variable-length path matching.
///
/// Specifies the minimum and maximum number of edge hops to match.
///
/// # Syntax
///
/// | Syntax | Min | Max | Description |
/// |--------|-----|-----|-------------|
/// | `*` | None | None | Any number of hops (0 to default max) |
/// | `*3` | 3 | 3 | Exactly 3 hops |
/// | `*2..5` | 2 | 5 | Between 2 and 5 hops |
/// | `*..5` | None | 5 | Up to 5 hops (including 0) |
/// | `*2..` | 2 | None | At least 2 hops |
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::PatternElement;
///
/// let query = parse("MATCH (a)-[*2..5]->(b) RETURN b").unwrap();
/// if let PatternElement::Edge(edge) = &query.match_clause.patterns[0].elements[1] {
///     let q = edge.quantifier.as_ref().unwrap();
///     assert_eq!(q.min, Some(2));
///     assert_eq!(q.max, Some(5));
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct PathQuantifier {
    /// Minimum number of hops (None means 0).
    pub min: Option<u32>,
    /// Maximum number of hops (None means unbounded, uses default max).
    pub max: Option<u32>,
}

// =============================================================================
// WHERE Clause
// =============================================================================

/// The WHERE clause containing a filter expression.
///
/// The expression is evaluated for each matched pattern, and only patterns
/// where the expression evaluates to true are included in results.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::Expression;
///
/// let query = parse("MATCH (n:Person) WHERE n.age > 21 RETURN n").unwrap();
/// let where_clause = query.where_clause.unwrap();
///
/// // The expression is a binary comparison: n.age > 21
/// assert!(matches!(where_clause.expression, Expression::BinaryOp { .. }));
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct WhereClause {
    /// The filter expression to evaluate.
    pub expression: Expression,
}

// =============================================================================
// RETURN Clause
// =============================================================================

/// The RETURN clause specifying what values to output.
///
/// Contains a list of expressions to evaluate for each matched pattern.
/// Results are returned as `Value` instances.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// // Return multiple values with aliases
/// let query = parse("MATCH (n:Person) RETURN n.name AS name, n.age AS age").unwrap();
/// assert_eq!(query.return_clause.items.len(), 2);
/// assert_eq!(query.return_clause.items[0].alias, Some("name".to_string()));
///
/// // Return with DISTINCT
/// let query = parse("MATCH (n) RETURN DISTINCT n.label").unwrap();
/// assert!(query.return_clause.distinct);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ReturnClause {
    /// Whether to deduplicate results (RETURN DISTINCT).
    pub distinct: bool,
    /// List of items to return.
    pub items: Vec<ReturnItem>,
}

/// A single item in a RETURN clause.
///
/// Each item has an expression to evaluate and an optional alias
/// for the result column name.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::Expression;
///
/// let query = parse("MATCH (n) RETURN n.name AS personName").unwrap();
/// let item = &query.return_clause.items[0];
///
/// assert!(matches!(&item.expression, Expression::Property { .. }));
/// assert_eq!(item.alias, Some("personName".to_string()));
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ReturnItem {
    /// The expression to evaluate.
    pub expression: Expression,
    /// Optional alias for the result (AS name).
    pub alias: Option<String>,
}

// =============================================================================
// ORDER BY Clause
// =============================================================================

/// The ORDER BY clause specifying result sorting.
///
/// Contains one or more ordering items, each specifying an expression
/// to sort by and the sort direction.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// let query = parse("MATCH (n:Person) RETURN n ORDER BY n.age DESC, n.name").unwrap();
/// let order = query.order_clause.unwrap();
///
/// assert_eq!(order.items.len(), 2);
/// assert!(order.items[0].descending);  // n.age DESC
/// assert!(!order.items[1].descending); // n.name (ascending by default)
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct OrderClause {
    /// List of ordering specifications.
    pub items: Vec<OrderItem>,
}

/// A single ordering specification in ORDER BY.
///
/// Specifies an expression to sort by and whether to sort in
/// descending order (ascending is the default).
#[derive(Debug, Clone, Serialize)]
pub struct OrderItem {
    /// The expression to sort by.
    pub expression: Expression,
    /// Whether to sort in descending order (default: false = ascending).
    pub descending: bool,
}

// =============================================================================
// GROUP BY Clause
// =============================================================================

/// The GROUP BY clause for grouping aggregation results.
///
/// Specifies which expressions to group by when using aggregate functions.
/// Non-aggregated expressions in RETURN should appear in GROUP BY.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// // Count players by position
/// let query = parse("MATCH (p:player) RETURN p.position, count(*) GROUP BY p.position").unwrap();
/// let group_by = query.group_by_clause.unwrap();
/// assert_eq!(group_by.expressions.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct GroupByClause {
    /// Expressions to group by.
    pub expressions: Vec<Expression>,
}

// =============================================================================
// HAVING Clause
// =============================================================================

/// The HAVING clause for filtering aggregated groups.
///
/// HAVING filters groups after aggregation, unlike WHERE which filters
/// rows before aggregation. HAVING can reference aggregate functions
/// and their aliases.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// // Filter groups with more than 2 members
/// let query = parse("MATCH (p:Person) RETURN p.city, count(*) AS cnt GROUP BY p.city HAVING count(*) > 2").unwrap();
/// let having = query.having_clause.unwrap();
/// // The expression is a comparison: count(*) > 2
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct HavingClause {
    /// The filter expression to evaluate on each group.
    pub expression: Expression,
}

// =============================================================================
// LIMIT Clause
// =============================================================================

/// The LIMIT clause for result pagination.
///
/// Specifies the maximum number of results to return and optionally
/// how many results to skip.
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
///
/// // LIMIT only
/// let query = parse("MATCH (n) RETURN n LIMIT 10").unwrap();
/// let limit = query.limit_clause.unwrap();
/// assert_eq!(limit.limit, 10);
/// assert_eq!(limit.offset, None);
///
/// // LIMIT with OFFSET
/// let query = parse("MATCH (n) RETURN n LIMIT 10 OFFSET 5").unwrap();
/// let limit = query.limit_clause.unwrap();
/// assert_eq!(limit.limit, 10);
/// assert_eq!(limit.offset, Some(5));
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct LimitClause {
    /// Maximum number of results to return.
    pub limit: u64,
    /// Number of results to skip (SKIP/OFFSET).
    pub offset: Option<u64>,
}

// =============================================================================
// Expression Types
// =============================================================================

/// An expression that can be evaluated to produce a value.
///
/// Expressions are used throughout GQL queries:
/// - In WHERE clauses for filtering
/// - In RETURN clauses for projection
/// - In ORDER BY clauses for sorting
///
/// # Variants
///
/// | Variant | Example | Description |
/// |---------|---------|-------------|
/// | `Variable` | `n` | Reference to a bound variable |
/// | `Property` | `n.name` | Property access on a variable |
/// | `Literal` | `42`, `"hello"` | Constant value |
/// | `BinaryOp` | `a + b`, `x = y` | Binary operation |
/// | `UnaryOp` | `NOT x`, `-n` | Unary operation |
/// | `IsNull` | `x IS NULL` | Null check |
/// | `InList` | `x IN [1,2,3]` | List membership |
/// | `List` | `[1, 2, 3]` | List literal |
/// | `FunctionCall` | `toUpper(s)` | Function invocation |
/// | `Aggregate` | `COUNT(*)` | Aggregate function |
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::{Expression, BinaryOperator};
///
/// let query = parse("MATCH (n) WHERE n.age >= 21 AND n.name STARTS WITH 'A' RETURN n").unwrap();
/// let expr = &query.where_clause.unwrap().expression;
///
/// // Top-level is AND
/// if let Expression::BinaryOp { op, .. } = expr {
///     assert_eq!(*op, BinaryOperator::And);
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    /// Variable reference: `n`
    ///
    /// References a variable bound in the MATCH clause.
    Variable(String),

    /// Property access: `n.name`
    ///
    /// Accesses a property on a bound variable (vertex or edge).
    Property {
        /// The variable to access.
        variable: String,
        /// The property name.
        property: String,
    },

    /// Literal value: `42`, `"hello"`, `true`, `null`
    Literal(Literal),

    /// Parameter reference: `$paramName`
    ///
    /// References a parameter that will be resolved at compile/execute time.
    /// Parameters allow safe value injection and query reuse.
    ///
    /// # Examples
    ///
    /// ```text
    /// -- Parameter in property filter
    /// MATCH (n:Person {id: $personId}) RETURN n
    ///
    /// -- Parameter in WHERE clause
    /// MATCH (n:Person) WHERE n.age > $minAge RETURN n
    ///
    /// -- Parameter in expression
    /// MATCH (n) RETURN n.value * $multiplier AS scaled
    /// ```
    Parameter(String),

    /// Binary operation: `a + b`, `x = y`, `p AND q`
    ///
    /// Applies a binary operator to two sub-expressions.
    BinaryOp {
        /// Left operand.
        left: Box<Expression>,
        /// The operator.
        op: BinaryOperator,
        /// Right operand.
        right: Box<Expression>,
    },

    /// Unary operation: `NOT x`, `-n`
    ///
    /// Applies a unary operator to an expression.
    UnaryOp {
        /// The operator.
        op: UnaryOperator,
        /// The operand expression.
        expr: Box<Expression>,
    },

    /// IS NULL / IS NOT NULL check.
    ///
    /// Tests whether an expression evaluates to null.
    IsNull {
        /// The expression to test.
        expr: Box<Expression>,
        /// True for `IS NOT NULL`, false for `IS NULL`.
        negated: bool,
    },

    /// IN list check: `x IN [1, 2, 3]` / `x NOT IN [...]`
    ///
    /// Tests whether a value is in a list of values.
    InList {
        /// The expression to test.
        expr: Box<Expression>,
        /// The list of values to check against.
        list: Vec<Expression>,
        /// True for `NOT IN`, false for `IN`.
        negated: bool,
    },

    /// List literal: `[1, 2, 3]`
    ///
    /// A list of expressions.
    List(Vec<Expression>),

    /// Function call: `toUpper(s)`, `abs(n)`
    ///
    /// Invokes a built-in function.
    FunctionCall {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<Expression>,
    },

    /// Aggregate function: `COUNT(*)`, `SUM(n.value)`
    ///
    /// Aggregates values across matched patterns.
    Aggregate {
        /// The aggregate function.
        func: AggregateFunc,
        /// Whether to apply DISTINCT before aggregating.
        distinct: bool,
        /// The expression to aggregate.
        expr: Box<Expression>,
    },

    /// EXISTS subquery: `EXISTS { (a)-[:KNOWS]->(b) }`
    ///
    /// Evaluates to true if the embedded pattern matches at least one path
    /// starting from the current element. The pattern can reference variables
    /// bound in the outer MATCH clause.
    ///
    /// # Examples
    ///
    /// ```text
    /// -- Find players who have won championships
    /// MATCH (p:player)
    /// WHERE EXISTS { (p)-[:won_championship_with]->() }
    /// RETURN p.name
    ///
    /// -- Find players who have NOT won championships
    /// MATCH (p:player)
    /// WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
    /// RETURN p.name
    /// ```
    Exists {
        /// The pattern to check for existence.
        pattern: Pattern,
        /// True for `NOT EXISTS`, false for `EXISTS`.
        negated: bool,
    },

    /// CASE expression with WHEN/THEN/ELSE branches.
    ///
    /// Evaluates conditions in order and returns the result of the first
    /// matching WHEN clause, or the ELSE result if no conditions match.
    ///
    /// # Examples
    ///
    /// ```text
    /// -- Simple categorization
    /// CASE
    ///     WHEN p.age > 35 THEN 'Senior'
    ///     WHEN p.age > 28 THEN 'Prime'
    ///     ELSE 'Young'
    /// END
    ///
    /// -- Conditional value selection
    /// CASE
    ///     WHEN p.score >= 90 THEN 'A'
    ///     WHEN p.score >= 80 THEN 'B'
    ///     WHEN p.score >= 70 THEN 'C'
    ///     ELSE 'F'
    /// END
    /// ```
    Case(CaseExpression),

    /// List comprehension: `[x IN list | expression]`
    ///
    /// Transforms and optionally filters a list. Each element of the input list
    /// is bound to the variable, optionally filtered, then transformed.
    ///
    /// # Examples
    ///
    /// ```text
    /// -- Basic transformation
    /// [x IN list | x.name]
    ///
    /// -- With filter
    /// [x IN list WHERE x.active | x.name]
    ///
    /// -- Expression transformation
    /// [n IN numbers | n * 2]
    ///
    /// -- String building
    /// [t IN types | t.category || ': ' || t.name]
    /// ```
    ListComprehension {
        /// Variable name bound to each element.
        variable: String,
        /// The list expression to iterate over.
        list: Box<Expression>,
        /// Optional filter condition.
        filter: Option<Box<Expression>>,
        /// Transformation expression applied to each element.
        transform: Box<Expression>,
    },

    /// Map literal: `{key: value, ...}`
    ///
    /// Creates a map/object value with key-value pairs.
    /// Keys must be identifiers (unquoted) or string literals.
    /// Values can be any expression.
    ///
    /// # Examples
    ///
    /// ```text
    /// -- Simple map literal
    /// {name: 'Alice', age: 30}
    ///
    /// -- Map with property references
    /// {personName: p.name, personAge: p.age}
    ///
    /// -- In COLLECT
    /// LET data = COLLECT({parent: parent, type: event.type})
    ///
    /// -- Nested maps
    /// RETURN {
    ///     name: p.name,
    ///     stats: {friends: friendCount, posts: postCount}
    /// } AS profile
    /// ```
    Map(Vec<(String, Expression)>),

    /// REDUCE expression: `REDUCE(acc = init, x IN list | expr)`
    ///
    /// Accumulates a value over a list (fold/reduce operation).
    /// The accumulator is initialized to the initial value, then for each
    /// element in the list, the expression is evaluated with both the
    /// accumulator and element in scope, producing the next accumulator value.
    ///
    /// # Examples
    ///
    /// ```text
    /// -- Sum numbers
    /// REDUCE(total = 0, x IN [1, 2, 3] | total + x)
    /// -- Result: 6
    ///
    /// -- String concatenation
    /// REDUCE(s = '', name IN ['Alice', 'Bob'] | s || name || ', ')
    /// -- Result: 'Alice, Bob, '
    ///
    /// -- Product of numbers
    /// REDUCE(product = 1, n IN numbers | product * n)
    ///
    /// -- Build a list (nested reduce)
    /// REDUCE(acc = [], x IN items | acc + [x * 2])
    /// ```
    Reduce {
        /// Accumulator variable name.
        accumulator: String,
        /// Initial value for accumulator.
        initial: Box<Expression>,
        /// Variable bound to each list element.
        variable: String,
        /// The list to iterate over.
        list: Box<Expression>,
        /// Expression computing next accumulator value.
        expression: Box<Expression>,
    },

    /// ALL list predicate: `ALL(x IN list WHERE condition)`
    ///
    /// Returns TRUE if all elements in the list satisfy the condition.
    /// Returns TRUE for an empty list (vacuous truth).
    /// Returns NULL if the list is NULL.
    ///
    /// # Example
    ///
    /// ```text
    /// -- Check if all scores are passing
    /// ALL(score IN s.scores WHERE score >= 60)
    ///
    /// -- Check if all items are in stock
    /// ALL(item IN order.items WHERE item.quantity > 0)
    /// ```
    All {
        /// Variable bound to each list element.
        variable: String,
        /// The list to iterate over.
        list: Box<Expression>,
        /// Condition that must be true for all elements.
        condition: Box<Expression>,
    },

    /// ANY list predicate: `ANY(x IN list WHERE condition)`
    ///
    /// Returns TRUE if at least one element in the list satisfies the condition.
    /// Returns FALSE for an empty list.
    /// Returns NULL if the list is NULL.
    ///
    /// # Example
    ///
    /// ```text
    /// -- Check if any tag is 'vip'
    /// ANY(tag IN p.tags WHERE tag = 'vip')
    ///
    /// -- Check if any friend is online
    /// ANY(friend IN p.friends WHERE friend.online = true)
    /// ```
    Any {
        /// Variable bound to each list element.
        variable: String,
        /// The list to iterate over.
        list: Box<Expression>,
        /// Condition that must be true for at least one element.
        condition: Box<Expression>,
    },

    /// NONE list predicate: `NONE(x IN list WHERE condition)`
    ///
    /// Returns TRUE if no elements in the list satisfy the condition.
    /// Returns TRUE for an empty list.
    /// Returns NULL if the list is NULL.
    ///
    /// # Example
    ///
    /// ```text
    /// -- Check if no reviews are negative
    /// NONE(review IN p.reviews WHERE review.rating < 3)
    ///
    /// -- Check if no items are expired
    /// NONE(item IN inventory WHERE item.expired = true)
    /// ```
    None {
        /// Variable bound to each list element.
        variable: String,
        /// The list to iterate over.
        list: Box<Expression>,
        /// Condition that must be false for all elements.
        condition: Box<Expression>,
    },

    /// SINGLE list predicate: `SINGLE(x IN list WHERE condition)`
    ///
    /// Returns TRUE if exactly one element in the list satisfies the condition.
    /// Returns FALSE for an empty list (zero matches).
    /// Returns NULL if the list is NULL.
    ///
    /// # Example
    ///
    /// ```text
    /// -- Check if exactly one player is captain
    /// SINGLE(p IN t.players WHERE p.captain = true)
    ///
    /// -- Check if exactly one item is selected
    /// SINGLE(item IN cart.items WHERE item.selected = true)
    /// ```
    Single {
        /// Variable bound to each list element.
        variable: String,
        /// The list to iterate over.
        list: Box<Expression>,
        /// Condition that must be true for exactly one element.
        condition: Box<Expression>,
    },
}

/// A CASE expression with WHEN/THEN/ELSE branches.
///
/// Evaluates conditions in order and returns the result of the first
/// matching WHEN clause, or the ELSE result if no conditions match.
/// If no conditions match and there's no ELSE clause, returns NULL.
///
/// # Example
///
/// ```text
/// CASE
///     WHEN p.age > 35 THEN 'Veteran'
///     WHEN p.age > 28 THEN 'Prime'
///     ELSE 'Young'
/// END
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CaseExpression {
    /// List of (condition, result) pairs for WHEN/THEN clauses.
    pub when_clauses: Vec<(Expression, Expression)>,
    /// Optional ELSE result expression.
    pub else_clause: Option<Box<Expression>>,
}

// =============================================================================
// Operators
// =============================================================================

/// Unary operators.
///
/// Applied to a single operand expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum UnaryOperator {
    /// Logical NOT: `NOT x`
    Not,
    /// Numeric negation: `-x`
    Neg,
}

/// Binary operators for expressions.
///
/// Applied to two operand expressions.
///
/// # Categories
///
/// | Category | Operators |
/// |----------|-----------|
/// | Comparison | `=`, `<>`, `<`, `<=`, `>`, `>=` |
/// | Logical | `AND`, `OR` |
/// | Arithmetic | `+`, `-`, `*`, `/`, `%`, `^` |
/// | String | `CONTAINS`, `STARTS WITH`, `ENDS WITH` |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinaryOperator {
    // Comparison operators
    /// Equality: `=`
    Eq,
    /// Inequality: `<>`
    Neq,
    /// Less than: `<`
    Lt,
    /// Less than or equal: `<=`
    Lte,
    /// Greater than: `>`
    Gt,
    /// Greater than or equal: `>=`
    Gte,

    // Logical operators
    /// Logical AND: `AND`
    And,
    /// Logical OR: `OR`
    Or,

    // Arithmetic operators
    /// Addition: `+`
    Add,
    /// Subtraction: `-`
    Sub,
    /// Multiplication: `*`
    Mul,
    /// Division: `/`
    Div,
    /// Modulo: `%`
    Mod,
    /// Power/exponentiation: `^`
    Pow,

    // String operators
    /// String contains: `CONTAINS`
    Contains,
    /// String prefix: `STARTS WITH`
    StartsWith,
    /// String suffix: `ENDS WITH`
    EndsWith,
    /// String concatenation: `||`
    Concat,
    /// Regex match: `=~`
    RegexMatch,
}

/// Aggregate functions for computing values across matched patterns.
///
/// These functions aggregate values from multiple matched elements
/// into a single result.
///
/// # Functions
///
/// | Function | Description |
/// |----------|-------------|
/// | `COUNT` | Count matched elements |
/// | `SUM` | Sum numeric values |
/// | `AVG` | Average of numeric values |
/// | `MIN` | Minimum value |
/// | `MAX` | Maximum value |
/// | `COLLECT` | Collect values into a list |
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::{Expression, AggregateFunc};
///
/// let query = parse("MATCH (n:Person) RETURN COUNT(DISTINCT n)").unwrap();
/// if let Expression::Aggregate { func, distinct, .. } = &query.return_clause.items[0].expression {
///     assert_eq!(*func, AggregateFunc::Count);
///     assert!(*distinct);
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AggregateFunc {
    /// Count elements: `COUNT(*)`
    Count,
    /// Sum numeric values: `SUM(n.value)`
    Sum,
    /// Average of numeric values: `AVG(n.value)`
    Avg,
    /// Minimum value: `MIN(n.value)`
    Min,
    /// Maximum value: `MAX(n.value)`
    Max,
    /// Collect values into a list: `COLLECT(n.name)`
    Collect,
}

// =============================================================================
// Literal Values
// =============================================================================

/// Literal values in expressions.
///
/// Represents constant values that appear directly in the query.
///
/// # Variants
///
/// | Variant | Example | Description |
/// |---------|---------|-------------|
/// | `Null` | `null` | Null/missing value |
/// | `Bool` | `true`, `false` | Boolean value |
/// | `Int` | `42`, `-7` | 64-bit signed integer |
/// | `Float` | `3.14`, `-0.5` | 64-bit floating point |
/// | `String` | `"hello"`, `'world'` | String value |
///
/// # Example
///
/// ```
/// use intersteller::gql::parse;
/// use intersteller::gql::{Expression, Literal};
///
/// let query = parse("MATCH (n) WHERE n.name = 'Alice' RETURN n").unwrap();
/// // The literal 'Alice' is parsed as Literal::String("Alice")
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Literal {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// String value.
    String(String),
}

impl From<Literal> for crate::value::Value {
    fn from(lit: Literal) -> Self {
        match lit {
            Literal::Null => crate::value::Value::Null,
            Literal::Bool(b) => crate::value::Value::Bool(b),
            Literal::Int(n) => crate::value::Value::Int(n),
            Literal::Float(f) => crate::value::Value::Float(f),
            Literal::String(s) => crate::value::Value::String(s),
        }
    }
}
