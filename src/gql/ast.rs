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
    /// LET clauses for binding computed values to variables.
    pub let_clauses: Vec<LetClause>,
    /// The RETURN clause specifying what values to output.
    pub return_clause: ReturnClause,
    /// Optional GROUP BY clause for grouping aggregation results.
    pub group_by_clause: Option<GroupByClause>,
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
