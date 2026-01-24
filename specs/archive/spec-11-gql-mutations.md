# Spec 11: GQL Mutation Clauses

## Overview

This specification defines the implementation of GQL mutation clauses for Interstellar. These clauses provide a declarative, Cypher-like syntax for creating, updating, and deleting graph elements through the GQL query interface.

GQL mutations build upon the existing Gremlin-style mutation steps (Spec 10) by providing an alternative, declarative syntax that integrates naturally with GQL's pattern-matching approach.

## Goals

1. Implement `CREATE` clause - Create vertices and edges using pattern syntax
2. Implement `SET` clause - Add/update properties on matched elements
3. Implement `REMOVE` clause - Remove properties from elements
4. Implement `DELETE` clause - Delete matched elements
5. Implement `DETACH DELETE` clause - Delete vertices with automatic edge removal
6. Implement `MERGE` clause - Upsert (create if not exists) vertices and edges
7. Ensure mutations compile to existing traversal mutation steps
8. Support combining MATCH with mutations for pattern-based updates

## Non-Goals

- Transaction management (handled at storage level)
- Batch optimization for bulk mutations (future work)
- CALL procedures or stored procedures
- FOREACH clause (future work)
- CREATE CONSTRAINT / CREATE INDEX (schema operations - future work)

---

## 1. Mutation Clauses

### 1.1 `CREATE` - Create Elements

Creates new vertices and/or edges using pattern syntax.

**GQL Syntax:**
```sql
-- Create a vertex
CREATE (n:Person {name: 'Alice', age: 30})

-- Create multiple vertices
CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})

-- Create vertex and edge together
CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})

-- Create edge between matched vertices
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2024}]->(b)
RETURN a, b
```

**Behavior:**
- Creates new vertices with specified labels and properties
- Creates new edges between vertices (new or matched)
- Variables bound in CREATE can be used in subsequent clauses (RETURN, SET, etc.)
- Multiple CREATE clauses can appear in a single query
- When creating edges, both endpoints must be resolvable (from MATCH or CREATE)
- Returns created elements if RETURN clause references them

**Return Type:** Created elements available for subsequent clauses

### 1.2 `SET` - Update Properties

Updates or adds properties on matched elements.

**GQL Syntax:**
```sql
-- Set a single property
MATCH (n:Person {name: 'Alice'})
SET n.age = 31
RETURN n

-- Set multiple properties
MATCH (n:Person {name: 'Alice'})
SET n.age = 31, n.city = 'New York'
RETURN n

-- Set properties from another node
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
SET a.city = b.city
RETURN a

-- Add a label (future enhancement)
-- MATCH (n:Person {name: 'Alice'})
-- SET n:Employee
-- RETURN n

-- Set multiple properties using map (future enhancement)
-- MATCH (n:Person {name: 'Alice'})
-- SET n += {age: 31, city: 'NYC'}
-- RETURN n
```

**Behavior:**
- Updates existing properties or adds new properties if not present
- Property values can be literals or expressions referencing other properties
- Multiple SET clauses or comma-separated assignments in one SET clause
- The modified element is passed to subsequent clauses
- SET without MATCH is an error (nothing to update)

**Return Type:** Modified elements

### 1.3 `REMOVE` - Remove Properties

Removes properties from matched elements.

**GQL Syntax:**
```sql
-- Remove a property
MATCH (n:Person {name: 'Alice'})
REMOVE n.age
RETURN n

-- Remove multiple properties
MATCH (n:Person {name: 'Alice'})
REMOVE n.age, n.city
RETURN n

-- Remove a label (future enhancement)
-- MATCH (n:Person:Employee {name: 'Alice'})
-- REMOVE n:Employee
-- RETURN n
```

**Behavior:**
- Removes specified properties from the element
- Removing a non-existent property is a no-op (no error)
- Multiple properties can be removed in one REMOVE clause
- The modified element is passed to subsequent clauses

**Return Type:** Modified elements

### 1.4 `DELETE` - Delete Elements

Deletes matched elements from the graph.

**GQL Syntax:**
```sql
-- Delete a vertex (fails if edges exist)
MATCH (n:Person {name: 'Alice'})
DELETE n

-- Delete an edge
MATCH (a:Person)-[r:KNOWS]->(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
DELETE r

-- Delete multiple elements
MATCH (a:Person {status: 'inactive'})-[r]-()
DELETE r, a
```

**Behavior:**
- Removes elements from the graph
- Deleting a vertex that has edges will fail with an error (use DETACH DELETE)
- Deleting an edge removes only the edge, not the connected vertices
- Multiple elements can be deleted in one DELETE clause
- DELETE is a terminal operation (no meaningful return value)

**Error Cases:**
- `VertexHasEdges` - Attempt to DELETE a vertex with connected edges

### 1.5 `DETACH DELETE` - Delete with Edge Cascade

Deletes vertices and automatically removes all connected edges.

**GQL Syntax:**
```sql
-- Delete vertex and all its edges
MATCH (n:Person {name: 'Alice'})
DETACH DELETE n

-- Bulk delete with cascade
MATCH (n:Person {status: 'deleted'})
DETACH DELETE n
```

**Behavior:**
- First removes all edges connected to the vertex (both incoming and outgoing)
- Then removes the vertex itself
- Equivalent to: `MATCH (n)-[r]-() DELETE r` followed by `DELETE n`
- Safe operation that won't fail due to existing edges

**Return Type:** Void (terminal operation)

### 1.6 `MERGE` - Upsert Elements

Creates elements if they don't exist, or matches existing elements.

**GQL Syntax:**
```sql
-- Merge a vertex (create if not exists)
MERGE (n:Person {name: 'Alice'})
RETURN n

-- Merge with ON CREATE and ON MATCH actions
MERGE (n:Person {name: 'Alice'})
ON CREATE SET n.created = timestamp()
ON MATCH SET n.lastSeen = timestamp()
RETURN n

-- Merge an edge between existing vertices
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
MERGE (a)-[r:KNOWS]->(b)
ON CREATE SET r.since = 2024
RETURN r

-- Merge with full pattern
MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
RETURN a, r, b
```

**Behavior:**
- Searches for existing elements matching the pattern
- If found, binds variables to existing elements
- If not found, creates new elements
- `ON CREATE SET` only executes when creating new elements
- `ON MATCH SET` only executes when matching existing elements
- Properties in the pattern are used as the match criteria

**Matching Logic:**
- Vertices match by label(s) AND specified properties
- Edges match by type, direction, AND connected vertices

---

## 2. Grammar Extensions

### 2.1 New Keywords

```pest
CREATE   = @{ ^"create" ~ !ASCII_ALPHANUMERIC }
SET      = @{ ^"set" ~ !ASCII_ALPHANUMERIC }
REMOVE   = @{ ^"remove" ~ !ASCII_ALPHANUMERIC }
DELETE   = @{ ^"delete" ~ !ASCII_ALPHANUMERIC }
DETACH   = @{ ^"detach" ~ !ASCII_ALPHANUMERIC }
MERGE    = @{ ^"merge" ~ !ASCII_ALPHANUMERIC }
ON       = @{ ^"on" ~ !ASCII_ALPHANUMERIC }
```

### 2.2 Statement Grammar

```pest
// Updated statement to support mutation queries
statement = { SOI ~ mutation_query ~ EOI | SOI ~ query ~ (union_clause ~ query)* ~ EOI }

// Mutation query structure
mutation_query = { 
    // CREATE-only query
    create_clause+ ~ return_clause?
    // MATCH with mutations
    | match_clause ~ optional_match_clause* ~ where_clause? ~ mutation_clauses+ ~ return_clause?
    // MERGE query
    | merge_clause ~ merge_action* ~ return_clause?
}

mutation_clauses = { create_clause | set_clause | remove_clause | delete_clause | detach_delete_clause }

// CREATE clause
create_clause = { CREATE ~ pattern ~ ("," ~ pattern)* }

// SET clause
set_clause = { SET ~ set_item ~ ("," ~ set_item)* }
set_item = { property_access ~ "=" ~ expression }

// REMOVE clause
remove_clause = { REMOVE ~ property_access ~ ("," ~ property_access)* }

// DELETE clause
delete_clause = { DELETE ~ variable ~ ("," ~ variable)* }

// DETACH DELETE clause
detach_delete_clause = { DETACH ~ DELETE ~ variable ~ ("," ~ variable)* }

// MERGE clause
merge_clause = { MERGE ~ pattern }
merge_action = { on_create | on_match }
on_create = { ON ~ CREATE ~ SET ~ set_item ~ ("," ~ set_item)* }
on_match = { ON ~ MATCH ~ SET ~ set_item ~ ("," ~ set_item)* }
```

---

## 3. AST Extensions

### 3.1 Statement Types

```rust
/// Extended Statement to support mutations
#[derive(Debug, Clone, Serialize)]
pub enum Statement {
    /// A read-only query
    Query(Box<Query>),
    /// A UNION of multiple queries
    Union { queries: Vec<Query>, all: bool },
    /// A mutation query
    Mutation(Box<MutationQuery>),
}

/// A mutation query (CREATE, SET, DELETE, MERGE, etc.)
#[derive(Debug, Clone, Serialize)]
pub struct MutationQuery {
    /// Optional MATCH clause for pattern-based mutations
    pub match_clause: Option<MatchClause>,
    /// Optional MATCH clauses
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    /// Optional WHERE clause for filtering
    pub where_clause: Option<WhereClause>,
    /// List of mutation clauses (CREATE, SET, DELETE, etc.)
    pub mutations: Vec<MutationClause>,
    /// Optional RETURN clause
    pub return_clause: Option<ReturnClause>,
}
```

### 3.2 Mutation Clause Types

```rust
/// A mutation clause (CREATE, SET, DELETE, etc.)
#[derive(Debug, Clone, Serialize)]
pub enum MutationClause {
    Create(CreateClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    DetachDelete(DetachDeleteClause),
    Merge(MergeClause),
}

/// CREATE clause - creates new vertices and edges
#[derive(Debug, Clone, Serialize)]
pub struct CreateClause {
    /// Patterns to create
    pub patterns: Vec<Pattern>,
}

/// SET clause - updates properties
#[derive(Debug, Clone, Serialize)]
pub struct SetClause {
    /// Property assignments
    pub items: Vec<SetItem>,
}

/// A single SET assignment
#[derive(Debug, Clone, Serialize)]
pub struct SetItem {
    /// The property to set (e.g., n.age)
    pub target: PropertyAccess,
    /// The value expression
    pub value: Expression,
}

/// REMOVE clause - removes properties
#[derive(Debug, Clone, Serialize)]
pub struct RemoveClause {
    /// Properties to remove
    pub properties: Vec<PropertyAccess>,
}

/// DELETE clause - deletes elements
#[derive(Debug, Clone, Serialize)]
pub struct DeleteClause {
    /// Variables referencing elements to delete
    pub variables: Vec<String>,
}

/// DETACH DELETE clause - deletes vertices with edge cascade
#[derive(Debug, Clone, Serialize)]
pub struct DetachDeleteClause {
    /// Variables referencing vertices to delete
    pub variables: Vec<String>,
}

/// MERGE clause - upsert operation
#[derive(Debug, Clone, Serialize)]
pub struct MergeClause {
    /// Pattern to merge
    pub pattern: Pattern,
    /// Actions to perform on create
    pub on_create: Option<Vec<SetItem>>,
    /// Actions to perform on match
    pub on_match: Option<Vec<SetItem>>,
}

/// Property access reference (e.g., n.age)
#[derive(Debug, Clone, Serialize)]
pub struct PropertyAccess {
    /// Variable name
    pub variable: String,
    /// Property name
    pub property: String,
}
```

---

## 4. Compilation to Traversal Steps

GQL mutations compile to the existing Gremlin-style mutation steps from Spec 10.

### 4.1 CREATE Compilation

```rust
// GQL: CREATE (n:Person {name: 'Alice', age: 30})
// Compiles to:
g.add_v("Person")
    .property("name", "Alice")
    .property("age", 30)
    .next()

// GQL: MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
//      CREATE (a)-[:KNOWS {since: 2024}]->(b)
// Compiles to:
g.v().has_label("Person").has_value("name", "Alice").as_("a")
    .v().has_label("Person").has_value("name", "Bob").as_("b")
    .select("a")
    .add_e("KNOWS")
    .to_label("b")
    .property("since", 2024)
    .next()
```

### 4.2 SET Compilation

```rust
// GQL: MATCH (n:Person {name: 'Alice'}) SET n.age = 31
// Compiles to:
g.v().has_label("Person").has_value("name", "Alice")
    .property("age", 31)
    .iterate()
```

### 4.3 DELETE Compilation

```rust
// GQL: MATCH (n:Person {name: 'Alice'}) DELETE n
// Compiles to (with edge check):
g.v().has_label("Person").has_value("name", "Alice")
    // First check for edges - error if any exist
    .filter(|v| v.both_e().count() == 0)
    .drop()
    .iterate()

// GQL: MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
// Compiles to:
g.v().has_label("Person").has_value("name", "Alice")
    .drop()  // drop() cascades to edges
    .iterate()
```

### 4.4 MERGE Compilation

```rust
// GQL: MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 123
// Compiles to (pseudo-code):
let existing = g.v().has_label("Person").has_value("name", "Alice").to_list();
if existing.is_empty() {
    g.add_v("Person")
        .property("name", "Alice")
        .property("created", 123)
        .next()
} else {
    existing[0]
}
```

---

## 5. Error Handling

### 5.1 Error Types

```rust
#[derive(Debug, Error)]
pub enum MutationError {
    #[error("cannot delete vertex with existing edges: {0:?} (use DETACH DELETE)")]
    VertexHasEdges(VertexId),
    
    #[error("cannot create edge: source vertex not found")]
    EdgeSourceNotFound,
    
    #[error("cannot create edge: target vertex not found")]
    EdgeTargetNotFound,
    
    #[error("unbound variable in mutation: {0}")]
    UnboundVariable(String),
    
    #[error("cannot SET without MATCH clause")]
    SetWithoutMatch,
    
    #[error("cannot DELETE without MATCH clause")]
    DeleteWithoutMatch,
    
    #[error("invalid property assignment: {0}")]
    InvalidPropertyAssignment(String),
    
    #[error("MERGE pattern must include at least one identifying property")]
    MergeRequiresProperties,
}
```

### 5.2 Validation Rules

1. **CREATE validation:**
   - All labels must be valid identifiers
   - Property values must be valid expressions
   - Edge patterns must have resolvable endpoints

2. **SET validation:**
   - Must have preceding MATCH or CREATE that binds the variable
   - Property keys must be valid identifiers
   - Expression must be evaluable

3. **DELETE validation:**
   - Must have preceding MATCH that binds the variable
   - For plain DELETE, vertices must not have edges

4. **MERGE validation:**
   - Pattern must include identifying properties (empty properties not allowed)
   - ON CREATE/ON MATCH actions must reference valid properties

---

## 6. Query Structure

### 6.1 Valid Mutation Query Patterns

```sql
-- CREATE only
CREATE (n:Person {name: 'Alice'})

-- CREATE with RETURN
CREATE (n:Person {name: 'Alice'})
RETURN n

-- MATCH + SET
MATCH (n:Person {name: 'Alice'})
SET n.age = 31

-- MATCH + SET + RETURN
MATCH (n:Person {name: 'Alice'})
SET n.age = 31
RETURN n

-- MATCH + CREATE (edge)
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)

-- MATCH + DELETE
MATCH (n:Person {status: 'inactive'})
DELETE n

-- MATCH + DETACH DELETE
MATCH (n:Person {status: 'deleted'})
DETACH DELETE n

-- MERGE with actions
MERGE (n:Person {name: 'Alice'})
ON CREATE SET n.created = 123
ON MATCH SET n.updated = 456
RETURN n

-- Multiple mutations
MATCH (n:Person {name: 'Alice'})
SET n.status = 'active'
CREATE (n)-[:LOGGED_IN {at: 123}]->(:Event {type: 'login'})
RETURN n
```

### 6.2 Clause Ordering

Valid clause order:
1. MATCH (optional, but required for SET/DELETE)
2. OPTIONAL MATCH (optional, after MATCH)
3. WHERE (optional, after MATCH)
4. CREATE / SET / REMOVE / DELETE / DETACH DELETE / MERGE (one or more)
5. RETURN (optional)

Note: No ORDER BY, GROUP BY, or LIMIT for mutations (these are read-only features).

---

## 7. Testing Requirements

### 7.1 Parser Tests

- Parse CREATE with single vertex
- Parse CREATE with vertex and properties
- Parse CREATE with edge between variables
- Parse SET with single property
- Parse SET with multiple properties
- Parse REMOVE with properties
- Parse DELETE with single variable
- Parse DELETE with multiple variables
- Parse DETACH DELETE
- Parse MERGE with pattern
- Parse MERGE with ON CREATE SET
- Parse MERGE with ON MATCH SET
- Parse MERGE with both actions
- Parse combined MATCH + CREATE
- Parse combined MATCH + SET + RETURN
- Error on SET without MATCH
- Error on DELETE without MATCH

### 7.2 Compiler Tests

- CREATE compiles to add_v step
- CREATE with properties compiles to add_v + property steps
- CREATE edge compiles to add_e step
- SET compiles to property step
- DELETE compiles to drop step (with edge check)
- DETACH DELETE compiles to drop step
- MATCH + CREATE compiles correctly
- MERGE compiles to conditional create/match logic

### 7.3 Integration Tests

- Create vertex via GQL, read back via traversal
- Create edge via GQL, verify connectivity
- Update property via SET, verify change
- Remove property via REMOVE, verify removal
- Delete edge via DELETE
- Delete vertex via DETACH DELETE
- MERGE creates when not exists
- MERGE matches when exists
- Complex multi-clause mutations

### 7.4 Error Case Tests

- DELETE vertex with edges fails (not DETACH)
- CREATE edge to non-existent vertex fails
- SET on unbound variable fails
- MERGE without properties fails
- Invalid property expressions fail

---

## 8. Example Usage

```rust
use interstellar::gql::{parse, compile, execute};
use interstellar::Graph;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = Graph::new();
    
    // Create vertices
    execute(&graph, "CREATE (a:Person {name: 'Alice', age: 30})")?;
    execute(&graph, "CREATE (b:Person {name: 'Bob', age: 32})")?;
    
    // Create relationship
    execute(&graph, "
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS {since: 2020}]->(b)
    ")?;
    
    // Update property
    execute(&graph, "
        MATCH (n:Person {name: 'Alice'})
        SET n.age = 31
    ")?;
    
    // Query the graph
    let results = execute(&graph, "
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.name, b.name
    ")?;
    // Results: [{"a.name": "Alice", "b.name": "Bob"}]
    
    // Upsert pattern
    execute(&graph, "
        MERGE (c:Person {name: 'Charlie'})
        ON CREATE SET c.created = 1234567890
        ON MATCH SET c.lastSeen = 1234567890
    ")?;
    
    // Delete with cascade
    execute(&graph, "
        MATCH (n:Person {name: 'Bob'})
        DETACH DELETE n
    ")?;
    
    Ok(())
}
```

---

## 9. Future Enhancements

- Label mutations: `SET n:Label`, `REMOVE n:Label`
- Map property assignment: `SET n += {key: value}`
- `FOREACH` clause for iterative mutations
- Schema constraints: `CREATE CONSTRAINT`, `CREATE INDEX`
- Returning mutation counts: `RETURN count(*) AS deleted`
- Conditional mutations with CASE expressions
- Subquery mutations
