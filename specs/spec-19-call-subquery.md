# Spec 19: CALL Subquery

## Overview

This specification defines the CALL subquery feature for Intersteller's GQL implementation. CALL subqueries allow executing a nested query block within a larger query, enabling correlated computations, nested aggregations, and query composition.

## Motivation

Currently, Intersteller's GQL lacks the ability to:

1. **Compute correlated aggregations** - Calculate per-row aggregates that depend on outer query variables
2. **Compose query fragments** - Build complex queries from smaller, reusable parts
3. **Perform nested operations** - Execute different operations per matched row
4. **Post-UNION processing** - Apply filtering/aggregation after combining results

CALL subqueries are a core feature in ISO GQL (ISO/IEC 39075:2024) and Cypher, and are essential for expressing complex graph analytics.

## Syntax

### Basic CALL Subquery

```sql
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f:Person)
    RETURN count(f) AS friendCount
}
RETURN p.name, friendCount
```

### CALL Without WITH (Uncorrelated)

```sql
MATCH (p:Person)
CALL {
    MATCH (t:Team)
    RETURN count(t) AS totalTeams
}
RETURN p.name, totalTeams
```

### CALL with UNION Inside

```sql
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:OWNS]->(c:Car)
    RETURN c.model AS vehicle
    UNION
    WITH p
    MATCH (p)-[:OWNS]->(b:Bike)
    RETURN b.model AS vehicle
}
RETURN p.name, vehicle
```

### Multiple CALL Clauses

```sql
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f)
    RETURN count(f) AS friendCount
}
CALL {
    WITH p
    MATCH (p)-[:WORKS_AT]->(c:Company)
    RETURN c.name AS company
}
RETURN p.name, friendCount, company
```

### CALL in Different Positions

```sql
-- After WHERE
MATCH (p:Person)
WHERE p.age > 21
CALL {
    WITH p
    MATCH (p)-[:POSTED]->(post)
    RETURN count(post) AS postCount
}
RETURN p.name, postCount

-- Between WITH clauses
MATCH (p:Person)-[:LIVES_IN]->(city:City)
WITH p, city
CALL {
    WITH city
    MATCH (city)<-[:LIVES_IN]-(resident)
    RETURN count(resident) AS population
}
RETURN p.name, city.name, population
```

## Grammar Extension

Add to `grammar.pest`:

```pest
// CALL subquery clause
call_clause = { CALL ~ "{" ~ call_body ~ "}" }

// Body of a CALL subquery - can be a single query or UNION
call_body = { 
    call_query ~ (union_clause ~ call_query)* 
}

// Query inside CALL - starts with optional WITH for importing variables
call_query = { 
    importing_with? ~ 
    match_clause? ~ 
    optional_match_clause* ~ 
    where_clause? ~ 
    call_clause* ~
    with_clause* ~
    return_clause 
}

// WITH clause that imports variables from outer scope
// Distinguished from regular WITH by position (must be first in call_query)
importing_with = { WITH ~ return_item ~ ("," ~ return_item)* }

// Add CALL keyword
CALL = @{ ^"call" ~ !ASCII_ALPHANUMERIC }

// Update query to include call_clause
query = { 
    match_clause ~ 
    optional_match_clause* ~ 
    with_path_clause? ~ 
    unwind_clause* ~ 
    where_clause? ~ 
    call_clause* ~        // <-- Add here
    let_clause* ~ 
    with_clause* ~ 
    return_clause ~ 
    group_by_clause? ~ 
    having_clause? ~ 
    order_clause? ~ 
    limit_clause? 
}
```

## AST Extensions

### New AST Types

```rust
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

/// Body of a CALL subquery.
#[derive(Debug, Clone, Serialize)]
pub enum CallBody {
    /// A single subquery.
    Single(Box<CallQuery>),
    /// A UNION of subqueries.
    Union {
        queries: Vec<CallQuery>,
        all: bool,
    },
}

/// A query inside a CALL clause.
///
/// Similar to a regular Query but:
/// - May start with an importing WITH (to bring outer variables into scope)
/// - MATCH is optional (can just transform imported variables)
/// - Must have a RETURN clause
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
```

### Update Query Struct

```rust
/// A complete GQL query.
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    pub match_clause: MatchClause,
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    pub with_path_clause: Option<WithPathClause>,
    pub unwind_clauses: Vec<UnwindClause>,
    pub where_clause: Option<WhereClause>,
    pub call_clauses: Vec<CallClause>,  // <-- Add this field
    pub let_clauses: Vec<LetClause>,
    pub with_clauses: Vec<WithClause>,
    pub return_clause: ReturnClause,
    pub group_by_clause: Option<GroupByClause>,
    pub having_clause: Option<HavingClause>,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}
```

## Semantic Rules

### Variable Scoping

1. **Importing variables**: Variables listed in the importing WITH must exist in the outer scope
2. **Shadowing forbidden**: Subquery cannot RETURN a variable that already exists in outer scope (unless it's the same variable imported and passed through)
3. **Subquery isolation**: Variables defined inside the subquery (other than returned ones) are not visible outside

```sql
-- Valid: imports p, returns new variable friendCount
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f)
    RETURN count(f) AS friendCount  -- new variable, OK
}
RETURN p.name, friendCount

-- Invalid: tries to return 'p' which already exists in outer scope
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f)
    RETURN p, count(f) AS friendCount  -- ERROR: p already in scope
}
RETURN p.name, friendCount
```

### Cardinality Rules

1. **Correlated subquery**: Executes once per outer row; result rows are combined with that outer row
2. **Uncorrelated subquery**: Executes once; result is cross-joined with all outer rows
3. **Empty results**: If subquery returns no rows for a given outer row, that outer row is excluded (like an inner join)

```sql
-- Correlated: if Alice has 3 friends and Bob has 2, output has 2 rows
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f)
    RETURN count(f) AS cnt
}
RETURN p.name, cnt  -- Returns: [('Alice', 3), ('Bob', 2)]

-- If Charlie has no friends, Charlie is excluded from results
-- To keep Charlie, use OPTIONAL MATCH inside the CALL
```

### Aggregation Behavior

1. **Per-row aggregation**: When correlated, aggregates apply per outer row
2. **Global aggregation**: When uncorrelated, aggregates apply globally

```sql
-- Per-row: count is per person
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f)
    RETURN count(f) AS friendCount  -- different per p
}
RETURN p.name, friendCount

-- Global: same count for all rows
MATCH (p:Person)
CALL {
    MATCH (:Person)-[:FRIEND]->(:Person)
    RETURN count(*) AS totalFriendships  -- same for all
}
RETURN p.name, totalFriendships
```

## Execution Model

### Algorithm for Correlated CALL

```
function execute_correlated_call(outer_rows, call_clause):
    result_rows = []
    
    for each outer_row in outer_rows:
        # Extract imported variables
        imported_vars = extract_imported_variables(outer_row, call_clause.importing_with)
        
        # Create subquery scope with imported variables
        subquery_scope = create_scope(imported_vars)
        
        # Execute subquery in this scope
        subquery_results = execute_query(call_clause.body, subquery_scope)
        
        # Combine outer row with each subquery result
        for each sub_row in subquery_results:
            combined = merge_rows(outer_row, sub_row)
            result_rows.append(combined)
    
    return result_rows
```

### Algorithm for Uncorrelated CALL

```
function execute_uncorrelated_call(outer_rows, call_clause):
    # Execute subquery once
    subquery_results = execute_query(call_clause.body, empty_scope)
    
    # Cross-join with outer rows
    result_rows = []
    for each outer_row in outer_rows:
        for each sub_row in subquery_results:
            combined = merge_rows(outer_row, sub_row)
            result_rows.append(combined)
    
    return result_rows
```

## Examples

### Example 1: Friend Count with Filtering

```sql
-- Find people with more than 5 friends
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:FRIEND]->(f:Person)
    RETURN count(f) AS friendCount
}
WHERE friendCount > 5
RETURN p.name, friendCount
ORDER BY friendCount DESC
```

### Example 2: Top N Per Group

```sql
-- Get top 3 highest-scoring players per team
MATCH (t:Team)
CALL {
    WITH t
    MATCH (t)<-[:PLAYS_FOR]-(p:Player)
    RETURN p
    ORDER BY p.score DESC
    LIMIT 3
}
RETURN t.name, collect(p.name) AS topPlayers
```

### Example 3: Conditional Subquery with UNION

```sql
-- Get all vehicles (cars or bikes) owned by a person
MATCH (p:Person)
CALL {
    WITH p
    MATCH (p)-[:OWNS]->(c:Car)
    RETURN c.model AS vehicle, 'car' AS type
    UNION ALL
    WITH p
    MATCH (p)-[:OWNS]->(b:Bike)
    RETURN b.model AS vehicle, 'bike' AS type
}
RETURN p.name, vehicle, type
```

### Example 4: Existence Check Alternative

```sql
-- Equivalent to EXISTS but with more flexibility
MATCH (p:Person)
CALL {
    WITH p
    OPTIONAL MATCH (p)-[:WON]->(trophy:Trophy)
    RETURN count(trophy) > 0 AS hasWon
}
WHERE hasWon = true
RETURN p.name
```

### Example 5: Nested CALL

```sql
-- Nested subqueries for complex aggregations
MATCH (company:Company)
CALL {
    WITH company
    MATCH (company)<-[:WORKS_AT]-(emp:Person)
    CALL {
        WITH emp
        MATCH (emp)-[:MANAGES]->(report:Person)
        RETURN count(report) AS reportCount
    }
    RETURN emp, reportCount
}
WHERE reportCount > 0
RETURN company.name, collect(emp.name) AS managers
```

## Error Handling

### Compile-Time Errors

| Error | Description |
|-------|-------------|
| `UnboundVariable` | Importing WITH references variable not in outer scope |
| `DuplicateVariable` | RETURN exports variable that already exists in outer scope |
| `MissingReturn` | CALL subquery body has no RETURN clause |
| `InvalidImportingWith` | WITH in subquery doesn't match importing pattern |

### Runtime Errors

| Error | Description |
|-------|-------------|
| `SubqueryExecutionError` | Wrapped error from subquery execution |

## Compiler Implementation Notes

### Integration with Existing Compiler

The compiler changes should:

1. **Parser**: Add `call_clause` rule and parse into `CallClause` AST
2. **Variable tracking**: Track outer scope variables; validate imports/exports
3. **Execution**: 
   - Detect correlated vs uncorrelated based on presence of importing WITH
   - For correlated: iterate outer rows, execute subquery per row
   - For uncorrelated: execute once, cross-join

### Row-Based Execution Path

CALL subqueries integrate naturally with the existing row-based execution path used for UNWIND, LET, and WITH:

```rust
// In compiler.rs, after processing MATCH/WHERE/UNWIND:

// Process CALL clauses
for call_clause in &query.call_clauses {
    current_rows = self.execute_call_clause(current_rows, call_clause)?;
}

fn execute_call_clause(
    &self,
    rows: Vec<HashMap<String, Value>>,
    call_clause: &CallClause,
) -> Result<Vec<HashMap<String, Value>>, CompileError> {
    let is_correlated = call_clause.has_importing_with();
    
    if is_correlated {
        self.execute_correlated_call(rows, call_clause)
    } else {
        self.execute_uncorrelated_call(rows, call_clause)
    }
}
```

## Testing Strategy

### Unit Tests

1. **Parser tests**: Verify grammar correctly parses CALL syntax variations
2. **AST tests**: Verify AST structure for parsed CALL clauses
3. **Scope tests**: Verify variable scoping rules (import, export, shadowing)

### Integration Tests

1. **Correlated aggregation**: Count, sum, avg per outer row
2. **Uncorrelated cross-join**: Global computation joined with all rows
3. **UNION inside CALL**: Multiple subqueries combined
4. **Nested CALL**: Subqueries containing subqueries
5. **Empty results**: Outer rows excluded when subquery returns nothing
6. **OPTIONAL MATCH in CALL**: Keeping outer rows with null subquery results

### Snapshot Tests

Add snapshot tests for parsed AST of various CALL patterns.

## Performance Considerations

1. **Correlated execution**: O(outer_rows * subquery_cost) - can be expensive
2. **Caching potential**: Uncorrelated subqueries should execute once
3. **Future optimization**: Query planner could potentially decorrelate some patterns

## Future Extensions

1. **CALL YIELD**: For calling procedures (`CALL db.labels() YIELD label`)
2. **Unit subqueries**: Subqueries that return exactly one row (scalar subqueries)
3. **Lateral joins**: Explicit LATERAL keyword for clearer semantics

## References

- ISO/IEC 39075:2024 (GQL Standard) - Subquery specifications
- Neo4j Cypher Manual - CALL subqueries
- openCypher Specification - Subquery semantics
