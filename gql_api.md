# GQL API Reference

This document provides comprehensive documentation for the GQL (Graph Query Language) implementation in Intersteller.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Query Syntax Reference](#query-syntax-reference)
5. [Pattern Matching](#pattern-matching)
6. [Expression Types](#expression-types)
7. [Operators](#operators)
8. [Built-in Functions](#built-in-functions)
9. [Aggregation](#aggregation)
10. [Advanced Features](#advanced-features)
11. [Mutation Operations](#mutation-operations)
12. [Error Handling](#error-handling)
13. [Limitations](#limitations)

---

## Overview

GQL is a declarative query language for property graphs, offering a SQL-like syntax for pattern matching, data retrieval, and mutations. The Intersteller GQL implementation provides:

- **Pattern Matching**: Find subgraphs using intuitive ASCII-art syntax
- **Filtering**: WHERE clause with comparison, logical, and string operators
- **Projection**: RETURN clause for selecting and transforming results
- **Aggregation**: COUNT, SUM, AVG, MIN, MAX, COLLECT functions
- **Sorting & Pagination**: ORDER BY, LIMIT, OFFSET
- **Mutations**: CREATE, SET, REMOVE, DELETE, DETACH DELETE, MERGE
- **Advanced Features**: UNION, OPTIONAL MATCH, EXISTS, CASE expressions, WITH PATH, WITH clause
- **Query Parameters**: Parameterized queries with `$paramName` syntax
- **LET Clause**: Bind intermediate computed values to variables
- **List Comprehensions**: Transform and filter lists with `[x IN list | expr]` syntax
- **Map Literals**: Create map values with `{key: value}` syntax
- **String Concatenation**: `||` operator for string operations
- **Inline WHERE**: Filter patterns directly within node/edge definitions
- **Regular Expressions**: Pattern matching with `=~` operator
- **REDUCE Function**: Fold/accumulate over lists
- **List Predicates**: ALL, ANY, NONE, SINGLE quantifier expressions
- **HAVING Clause**: Filter aggregated results post-GROUP BY

---

## Architecture

### Pipeline

The GQL implementation follows a pipeline architecture:

```
GQL Query Text → Parser (pest) → AST → Compiler/Executor → Results
```

| Stage | Description |
|-------|-------------|
| **Parser** | Converts GQL text into a typed AST using pest PEG grammar |
| **AST** | Typed representation of query structure |
| **Compiler** | Transforms read-only AST into traversal operations |
| **Mutation Executor** | Executes mutation statements directly on storage |

### Module Structure

```
src/gql/
├── mod.rs        # Public API exports
├── grammar.pest  # PEG grammar definition (344 lines)
├── ast.rs        # AST type definitions
├── parser.rs     # Parser implementation
├── compiler.rs   # Query compiler for read operations
├── mutation.rs   # Mutation execution engine
└── error.rs      # Error types (ParseError, CompileError, MutationError)
```

### Read vs Write Operations

| Operation Type | Access | Entry Point |
|---------------|--------|-------------|
| Read queries | `GraphSnapshot` (immutable) | `snapshot.gql(query)` or `compile(&query, &snapshot)` |
| Mutations | `GraphStorageMut` (mutable) | `execute_mutation(&stmt, &mut storage)` |

---

## Quick Start

### Read Queries

The simplest way to execute a GQL query:

```rust
use intersteller::prelude::*;
use intersteller::storage::InMemoryGraph;
use std::sync::Arc;

// Create a graph with data
let mut storage = InMemoryGraph::new();
let mut props = std::collections::HashMap::new();
props.insert("name".to_string(), Value::from("Alice"));
props.insert("age".to_string(), Value::from(30i64));
storage.add_vertex("Person", props);

let graph = Graph::new(Arc::new(storage));
let snapshot = graph.snapshot();

// Execute GQL query
let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
assert_eq!(results.len(), 1);
```

### Mutations

For mutations (CREATE, SET, DELETE, etc.), use `execute_mutation` with mutable storage:

```rust
use intersteller::gql::{parse_statement, execute_mutation};
use intersteller::storage::{GraphStorage, InMemoryGraph};

let mut storage = InMemoryGraph::new();

// CREATE a new vertex
let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
execute_mutation(&stmt, &mut storage).unwrap();

assert_eq!(storage.vertex_count(), 1);

// UPDATE with SET
let stmt = parse_statement("MATCH (n:Person {name: 'Alice'}) SET n.age = 31").unwrap();
execute_mutation(&stmt, &mut storage).unwrap();

// DELETE
let stmt = parse_statement("MATCH (n:Person {name: 'Alice'}) DELETE n").unwrap();
execute_mutation(&stmt, &mut storage).unwrap();
```

---

## Query Syntax Reference

### Complete Query Structure

```
[MATCH pattern [, pattern ...]]
[OPTIONAL MATCH pattern [, pattern ...]]
[WITH PATH [AS alias]]
[UNWIND expression AS variable]
[WHERE expression]
[LET variable = expression]...
[WITH [DISTINCT] expression [AS alias] [, ...]
  [WHERE expression]
  [ORDER BY expression [ASC|DESC] [, ...]]
  [LIMIT n [OFFSET|SKIP m]]]...
RETURN [DISTINCT] expression [AS alias] [, ...]
[GROUP BY expression [, ...]]
[HAVING expression]
[ORDER BY expression [ASC|DESC] [, ...]]
[LIMIT n [OFFSET|SKIP m]]
[UNION [ALL] query]
```

### MATCH Clause

The `MATCH` clause specifies patterns to find in the graph.

```sql
-- Match all Person vertices
MATCH (n:Person) RETURN n

-- Match with property constraint
MATCH (n:Person {name: 'Alice'}) RETURN n

-- Match connected vertices
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b

-- Multiple patterns (comma-separated)
MATCH (a:Person), (b:Team) RETURN a, b
```

### OPTIONAL MATCH Clause

`OPTIONAL MATCH` matches patterns if possible, producing `null` values if no match is found (similar to SQL LEFT JOIN).

```sql
-- Find all players, with their championship teams (if any)
MATCH (p:Player)
OPTIONAL MATCH (p)-[:won_championship_with]->(t:Team)
RETURN p.name, t.name

-- Players without championships will have null for t.name
```

### WITH PATH Clause

Enables path tracking for retrieving the traversal path using the `path()` function.

```sql
MATCH (p1:Player)-[:played_for]->(t:Team)<-[:played_for]-(p2:Player)
WITH PATH
RETURN path(), p2.name
```

### UNWIND Clause

Expands a list into individual rows.

```sql
-- Expand a literal list
UNWIND [1, 2, 3] AS num
RETURN num * 2
-- Returns: 2, 4, 6

-- Expand collected values
MATCH (p:Player)
UNWIND collect(p.name) AS name
RETURN name
```

### WHERE Clause

Filters results using boolean expressions.

```sql
-- Comparison operators
MATCH (p:Person) WHERE p.age > 25 RETURN p

-- Combined conditions
MATCH (p:Person)
WHERE p.age >= 25 AND p.age <= 35
RETURN p

-- String matching
MATCH (p:Person)
WHERE p.name STARTS WITH 'A'
RETURN p

-- Null checks
MATCH (p:Person)
WHERE p.email IS NOT NULL
RETURN p

-- List membership
MATCH (p:Person)
WHERE p.status IN ['active', 'pending']
RETURN p

-- EXISTS subquery
MATCH (p:Player)
WHERE EXISTS { (p)-[:won_championship_with]->() }
RETURN p.name
```

### RETURN Clause

Specifies what data to return.

```sql
-- Return entire vertex
MATCH (n:Person) RETURN n

-- Return specific properties
MATCH (n:Person) RETURN n.name, n.age

-- With aliases
MATCH (n:Person) RETURN n.name AS personName, n.age AS years

-- Return distinct values
MATCH (n:Person) RETURN DISTINCT n.city

-- Return literals
MATCH (n:Person) RETURN n.name, 'constant' AS label

-- Return computed expressions
MATCH (n:Person) RETURN n.name, n.age * 12 AS ageInMonths
```

### GROUP BY Clause

Groups results for aggregation.

```sql
-- Count players by position
MATCH (p:Player)
RETURN p.position, count(*)
GROUP BY p.position

-- Average age by team
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, avg(p.age)
GROUP BY t.name
```

### HAVING Clause

The `HAVING` clause filters results after aggregation, similar to SQL's HAVING. Use it to filter on aggregate values.

```sql
-- Filter groups by aggregate value
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, COUNT(*) AS playerCount
GROUP BY t.name
HAVING playerCount > 10

-- Filter by average
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, AVG(p.points) AS avgPoints
GROUP BY t.name
HAVING avgPoints > 15

-- Multiple conditions in HAVING
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, COUNT(*) AS count, AVG(p.points) AS avg
GROUP BY t.name
HAVING count >= 5 AND avg > 10
```

**HAVING vs WHERE:**

| Clause | When Applied | Use For |
|--------|--------------|---------|
| `WHERE` | Before aggregation | Filter individual rows |
| `HAVING` | After aggregation | Filter aggregated groups |

```sql
-- Combined WHERE and HAVING
MATCH (p:Player)-[:plays_for]->(t:Team)
WHERE p.active = true              -- Filter before grouping
RETURN t.name, COUNT(*) AS count
GROUP BY t.name
HAVING count > 5                   -- Filter after grouping
```

### ORDER BY Clause

Sorts results.

```sql
-- Ascending (default)
MATCH (p:Person) RETURN p ORDER BY p.age

-- Descending
MATCH (p:Person) RETURN p ORDER BY p.age DESC

-- Multiple sort keys
MATCH (p:Person)
RETURN p
ORDER BY p.age DESC, p.name ASC
```

### LIMIT and OFFSET Clauses

Pagination support. `SKIP` is supported as an alias for `OFFSET`.

```sql
-- First 10 results
MATCH (p:Person) RETURN p LIMIT 10

-- Skip 20, take 10 (using OFFSET)
MATCH (p:Person) RETURN p LIMIT 10 OFFSET 20

-- Skip 20, take 10 (using SKIP alias)
MATCH (p:Person) RETURN p LIMIT 10 SKIP 20
```

### UNION Clause

Combines results from multiple queries.

```sql
-- UNION (deduplicates results)
MATCH (p:Player)-[:played_for]->(t:Team) RETURN t.name
UNION
MATCH (p:Player)-[:won_championship_with]->(t:Team) RETURN t.name

-- UNION ALL (keeps duplicates)
MATCH (p:Player)-[:played_for]->(t:Team) RETURN t.name
UNION ALL
MATCH (p:Player)-[:won_championship_with]->(t:Team) RETURN t.name
```

### WITH Clause

The `WITH` clause allows intermediate result projection and filtering within a query. It enables query chaining by passing computed values between query parts.

**Basic Syntax:**

```sql
MATCH (pattern)
WITH expression [AS alias] [, ...]
[WHERE expression]
[ORDER BY expression [ASC|DESC]]
[LIMIT n [OFFSET|SKIP m]]
RETURN ...
```

**Basic Projection:**

```sql
-- Pass selected properties to next stage
MATCH (p:Player)-[:plays_for]->(t:Team)
WITH p.name AS playerName, t.name AS teamName
RETURN playerName, teamName
```

**Aggregation in WITH:**

```sql
-- Count friends and filter by count
MATCH (p:Person)-[:KNOWS]->(friend)
WITH p, COUNT(friend) AS friendCount
WHERE friendCount > 5
RETURN p.name, friendCount

-- Calculate statistics before further processing
MATCH (p:Player)
WITH p.position AS position, AVG(p.points) AS avgPoints, COUNT(*) AS count
WHERE count > 3
RETURN position, avgPoints
ORDER BY avgPoints DESC
```

**WHERE After WITH:**

```sql
-- Filter on computed values
MATCH (p:Player)-[:plays_for]->(t:Team)
WITH t, COUNT(p) AS playerCount
WHERE playerCount >= 10
RETURN t.name, playerCount
```

**WITH DISTINCT:**

```sql
-- Remove duplicate rows
MATCH (p:Player)-[:played_for]->(t:Team)
WITH DISTINCT t.conference AS conference
RETURN conference
```

**ORDER BY and LIMIT in WITH:**

```sql
-- Get top 5 scorers, then find their teams
MATCH (p:Player)
WITH p
ORDER BY p.points DESC
LIMIT 5
RETURN p.name, p.points
```

**Chaining Multiple WITH Clauses:**

```sql
MATCH (p:Player)-[:plays_for]->(t:Team)
WITH t, COUNT(p) AS playerCount
WITH t.name AS teamName, playerCount
WHERE playerCount > 10
RETURN teamName, playerCount
```

**Complete Example:**

```sql
-- Find teams with high-scoring players and get their average
MATCH (p:Player)-[:plays_for]->(t:Team)
WITH t, AVG(p.points) AS avgPoints, MAX(p.points) AS topScore
WHERE avgPoints > 15
RETURN t.name AS team, avgPoints, topScore
ORDER BY avgPoints DESC
LIMIT 10
```

---

## Pattern Matching

Patterns describe the graph structure to match using an intuitive ASCII-art syntax.

### Node Patterns

Node patterns are enclosed in parentheses and can include variable bindings, labels, and property constraints.

| Syntax | Description |
|--------|-------------|
| `(n)` | Any vertex, bound to variable `n` |
| `(n:Person)` | Vertex with label `Person` |
| `(n:Person:Employee)` | Vertex with multiple labels |
| `(n {name: 'Alice'})` | Vertex with property constraint |
| `(n:Person {name: 'Alice'})` | Label and property constraint |
| `(:Person)` | Anonymous (unbound) vertex with label |
| `()` | Any vertex (anonymous) |

**Examples:**

```sql
-- Match any vertex
MATCH (n) RETURN n

-- Match by label
MATCH (p:Person) RETURN p

-- Match by multiple labels
MATCH (e:Person:Employee) RETURN e

-- Match by property value
MATCH (p:Person {name: 'Alice', age: 30}) RETURN p
```

### Edge Patterns

Edge patterns specify relationship types and directions.

| Syntax | Description |
|--------|-------------|
| `-[:KNOWS]->` | Outgoing edge with label `KNOWS` |
| `<-[:KNOWS]-` | Incoming edge with label `KNOWS` |
| `-[:KNOWS]-` | Bidirectional (either direction) |
| `-[e:KNOWS]->` | Edge bound to variable `e` |
| `-[]->` | Any outgoing edge |
| `-[e]->` | Any outgoing edge, bound to `e` |

**Edge Direction Summary:**

| Arrow | Direction | Traversal Step |
|-------|-----------|----------------|
| `-->` | Outgoing | `out()` |
| `<--` | Incoming | `in_()` |
| `--` | Both | `both()` |

**Examples:**

```sql
-- Outgoing relationship
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b

-- Incoming relationship
MATCH (a:Person)<-[:WORKS_FOR]-(b:Person) RETURN a, b

-- Either direction
MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a, b

-- Bind edge to variable
MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b

-- Multiple relationship types (match any)
MATCH (a)-[:KNOWS|:WORKS_WITH]->(b) RETURN a, b
```

### Variable-Length Paths

Variable-length paths match paths of varying lengths using quantifiers.

| Syntax | Min | Max | Description |
|--------|-----|-----|-------------|
| `*` | 0 | 10 (default) | Any number of hops |
| `*3` | 3 | 3 | Exactly 3 hops |
| `*2..5` | 2 | 5 | Between 2 and 5 hops |
| `*..5` | 0 | 5 | Up to 5 hops |
| `*2..` | 2 | 10 (default) | At least 2 hops |

**Examples:**

```sql
-- Any number of KNOWS hops
MATCH (a:Person)-[:KNOWS*]->(b:Person)
RETURN a.name, b.name

-- Exactly 2 hops
MATCH (a:Person)-[:KNOWS*2]->(b:Person)
RETURN a.name, b.name

-- Between 1 and 3 hops
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
RETURN a.name, b.name

-- Friends of friends (2 hops)
MATCH (me:Person {name: 'Alice'})-[:KNOWS*2]->(fof:Person)
WHERE NOT (me)-[:KNOWS]->(fof)
RETURN fof.name
```

### EXISTS Patterns

The `EXISTS` expression checks if a subpattern matches from the current context.

```sql
-- Players who have won championships
MATCH (p:Player)
WHERE EXISTS { (p)-[:won_championship_with]->(:Team) }
RETURN p.name

-- Players who have NOT won championships  
MATCH (p:Player)
WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
RETURN p.name

-- Complex existence check
MATCH (p:Player)
WHERE EXISTS { (p)-[:played_for]->(:Team {name: 'Lakers'}) }
RETURN p.name
```

---

## Expression Types

Expressions are used in WHERE, RETURN, ORDER BY, and other clauses.

### Literals

| Type | Examples | Description |
|------|----------|-------------|
| String | `'hello'`, `'Alice'` | Single-quoted strings. Use `''` to escape quotes. |
| Integer | `42`, `-7`, `0` | 64-bit signed integers |
| Float | `3.14`, `-0.5` | 64-bit floating point |
| Boolean | `true`, `false` | Case-insensitive |
| Null | `null` | Represents missing/unknown value |
| List | `[1, 2, 3]`, `['a', 'b']` | Ordered collection |

### Variable References

```sql
-- Reference a bound variable
MATCH (n:Person) RETURN n

-- Variables can be nodes or edges
MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b
```

### Property Access

```sql
-- Access vertex property
MATCH (n:Person) RETURN n.name

-- Access edge property
MATCH (a)-[r:KNOWS]->(b) RETURN r.since

-- Nested in expressions
MATCH (n:Person) WHERE n.age > 21 RETURN n
```

### CASE Expressions

Conditional logic with WHEN/THEN/ELSE branches.

```sql
-- Simple categorization
MATCH (p:Player)
RETURN p.name,
  CASE
    WHEN p.age > 35 THEN 'Veteran'
    WHEN p.age > 28 THEN 'Prime'
    ELSE 'Young'
  END AS category

-- Multiple conditions
MATCH (s:Student)
RETURN s.name,
  CASE
    WHEN s.score >= 90 THEN 'A'
    WHEN s.score >= 80 THEN 'B'
    WHEN s.score >= 70 THEN 'C'
    ELSE 'F'
  END AS grade

-- CASE without ELSE returns null
MATCH (p:Person)
RETURN CASE WHEN p.age > 65 THEN 'Senior' END
```

---

## Operators

### Operator Precedence (highest to lowest)

| Precedence | Operators | Description |
|------------|-----------|-------------|
| 1 | `()` | Parentheses |
| 2 | `-` (unary) | Negation |
| 3 | `^` | Exponentiation |
| 4 | `*`, `/`, `%` | Multiplication, Division, Modulo |
| 5 | `+`, `-` | Addition, Subtraction |
| 6 | `\|\|` | String Concatenation |
| 7 | `=`, `<>`, `<`, `<=`, `>`, `>=` | Comparison |
| 7 | `=~` | Regular expression match |
| 7 | `CONTAINS`, `STARTS WITH`, `ENDS WITH` | String comparison |
| 7 | `IS NULL`, `IS NOT NULL` | Null checks |
| 7 | `IN`, `NOT IN` | List membership |
| 8 | `NOT` | Logical negation |
| 9 | `AND` | Logical conjunction |
| 10 | `OR` | Logical disjunction |

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equality | `n.age = 30` |
| `<>` or `!=` | Inequality | `n.status <> 'inactive'` |
| `<` | Less than | `n.age < 30` |
| `<=` | Less than or equal | `n.age <= 30` |
| `>` | Greater than | `n.age > 30` |
| `>=` | Greater than or equal | `n.age >= 30` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Logical AND | `n.age > 20 AND n.age < 40` |
| `OR` | Logical OR | `n.city = 'NYC' OR n.city = 'LA'` |
| `NOT` | Logical NOT | `NOT n.inactive` |

### Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `n.salary + 1000` |
| `-` | Subtraction | `n.age - 5` |
| `*` | Multiplication | `n.price * n.quantity` |
| `/` | Division | `n.total / n.count` |
| `%` | Modulo | `n.value % 10` |
| `^` | Exponentiation | `n.base ^ 2` |

### String Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `\|\|` | String concatenation | `p.firstName \|\| ' ' \|\| p.lastName` |
| `CONTAINS` | Substring match | `n.name CONTAINS 'son'` |
| `STARTS WITH` | Prefix match | `n.name STARTS WITH 'A'` |
| `ENDS WITH` | Suffix match | `n.email ENDS WITH '.com'` |

### Null Check Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `IS NULL` | Check for null | `n.email IS NULL` |
| `IS NOT NULL` | Check for non-null | `n.email IS NOT NULL` |

### List Membership Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `IN` | Value in list | `n.status IN ['active', 'pending']` |
| `NOT IN` | Value not in list | `n.status NOT IN ['deleted', 'banned']` |

### Regular Expression Operators

The `=~` operator performs regular expression pattern matching against strings.

| Operator | Description | Example |
|----------|-------------|---------|
| `=~` | Regex match | `n.email =~ '.*@gmail\\.com$'` |

**Basic Usage:**

```sql
-- Match emails ending with @gmail.com
MATCH (p:Person)
WHERE p.email =~ '.*@gmail\\.com$'
RETURN p.name, p.email

-- Match names starting with 'J'
MATCH (p:Person)
WHERE p.name =~ '^J.*'
RETURN p.name

-- Match phone numbers with pattern
MATCH (c:Contact)
WHERE c.phone =~ '^\\d{3}-\\d{3}-\\d{4}$'
RETURN c.name, c.phone
```

**Case-Insensitive Matching:**

Use the `(?i)` flag at the start of the pattern for case-insensitive matching:

```sql
-- Case-insensitive match
MATCH (p:Person)
WHERE p.name =~ '(?i)^john.*'
RETURN p.name

-- Match 'Smith', 'SMITH', 'smith', etc.
MATCH (p:Person)
WHERE p.lastName =~ '(?i)smith'
RETURN p.name
```

**Common Regex Patterns:**

| Pattern | Description | Example |
|---------|-------------|---------|
| `.*` | Any characters | `'.*test.*'` matches 'testing' |
| `^` | Start of string | `'^Hello'` matches 'Hello World' |
| `$` | End of string | `'world$'` matches 'Hello world' |
| `\\d` | Any digit | `'\\d+'` matches '123' |
| `\\w` | Word character | `'\\w+'` matches 'hello' |
| `[abc]` | Character class | `'[aeiou]'` matches vowels |
| `(?i)` | Case insensitive | `'(?i)hello'` matches 'HELLO' |

**Note:** Backslashes must be escaped in GQL string literals (`\\d` instead of `\d`).

---

## Built-in Functions

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `TOUPPER(s)` / `UPPER(s)` | Convert to uppercase | `TOUPPER(n.name)` → `'ALICE'` |
| `TOLOWER(s)` / `LOWER(s)` | Convert to lowercase | `TOLOWER(n.name)` → `'alice'` |
| `SIZE(s)` / `LENGTH(s)` | String/list length | `SIZE(n.name)` → `5` |
| `TRIM(s)` | Remove leading/trailing whitespace | `TRIM('  hello  ')` → `'hello'` |
| `LTRIM(s)` | Remove leading whitespace | `LTRIM('  hello')` → `'hello'` |
| `RTRIM(s)` | Remove trailing whitespace | `RTRIM('hello  ')` → `'hello'` |
| `SUBSTRING(s, start[, len])` | Extract substring | `SUBSTRING('hello', 1, 3)` → `'ell'` |
| `REPLACE(s, search, repl)` | Replace occurrences | `REPLACE('hello', 'l', 'L')` → `'heLLo'` |

**Examples:**

```sql
MATCH (p:Person)
RETURN TOUPPER(p.name) AS upperName

MATCH (p:Person)
WHERE SIZE(p.name) > 5
RETURN p.name

MATCH (p:Person)
RETURN SUBSTRING(p.email, 0, SUBSTRING(p.email, '@') - 1) AS username
```

### Numeric Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(n)` | Absolute value | `ABS(-5)` → `5` |
| `CEIL(n)` / `CEILING(n)` | Round up | `CEIL(4.2)` → `5.0` |
| `FLOOR(n)` | Round down | `FLOOR(4.8)` → `4.0` |
| `ROUND(n)` | Round to nearest | `ROUND(4.5)` → `5.0` |
| `SIGN(n)` | Sign of number (-1, 0, 1) | `SIGN(-5)` → `-1` |
| `SQRT(n)` | Square root | `SQRT(16)` → `4.0` |
| `POW(base, exp)` / `POWER(base, exp)` | Exponentiation | `POW(2, 3)` → `8.0` |
| `LOG(n)` / `LN(n)` | Natural logarithm | `LOG(e)` → `1.0` |
| `LOG10(n)` | Base-10 logarithm | `LOG10(100)` → `2.0` |
| `EXP(n)` | e^n | `EXP(1)` → `2.718...` |

**Examples:**

```sql
MATCH (p:Product)
RETURN p.name, ABS(p.profit) AS absoluteProfit

MATCH (c:Circle)
RETURN SQRT(c.area / 3.14159) AS radius
```

### Trigonometric Functions

All trigonometric functions work with radians.

| Function | Description |
|----------|-------------|
| `SIN(n)` | Sine |
| `COS(n)` | Cosine |
| `TAN(n)` | Tangent |
| `ASIN(n)` | Inverse sine (input: -1 to 1) |
| `ACOS(n)` | Inverse cosine (input: -1 to 1) |
| `ATAN(n)` | Inverse tangent |
| `ATAN2(y, x)` | Two-argument arctangent |

### Angle Conversion Functions

| Function | Description | Example |
|----------|-------------|---------|
| `DEGREES(radians)` | Radians to degrees | `DEGREES(3.14159)` → `180.0` |
| `RADIANS(degrees)` | Degrees to radians | `RADIANS(180)` → `3.14159` |

### Mathematical Constants

| Function | Description | Value |
|----------|-------------|-------|
| `PI()` | Pi constant | `3.141592653589793` |
| `E()` | Euler's number | `2.718281828459045` |

### Type Conversion Functions

| Function | Description | Example |
|----------|-------------|---------|
| `TOSTRING(v)` | Convert to string | `TOSTRING(42)` → `'42'` |
| `TOINTEGER(v)` / `TOINT(v)` | Convert to integer | `TOINTEGER('42')` → `42` |
| `TOFLOAT(v)` | Convert to float | `TOFLOAT('3.14')` → `3.14` |
| `TOBOOLEAN(v)` / `TOBOOL(v)` | Convert to boolean | `TOBOOLEAN('true')` → `true` |

### Introspection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ID(n)` | Get element ID | `ID(n)` → vertex/edge ID |
| `LABELS(n)` | Get vertex labels | `LABELS(n)` → `['Person']` |
| `TYPE(r)` | Get edge type | `TYPE(r)` → `'KNOWS'` |
| `PROPERTIES(n)` | Get all properties as map | `PROPERTIES(n)` → `{name: 'Alice', age: 30}` |

**Examples:**

```sql
MATCH (n:Person)
RETURN ID(n) AS id, LABELS(n) AS labels

MATCH (a)-[r]->(b)
RETURN TYPE(r) AS relationType
```

### Special Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COALESCE(v1, v2, ...)` | First non-null value | `COALESCE(n.nickname, n.name)` |
| `PATH()` | Get traversal path (requires WITH PATH) | See below |
| `MATH(expr, args...)` | Evaluate mathexpr expression | See below |

### COALESCE Function

Returns the first non-null argument:

```sql
MATCH (p:Person)
RETURN COALESCE(p.nickname, p.name) AS displayName

-- With multiple fallbacks
MATCH (p:Person)
RETURN COALESCE(p.email, p.phone, 'No contact') AS contact
```

### PATH Function

Retrieves the full traversal path. Requires `WITH PATH` clause:

```sql
MATCH (p1:Player)-[:played_for]->(t:Team)<-[:played_for]-(p2:Player)
WITH PATH
RETURN path(), p2.name

-- Path returns list: [vertex, edge, vertex, edge, vertex, ...]
```

### MATH Function (mathexpr Integration)

Evaluates complex mathematical expressions using the mathexpr library:

```sql
-- Basic math expression with literal arguments
MATCH (n:Number)
RETURN MATH('sqrt(a^2 + b^2)', 3, 4) AS hypotenuse
-- Returns: 5.0

-- Using property values as arguments
MATCH (n:Point)
RETURN MATH('sqrt(a^2 + b^2)', n.x, n.y) AS distance

-- Complex expressions
MATCH (n:Data)
RETURN MATH('sin(x) * cos(y) + exp(-z)', n.x, n.y, n.z) AS result
```

The MATH function supports:
- Variables: `a`, `b`, `c`, `d`, `e`, `f` (positional arguments)
- Constants: `pi`, `e`, `tau`
- Operators: `+`, `-`, `*`, `/`, `%`, `^`
- Functions: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `sqrt`, `cbrt`, `abs`, `floor`, `ceil`, `round`, `exp`, `ln`, `log`, `log2`, `log10`, `min`, `max`, `clamp`

### REDUCE Function

The `REDUCE` function folds/accumulates over a list, similar to reduce/fold operations in functional programming.

**Syntax:**

```
REDUCE(accumulator = initialValue, variable IN list | expression)
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `accumulator` | Variable name for the accumulated value |
| `initialValue` | Starting value for the accumulator |
| `variable` | Variable bound to each list element |
| `list` | The list to iterate over |
| `expression` | Expression that computes the new accumulator value |

**Examples:**

```sql
-- Sum a list of numbers
RETURN REDUCE(total = 0, x IN [1, 2, 3, 4, 5] | total + x) AS sum
-- Returns: 15

-- Product of list elements
RETURN REDUCE(product = 1, n IN [2, 3, 4] | product * n) AS result
-- Returns: 24

-- Concatenate strings
RETURN REDUCE(str = '', s IN ['a', 'b', 'c'] | str || s) AS combined
-- Returns: 'abc'

-- With separator
RETURN REDUCE(str = '', s IN ['hello', 'world'] | 
  CASE WHEN str = '' THEN s ELSE str || ', ' || s END
) AS joined
-- Returns: 'hello, world'
```

**Using with Query Results:**

```sql
-- Sum prices from collected items
MATCH (p:Person)-[:PURCHASED]->(item:Product)
LET items = COLLECT(item.price)
RETURN p.name, REDUCE(total = 0, price IN items | total + price) AS totalSpent

-- Calculate path length
MATCH (a:Person)-[r:KNOWS*1..5]->(b:Person)
RETURN REDUCE(len = 0, rel IN r | len + 1) AS pathLength
```

**Complex Accumulation:**

```sql
-- Build a running maximum
RETURN REDUCE(maxVal = 0, x IN [3, 1, 4, 1, 5, 9] | 
  CASE WHEN x > maxVal THEN x ELSE maxVal END
) AS maxValue
-- Returns: 9

-- Count matching elements
RETURN REDUCE(count = 0, x IN [1, 2, 3, 4, 5] | 
  CASE WHEN x > 2 THEN count + 1 ELSE count END
) AS countGreaterThan2
-- Returns: 3
```

### List Predicate Functions

List predicates test conditions across list elements. They return boolean values.

| Function | Description | Returns `true` when |
|----------|-------------|---------------------|
| `ALL(x IN list WHERE cond)` | All elements match | Every element satisfies condition |
| `ANY(x IN list WHERE cond)` | At least one matches | At least one element satisfies |
| `NONE(x IN list WHERE cond)` | No elements match | No element satisfies condition |
| `SINGLE(x IN list WHERE cond)` | Exactly one matches | Exactly one element satisfies |

**ALL - Every Element Must Match:**

```sql
-- Check if all numbers are positive
RETURN ALL(x IN [1, 2, 3] WHERE x > 0) AS allPositive
-- Returns: true

RETURN ALL(x IN [1, -2, 3] WHERE x > 0) AS allPositive
-- Returns: false

-- Check if all friends are adults
MATCH (p:Person)-[:KNOWS]->(f:Person)
LET friendAges = COLLECT(f.age)
WHERE ALL(age IN friendAges WHERE age >= 18)
RETURN p.name
```

**ANY - At Least One Must Match:**

```sql
-- Check if any number is negative
RETURN ANY(x IN [1, -2, 3] WHERE x < 0) AS hasNegative
-- Returns: true

-- Check if player has any championship
MATCH (p:Player)
LET rings = COLLECT { MATCH (p)-[:won_championship_with]->() RETURN 1 }
WHERE ANY(x IN rings WHERE x = 1)
RETURN p.name AS champions
```

**NONE - No Element Must Match:**

```sql
-- Check if no numbers are negative
RETURN NONE(x IN [1, 2, 3] WHERE x < 0) AS noNegatives
-- Returns: true

-- Find players with no losses
MATCH (p:Player)
LET results = [10, 5, 8, 12]  -- example scores
WHERE NONE(score IN results WHERE score < 5)
RETURN p.name
```

**SINGLE - Exactly One Must Match:**

```sql
-- Check if exactly one element equals 5
RETURN SINGLE(x IN [1, 5, 3] WHERE x = 5) AS exactlyOne
-- Returns: true

RETURN SINGLE(x IN [5, 5, 3] WHERE x = 5) AS exactlyOne
-- Returns: false (two matches)

-- Find teams with exactly one star player
MATCH (t:Team)<-[:plays_for]-(p:Player)
LET scores = COLLECT(p.points)
WHERE SINGLE(pts IN scores WHERE pts > 25)
RETURN t.name AS teamWithOneStar
```

**Edge Cases:**

```sql
-- Empty list behavior
RETURN ALL(x IN [] WHERE x > 0)    -- true (vacuously true)
RETURN ANY(x IN [] WHERE x > 0)    -- false (no elements match)
RETURN NONE(x IN [] WHERE x > 0)   -- true (no elements fail)
RETURN SINGLE(x IN [] WHERE x > 0) -- false (no elements match)
```

---

## Aggregation

Aggregate functions compute values across multiple matched patterns.

### Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all results | `COUNT(*)` |
| `COUNT(expr)` | Count non-null values | `COUNT(n.email)` |
| `COUNT(DISTINCT expr)` | Count unique values | `COUNT(DISTINCT n.city)` |
| `SUM(expr)` | Sum numeric values | `SUM(n.salary)` |
| `AVG(expr)` | Average of numeric values | `AVG(n.age)` |
| `MIN(expr)` | Minimum value | `MIN(n.price)` |
| `MAX(expr)` | Maximum value | `MAX(n.score)` |
| `COLLECT(expr)` | Collect values into list | `COLLECT(n.name)` |

### Basic Aggregation Examples

```sql
-- Count all vertices
MATCH (n:Person) RETURN COUNT(*)

-- Count non-null property values
MATCH (n:Person) RETURN COUNT(n.email)

-- Count distinct values
MATCH (n:Person) RETURN COUNT(DISTINCT n.city)

-- Sum, average, min, max
MATCH (e:Employee)
RETURN SUM(e.salary), AVG(e.salary), MIN(e.salary), MAX(e.salary)

-- Collect into list
MATCH (p:Person)-[:LIVES_IN]->(c:City {name: 'NYC'})
RETURN COLLECT(p.name) AS nycResidents
```

### GROUP BY Aggregation

When using aggregate functions with non-aggregated expressions, use GROUP BY:

```sql
-- Count players by position
MATCH (p:Player)
RETURN p.position, COUNT(*) AS count
GROUP BY p.position

-- Average salary by department
MATCH (e:Employee)-[:WORKS_IN]->(d:Department)
RETURN d.name AS department, AVG(e.salary) AS avgSalary
GROUP BY d.name

-- Multiple group keys
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, p.position, COUNT(*) AS count
GROUP BY t.name, p.position
ORDER BY t.name, count DESC
```

### Aggregation with Filtering

```sql
-- Filter before aggregation (WHERE)
MATCH (p:Player)
WHERE p.active = true
RETURN p.position, AVG(p.salary) AS avgSalary
GROUP BY p.position

-- Complex aggregation query
MATCH (p:Player)-[:played_for]->(t:Team)
WHERE p.draft_year >= 2010
RETURN t.name,
       COUNT(*) AS totalPlayers,
       AVG(p.career_points) AS avgPoints,
       MAX(p.career_points) AS topScorer
GROUP BY t.name
ORDER BY avgPoints DESC
LIMIT 10
```

### COLLECT Function

Collects values into a list:

```sql
-- Collect all names
MATCH (p:Person)
RETURN COLLECT(p.name) AS allNames

-- Collect with grouping
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, COLLECT(p.name) AS players
GROUP BY t.name

-- Collect distinct values
MATCH (p:Player)-[:played_for]->(t:Team)
RETURN p.name, COLLECT(DISTINCT t.name) AS teams
GROUP BY p.name
```

---

## Advanced Features

This section covers advanced GQL features for complex analytical queries.

### Query Parameters

Parameterized queries allow safe value injection and query reuse using `$paramName` syntax.

**Syntax:**

```sql
-- Parameter in property filter
MATCH (n:Person {id: $personId}) RETURN n

-- Parameter in WHERE clause
MATCH (n:Person) WHERE n.age > $minAge RETURN n

-- Parameter in expression
MATCH (n) RETURN n.value * $multiplier AS scaled

-- Multiple parameters
MATCH (a:Person {id: $fromId})-[:KNOWS]->(b:Person {id: $toId})
RETURN a, b
```

**Rust Usage:**

```rust
use intersteller::gql::{execute_with_params, Parameters};
use intersteller::Value;

let mut params = Parameters::new();
params.insert("personId".to_string(), Value::Int(123));
params.insert("minAge".to_string(), Value::Int(18));

let results = execute_with_params(
    &graph,
    "MATCH (p:Person {id: $personId})-[:FRIEND]->(f) 
     WHERE f.age >= $minAge 
     RETURN f.name",
    &params,
)?;
```

**Supported parameter types:** String, Int, Float, Bool, List, Map, Null

### Inline WHERE in Patterns

Filter nodes and edges directly within pattern syntax during pattern matching.

**Syntax:**

```sql
-- Node with inline WHERE
MATCH (n:Person WHERE n.age > 21) RETURN n

-- Edge with inline WHERE
MATCH (a)-[r:KNOWS WHERE r.since > 2020]->(b) RETURN a, b

-- Combined filters
MATCH (a:Person WHERE a.status = 'active')-[r:FOLLOWS WHERE r.weight > 0.5]->(b)
RETURN a, b
```

**Semantics:**

- Inline WHERE is evaluated during pattern matching, not after
- Can only reference properties of the current element (not other pattern variables)
- Combines with label filters (both must match)

**Equivalent queries:**

```sql
-- These are semantically equivalent:
MATCH (n:Person WHERE n.age > 21) RETURN n
MATCH (n:Person) WHERE n.age > 21 RETURN n

-- But inline WHERE is useful for edge filtering in complex patterns
MATCH (a)-[r:KNOWS WHERE r.weight > 0.5]->(b)-[s:WORKS_AT]->(c)
RETURN a, b, c
```

### LET Clause

The LET clause binds the result of an expression to a variable for use in subsequent clauses.

**Syntax:**

```sql
-- Basic LET
MATCH (p:Person)-[:FRIEND]->(f)
LET friendCount = COUNT(f)
RETURN p.name, friendCount

-- LET with COLLECT
MATCH (p:Person)-[:PURCHASED]->(item)
LET purchases = COLLECT(item)
LET totalSpent = SUM(item.price)
RETURN p.name, purchases, totalSpent

-- LET with CASE expression
MATCH (p:Person)
LET ageCategory = CASE 
    WHEN p.age < 18 THEN 'minor'
    WHEN p.age < 65 THEN 'adult'
    ELSE 'senior'
END
RETURN p.name, ageCategory

-- Multiple LET clauses (later LETs can reference earlier ones)
MATCH (person)-[:WORKS_AT]->(company)
LET colleagues = COLLECT(person)
LET companySize = SIZE(colleagues)
LET avgSalary = AVG(person.salary)
RETURN company.name, companySize, avgSalary
```

**Clause ordering:**

```
MATCH -> OPTIONAL MATCH -> WHERE -> LET -> RETURN -> GROUP BY -> ORDER BY -> LIMIT
```

### List Comprehensions

Transform and filter lists using a concise syntax similar to Python list comprehensions.

**Syntax:**

```sql
-- Basic transformation: [variable IN list | expression]
[x IN list | x.name]

-- With filter: [variable IN list WHERE condition | expression]
[x IN list WHERE x.active | x.name]
```

**Examples:**

```sql
-- Get names from list of people
LET names = [p IN people | p.name]
-- Input: [{name: 'Alice'}, {name: 'Bob'}]
-- Output: ['Alice', 'Bob']

-- Filter and transform
LET adultNames = [p IN people WHERE p.age >= 18 | p.name]
-- Input: [{name: 'Alice', age: 25}, {name: 'Bob', age: 15}]
-- Output: ['Alice']

-- Build formatted strings
LET labels = [t IN types | t.category || '/' || t.name]
-- Input: [{category: 'A', name: 'foo'}, {category: 'B', name: 'bar'}]
-- Output: ['A/foo', 'B/bar']

-- Complex expressions
[p IN people | CASE WHEN p.age > 18 THEN 'adult' ELSE 'minor' END]
```

**Semantics:**

- The variable is scoped to the comprehension only
- If input is NULL or not a list, returns NULL
- Empty list input returns empty list

### String Concatenation Operator

The `||` operator concatenates strings, following SQL/GQL standard.

**Syntax:**

```sql
-- Basic concatenation
'Hello' || ' ' || 'World'
-- Result: 'Hello World'

-- With properties
p.firstName || ' ' || p.lastName

-- In expressions
RETURN n.type || '/' || n.subtype AS fullType

-- With COALESCE for null handling
COALESCE(p.nickname, p.firstName) || ' ' || p.lastName
```

**Semantics:**

- If either operand is NULL, result is NULL
- Non-string operands are automatically converted to strings:
  - Int/Float: Decimal representation
  - Bool: `"true"` / `"false"`
  - List: `"[elem1, elem2, ...]"`
  - Map: `"{key1: val1, key2: val2}"`

### Map Literals

Create map/object values in expressions, particularly useful with COLLECT and RETURN.

**Syntax:**

```sql
-- Map literal
{name: 'Alice', age: 30}

-- Map with property references
{personName: p.name, personAge: p.age}

-- In COLLECT
LET data = COLLECT({parent: parent, type: event.type})

-- In RETURN
RETURN {
    name: p.name,
    stats: {
        friends: friendCount,
        posts: postCount
    }
} AS profile

-- Nested maps supported
{outer: {inner: value}}
```

**Keys:** Must be identifiers (unquoted) or string literals

### Complete Advanced Query Example

Combining multiple advanced features:

```sql
MATCH (person:Person WHERE person.id = $personId)
      -[r1:PARTICIPATED_IN WHERE r1.role = 'child']->(birthEvent:Birth)
      <-[r2:PARTICIPATED_IN WHERE r2.role = 'parent']-(parent:Person),
      (parent)-[:PARTICIPATED_IN]->(otherBirth:Birth)
      <-[r3:PARTICIPATED_IN WHERE r3.role = 'child']-(sibling:Person)
WHERE sibling <> person
LET siblingInfo = COLLECT({
    sibling: sibling,
    parent: parent,
    sharedEvent: birthEvent
})
RETURN sibling.name,
       SIZE(siblingInfo) AS connectionCount,
       [s IN siblingInfo | s.parent.name] AS sharedParents
GROUP BY sibling
```

---

## Mutation Operations

Mutations modify the graph and require mutable storage access.

### Mutation Statement Structure

```
[MATCH pattern [WHERE expression]]
<mutation_clause>+
[RETURN expression [AS alias] [, ...]]
```

Or for MERGE:

```
MERGE pattern
[ON CREATE SET assignments]
[ON MATCH SET assignments]
[RETURN ...]
```

### CREATE Clause

Creates new vertices and edges.

```sql
-- Create a single vertex
CREATE (n:Person {name: 'Alice', age: 30})

-- Create multiple vertices
CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})

-- Create with RETURN to get the created element
CREATE (n:Person {name: 'Alice'}) RETURN n

-- Create a vertex with edge (requires existing endpoint or creates inline)
CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
```

**Rust Usage:**

```rust
use intersteller::gql::{parse_statement, execute_mutation};
use intersteller::storage::InMemoryGraph;

let mut storage = InMemoryGraph::new();

let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
execute_mutation(&stmt, &mut storage).unwrap();

assert_eq!(storage.vertex_count(), 1);
```

### SET Clause

Updates properties on matched elements.

```sql
-- Update single property
MATCH (n:Person {name: 'Alice'})
SET n.age = 31

-- Update multiple properties
MATCH (n:Person {name: 'Alice'})
SET n.age = 31, n.status = 'active'

-- Update with expression
MATCH (n:Person {name: 'Alice'})
SET n.age = n.age + 1

-- Update and return
MATCH (n:Person {name: 'Alice'})
SET n.lastUpdated = 1234567890
RETURN n
```

### REMOVE Clause

Removes properties from elements (sets them to null/removes the key).

```sql
-- Remove single property
MATCH (n:Person {name: 'Alice'})
REMOVE n.temporaryField

-- Remove multiple properties
MATCH (n:Person)
REMOVE n.tempA, n.tempB
```

### DELETE Clause

Deletes matched elements. Fails if deleting a vertex that has connected edges.

```sql
-- Delete matched vertices (must have no edges)
MATCH (n:Person {status: 'inactive'})
DELETE n

-- Delete edges
MATCH (a:Person)-[r:KNOWS]->(b:Person)
WHERE r.since < 2020
DELETE r

-- Delete multiple elements
MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person)
DELETE r, b
```

### DETACH DELETE Clause

Deletes vertices along with all their connected edges automatically.

```sql
-- Delete vertex and its edges
MATCH (n:Person {name: 'Alice'})
DETACH DELETE n

-- Delete multiple vertices with edges
MATCH (n:Person)
WHERE n.status = 'deleted'
DETACH DELETE n
```

### MERGE Clause

MERGE is an "upsert" operation: matches existing patterns or creates them if not found.

```sql
-- Simple merge (create if not exists)
MERGE (n:Person {name: 'Alice'})

-- Merge with ON CREATE (set properties only when creating)
MERGE (n:Person {name: 'Alice'})
ON CREATE SET n.created = 1234567890

-- Merge with ON MATCH (set properties only when matching existing)
MERGE (n:Person {name: 'Alice'})
ON MATCH SET n.lastSeen = 1234567890

-- Merge with both actions
MERGE (n:Person {name: 'Alice'})
ON CREATE SET n.created = 1234567890, n.visits = 1
ON MATCH SET n.lastSeen = 1234567890, n.visits = n.visits + 1
RETURN n
```

### Complete Mutation Examples

```rust
use intersteller::gql::{parse_statement, execute_mutation};
use intersteller::storage::{GraphStorage, InMemoryGraph};

let mut storage = InMemoryGraph::new();

// Create initial data
let stmt = parse_statement(r#"
    CREATE (alice:Person {name: 'Alice', age: 30}),
           (bob:Person {name: 'Bob', age: 25}),
           (alice)-[:KNOWS {since: 2020}]->(bob)
"#).unwrap();
execute_mutation(&stmt, &mut storage).unwrap();

// Update a property
let stmt = parse_statement(r#"
    MATCH (n:Person {name: 'Alice'})
    SET n.age = 31
"#).unwrap();
execute_mutation(&stmt, &mut storage).unwrap();

// Delete a relationship
let stmt = parse_statement(r#"
    MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person)
    DELETE r
"#).unwrap();
execute_mutation(&stmt, &mut storage).unwrap();

// Merge (upsert) a person
let stmt = parse_statement(r#"
    MERGE (n:Person {name: 'Charlie'})
    ON CREATE SET n.created = 1234567890
    ON MATCH SET n.lastSeen = 1234567890
"#).unwrap();
execute_mutation(&stmt, &mut storage).unwrap();
```

---

## Error Handling

The GQL module defines three error types for different stages of query processing.

### ParseError

Errors during query parsing (syntax errors).

| Variant | Description |
|---------|-------------|
| `SyntaxAt { span, message }` | Syntax error at specific position |
| `Syntax(String)` | General syntax error |
| `Empty` | Empty query string |
| `MissingClause { clause, span }` | Required clause missing |
| `InvalidLiteral { value, span, reason }` | Invalid literal value |
| `UnexpectedToken { span, found, expected }` | Unexpected token encountered |
| `UnexpectedEof { span, expected }` | Unexpected end of input |
| `InvalidRange { range, span, reason }` | Invalid path quantifier range |

**Example:**

```rust
use intersteller::gql::{parse, ParseError};

match parse("MATCH (n:Person) RETURN") {
    Ok(_) => println!("Parsed successfully"),
    Err(ParseError::SyntaxAt { span, message }) => {
        eprintln!("Syntax error at position {}: {}", span.start, message);
    }
    Err(e) => eprintln!("Parse error: {}", e),
}
```

### CompileError

Errors during compilation (semantic errors).

| Variant | Description |
|---------|-------------|
| `UndefinedVariable { name }` | Reference to undefined variable |
| `DuplicateVariable { name }` | Variable bound multiple times |
| `EmptyPattern` | MATCH clause has no patterns |
| `PatternMustStartWithNode` | Pattern starts with edge instead of node |
| `UnsupportedExpression { expr }` | Expression not supported in context |
| `AggregateInWhere { func }` | Aggregate function used in WHERE |
| `InvalidPropertyAccess { variable }` | Property access on non-element |
| `UnsupportedAggregation { func }` | Unknown aggregate function |
| `TypeMismatch { message }` | Type error in expression |
| `ExpressionNotInGroupBy { expr }` | Non-aggregated expression missing from GROUP BY |
| `UnsupportedFeature(String)` | Feature not implemented |

**Example:**

```rust
use intersteller::gql::{parse, compile, CompileError};
use intersteller::Graph;

let graph = Graph::in_memory();
let snapshot = graph.snapshot();

let query = parse("MATCH (n:Person) RETURN x").unwrap();
match compile(&query, &snapshot) {
    Ok(_) => println!("Success"),
    Err(CompileError::UndefinedVariable { name }) => {
        eprintln!("Variable '{}' is not defined in MATCH", name);
    }
    Err(e) => eprintln!("Compile error: {}", e),
}
```

### MutationError

Errors during mutation execution.

| Variant | Description |
|---------|-------------|
| `Compile(CompileError)` | Underlying compilation error |
| `Storage(StorageError)` | Storage operation failed |
| `UnboundVariable(String)` | Variable not bound during execution |
| `VertexHasEdges(VertexId)` | DELETE on vertex with edges (use DETACH DELETE) |
| `InvalidElementType { operation, expected, actual }` | Wrong element type for operation |
| `MissingLabel` | CREATE vertex without label |
| `IncompleteEdge` | Edge missing source or target |

**Example:**

```rust
use intersteller::gql::{parse_statement, execute_mutation, MutationError};
use intersteller::storage::InMemoryGraph;

let mut storage = InMemoryGraph::new();

// Create vertex with an edge
parse_statement("CREATE (a:Person)-[:KNOWS]->(b:Person)").map(|s| execute_mutation(&s, &mut storage));

// Try to DELETE (not DETACH DELETE) - will fail
let stmt = parse_statement("MATCH (n:Person) DELETE n").unwrap();
match execute_mutation(&stmt, &mut storage) {
    Ok(_) => println!("Deleted"),
    Err(MutationError::VertexHasEdges(vid)) => {
        eprintln!("Cannot delete vertex {:?}: has edges. Use DETACH DELETE.", vid);
    }
    Err(e) => eprintln!("Mutation error: {}", e),
}
```

### GqlError (Top-level)

Wraps both parse and compile errors for convenience:

```rust
use intersteller::gql::GqlError;

let graph = intersteller::Graph::in_memory();
let snapshot = graph.snapshot();

match snapshot.gql("MATCH (n:Person) RETURN x") {
    Ok(results) => println!("Found {} results", results.len()),
    Err(GqlError::Parse(e)) => eprintln!("Syntax error: {}", e),
    Err(GqlError::Compile(e)) => eprintln!("Compilation error: {}", e),
}
```

---

## Limitations

The current GQL implementation has the following limitations:

### Not Supported

| Feature | Status | Notes |
|---------|--------|-------|
| Subqueries | Not supported | No nested `CALL` or `MATCH` within expressions |
| `FOREACH` | Not supported | No iterative mutations |
| `LOAD CSV` | Not supported | No external data import |
| Multiple graphs | Not supported | Single graph queries only |
| Returning paths directly | Partial | Use `WITH PATH` + `path()` function |
| `CALL` procedures | Not supported | No stored procedures |
| Pattern comprehensions | Not supported | `[(p)-[:KNOWS]->(f) | f.name]` syntax |

### Partial Support

| Feature | Limitation |
|---------|------------|
| `UNWIND` | Supported but may have limitations in complex nested contexts |
| Anonymous endpoint patterns | `MATCH ()-[r]->()` may require explicit labels on endpoints |
| Multi-pattern MATCH | Only first pattern fully used; subsequent patterns joined via comma |
| Variable-length paths | Default max of 10 hops; custom max supported via `*n..m` syntax |

### Known Behaviors

1. **Keywords are case-insensitive**: `MATCH`, `match`, `Match` are all valid
2. **Identifiers are case-sensitive**: `n.Name` and `n.name` are different properties
3. **String literals use single quotes**: `'Alice'` not `"Alice"`
4. **NULL propagation**: Operations involving NULL typically return NULL
5. **Empty MATCH results**: If MATCH finds nothing, mutations don't execute

### Error on Mutation Without Match

```sql
-- This returns empty results (no error)
MATCH (n:NonExistent) SET n.prop = 1

-- To ensure data exists, check count or use MERGE
```

---

## API Reference

### Public Functions

```rust
// Parse a single query (returns Query)
pub fn parse(input: &str) -> Result<Query, ParseError>;

// Parse a statement (query, UNION, or mutation)
pub fn parse_statement(input: &str) -> Result<Statement, ParseError>;

// Compile and execute a query
pub fn compile<'g>(query: &Query, snapshot: &'g GraphSnapshot<'g>) -> Result<Vec<Value>, CompileError>;

// Compile and execute a statement
pub fn compile_statement<'g>(stmt: &Statement, snapshot: &'g GraphSnapshot<'g>) -> Result<Vec<Value>, CompileError>;

// Compile and execute a query with parameters
pub fn compile_with_params<'g>(
    query: &str,
    params: &Parameters,
    snapshot: &'g GraphSnapshot<'g>,
) -> Result<Vec<Value>, GqlError>;

// Execute a query with parameters (convenience function)
pub fn execute_with_params<G: Graph>(
    graph: &G,
    query: &str,
    params: &Parameters,
) -> Result<Vec<Value>, GqlError>;

// Execute a mutation
pub fn execute_mutation<S: GraphStorage + GraphStorageMut>(
    stmt: &Statement,
    storage: &mut S,
) -> Result<Vec<Value>, MutationError>;

// Execute a mutation query directly
pub fn execute_mutation_query<S: GraphStorage + GraphStorageMut>(
    query: &MutationQuery,
    storage: &mut S,
) -> Result<Vec<Value>, MutationError>;
```

### Types

```rust
/// Parameters passed to query execution
pub type Parameters = HashMap<String, Value>;
```

### Convenience Method

```rust
// On GraphSnapshot
impl GraphSnapshot {
    pub fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError>;
}
```

### Re-exports

The `intersteller::gql` module re-exports:

- All AST types from `ast.rs`
- `compile`, `compile_statement` from `compiler.rs`
- `ParseError`, `CompileError`, `GqlError`, `Span` from `error.rs`
- `execute_mutation`, `execute_mutation_query`, `MutationContext`, `MutationError`, `Element` from `mutation.rs`
- `parse`, `parse_statement` from `parser.rs`
