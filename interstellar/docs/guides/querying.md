# Querying Guide

Patterns and techniques for writing effective graph queries.

## Choosing an API

Interstellar offers two query APIs:

| API | Style | Best For |
|-----|-------|----------|
| **Gremlin** | Fluent/chainable | Complex traversals, Rust integration |
| **GQL** | SQL-like strings | Simple queries, user input, ad-hoc queries |

### Gremlin Example

```rust
g.v()
    .has_label("person")
    .has_where("age", p::gt(21))
    .out("knows")
    .values("name")
    .to_list()
```

### GQL Example

```rust
snapshot.gql("
    MATCH (p:person)-[:knows]->(friend:person)
    WHERE p.age > 21
    RETURN friend.name
")
```

---

## Common Query Patterns

### Find by Property

```rust
// Gremlin
g.v().has_label("person").has_value("name", "Alice").to_list()

// GQL
"MATCH (p:person {name: 'Alice'}) RETURN p"
```

### Get Neighbors

```rust
// Outgoing relationships
g.v_ids([alice]).out("knows").to_list()

// Incoming relationships
g.v_ids([alice]).in_("knows").to_list()

// Both directions
g.v_ids([alice]).both("knows").to_list()
```

### Multi-Hop Traversal

```rust
// Friends of friends
g.v_ids([alice])
    .out("knows")
    .out("knows")
    .dedup()  // Remove duplicates
    .to_list()

// GQL
"MATCH (a:person {name: 'Alice'})-[:knows]->()-[:knows]->(fof)
 RETURN DISTINCT fof"
```

### Filter with Predicates

```rust
// Comparison
g.v().has_where("age", p::gt(30))
g.v().has_where("score", p::between(80, 100))

// Text matching
g.v().has_where("name", p::containing("son"))
g.v().has_where("email", p::ending_with("@gmail.com"))

// Logical combinations
g.v().has_where("age", p::and(p::gte(18), p::lt(65)))
```

### Conditional Filtering

```rust
// Filter based on sub-traversal existence
g.v()
    .has_label("person")
    .where_(__.out("knows").has_label("celebrity"))
    .to_list()

// Exclude based on sub-traversal
g.v()
    .has_label("person")
    .not(__.out("blocked"))
    .to_list()
```

---

## Path Queries

### Get Traversal Path

```rust
// Enable path tracking and retrieve paths
g.v_ids([start])
    .repeat(__.out())
    .times(3)
    .path()
    .to_list()
```

### Labeled Positions

```rust
// Label positions and select later
g.v()
    .has_label("person").as_("a")
    .out("knows").as_("b")
    .out("knows").as_("c")
    .select(["a", "b", "c"])
    .to_list()
```

### Simple Paths Only

```rust
// Avoid cycles
g.v_ids([start])
    .repeat(__.out().simple_path())
    .until(__.has_id(target))
    .path()
    .limit(1)
    .next()
```

---

## Aggregation Queries

### Counting

```rust
// Count all
g.v().has_label("person").count()

// Count per group
g.v().has_label("person").group_count_by_label().next()
```

### Grouping

```rust
// Group by property
g.v()
    .has_label("person")
    .group_by_key("city")
    .to_list()

// GQL
"MATCH (p:person)
 RETURN p.city, COUNT(*) AS count
 GROUP BY p.city"
```

### Statistics

```rust
// Sum, min, max, mean
g.v().has_label("order").values("total").sum()
g.v().has_label("product").values("price").min()
g.v().has_label("product").values("price").max()
g.v().has_label("employee").values("salary").mean()
```

---

## Branching Queries

### Union (Combine Results)

```rust
// Merge results from multiple paths
g.v_ids([alice])
    .union([
        __.out("knows"),
        __.out("follows"),
        __.out("works_with"),
    ])
    .dedup()
    .to_list()
```

### Coalesce (First Match)

```rust
// Use first available value
g.v()
    .coalesce([
        __.values("nickname"),
        __.values("name"),
        __.constant("Unknown"),
    ])
    .to_list()
```

### Conditional (Choose)

```rust
// If/else based on condition
g.v()
    .choose(
        __.has("premium"),
        __.values("premium_name"),
        __.values("name"),
    )
    .to_list()
```

---

## Iterative Queries

### Fixed Iterations

```rust
// Traverse exactly 3 hops
g.v_ids([start])
    .repeat(__.out())
    .times(3)
    .to_list()
```

### Until Condition

```rust
// Traverse until reaching target
g.v_ids([start])
    .repeat(__.out("parent"))
    .until(__.has_label("root"))
    .to_list()
```

### Emit Intermediate Results

```rust
// Include results from each iteration
g.v_ids([start])
    .repeat(__.out("knows"))
    .times(3)
    .emit()
    .to_list()
```

---

## Performance Tips

### 1. Filter Early

```rust
// Good: Filter before expensive operations
g.v()
    .has_label("person")  // Filter first
    .has_value("active", true)
    .out("knows")
    .to_list()

// Avoid: Filter late
g.v()
    .out("knows")  // Process all vertices first
    .has_label("person")
    .has_value("active", true)
    .to_list()
```

### 2. Limit Results

```rust
// Stop early when possible
g.v().has_label("person").limit(10).to_list()

// Check existence without collecting all
g.v().has_label("admin").has_next()
```

### 3. Use Specific Start Points

```rust
// Good: Start from known ID
g.v_ids([known_id]).out("knows").to_list()

// Avoid: Scan all vertices
g.v().has_value("id", known_id).out("knows").to_list()
```

### 4. Avoid Unnecessary Path Tracking

```rust
// Path tracking adds overhead
// Only use when you need the path
g.v().out().out().path()  // Enables path tracking

// If you don't need the path, don't call path()
g.v().out().out().to_list()  // Faster
```

---

## Full-Text Search

Interstellar ships a Tantivy-backed full-text index (feature flag: `full-text`) reachable from the Rust API, Gremlin, and GQL.

**Gremlin** — `searchTextV` / `searchTextE` start a traversal from a BM25-ranked hit list; `textScore()` reads the score back off the traverser sack:

```rust
graph.execute_script(
    "g.searchTextV('body', 'raft consensus', 10).hasLabel('article').values('title')"
)?;
// Compound queries use the TextQ DSL:
graph.execute_script(
    "g.searchTextV('body', TextQ.phrase('distributed consensus'), 5).textScore()"
)?;
```

**GQL** — eight `CALL` procedures (`interstellar.searchText{,All,Phrase,Prefix}{V,E}`) with `YIELD elem | elemId | score`. GQL requires a leading `MATCH`, so anchor against one row:

```rust
graph.gql(
    "MATCH (anchor) WHERE id(anchor) = 0
     CALL interstellar.searchTextV('body', 'raft', 5)
     YIELD elemId, score
     RETURN elemId, score"
)?;
```

See the [Full-Text Search guide](full-text-search.md) for indexing, tokenization, and the full `TextQ` / `CALL` reference.

---

## GQL-Specific Patterns

### Optional Matches

```rust
"MATCH (p:person)
 OPTIONAL MATCH (p)-[:works_at]->(c:company)
 RETURN p.name, c.name"
```

### Exists Subqueries

```rust
"MATCH (p:person)
 WHERE EXISTS { (p)-[:won]->(:award) }
 RETURN p.name"
```

### Ordering and Pagination

```rust
"MATCH (p:person)
 RETURN p.name, p.age
 ORDER BY p.age DESC
 LIMIT 10 OFFSET 20"
```

---

## See Also

- [Gremlin API](../api/gremlin.md) - Complete step reference
- [GQL API](../api/gql.md) - Full GQL syntax
- [Predicates](../api/predicates.md) - Filter functions
- [Full-Text Search](full-text-search.md) - Indexing and BM25 queries
- [Performance Guide](performance.md) - Optimization tips
