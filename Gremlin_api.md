# Interstellar API Reference

This document maps standard Gremlin steps (TinkerPop 3.x) to their Rust implementations in Interstellar.

## Legend

| Symbol | Meaning |
|--------|---------|
| `rust_function` | Implemented - function name in Rust |
| - | Not implemented |

## API Naming Differences

| Gremlin | Rust | Reason |
|---------|------|--------|
| `in()` | `in_()` | `in` is a Rust keyword |
| `as()` | `as_()` | `as` is a Rust keyword |
| `where()` | `where_()` | `where` is a Rust keyword |
| `has(key, value)` | `has_value(key, value)` | Distinguishes from `has(key)` for property existence |
| `and()` | `and_()` | `and` is a Rust keyword |
| `or()` | `or_()` | `or` is a Rust keyword |
| `is()` | `is_()` | `is` is a Rust keyword |

---

## Source Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `V()` | `v()`, `v_ids()`, `v_by_id()` | `traversal::source` |
| `V()` + index | `v_by_property()`, `v_by_property_range()` | `traversal::source` |
| `E()` | `e()`, `e_ids()` | `traversal::source` |
| `E()` + index | `e_by_property()` | `traversal::source` |
| `addV()` | `add_v()` | `traversal::source`, `traversal::mutation` |
| `addE()` | `add_e()` | `traversal::source`, `traversal::mutation` |
| `inject()` | `inject()` | `traversal::source` |

---

## Filter Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `has(key)` | `has(key)` | `traversal::filter` |
| `has(key, value)` | `has_value(key, value)` | `traversal::filter` |
| `has(key, predicate)` | `has_where(key, predicate)` | `traversal::filter` |
| `hasLabel(label)` | `has_label(label)` | `traversal::filter` |
| `hasLabel(label...)` | `has_label_any(labels)` | `traversal::filter` |
| `hasId(id)` | `has_id(id)` | `traversal::filter` |
| `hasId(id...)` | `has_ids(ids)` | `traversal::filter` |
| `hasNot(key)` | `has_not(key)` | `traversal::filter` |
| `hasKey()` | `has_key(key)` | `traversal::filter` |
| `hasKey(key...)` | `has_key_any(keys)` | `traversal::filter` |
| `hasValue()` | `has_prop_value(value)` | `traversal::filter` |
| `hasValue(value...)` | `has_prop_value_any(values)` | `traversal::filter` |
| `filter(traversal)` | `filter(closure)` | `traversal::filter` |
| `where(traversal)` | `where_(traversal)` | `traversal::branch` |
| `where(predicate)` | `where_p(predicate)` | `traversal::filter` |
| `not(traversal)` | `not(traversal)` | `traversal::branch` |
| `and(traversal...)` | `and_(traversals)` | `traversal::branch` |
| `or(traversal...)` | `or_(traversals)` | `traversal::branch` |
| `is(value)` | `is_eq(value)` | `traversal::filter` |
| `is(predicate)` | `is_(predicate)` | `traversal::filter` |
| `dedup()` | `dedup()` | `traversal::filter` |
| `dedup().by(key)` | `dedup_by_key(key)` | `traversal::filter` |
| `dedup().by(label)` | `dedup_by_label()` | `traversal::filter` |
| `dedup().by(traversal)` | `dedup_by(traversal)` | `traversal::filter` |
| `limit(n)` | `limit(n)` | `traversal::filter` |
| `skip(n)` | `skip(n)` | `traversal::filter` |
| `range(start, end)` | `range(start, end)` | `traversal::filter` |
| `tail()` | `tail()` | `traversal::filter` |
| `tail(n)` | `tail_n(n)` | `traversal::filter` |
| `coin(probability)` | `coin(probability)` | `traversal::filter` |
| `sample(n)` | `sample(n)` | `traversal::filter` |
| `simplePath()` | `simple_path()` | `traversal::filter` |
| `cyclicPath()` | `cyclic_path()` | `traversal::filter` |
| `timeLimit()` | - | - |
| `drop()` | `drop()` | `traversal::mutation` |

---

## Navigation Steps (Vertex to Vertex)

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `out()` | `out()` | `traversal::navigation` |
| `out(label...)` | `out_labels(labels)` | `traversal::navigation` |
| `in()` | `in_()` | `traversal::navigation` |
| `in(label...)` | `in_labels(labels)` | `traversal::navigation` |
| `both()` | `both()` | `traversal::navigation` |
| `both(label...)` | `both_labels(labels)` | `traversal::navigation` |

## Navigation Steps (Vertex to Edge)

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `outE()` | `out_e()` | `traversal::navigation` |
| `outE(label...)` | `out_e_labels(labels)` | `traversal::navigation` |
| `inE()` | `in_e()` | `traversal::navigation` |
| `inE(label...)` | `in_e_labels(labels)` | `traversal::navigation` |
| `bothE()` | `both_e()` | `traversal::navigation` |
| `bothE(label...)` | `both_e_labels(labels)` | `traversal::navigation` |

## Navigation Steps (Edge to Vertex)

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `outV()` | `out_v()` | `traversal::navigation` |
| `inV()` | `in_v()` | `traversal::navigation` |
| `bothV()` | `both_v()` | `traversal::navigation` |
| `otherV()` | `other_v()` | `traversal::navigation` |

---

## Transform / Map Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `map(traversal)` | `map(closure)` | `traversal::transform` |
| `flatMap(traversal)` | `flat_map(closure)` | `traversal::transform` |
| `identity()` | `identity()` | `traversal::step` |
| `constant(value)` | `constant(value)` | `traversal::transform` |
| `id()` | `id()` | `traversal::transform` |
| `label()` | `label()` | `traversal::transform` |
| `properties()` | `properties()` | `traversal::transform` |
| `properties(key...)` | `properties_keys(keys)` | `traversal::transform` |
| `values(key)` | `values(key)` | `traversal::transform` |
| `values(key...)` | `values_multi(keys)` | `traversal::transform` |
| `propertyMap()` | `property_map()` | `traversal::transform` |
| `propertyMap(key...)` | `property_map_keys(keys)` | `traversal::transform` |
| `valueMap()` | `value_map()` | `traversal::transform` |
| `valueMap(key...)` | `value_map_keys(keys)` | `traversal::transform` |
| `valueMap(true)` | `value_map_with_tokens()` | `traversal::transform` |
| `elementMap()` | `element_map()` | `traversal::transform` |
| `elementMap(key...)` | `element_map_keys(keys)` | `traversal::transform` |
| `key()` | `key()` | `traversal::transform` |
| `value()` | `value()` | `traversal::transform` |
| `path()` | `path()` | `traversal::transform` |
| `select(labels...)` | `select(labels)` | `traversal::transform` |
| `select(label)` | `select_one(label)` | `traversal::transform` |
| `project(keys...)` | `project(keys).by().build()` | `traversal::transform` |
| `unfold()` | `unfold()` | `traversal::transform` |
| `fold()` | `fold()` | `traversal::source` (terminal) |
| `count()` | `count()` | `traversal::source` (terminal) |
| `sum()` | `sum()` | `traversal::source` (terminal) |
| `max()` | `max()` | `traversal::source` (terminal) |
| `min()` | `min()` | `traversal::source` (terminal) |
| `mean()` | `mean()` | `traversal::transform` |
| `order()` | `order().build()` | `traversal::transform` |
| `order().by(key)` | `order().by_key_asc(key).build()` | `traversal::transform` |
| `order().by(key, desc)` | `order().by_key_desc(key).build()` | `traversal::transform` |
| `math(expression)` | `math(expression).build()` | `traversal::transform` |
| `index()` | `index()` | `traversal::transform` |
| `loops()` | `loops()` | `traversal::transform` |

---

## Aggregation Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `group()` | `group().by_*().build()` | `traversal::aggregate` |
| `group().by(key)` | `group().by_key(key).build()` | `traversal::aggregate` |
| `group().by(label)` | `group().by_label().build()` | `traversal::aggregate` |
| `groupCount()` | `group_count().by_*().build()` | `traversal::aggregate` |
| `groupCount().by(key)` | `group_count().by_key(key).build()` | `traversal::aggregate` |
| `groupCount().by(label)` | `group_count().by_label().build()` | `traversal::aggregate` |

---

## Branch Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `branch(traversal)` | `branch(traversal).option()` | `traversal::branch` |
| `choose(cond, true, false)` | `choose(condition, if_true, if_false)` | `traversal::branch` |
| `choose(traversal).option()` | `choose_by(traversal).option()`, `branch(traversal).option()` | `traversal::branch` |
| `union(traversal...)` | `union(traversals)` | `traversal::branch` |
| `coalesce(traversal...)` | `coalesce(traversals)` | `traversal::branch` |
| `optional(traversal)` | `optional(traversal)` | `traversal::branch` |
| `local(traversal)` | `local(traversal)` | `traversal::branch` |

---

## Repeat Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `repeat(traversal)` | `repeat(traversal)` | `traversal::repeat` |
| `repeat().times(n)` | `repeat().times(n)` | `traversal::repeat` |
| `repeat().until(traversal)` | `repeat().until(traversal)` | `traversal::repeat` |
| `repeat().emit()` | `repeat().emit()` | `traversal::repeat` |
| `repeat().emit(traversal)` | `repeat().emit_if(traversal)` | `traversal::repeat` |
| `repeat().emit()` (before loop) | `repeat().emit_first()` | `traversal::repeat` |
| `loops()` | `loops()` | `traversal::repeat` |

---

## Side Effect Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `sideEffect(traversal)` | `side_effect(traversal)` | `traversal::sideeffect` |
| `aggregate(key)` | `aggregate(key)` | `traversal::sideeffect` |
| `store(key)` | `store(key)` | `traversal::sideeffect` |
| `subgraph()` | - | - |
| `cap(key)` | `cap(key)` | `traversal::sideeffect` |
| `cap(key...)` | `cap_multi(keys)` | `traversal::sideeffect` |
| `profile()` | `profile()`, `profile_as(key)` | `traversal::sideeffect` |

---

## Mutation Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `addV()` | `add_v()` | `traversal::source`, `traversal::mutation` |
| `addE()` | `add_e().from_vertex().to_vertex()` | `traversal::source`, `traversal::mutation` |
| `property()` | `property()` | `traversal::mutation` |
| `from()` | `.from_vertex()`, `.from_label()` | `traversal::mutation` |
| `to()` | `.to_vertex()`, `.to_label()` | `traversal::mutation` |
| `drop()` | `drop()` | `traversal::mutation` |
| `mergeV()` | - | - |
| `mergeE()` | - | - |

---

## Modulator Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `as(label)` | `as_(label)` | `traversal::transform` |
| `by()` | `.by_*()` methods on builders | Various |
| `with()` | - | - |
| `option(key, traversal)` | `.option(key, traversal)` on `BranchBuilder` | `traversal::source` |
| `option(none)` | `.option_none(traversal)` on `BranchBuilder` | `traversal::source` |
| - | `with_path()` | `traversal::source` |
| `from()` | - | - |
| `to()` | - | - |
| `read()` | - | - |
| `write()` | - | - |

---

## Terminal Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `next()` | `next()` | `traversal::source` |
| `next(n)` | `take(n)` | `traversal::source` |
| `toList()` | `to_list()` | `traversal::source` |
| `toSet()` | `to_set()` | `traversal::source` |
| `toBulkSet()` | - | - |
| `iterate()` | `iterate()` | `traversal::source` |
| `hasNext()` | `has_next()` | `traversal::source` |
| `tryNext()` | - | - |
| `one()` | `one()` | `traversal::source` |
| `explain()` | - | - |
| `profile()` | - | - |
| - | `iter()` | `traversal::source` |
| - | `traversers()` | `traversal::source` |
| - | `fold()` | `traversal::source` |
| - | `sum()` | `traversal::source` |
| - | `min()` | `traversal::source` |
| - | `max()` | `traversal::source` |
| - | `count()` | `traversal::source` |
| - | `to_vertex_list()` | `traversal::source` |
| - | `next_vertex()` | `traversal::source` |
| - | `one_vertex()` | `traversal::source` |
| - | `to_edge_list()` | `traversal::source` |
| - | `next_edge()` | `traversal::source` |
| - | `one_edge()` | `traversal::source` |

---

## Graph Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `V()` | `v()`, `v_ids()`, `v_by_id()` | `traversal::source` |
| `V()` + index | `v_by_property()`, `v_by_property_range()` | `traversal::source` |
| `E()` | `e()`, `e_ids()` | `traversal::source` |
| `E()` + index | `e_by_property()` | `traversal::source` |
| `tx()` | - | - |
| `io()` | - | - |
| `call()` | - | - |

---

## Predicate Functions (P module)

| Gremlin Predicate | Rust Predicate | Module |
|------------------|----------------|--------|
| `P.eq(value)` | `p::eq(value)` | `traversal::predicate` |
| `P.neq(value)` | `p::neq(value)` | `traversal::predicate` |
| `P.lt(value)` | `p::lt(value)` | `traversal::predicate` |
| `P.lte(value)` | `p::lte(value)` | `traversal::predicate` |
| `P.gt(value)` | `p::gt(value)` | `traversal::predicate` |
| `P.gte(value)` | `p::gte(value)` | `traversal::predicate` |
| `P.between(start, end)` | `p::between(start, end)` | `traversal::predicate` |
| `P.inside(start, end)` | `p::inside(start, end)` | `traversal::predicate` |
| `P.outside(start, end)` | `p::outside(start, end)` | `traversal::predicate` |
| `P.within(values...)` | `p::within(values)` | `traversal::predicate` |
| `P.without(values...)` | `p::without(values)` | `traversal::predicate` |
| `P.and(p1, p2)` | `p::and(p1, p2)` | `traversal::predicate` |
| `P.or(p1, p2)` | `p::or(p1, p2)` | `traversal::predicate` |
| `P.not(predicate)` | `p::not(predicate)` | `traversal::predicate` |

## Text Predicates (TextP module)

| Gremlin Predicate | Rust Predicate | Module |
|------------------|----------------|--------|
| `TextP.containing(str)` | `p::containing(str)` | `traversal::predicate` |
| `TextP.startingWith(str)` | `p::starting_with(str)` | `traversal::predicate` |
| `TextP.endingWith(str)` | `p::ending_with(str)` | `traversal::predicate` |
| `TextP.notContaining(str)` | `p::not_containing(str)` | `traversal::predicate` |
| `TextP.notStartingWith(str)` | `p::not_starting_with(str)` | `traversal::predicate` |
| `TextP.notEndingWith(str)` | `p::not_ending_with(str)` | `traversal::predicate` |
| `TextP.regex(pattern)` | `p::regex(pattern)` | `traversal::predicate` |

---

## Anonymous Traversal Factory (`__` module)

The double underscore `__` provides anonymous traversal spawning for nested traversals used in steps like `where()`, `filter()`, `map()`, etc.

All implemented steps are available as factory functions in the `__` module for creating anonymous traversals:

```rust
use interstellar::traversal::__;

// Examples
let friends = __::out_labels(&["knows"]).has_label("person");
let adults = __::has_where("age", p::gte(18));
let names = __::values("name");
```

### Available Factory Functions

**Identity:**
- `__::identity()`

**Navigation (Vertex to Vertex):**
- `__::out()`, `__::out_labels()`
- `__::in_()`, `__::in_labels()`
- `__::both()`, `__::both_labels()`

**Navigation (Vertex to Edge):**
- `__::out_e()`, `__::out_e_labels()`
- `__::in_e()`, `__::in_e_labels()`
- `__::both_e()`, `__::both_e_labels()`

**Navigation (Edge to Vertex):**
- `__::out_v()`, `__::in_v()`, `__::both_v()`, `__::other_v()`

**Filter:**
- `__::has_label()`, `__::has_label_any()`
- `__::has()`, `__::has_not()`, `__::has_value()`, `__::has_where()`
- `__::has_id()`, `__::has_ids()`
- `__::has_key()`, `__::has_key_any()`, `__::has_prop_value()`, `__::has_prop_value_any()`
- `__::is_()`, `__::is_eq()`, `__::filter()`
- `__::dedup()`, `__::dedup_by_key()`, `__::dedup_by_label()`, `__::dedup_by()`
- `__::limit()`, `__::skip()`, `__::range()`
- `__::tail()`, `__::tail_n()`, `__::coin()`, `__::sample()`
- `__::simple_path()`, `__::cyclic_path()`

**Transform:**
- `__::values()`, `__::values_multi()`
- `__::properties()`, `__::properties_keys()`
- `__::value_map()`, `__::value_map_keys()`, `__::value_map_with_tokens()`
- `__::element_map()`, `__::element_map_keys()`
- `__::property_map()`, `__::property_map_keys()`
- `__::id()`, `__::label()`, `__::constant()`, `__::path()`
- `__::key()`, `__::value()`, `__::index()`, `__::loops()`
- `__::unfold()`, `__::mean()`, `__::order()`, `__::math()`, `__::project()`
- `__::map()`, `__::flat_map()`

**Aggregation:**
- `__::group()`, `__::group_count()`

**Side Effect:**
- `__::as_()`, `__::select()`, `__::select_one()`
- `__::store()`, `__::aggregate()`, `__::cap()`
- `__::side_effect()`, `__::profile()`

**Branch/Filter with Sub-traversals:**
- `__::where_()`, `__::where_p()`, `__::not()`, `__::and_()`, `__::or_()`
- `__::union()`, `__::coalesce()`, `__::choose()`, `__::optional()`, `__::local()`
- `__::branch()`

**Mutation:**
- `__::add_v()`, `__::add_e()`, `__::property()`, `__::drop()`

---

## GQL (Graph Query Language) Mutations

Interstellar also supports GQL, a declarative SQL-like query language for graphs. GQL mutations provide an alternative to the Gremlin fluent API for modifying graphs.

### Using GQL Mutations

GQL mutations are executed via `execute_mutation()` with mutable storage:

```rust
use interstellar::gql::{parse_statement, execute_mutation};
use interstellar::storage::InMemoryGraph;

let mut storage = InMemoryGraph::new();

// Parse and execute a mutation
let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
execute_mutation(&stmt, &mut storage).unwrap();
```

### GQL Mutation Clauses

| GQL Clause | Gremlin Equivalent | Description |
|------------|-------------------|-------------|
| `CREATE (n:Label {props})` | `g.addV("Label").property(...)` | Create a new vertex |
| `CREATE (a)-[:REL]->(b)` | `g.addE("REL").from(a).to(b)` | Create an edge |
| `SET n.prop = value` | `g.V(id).property("prop", value)` | Update properties |
| `REMOVE n.prop` | - | Remove a property (set to null) |
| `DELETE n` | `g.V(id).drop()` | Delete element (vertex must have no edges) |
| `DETACH DELETE n` | `g.V(id).drop()` with edge cleanup | Delete vertex and all connected edges |
| `MERGE (n:Label {key: value})` | `g.mergeV()` (not implemented) | Match or create (upsert) |

### CREATE - Adding Elements

```sql
-- Create a vertex
CREATE (n:Person {name: 'Alice', age: 30})

-- Create multiple vertices
CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})

-- Create vertex and edge pattern
CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})

-- Create chain of relationships
CREATE (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person)

-- Create with RETURN to get the created element
CREATE (n:Person {name: 'Alice'}) RETURN n
CREATE (n:Person {name: 'Alice'}) RETURN n.name
```

### SET - Updating Properties

```sql
-- Update single property
MATCH (n:Person {name: 'Alice'}) SET n.age = 31

-- Update multiple properties
MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.status = 'active'

-- Computed values
MATCH (n:Person {name: 'Alice'}) SET n.next_age = n.age + 1

-- Update with WHERE clause filtering
MATCH (n:Person) WHERE n.age > 30 SET n.senior = true

-- Update with RETURN
MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n.age
```

### REMOVE - Removing Properties

```sql
-- Remove a property (sets to null)
MATCH (n:Person {name: 'Alice'}) REMOVE n.temporary_field

-- Remove multiple properties
MATCH (n:Person) REMOVE n.cache, n.temp_data
```

### DELETE - Removing Elements

```sql
-- Delete a vertex (must have no edges)
MATCH (n:Person {name: 'Alice'}) DELETE n

-- Delete an edge
MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r

-- Delete multiple elements
MATCH (n:Temp) DELETE n
```

**Note:** `DELETE` will fail with `VertexHasEdges` error if you try to delete a vertex that has connected edges. Use `DETACH DELETE` instead.

### DETACH DELETE - Removing Vertices with Edges

```sql
-- Delete vertex and all its connected edges
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n

-- Delete multiple vertices with edges
MATCH (n:Inactive) DETACH DELETE n
```

### MERGE - Upsert Operations

```sql
-- Match or create
MERGE (n:Person {name: 'Alice'})

-- With ON CREATE action (runs if created)
MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created_at = 1234567890

-- With ON MATCH action (runs if matched existing)
MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.last_seen = 1234567890

-- With both actions
MERGE (n:Person {name: 'Alice'}) 
ON CREATE SET n.status = 'new', n.created = true
ON MATCH SET n.status = 'existing', n.visits = n.visits + 1
```

### GQL vs Gremlin Comparison

| Operation | GQL | Gremlin |
|-----------|-----|---------|
| Create vertex | `CREATE (n:Person {name: 'Alice'})` | `g.addV("Person").property("name", "Alice")` |
| Create edge | `CREATE (a)-[:KNOWS]->(b)` | `g.addE("KNOWS").from(a).to(b)` |
| Update property | `MATCH (n) SET n.age = 31` | `g.V(id).property("age", 31)` |
| Delete vertex | `MATCH (n) DELETE n` | `g.V(id).drop()` |
| Delete with edges | `MATCH (n) DETACH DELETE n` | Manual edge cleanup + drop |
| Upsert | `MERGE (n:Person {name: 'Alice'})` | Not directly available |

### Error Handling

GQL mutations can fail with `MutationError`:

```rust
use interstellar::gql::MutationError;

match execute_mutation(&stmt, &mut storage) {
    Ok(results) => println!("Success: {:?}", results),
    Err(MutationError::VertexHasEdges(id)) => {
        println!("Cannot delete vertex {:?}: has edges", id);
    }
    Err(MutationError::UnboundVariable(var)) => {
        println!("Variable '{}' not found in MATCH", var);
    }
    Err(e) => println!("Error: {}", e),
}
```

### Current Limitations

GQL mutations do not currently support:
- Comma-separated MATCH patterns (`MATCH (a), (b)`)
- Anonymous endpoint patterns (`MATCH ()-[r]->()`)
- Label mutations (`SET n:Label`, `REMOVE n:Label`)
- Map property assignment (`SET n += {key: value}`)
- FOREACH clause

---

## Unsupported Gremlin Features

The following Gremlin features are not currently planned for support:

| Feature | Reason |
|---------|--------|
| `subgraph()` | Complex graph construction |
| `tree()` | Specialized data structure |
| `sack()` / `withSack()` | Requires stateful traverser |
| `barrier()` | Explicit synchronization (implicit in reduce steps) |
| `match()` | Complex pattern matching |
| `program()` | VertexProgram execution |
| `io()` | Graph I/O (use native import/export) |
| `call()` | Procedure calls |
| `tx()` | Transaction management (handled at storage level) |
| Lambda steps | Security/portability concerns |

---

## Implementation Summary

| Category | Implemented | Not Implemented |
|----------|-------------|-----------------|
| Source Steps | 8 | 0 |
| Filter Steps | 34 | 1 |
| Navigation Steps | 16 | 0 |
| Transform/Map Steps | 30 | 0 |
| Aggregation Steps | 6 | 0 |
| Branch Steps | 8 | 0 |
| Repeat Steps | 7 | 0 |
| Side Effect Steps | 6 | 1 |
| Mutation Steps | 6 | 2 |
| Modulator Steps | 5 | 4 |
| Terminal Steps | 19 | 4 |
| Predicates (P) | 14 | 0 |
| Text Predicates | 7 | 0 |
| **Total** | **~166** | **~11** |
