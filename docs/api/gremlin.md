# Interstellar API Reference

This document maps standard Gremlin steps (TinkerPop 3.x) to their Rust and Rhai implementations in Interstellar.

## Legend

| Symbol | Meaning |
|--------|---------|
| `function_name` | Implemented |
| - | Not implemented |

## API Naming Differences

| Gremlin | Rust | Rhai | Reason |
|---------|------|------|--------|
| `in()` | `in_()` | `in_()` | `in` is a keyword in both Rust and Rhai |
| `as()` | `as_()` | `as_()` | `as` is a keyword in both Rust and Rhai |
| `is()` | `is_()` | `is_()` | `is` is a Rust keyword; reserved in Rhai |
| `where()` | `where_()` | `where_()` | `where` is a Rust keyword; Rhai follows for consistency |
| `and()` | `and_()` | `and_()` | `and` is a Rust keyword; Rhai follows for consistency |
| `or()` | `or_()` | `or_()` | `or` is a Rust keyword; Rhai follows for consistency |
| `has(key, value)` | `has_value(key, value)` | `has_value(key, value)` | Distinguishes from `has(key)` existence check |
| `drop()` | `drop()` | `drop_()` | `drop` is a reserved function name in Rhai |
| `__` (anonymous) | `__.` | `A.` | Rhai doesn't allow identifiers starting with `_` |
| `value()` (property) | `value()` | `prop_value()` | Avoids conflict with Rhai's `Value` type |

---

## Source Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `V()` | `v()` | `g.v()` |
| `V(id)` | `v_by_id(id)` | `g.v_id(id)` |
| `V(id...)` | `v_ids(ids)` | `g.v_ids([ids])` |
| `V()` + index | `v_by_property()`, `v_by_property_range()` | - |
| `E()` | `e()` | `g.e()` |
| `E(id)` | - | `g.e_id(id)` |
| `E(id...)` | `e_ids(ids)` | `g.e_ids([ids])` |
| `E()` + index | `e_by_property()` | - |
| `addV(label)` | `add_v(label)` | `g.add_v("label")` |
| `addE(label)` | `add_e(label)` | `g.add_e("label")` |
| `inject(values...)` | `inject(values)` | `g.inject([values])` |

---

## Navigation Steps (Vertex to Vertex)

| Gremlin | Rust | Rhai |
|---------|------|------|
| `out()` | `out()` | `out()` |
| `out(label...)` | `out_labels(&[labels])` | `out_labels(["labels"])` |
| `in()` | `in_()` | `in_()` |
| `in(label...)` | `in_labels(&[labels])` | `in_labels(["labels"])` |
| `both()` | `both()` | `both()` |
| `both(label...)` | `both_labels(&[labels])` | `both_labels(["labels"])` |

## Navigation Steps (Vertex to Edge)

| Gremlin | Rust | Rhai |
|---------|------|------|
| `outE()` | `out_e()` | `out_e()` |
| `outE(label...)` | `out_e_labels(&[labels])` | `out_e_labels(["labels"])` |
| `inE()` | `in_e()` | `in_e()` |
| `inE(label...)` | `in_e_labels(&[labels])` | `in_e_labels(["labels"])` |
| `bothE()` | `both_e()` | `both_e()` |
| `bothE(label...)` | `both_e_labels(&[labels])` | `both_e_labels(["labels"])` |

## Navigation Steps (Edge to Vertex)

| Gremlin | Rust | Rhai |
|---------|------|------|
| `outV()` | `out_v()` | `out_v()` |
| `inV()` | `in_v()` | `in_v()` |
| `bothV()` | `both_v()` | `both_v()` |
| `otherV()` | `other_v()` | `other_v()` |

---

## Filter Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `has(key)` | `has(key)` | `has("key")` |
| `has(key, value)` | `has_value(key, value)` | `has_value("key", value)` |
| `has(key, predicate)` | `has_where(key, predicate)` | `has_where("key", predicate)` |
| `hasLabel(label)` | `has_label(label)` | `has_label("label")` |
| `hasLabel(label...)` | `has_label_any(&[labels])` | `has_label_any(["labels"])` |
| `hasId(id)` | `has_id(id)` | `has_id(id)` |
| `hasId(id...)` | `has_ids(&[ids])` | `has_ids([ids])` |
| `hasNot(key)` | `has_not(key)` | `has_not("key")` |
| `hasKey(key)` | `has_key(key)` | - |
| `hasKey(key...)` | `has_key_any(&[keys])` | - |
| `hasValue(value)` | `has_prop_value(value)` | - |
| `hasValue(value...)` | `has_prop_value_any(&[values])` | - |
| `filter(traversal)` | `filter(closure)` | - |
| `where(traversal)` | `where_(traversal)` | `where_(traversal)` |
| `where(predicate)` | `where_p(predicate)` | - |
| `not(traversal)` | `not(traversal)` | `not(traversal)` |
| `and(traversal...)` | `and_(&[traversals])` | `and_([traversals])` |
| `or(traversal...)` | `or_(&[traversals])` | `or_([traversals])` |
| `is(value)` | `is_eq(value)` | `is_eq(value)` |
| `is(predicate)` | `is_(predicate)` | `is_(predicate)` |
| `dedup()` | `dedup()` | `dedup()` |
| `dedup().by(key)` | `dedup_by_key(key)` | `dedup_by_key("key")` |
| `dedup().by(label)` | `dedup_by_label()` | `dedup_by_label()` |
| `dedup().by(traversal)` | `dedup_by(traversal)` | `dedup_by(traversal)` |
| `limit(n)` | `limit(n)` | `limit(n)` |
| `skip(n)` | `skip(n)` | `skip(n)` |
| `range(start, end)` | `range(start, end)` | `range(start, end)` |
| `tail()` | `tail()` | `tail()` |
| `tail(n)` | `tail_n(n)` | `tail_n(n)` |
| `coin(probability)` | `coin(probability)` | `coin(probability)` |
| `sample(n)` | `sample(n)` | `sample(n)` |
| `simplePath()` | `simple_path()` | `simple_path()` |
| `cyclicPath()` | `cyclic_path()` | `cyclic_path()` |
| `timeLimit()` | - | - |

---

## Transform / Map Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `map(traversal)` | `map(closure)` | - |
| `flatMap(traversal)` | `flat_map(closure)` | - |
| `identity()` | `identity()` | `identity()` |
| `constant(value)` | `constant(value)` | `constant(value)` |
| `id()` | `id()` | `id()` |
| `label()` | `label()` | `label()` |
| `properties()` | `properties()` | `properties()` |
| `properties(key...)` | `properties_keys(&[keys])` | `properties_keys(["keys"])` |
| `values(key)` | `values(key)` | `values("key")` |
| `values(key...)` | `values_multi(&[keys])` | `values_multi(["keys"])` |
| `propertyMap()` | `property_map()` | - |
| `propertyMap(key...)` | `property_map_keys(&[keys])` | - |
| `valueMap()` | `value_map()` | `value_map()` |
| `valueMap(key...)` | `value_map_keys(&[keys])` | `value_map_keys(["keys"])` |
| `valueMap(true)` | `value_map_with_tokens()` | `value_map_with_tokens()` |
| `elementMap()` | `element_map()` | `element_map()` |
| `elementMap(key...)` | `element_map_keys(&[keys])` | - |
| `key()` | `key()` | `key()` |
| `value()` | `value()` | `prop_value()` |
| `path()` | `path()` | `path()` |
| `select(labels...)` | `select(&[labels])` | `select(["labels"])` |
| `select(label)` | `select_one(label)` | `select_one("label")` |
| `project(keys...)` | `project(&[keys]).by().build()` | `project(keys, projections)` |
| `unfold()` | `unfold()` | `unfold()` |
| `fold()` | `fold()` | `fold()` |
| `count()` | `count()` | `count()` / `count_step()` |
| `sum()` | `sum()` | `sum()` |
| `max()` | `max()` | `max()` |
| `min()` | `min()` | `min()` |
| `mean()` | `mean()` | `mean()` |
| `order()` | `order().build()` | `order_asc()` / `order_desc()` |
| `order().by(key)` | `order().by_key_asc(key).build()` | `order_by("key")` |
| `order().by(key, desc)` | `order().by_key_desc(key).build()` | `order_by_desc("key")` |
| `order().by(traversal)` | `order().by_traversal(t).build()` | `order_by_traversal(t)` |
| `math(expression)` | `math(expr).build()` | `math("expr")` |
| `math()` with bindings | `math(expr).bind(k,v).build()` | `math_with_bindings(expr, bindings)` |
| `index()` | `index()` | `index()` |
| `loops()` | `loops()` | - |

---

## Aggregation Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `group()` | `group().by_*().build()` | `group(key_selector, value_collector)` |
| `group().by(key)` | `group().by_key(key).build()` | `group_by_key("key")` |
| `group().by(label)` | `group().by_label().build()` | `group_by_label()` |
| `groupCount()` | `group_count().by_*().build()` | `group_count(key_selector)` |
| `groupCount().by(key)` | `group_count().by_key(key).build()` | `group_count_by_key("key")` |
| `groupCount().by(label)` | `group_count().by_label().build()` | `group_count_by_label()` |

---

## Branch Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `branch(traversal)` | `branch(traversal).option()` | - |
| `choose(cond, true, false)` | `choose(cond, if_true, if_false)` | `choose_binary(cond, true_t, false_t)` |
| `choose(traversal).option()` | `choose_by(t).option()` | `choose_options(key_t, options, default)` |
| `union(traversal...)` | `union(&[traversals])` | `union([traversals])` |
| `coalesce(traversal...)` | `coalesce(&[traversals])` | `coalesce([traversals])` |
| `optional(traversal)` | `optional(traversal)` | `optional(traversal)` |
| `local(traversal)` | `local(traversal)` | `local(traversal)` |

---

## Repeat Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `repeat(traversal)` | `repeat(traversal)` | - |
| `repeat().times(n)` | `repeat().times(n)` | `repeat_times(traversal, n)` |
| `repeat().until(traversal)` | `repeat().until(traversal)` | `repeat_until(traversal, until)` |
| `repeat().emit()` | `repeat().emit()` | `repeat_emit(traversal, n)` |
| `repeat().emit(traversal)` | `repeat().emit_if(traversal)` | `repeat_emit_until(traversal, until)` |
| `repeat().emit()` (before) | `repeat().emit_first()` | - |
| `loops()` | `loops()` | - |

---

## Side Effect Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `sideEffect(traversal)` | `side_effect(traversal)` | `side_effect(traversal)` |
| `aggregate(key)` | `aggregate(key)` | `aggregate("key")` |
| `store(key)` | `store(key)` | `store("key")` |
| `subgraph()` | - | - |
| `cap(key)` | `cap(key)` | `cap("key")` |
| `cap(key...)` | `cap_multi(&[keys])` | `cap_multi(["keys"])` |
| `profile()` | `profile()`, `profile_as(key)` | - |

---

## Mutation Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `addV(label)` | `add_v(label)` | `add_v("label")` |
| `addE(label)` | `add_e(label)` | `add_e("label")` |
| `property(key, value)` | `property(key, value)` | `property("key", value)` |
| `from(vertex)` | `from_vertex(id)` | `from_v(id)` |
| `from(label)` | `from_label(label)` | `from_label("label")` |
| `to(vertex)` | `to_vertex(id)` | `to_v(id)` |
| `to(label)` | `to_label(label)` | `to_label("label")` |
| `drop()` | `drop()` | `drop_()` |
| `mergeV()` | - | - |
| `mergeE()` | - | - |

---

## Modulator Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `as(label)` | `as_(label)` | `as_("label")` |
| `by()` | `.by_*()` methods | (integrated in steps) |
| `with()` | - | - |
| `option(key, traversal)` | `.option(key, traversal)` | (via `choose_options`) |
| `option(none)` | `.option_none(traversal)` | (via `choose_options`) |
| - | `with_path()` | `with_path()` |
| `read()` | - | - |
| `write()` | - | - |

---

## Terminal Steps

| Gremlin | Rust | Rhai |
|---------|------|------|
| `next()` | `next()` | `first()` |
| `next(n)` | `take(n)` | `take(n)` |
| `toList()` | `to_list()` | `to_list()` |
| `toSet()` | `to_set()` | `to_set()` |
| `toBulkSet()` | - | - |
| `iterate()` | `iterate()` | `iterate()` |
| `hasNext()` | `has_next()` | `has_next()` |
| `tryNext()` | - | - |
| `one()` | `one()` | `one()` |
| `explain()` | - | - |
| `profile()` | - | - |
| - | `iter()` | - |
| - | `traversers()` | - |
| - | `to_vertex_list()` | - |
| - | `next_vertex()` | - |
| - | `one_vertex()` | - |
| - | `to_edge_list()` | - |
| - | `next_edge()` | - |
| - | `one_edge()` | - |
| - | - | `to_rich_list()` |

---

## Predicate Functions

| Gremlin | Rust | Rhai |
|---------|------|------|
| `P.eq(value)` | `p::eq(value)` | `eq(value)` |
| `P.neq(value)` | `p::neq(value)` | `neq(value)` |
| `P.lt(value)` | `p::lt(value)` | `lt(value)` |
| `P.lte(value)` | `p::lte(value)` | `lte(value)` |
| `P.gt(value)` | `p::gt(value)` | `gt(value)` |
| `P.gte(value)` | `p::gte(value)` | `gte(value)` |
| `P.between(start, end)` | `p::between(start, end)` | `between(start, end)` |
| `P.inside(start, end)` | `p::inside(start, end)` | `inside(start, end)` |
| `P.outside(start, end)` | `p::outside(start, end)` | `outside(start, end)` |
| `P.within(values...)` | `p::within(&[values])` | `within([values])` |
| `P.without(values...)` | `p::without(&[values])` | `without([values])` |
| `P.and(p1, p2)` | `p::and(p1, p2)` | `pred_and(p1, p2)` |
| `P.or(p1, p2)` | `p::or(p1, p2)` | `pred_or(p1, p2)` |
| `P.not(predicate)` | `p::not(predicate)` | `pred_not(predicate)` |

## Text Predicates

| Gremlin | Rust | Rhai |
|---------|------|------|
| `TextP.containing(str)` | `p::containing(str)` | `containing("str")` |
| `TextP.startingWith(str)` | `p::starting_with(str)` | `starting_with("str")` |
| `TextP.endingWith(str)` | `p::ending_with(str)` | `ending_with("str")` |
| `TextP.notContaining(str)` | `p::not_containing(str)` | `not_containing("str")` |
| `TextP.notStartingWith(str)` | `p::not_starting_with(str)` | `not_starting_with("str")` |
| `TextP.notEndingWith(str)` | `p::not_ending_with(str)` | `not_ending_with("str")` |
| `TextP.regex(pattern)` | `p::regex(pattern)` | `regex("pattern")` |

---

## Anonymous Traversal Factory

| Gremlin | Rust | Rhai |
|---------|------|------|
| `__.identity()` | `__.identity()` | `A.identity()` |
| `__.out()` | `__.out()` | `A.out()` |
| `__.out(label)` | `__.out_labels(&[label])` | `A.out("label")` |
| `__.in()` | `__.in_()` | `A.in_()` |
| `__.in(label)` | `__.in_labels(&[label])` | `A.in_("label")` |
| `__.both()` | `__.both()` | `A.both()` |
| `__.outE()` | `__.out_e()` | `A.out_e()` |
| `__.inE()` | `__.in_e()` | `A.in_e()` |
| `__.bothE()` | `__.both_e()` | `A.both_e()` |
| `__.outV()` | `__.out_v()` | `A.out_v()` |
| `__.inV()` | `__.in_v()` | `A.in_v()` |
| `__.otherV()` | `__.other_v()` | `A.other_v()` |
| `__.hasLabel(label)` | `__.has_label(label)` | `A.has_label("label")` |
| `__.has(key)` | `__.has(key)` | `A.has("key")` |
| `__.hasNot(key)` | `__.has_not(key)` | `A.has_not("key")` |
| `__.has(key, value)` | `__.has_value(key, value)` | `A.has_value("key", value)` |
| `__.dedup()` | `__.dedup()` | `A.dedup()` |
| `__.limit(n)` | `__.limit(n)` | `A.limit(n)` |
| `__.id()` | `__.id()` | `A.id()` |
| `__.label()` | `__.label()` | `A.label()` |
| `__.values(key)` | `__.values(key)` | `A.values("key")` |
| `__.valueMap()` | `__.value_map()` | `A.value_map()` |
| `__.path()` | `__.path()` | `A.path()` |
| `__.constant(value)` | `__.constant(value)` | `A.constant(value)` |
| `__.fold()` | `__.fold()` | `A.fold()` |
| `__.unfold()` | `__.unfold()` | `A.unfold()` |
| `__.as(label)` | `__.as_(label)` | `A.as_("label")` |

### Additional Rust-only Anonymous Functions

These are available in Rust via `__.` but not yet exposed in Rhai's `A` factory:

- **Filter:** `has_label_any`, `has_id`, `has_ids`, `has_key`, `has_key_any`, `has_prop_value`, `has_prop_value_any`, `is_`, `is_eq`, `filter`, `skip`, `range`, `tail`, `tail_n`, `coin`, `sample`, `simple_path`, `cyclic_path`, `dedup_by_key`, `dedup_by_label`, `dedup_by`
- **Transform:** `values_multi`, `properties`, `properties_keys`, `value_map_keys`, `value_map_with_tokens`, `element_map`, `element_map_keys`, `property_map`, `property_map_keys`, `key`, `value`, `index`, `loops`, `mean`, `order`, `math`, `project`, `map`, `flat_map`
- **Aggregation:** `group`, `group_count`
- **Side Effect:** `select`, `select_one`, `store`, `aggregate`, `cap`, `side_effect`, `profile`
- **Branch:** `where_`, `where_p`, `not`, `and_`, `or_`, `union`, `coalesce`, `choose`, `optional`, `local`, `branch`
- **Mutation:** `add_v`, `add_e`, `property`, `drop`

---

## Rhai Scripting Quick Start

```rust
use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use std::sync::Arc;

let engine = RhaiEngine::new();
let graph = Arc::new(Graph::new());
// ... populate graph ...

let script = r#"
    let g = graph.gremlin();
    g.v().has_label("person").values("name").to_list()
"#;

let result = engine.eval_with_graph(graph, script)?;
```

### Rhai Value Constructors

```javascript
let v = value_int(42);
let v = value_float(3.14);
let v = value_string("hello");
let v = value_bool(true);
```

### Rhai Storage Backend Support

```rust
// In-memory graph
let engine = RhaiEngine::new();
let graph = Arc::new(Graph::new());
let result = engine.eval_with_graph(graph, script)?;

// Persistent mmap graph (requires "mmap" feature)
#[cfg(feature = "mmap")]
{
    let mmap_graph = Arc::new(CowMmapGraph::open("data.db")?);
    let result = engine.eval_with_mmap_graph(mmap_graph, script)?;
}
```

### Rhai Example Scripts

```javascript
// Find friends of Alice over 25
let g = graph.gremlin();
g.v()
  .has_value("name", "Alice")
  .out_labels(["knows"])
  .has_where("age", gt(25))
  .values("name")
  .to_list()

// Count vertices by label
let g = graph.gremlin();
g.v().group_count_by_label().to_list()

// Find all paths of length 2
let g = graph.gremlin();
g.v()
  .with_path()
  .out().out()
  .path()
  .to_list()

// Using predicates
let g = graph.gremlin();
g.v()
  .has_where("age", pred_and(gte(18), lt(65)))
  .values("name")
  .to_list()

// Create and query
let g = graph.gremlin();
let id = g.add_v("person").property("name", "Dave").first();
g.v_id(id).values("name").first()
```

---

## GQL (Graph Query Language) Mutations

Interstellar also supports GQL, a declarative SQL-like query language for graphs.

### GQL Mutation Clauses

| GQL Clause | Gremlin Equivalent | Rust |
|------------|-------------------|------|
| `CREATE (n:Label {props})` | `addV("Label").property(...)` | `parse_statement()` + `execute_mutation()` |
| `CREATE (a)-[:REL]->(b)` | `addE("REL").from(a).to(b)` | `parse_statement()` + `execute_mutation()` |
| `SET n.prop = value` | `V(id).property("prop", value)` | `parse_statement()` + `execute_mutation()` |
| `REMOVE n.prop` | - | `parse_statement()` + `execute_mutation()` |
| `DELETE n` | `V(id).drop()` | `parse_statement()` + `execute_mutation()` |
| `DETACH DELETE n` | `V(id).drop()` + edges | `parse_statement()` + `execute_mutation()` |
| `MERGE (n:Label {key: value})` | `mergeV()` (not impl) | `parse_statement()` + `execute_mutation()` |

### GQL Examples

```sql
-- Create a vertex
CREATE (n:Person {name: 'Alice', age: 30})

-- Create vertex and edge pattern
CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})

-- Update properties
MATCH (n:Person {name: 'Alice'}) SET n.age = 31

-- Delete with edges
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n

-- Upsert
MERGE (n:Person {name: 'Alice'}) 
ON CREATE SET n.status = 'new'
ON MATCH SET n.visits = n.visits + 1
```

---

## Unsupported Gremlin Features

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

| Category | Gremlin | Rust | Rhai |
|----------|---------|------|------|
| Source Steps | 7 | 8 | 9 |
| Navigation Steps | 12 | 16 | 16 |
| Filter Steps | 33 | 34 | 28 |
| Transform/Map Steps | 28 | 30 | 26 |
| Aggregation Steps | 6 | 6 | 6 |
| Branch Steps | 7 | 8 | 6 |
| Repeat Steps | 6 | 7 | 4 |
| Side Effect Steps | 6 | 7 | 6 |
| Mutation Steps | 8 | 8 | 8 |
| Modulator Steps | 8 | 6 | 3 |
| Terminal Steps | 10 | 19 | 9 |
| Predicates (P) | 14 | 14 | 14 |
| Text Predicates | 7 | 7 | 7 |
| Anonymous Factory | 27 | 50+ | 27 |
