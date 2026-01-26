# Gremlin API Reference

This document maps standard Gremlin steps (TinkerPop 3.x) to their implementations in Interstellar's Rust API and Gremlin text parser.

## Quick Start

### Rust Fluent API

```rust
use interstellar::prelude::*;

let graph = Graph::new();
// ... populate graph ...

let snapshot = graph.snapshot();
let g = snapshot.gremlin();

// Fluent API traversal
let names = g.v()
    .has_label("person")
    .out_labels(&["knows"])
    .values("name")
    .to_list();
```

### Gremlin Text Parser

```rust
use interstellar::prelude::*;
use interstellar::gremlin::ExecutionResult;

let graph = Graph::new();
// ... populate graph ...

// Execute a Gremlin query string
let result = graph.query("g.V().hasLabel('person').out('knows').values('name').toList()")?;

if let ExecutionResult::List(names) = result {
    for name in names {
        println!("{}", name);
    }
}
```

### Lower-Level Parser API

```rust
use interstellar::gremlin::{parse, compile, ExecutionResult};

// Parse query to AST (can be reused)
let ast = parse("g.V().hasLabel('person').values('name').toList()")?;

// Compile and execute
let snapshot = graph.snapshot();
let g = snapshot.gremlin();
let compiled = compile(&ast, &g)?;
let result = compiled.execute();
```

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✓ | Implemented in both Rust API and Gremlin parser |
| Rust | Implemented in Rust API only |
| Parser | Implemented in Gremlin parser only |
| - | Not implemented |

## API Naming Differences

| Gremlin | Rust | Reason |
|---------|------|--------|
| `in()` | `in_()` | `in` is a Rust keyword |
| `as()` | `as_()` | `as` is a Rust keyword |
| `is()` | `is_()` | `is` is a Rust keyword |
| `where()` | `where_()` | `where` is a Rust keyword |
| `and()` | `and_()` | `and` is a Rust keyword |
| `or()` | `or_()` | `or` is a Rust keyword |
| `has(key, value)` | `has_value(key, value)` | Distinguishes from `has(key)` existence check |

---

## Source Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `g.V()` | `g.v()` | `g.V()` | ✓ |
| `g.V(id)` | `g.v_by_id(id)` | `g.V(id)` | ✓ |
| `g.V(id...)` | `g.v_ids(ids)` | `g.V(id, id, ...)` | ✓ |
| `g.E()` | `g.e()` | `g.E()` | ✓ |
| `g.E(id)` | `g.e_by_id(id)` | `g.E(id)` | ✓ |
| `g.E(id...)` | `g.e_ids(ids)` | `g.E(id, id, ...)` | ✓ |
| `g.addV(label)` | `g.add_v(label)` | `g.addV('label')` | ✓ |
| `g.addE(label)` | `g.add_e(label)` | `g.addE('label')` | ✓ |
| `g.inject(values...)` | `g.inject(values)` | `g.inject(v1, v2, ...)` | ✓ |

---

## Navigation Steps (Vertex to Vertex)

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `out()` | `out()` | `out()` | ✓ |
| `out(label...)` | `out_labels(&[labels])` | `out('label', ...)` | ✓ |
| `in()` | `in_()` | `in()` | ✓ |
| `in(label...)` | `in_labels(&[labels])` | `in('label', ...)` | ✓ |
| `both()` | `both()` | `both()` | ✓ |
| `both(label...)` | `both_labels(&[labels])` | `both('label', ...)` | ✓ |

## Navigation Steps (Vertex to Edge)

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `outE()` | `out_e()` | `outE()` | ✓ |
| `outE(label...)` | `out_e_labels(&[labels])` | `outE('label', ...)` | ✓ |
| `inE()` | `in_e()` | `inE()` | ✓ |
| `inE(label...)` | `in_e_labels(&[labels])` | `inE('label', ...)` | ✓ |
| `bothE()` | `both_e()` | `bothE()` | ✓ |
| `bothE(label...)` | `both_e_labels(&[labels])` | `bothE('label', ...)` | ✓ |

## Navigation Steps (Edge to Vertex)

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `outV()` | `out_v()` | `outV()` | ✓ |
| `inV()` | `in_v()` | `inV()` | ✓ |
| `bothV()` | `both_v()` | `bothV()` | ✓ |
| `otherV()` | `other_v()` | `otherV()` | ✓ |

---

## Filter Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `has(key)` | `has(key)` | `has('key')` | ✓ |
| `has(key, value)` | `has_value(key, value)` | `has('key', value)` | ✓ |
| `has(key, predicate)` | `has_where(key, predicate)` | `has('key', P.gt(x))` | ✓ |
| `has(label, key, value)` | `has_label(...).has_value(...)` | `has('label', 'key', value)` | ✓ |
| `hasLabel(label)` | `has_label(label)` | `hasLabel('label')` | ✓ |
| `hasLabel(label...)` | `has_label_any(&[labels])` | `hasLabel('l1', 'l2')` | ✓ |
| `hasId(id)` | `has_id(id)` | `hasId(id)` | ✓ |
| `hasId(id...)` | `has_ids(&[ids])` | `hasId(id1, id2)` | ✓ |
| `hasNot(key)` | `has_not(key)` | `hasNot('key')` | ✓ |
| `hasKey(key)` | `has_key(key)` | `hasKey('key')` | ✓ |
| `hasKey(key...)` | `has_key_any(&[keys])` | `hasKey('k1', 'k2')` | ✓ |
| `hasValue(value)` | `has_prop_value(value)` | `hasValue(value)` | ✓ |
| `hasValue(value...)` | `has_prop_value_any(&[values])` | `hasValue(v1, v2)` | ✓ |
| `filter(traversal)` | `filter(closure)` | - | Rust |
| `where(traversal)` | `where_(traversal)` | `where(__.out())` | ✓ |
| `where(predicate)` | `where_p(predicate)` | `where(P.gt(x))` | ✓ |
| `not(traversal)` | `not(traversal)` | `not(__.out())` | ✓ |
| `and(traversal...)` | `and_(&[traversals])` | `and(__.t1(), __.t2())` | ✓ |
| `or(traversal...)` | `or_(&[traversals])` | `or(__.t1(), __.t2())` | ✓ |
| `is(value)` | `is_eq(value)` | `is(value)` | ✓ |
| `is(predicate)` | `is_(predicate)` | `is(P.gt(x))` | ✓ |
| `dedup()` | `dedup()` | `dedup()` | ✓ |
| `dedup().by(key)` | `dedup_by_key(key)` | - | Rust |
| `dedup().by(label)` | `dedup_by_label()` | - | Rust |
| `dedup().by(traversal)` | `dedup_by(traversal)` | - | Rust |
| `limit(n)` | `limit(n)` | `limit(n)` | ✓ |
| `skip(n)` | `skip(n)` | `skip(n)` | ✓ |
| `range(start, end)` | `range(start, end)` | `range(start, end)` | ✓ |
| `tail()` | `tail()` | `tail()` | ✓ |
| `tail(n)` | `tail_n(n)` | `tail(n)` | ✓ |
| `coin(probability)` | `coin(probability)` | `coin(0.5)` | ✓ |
| `sample(n)` | `sample(n)` | `sample(n)` | ✓ |
| `simplePath()` | `simple_path()` | `simplePath()` | ✓ |
| `cyclicPath()` | `cyclic_path()` | `cyclicPath()` | ✓ |
| `timeLimit()` | - | - | - |

---

## Transform / Map Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `map(traversal)` | `map(closure)` | - | Rust |
| `flatMap(traversal)` | `flat_map(closure)` | - | Rust |
| `identity()` | `identity()` | `identity()` | ✓ |
| `constant(value)` | `constant(value)` | `constant(value)` | ✓ |
| `id()` | `id()` | `id()` | ✓ |
| `label()` | `label()` | `label()` | ✓ |
| `properties()` | `properties()` | `properties()` | ✓ |
| `properties(key...)` | `properties_keys(&[keys])` | `properties('k1', 'k2')` | ✓ |
| `values(key)` | `values(key)` | `values('key')` | ✓ |
| `values(key...)` | `values_multi(&[keys])` | `values('k1', 'k2')` | ✓ |
| `propertyMap()` | `property_map()` | - | Rust |
| `propertyMap(key...)` | `property_map_keys(&[keys])` | - | Rust |
| `valueMap()` | `value_map()` | `valueMap()` | ✓ |
| `valueMap(key...)` | `value_map_keys(&[keys])` | `valueMap('k1', 'k2')` | ✓ |
| `valueMap(true)` | `value_map_with_tokens()` | `valueMap(true)` | ✓ |
| `elementMap()` | `element_map()` | `elementMap()` | ✓ |
| `elementMap(key...)` | `element_map_keys(&[keys])` | `elementMap('k1')` | ✓ |
| `key()` | `key()` | `key()` | ✓ |
| `value()` | `value()` | `value()` | ✓ |
| `path()` | `path()` | `path()` | ✓ |
| `select(labels...)` | `select(&[labels])` | `select('a', 'b')` | ✓ |
| `select(label)` | `select_one(label)` | `select('a')` | ✓ |
| `project(keys...).by()` | `project(&[keys]).by().build()` | `project('a', 'b').by(...)` | ✓ |
| `unfold()` | `unfold()` | `unfold()` | ✓ |
| `fold()` | `fold()` | `fold()` | ✓ |
| `count()` | `count()` | `count()` | ✓ |
| `sum()` | `sum()` | `sum()` | ✓ |
| `max()` | `max()` | `max()` | ✓ |
| `min()` | `min()` | `min()` | ✓ |
| `mean()` | `mean()` | `mean()` | ✓ |
| `order()` | `order().build()` | `order()` | ✓ |
| `order().by(key)` | `order().by_key_asc(key).build()` | `order().by('key')` | ✓ |
| `order().by(key, desc)` | `order().by_key_desc(key).build()` | `order().by('key', desc)` | ✓ |
| `order().by(traversal)` | `order().by_traversal(t).build()` | `order().by(__.out())` | ✓ |
| `math(expression)` | `math(expr).build()` | - | Rust |
| `index()` | `index()` | `index()` | ✓ |
| `loops()` | `loops()` | `loops()` | ✓ |

---

## Aggregation Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `group()` | `group().by_*().build()` | - | Rust |
| `group().by(key)` | `group().by_key(key).build()` | - | Rust |
| `group().by(label)` | `group().by_label().build()` | - | Rust |
| `groupCount()` | `group_count().by_*().build()` | - | Rust |
| `groupCount().by(key)` | `group_count().by_key(key).build()` | - | Rust |
| `groupCount().by(label)` | `group_count().by_label().build()` | - | Rust |

---

## Branch Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `branch(traversal)` | `branch(traversal).option()` | - | Rust |
| `choose(cond, true, false)` | `choose(cond, if_true, if_false)` | `choose(__.cond(), __.t(), __.f())` | ✓ |
| `choose(traversal).option()` | `choose_by(t).option()` | - | Rust |
| `union(traversal...)` | `union(&[traversals])` | `union(__.t1(), __.t2())` | ✓ |
| `coalesce(traversal...)` | `coalesce(&[traversals])` | `coalesce(__.t1(), __.t2())` | ✓ |
| `optional(traversal)` | `optional(traversal)` | `optional(__.out())` | ✓ |
| `local(traversal)` | `local(traversal)` | `local(__.out())` | ✓ |

---

## Repeat Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `repeat(traversal)` | `repeat(traversal)` | `repeat(__.out())` | ✓ |
| `repeat().times(n)` | `repeat().times(n)` | `repeat(__.out()).times(n)` | ✓ |
| `repeat().until(traversal)` | `repeat().until(traversal)` | `repeat(__.out()).until(__.t())` | ✓ |
| `repeat().emit()` | `repeat().emit()` | `repeat(__.out()).emit()` | ✓ |
| `repeat().emit(traversal)` | `repeat().emit_if(traversal)` | `repeat(__.out()).emit(__.t())` | ✓ |
| `repeat().emit()` (first) | `repeat().emit_first()` | - | Rust |
| `loops()` | `loops()` | `loops()` | ✓ |

---

## Side Effect Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `sideEffect(traversal)` | `side_effect(traversal)` | `sideEffect(__.t())` | ✓ |
| `aggregate(key)` | `aggregate(key)` | `aggregate('key')` | ✓ |
| `store(key)` | `store(key)` | `store('key')` | ✓ |
| `cap(key)` | `cap(key)` | `cap('key')` | ✓ |
| `cap(key...)` | `cap_multi(&[keys])` | `cap('k1', 'k2')` | ✓ |
| `subgraph()` | - | - | - |
| `profile()` | `profile()` | - | Rust |

---

## Mutation Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `addV(label)` | `add_v(label)` | `addV('label')` | ✓ |
| `addE(label)` | `add_e(label)` | `addE('label')` | ✓ |
| `property(key, value)` | `property(key, value)` | `property('key', value)` | ✓ |
| `property(cardinality, k, v)` | `property_with_cardinality(...)` | `property(single, 'k', v)` | ✓ |
| `from(vertex)` | `from_vertex(id)` | `from(__.V(id))` | ✓ |
| `from(label)` | `from_label(label)` | `from('label')` | ✓ |
| `to(vertex)` | `to_vertex(id)` | `to(__.V(id))` | ✓ |
| `to(label)` | `to_label(label)` | `to('label')` | ✓ |
| `drop()` | `drop()` | `drop()` | ✓ |
| `mergeV()` | - | - | - |
| `mergeE()` | - | - | - |

---

## Modulator Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `as(label)` | `as_(label)` | `as('label')` | ✓ |
| `by()` | `.by_*()` methods | `.by(...)` | ✓ |
| `with()` | - | - | - |
| `option(key, traversal)` | `.option(key, traversal)` | - | Rust |
| `option(none)` | `.option_none(traversal)` | - | Rust |
| - | `with_path()` | - | Rust |

---

## Terminal Steps

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `next()` | `next()` | `next()` | ✓ |
| `next(n)` | `take(n)` | `next(n)` | ✓ |
| `toList()` | `to_list()` | `toList()` | ✓ |
| `toSet()` | `to_set()` | `toSet()` | ✓ |
| `toBulkSet()` | - | - | - |
| `iterate()` | `iterate()` | `iterate()` | ✓ |
| `hasNext()` | `has_next()` | `hasNext()` | ✓ |
| `tryNext()` | - | - | - |
| `one()` | `one()` | - | Rust |
| `explain()` | - | - | - |
| - | `iter()` | - | Rust |
| - | `traversers()` | - | Rust |
| - | `to_vertex_list()` | - | Rust |
| - | `next_vertex()` | - | Rust |
| - | `to_edge_list()` | - | Rust |
| - | `next_edge()` | - | Rust |

---

## Predicate Functions (P.)

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `P.eq(value)` | `p::eq(value)` | `P.eq(value)` | ✓ |
| `P.neq(value)` | `p::neq(value)` | `P.neq(value)` | ✓ |
| `P.lt(value)` | `p::lt(value)` | `P.lt(value)` | ✓ |
| `P.lte(value)` | `p::lte(value)` | `P.lte(value)` | ✓ |
| `P.gt(value)` | `p::gt(value)` | `P.gt(value)` | ✓ |
| `P.gte(value)` | `p::gte(value)` | `P.gte(value)` | ✓ |
| `P.between(start, end)` | `p::between(start, end)` | `P.between(s, e)` | ✓ |
| `P.inside(start, end)` | `p::inside(start, end)` | `P.inside(s, e)` | ✓ |
| `P.outside(start, end)` | `p::outside(start, end)` | `P.outside(s, e)` | ✓ |
| `P.within(values...)` | `p::within(&[values])` | `P.within(v1, v2)` | ✓ |
| `P.without(values...)` | `p::without(&[values])` | `P.without(v1, v2)` | ✓ |
| `P.and(p1, p2)` | `p::and(p1, p2)` | `P.gt(x).and(P.lt(y))` | ✓ |
| `P.or(p1, p2)` | `p::or(p1, p2)` | `P.lt(x).or(P.gt(y))` | ✓ |
| `P.not(predicate)` | `p::not(predicate)` | `P.not(P.eq(x))` | ✓ |

## Text Predicates (TextP.)

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `TextP.containing(str)` | `p::containing(str)` | `TextP.containing('str')` | ✓ |
| `TextP.startingWith(str)` | `p::starting_with(str)` | `TextP.startingWith('str')` | ✓ |
| `TextP.endingWith(str)` | `p::ending_with(str)` | `TextP.endingWith('str')` | ✓ |
| `TextP.notContaining(str)` | `p::not_containing(str)` | `TextP.notContaining('str')` | ✓ |
| `TextP.notStartingWith(str)` | `p::not_starting_with(str)` | `TextP.notStartingWith('str')` | ✓ |
| `TextP.notEndingWith(str)` | `p::not_ending_with(str)` | `TextP.notEndingWith('str')` | ✓ |
| `TextP.regex(pattern)` | `p::regex(pattern)` | `TextP.regex('pattern')` | ✓ |

---

## Anonymous Traversal Factory (__)

The anonymous traversal factory `__` creates traversal fragments for use in steps like `where()`, `choose()`, `repeat()`, etc.

| Gremlin | Rust | Parser | Status |
|---------|------|--------|--------|
| `__.identity()` | `__.identity()` | `__.identity()` | ✓ |
| `__.out()` | `__.out()` | `__.out()` | ✓ |
| `__.out(label)` | `__.out_labels(&[label])` | `__.out('label')` | ✓ |
| `__.in()` | `__.in_()` | `__.in()` | ✓ |
| `__.in(label)` | `__.in_labels(&[label])` | `__.in('label')` | ✓ |
| `__.both()` | `__.both()` | `__.both()` | ✓ |
| `__.outE()` | `__.out_e()` | `__.outE()` | ✓ |
| `__.inE()` | `__.in_e()` | `__.inE()` | ✓ |
| `__.bothE()` | `__.both_e()` | `__.bothE()` | ✓ |
| `__.outV()` | `__.out_v()` | `__.outV()` | ✓ |
| `__.inV()` | `__.in_v()` | `__.inV()` | ✓ |
| `__.otherV()` | `__.other_v()` | `__.otherV()` | ✓ |
| `__.bothV()` | `__.both_v()` | `__.bothV()` | ✓ |
| `__.hasLabel(label)` | `__.has_label(label)` | `__.hasLabel('label')` | ✓ |
| `__.has(key)` | `__.has(key)` | `__.has('key')` | ✓ |
| `__.hasNot(key)` | `__.has_not(key)` | `__.hasNot('key')` | ✓ |
| `__.has(key, value)` | `__.has_value(key, value)` | `__.has('key', value)` | ✓ |
| `__.dedup()` | `__.dedup()` | `__.dedup()` | ✓ |
| `__.limit(n)` | `__.limit(n)` | `__.limit(n)` | ✓ |
| `__.skip(n)` | `__.skip(n)` | `__.skip(n)` | ✓ |
| `__.range(s, e)` | `__.range(s, e)` | `__.range(s, e)` | ✓ |
| `__.id()` | `__.id()` | `__.id()` | ✓ |
| `__.label()` | `__.label()` | `__.label()` | ✓ |
| `__.values(key)` | `__.values(key)` | `__.values('key')` | ✓ |
| `__.valueMap()` | `__.value_map()` | `__.valueMap()` | ✓ |
| `__.path()` | `__.path()` | `__.path()` | ✓ |
| `__.constant(value)` | `__.constant(value)` | `__.constant(value)` | ✓ |
| `__.fold()` | `__.fold()` | `__.fold()` | ✓ |
| `__.unfold()` | `__.unfold()` | `__.unfold()` | ✓ |
| `__.count()` | `__.count()` | `__.count()` | ✓ |
| `__.sum()` | `__.sum()` | `__.sum()` | ✓ |
| `__.as(label)` | `__.as_(label)` | `__.as('label')` | ✓ |

### Additional Rust-only Anonymous Functions

These are available in Rust via `__.` but not yet in the Gremlin parser:

- **Filter:** `has_label_any`, `has_id`, `has_ids`, `has_key`, `has_key_any`, `has_prop_value`, `has_prop_value_any`, `is_`, `is_eq`, `filter`, `tail`, `tail_n`, `coin`, `sample`, `simple_path`, `cyclic_path`, `dedup_by_key`, `dedup_by_label`, `dedup_by`
- **Transform:** `values_multi`, `properties`, `properties_keys`, `value_map_keys`, `value_map_with_tokens`, `element_map`, `element_map_keys`, `property_map`, `property_map_keys`, `key`, `value`, `index`, `loops`, `mean`, `order`, `math`, `project`, `map`, `flat_map`
- **Aggregation:** `group`, `group_count`
- **Side Effect:** `select`, `select_one`, `store`, `aggregate`, `cap`, `side_effect`, `profile`
- **Branch:** `where_`, `where_p`, `not`, `and_`, `or_`, `union`, `coalesce`, `choose`, `optional`, `local`, `branch`
- **Mutation:** `add_v`, `add_e`, `property`, `drop`

---

## Convenience Methods

### Graph::query()

Execute a Gremlin query string directly on a Graph:

```rust
let result = graph.query("g.V().hasLabel('person').values('name').toList()")?;
```

This takes an internal snapshot, so it provides a consistent view at call time.

### GraphSnapshot::query()

Execute a Gremlin query string on a snapshot:

```rust
let snapshot = graph.snapshot();
let result = snapshot.query("g.V().out('knows').values('name').toList()")?;
```

### Graph::mutate()

Execute a Gremlin mutation query that actually modifies the graph:

```rust
// Create vertices
graph.mutate("g.addV('person').property('name', 'Alice')")?;
graph.mutate("g.addV('person').property('name', 'Bob')")?;

// Get vertex IDs for edge creation
let alice_id = 0; // First vertex
let bob_id = 1;   // Second vertex

// Create an edge between them
graph.mutate(&format!("g.addE('knows').from({}).to({}).property('since', 2020)", alice_id, bob_id))?;

// Update a property on an existing vertex
graph.mutate(&format!("g.V({}).property('age', 30)", alice_id))?;

// Delete elements
graph.mutate(&format!("g.V({}).drop()", bob_id))?;
```

**Important:** Use `mutate()` instead of `query()` when you need mutations to actually execute:
- `query()` returns mutation placeholders but doesn't modify the graph
- `mutate()` executes pending mutations and modifies the graph

### ExecutionResult

Query results are returned as an `ExecutionResult` enum:

```rust
pub enum ExecutionResult {
    List(Vec<Value>),        // toList()
    Single(Option<Value>),   // next()
    Set(HashSet<Value>),     // toSet()
    Bool(bool),              // hasNext()
    Unit,                    // iterate()
}
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

| Category | Gremlin Steps | Rust API | Parser |
|----------|--------------|----------|--------|
| Source Steps | 9 | 9 | 9 |
| Navigation (V→V) | 6 | 6 | 6 |
| Navigation (V→E) | 6 | 6 | 6 |
| Navigation (E→V) | 4 | 4 | 4 |
| Filter Steps | ~30 | ~34 | ~25 |
| Transform/Map Steps | ~30 | ~32 | ~28 |
| Aggregation Steps | 6 | 6 | 0 |
| Branch Steps | 7 | 8 | 5 |
| Repeat Steps | 6 | 7 | 6 |
| Side Effect Steps | 6 | 7 | 5 |
| Mutation Steps | 10 | 10 | 9 |
| Modulator Steps | 5 | 6 | 2 |
| Terminal Steps | 8 | 15 | 6 |
| Predicates (P.) | 14 | 14 | 14 |
| Text Predicates | 7 | 7 | 7 |
| Anonymous Factory | ~30 | 50+ | ~30 |

**Total Parser Coverage:** ~85% of common Gremlin operations
