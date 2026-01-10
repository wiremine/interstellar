# RustGremlin API Reference

This document maps standard Gremlin steps (TinkerPop 3.x) to their Rust implementations in RustGremlin.

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
| `V()` | `v()`, `v_ids()` | `traversal::source` |
| `E()` | `e()`, `e_ids()` | `traversal::source` |
| `addV()` | - | - |
| `addE()` | - | - |
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
| `hasKey()` | - | - |
| `hasValue()` | - | - |
| `filter(traversal)` | `filter(closure)` | `traversal::filter` |
| `where(traversal)` | `where_(traversal)` | `traversal::branch` |
| `where(predicate)` | - | - |
| `not(traversal)` | `not(traversal)` | `traversal::branch` |
| `and(traversal...)` | `and_(traversals)` | `traversal::branch` |
| `or(traversal...)` | `or_(traversals)` | `traversal::branch` |
| `is(value)` | `is_eq(value)` | `traversal::filter` |
| `is(predicate)` | `is_(predicate)` | `traversal::filter` |
| `dedup()` | `dedup()` | `traversal::filter` |
| `dedup(by)` | - | - |
| `limit(n)` | `limit(n)` | `traversal::filter` |
| `skip(n)` | `skip(n)` | `traversal::filter` |
| `range(start, end)` | `range(start, end)` | `traversal::filter` |
| `tail()` | - | - |
| `tail(n)` | - | - |
| `coin(probability)` | - | - |
| `sample(n)` | - | - |
| `simplePath()` | `simple_path()` | `traversal::filter` |
| `cyclicPath()` | `cyclic_path()` | `traversal::filter` |
| `timeLimit()` | - | - |
| `drop()` | - | - |

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
| `propertyMap()` | - | - |
| `valueMap()` | `value_map()` | `traversal::transform` |
| `valueMap(key...)` | `value_map_keys(keys)` | `traversal::transform` |
| `valueMap(true)` | `value_map_with_tokens()` | `traversal::transform` |
| `elementMap()` | `element_map()` | `traversal::transform` |
| `elementMap(key...)` | `element_map_keys(keys)` | `traversal::transform` |
| `key()` | - | - |
| `value()` | - | - |
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
| `index()` | - | - |
| `loops()` | - | - |

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
| `branch()` | - | - |
| `choose(cond, true, false)` | `choose(condition, if_true, if_false)` | `traversal::branch` |
| `choose(traversal).option()` | - | - |
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
| `loops()` | - | - |

---

## Side Effect Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `sideEffect()` | - | - |
| `aggregate()` | - | - |
| `store()` | - | - |
| `subgraph()` | - | - |
| `cap()` | - | - |
| `profile()` | - | - |

---

## Mutation Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `addV()` | - | - |
| `addE()` | - | - |
| `property()` | - | - |
| `from()` | - | - |
| `to()` | - | - |
| `drop()` | - | - |
| `mergeV()` | - | - |
| `mergeE()` | - | - |

---

## Modulator Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `as(label)` | `as_(label)` | `traversal::transform` |
| `by()` | `.by_*()` methods on builders | Various |
| `with()` | - | - |
| `option()` | - | - |
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

---

## Graph Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `V()` | `v()`, `v_ids()` | `traversal::source` |
| `E()` | `e()`, `e_ids()` | `traversal::source` |
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
| `TextP.notContaining(str)` | - | - |
| `TextP.notStartingWith(str)` | - | - |
| `TextP.notEndingWith(str)` | - | - |
| `TextP.regex(pattern)` | - | - |

---

## Anonymous Traversal Factory (`__` module)

The double underscore `__` provides anonymous traversal spawning for nested traversals used in steps like `where()`, `filter()`, `map()`, etc.

All implemented steps are available as factory functions in the `__` module for creating anonymous traversals:

```rust
use rustgremlin::traversal::__;

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
- `__::is_()`, `__::is_eq()`, `__::filter()`
- `__::dedup()`, `__::limit()`, `__::skip()`, `__::range()`
- `__::simple_path()`, `__::cyclic_path()`

**Transform:**
- `__::values()`, `__::values_multi()`
- `__::properties()`, `__::properties_keys()`
- `__::value_map()`, `__::value_map_keys()`, `__::value_map_with_tokens()`
- `__::element_map()`, `__::element_map_keys()`
- `__::id()`, `__::label()`, `__::constant()`, `__::path()`
- `__::unfold()`, `__::mean()`, `__::order()`, `__::math()`, `__::project()`
- `__::map()`, `__::flat_map()`

**Aggregation:**
- `__::group()`, `__::group_count()`

**Side Effect:**
- `__::as_()`, `__::select()`, `__::select_one()`

**Branch/Filter with Sub-traversals:**
- `__::where_()`, `__::not()`, `__::and_()`, `__::or_()`
- `__::union()`, `__::coalesce()`, `__::choose()`, `__::optional()`, `__::local()`

---

## Unsupported Gremlin Features

The following Gremlin features are not currently planned for support:

| Feature | Reason |
|---------|--------|
| `subgraph()` | Complex graph construction |
| `tree()` | Specialized data structure |
| `sack()` / `withSack()` | Requires stateful traverser |
| `barrier()` | Explicit synchronization (implicit in reduce steps) |
| `cap()` | Side effect capture |
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
| Source Steps | 3 | 2 |
| Filter Steps | 18 | 10 |
| Navigation Steps | 16 | 0 |
| Transform/Map Steps | 25 | 5 |
| Aggregation Steps | 6 | 0 |
| Branch Steps | 5 | 2 |
| Repeat Steps | 5 | 1 |
| Side Effect Steps | 1 | 5 |
| Mutation Steps | 0 | 8 |
| Terminal Steps | 7 | 4 |
| Predicates (P) | 14 | 0 |
| Text Predicates | 3 | 4 |
| **Total** | **~103** | **~41** |
