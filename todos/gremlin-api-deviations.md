# Gremlin API Deviations from TinkerPop Standard

This document catalogs all deviations between Interstellar's Gremlin-style API and the standard Apache TinkerPop Gremlin specification.

## Overview

Interstellar provides a Gremlin-style fluent traversal API adapted for Rust's type system and idioms. While we aim for familiarity, several deviations exist for technical and ergonomic reasons.

### Deviation Categories

1. **Required** - Rust keyword conflicts (unavoidable)
2. **Builder Pattern** - Methods requiring `.build()` finalization
3. **Naming** - Method name differences for disambiguation
4. **Syntax** - Slice/array vs varargs, closures vs traversals
5. **Semantic** - Behavioral differences in API design
6. **Extensions** - Interstellar-specific additions (not in standard Gremlin)

---

## 1. Required Deviations (Rust Keywords)

These use trailing underscores to avoid Rust keyword conflicts:

| Interstellar | Standard Gremlin | Reason |
|--------------|------------------|--------|
| `in_()` | `in()` | `in` is a Rust keyword |
| `as_()` | `as()` | `as` is a Rust keyword |
| `is_()` | `is()` | `is` is a Rust keyword (future) |
| `where_()` | `where()` | `where` is a Rust keyword |
| `and_()` | `and()` | `and` reserved for logical operators |
| `or_()` | `or()` | `or` reserved for logical operators |
| `match_()` | `match()` | `match` is a Rust keyword |

**Migration Path**: None needed - these are documented and expected.

---

## 2. Builder Pattern Deviations

These methods require an explicit `.build()` call to finalize configuration. Standard Gremlin uses implicit finalization.

### order()

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `order().by_asc().build()` | `order().by(asc)` |
| `order().by_desc().build()` | `order().by(desc)` |
| `order().by_key_asc("prop").build()` | `order().by("prop", asc)` |
| `order().by_key_desc("prop").build()` | `order().by("prop", desc)` |
| `order().by_traversal(t, true).build()` | `order().by(__.t(), desc)` |
| `order().build()` (defaults) | `order()` |

**Note**: Direction is specified via method suffix (`_asc`/`_desc`) or boolean (`true` = descending) rather than Gremlin's `Order.asc`/`Order.desc` enum.

### group()

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `group().by_label().by_value().build()` | `group().by(label)` |
| `group().by_key("prop").by_value().build()` | `group().by("prop")` |
| `group().by_key("k").by_value_traversal(t).build()` | `group().by("k").by(__.t())` |
| `GroupKey::by_label()` | Fluent `.by(label)` |
| `GroupValue::identity()` | Implicit in Gremlin |

**Note**: Interstellar uses static constructors (`GroupKey::`, `GroupValue::`) rather than fluent modulators.

### group_count()

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `group_count().by_key("prop").build()` | `groupCount().by("prop")` |
| `group_count().by_label().build()` | `groupCount().by(label)` |
| `group_count().by_traversal(t).build()` | `groupCount().by(__.t())` |
| `group_count().build()` (defaults) | `groupCount()` |

### project()

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `project(&["a", "b"]).by(...).build()` | `project("a", "b").by(...).by(...)` |

### math()

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `math("a * 2").by(...).build()` | `math("a * 2").by(...)` |

**Reason**: Builder pattern provides compile-time safety and explicit state transitions in Rust's type system.

**Migration Path**: Could implement `Deref` or custom drop to auto-finalize, but explicit `.build()` prevents subtle bugs.

---

## 3. Naming Deviations

### Overload Disambiguation

Rust doesn't support method overloading, so we use distinct method names:

| Interstellar | Standard Gremlin | Purpose |
|--------------|------------------|---------|
| `is_eq(value)` | `is(value)` | Equality check |
| `is_(predicate)` | `is(predicate)` | Predicate check |
| `has_value(key, value)` | `has(key, value)` | Property equality |
| `has_where(key, pred)` | `has(key, predicate)` | Property with predicate |
| `tail_n(n)` | `tail(n)` | Take last n |
| `tail()` | `tail()` | Take last 1 |
| `select_one("x")` | `select("x")` | Single label select |
| `select(&["a", "b"])` | `select("a", "b")` | Multi-label select |
| `where_p(predicate)` | `where(predicate)` | Predicate filter |
| `where_(traversal)` | `where(traversal)` | Traversal filter |
| `cap_multi(["a", "b"])` | `cap("a", "b")` | Multi-label cap |
| `profile_as("name")` | `profile("name")` | Named profile |

### Label Filtering Methods

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `out_labels(&["knows"])` | `out("knows")` |
| `in_labels(&["knows"])` | `in("knows")` |
| `both_labels(&["knows"])` | `both("knows")` |
| `out_e_labels(&["knows"])` | `outE("knows")` |
| `in_e_labels(&["knows"])` | `inE("knows")` |
| `both_e_labels(&["knows"])` | `bothE("knows")` |

**Note**: Base methods (`out()`, `in_()`, etc.) traverse all edges. Label filtering requires the `_labels` variant.

### Merged Modulator Methods

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `dedup_by_key("status")` | `dedup().by("status")` |
| `dedup_by_label()` | `dedup().by(label)` |
| `dedup_by(traversal)` | `dedup().by(__.t())` |
| `has_label_any(["a", "b"])` | `hasLabel("a", "b")` |
| `has_key_any(["a", "b"])` | `hasKey("a", "b")` |
| `has_ids([id1, id2])` | `hasId(id1, id2)` |
| `has_prop_value_any(["x"])` | `hasValue("x")` |

### Property Access Methods

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `values_multi(["a", "b"])` | `values("a", "b")` |
| `properties_keys(&["a"])` | `properties("a")` |
| `value_map_keys(["a"])` | `valueMap("a")` |
| `value_map_with_tokens()` | `valueMap(true)` |
| `element_map_keys(["a"])` | `elementMap("a")` |
| `property_map_keys(["a"])` | `propertyMap("a")` |

---

## 4. Syntax Deviations

### Slice/Array vs Varargs

Rust doesn't have varargs. Multi-value parameters use slices or arrays:

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `out_labels(&["a", "b"])` | `out("a", "b")` |
| `select(&["a", "b"])` | `select("a", "b")` |
| `union(vec![t1, t2])` | `union(t1, t2)` |
| `coalesce(vec![t1, t2])` | `coalesce(t1, t2)` |
| `and_(vec![t1, t2])` | `and(t1, t2)` |
| `or_(vec![t1, t2])` | `or(t1, t2)` |

### Closure vs Traversal

Some steps accept Rust closures where Gremlin uses traversals:

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `flat_map(\|ctx, v\| {...})` | `flatMap(__.traversal())` |
| `map(\|ctx, v\| {...})` | `map(__.traversal())` |
| `filter(\|ctx, v\| {...})` | `filter(__.traversal())` |
| `side_effect(\|ctx, v\| {...})` | `sideEffect(__.traversal())` |

**Note**: Traversal-based variants may also be available (e.g., `flat_map_traversal()`).

---

## 5. Semantic Deviations

### emit() Positioning

**Issue**: See `todos/emit-first-gremlin-compatibility.md` for full details.

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `.repeat(t).emit().emit_first()` | `.emit().repeat(t)` (emit before repeat) |
| `.repeat(t).emit()` | `.repeat(t).emit()` (emit after repeat) |

**Status**: `emit_first()` is Interstellar-specific. Gremlin uses emit positioning relative to repeat.

### Boolean Direction vs Enum

| Interstellar | Standard Gremlin |
|--------------|------------------|
| `by_traversal(t, true)` | `by(t, desc)` (true = descending) |
| `by_traversal(t, false)` | `by(t, asc)` (false = ascending) |

---

## 6. Interstellar Extensions

These methods don't exist in standard Gremlin but provide Rust-specific ergonomics:

| Method | Purpose |
|--------|---------|
| `one()` | Returns single result or error (vs `next()` returning Option) |
| `to_vertex_list()` | Type-safe vertex collection |
| `to_edge_list()` | Type-safe edge collection |
| `next_vertex()` | Type-safe single vertex |
| `next_edge()` | Type-safe single edge |
| `with_path()` | Explicit path tracking enablement |
| `append(traversal)` | Anonymous traversal composition |
| `emit_first()` | Explicit starting vertex emission |

---

## Priority Matrix

| Category | Deviation Count | Fix Priority | Effort |
|----------|-----------------|--------------|--------|
| Required (keywords) | 7 | None | N/A |
| Builder pattern | 15+ | Low | High |
| Naming (overloads) | 15+ | Medium | Medium |
| Naming (merged) | 8 | Low | Low |
| Syntax (slices) | 10+ | Low | High |
| Semantic (emit) | 1 | Medium | Medium |
| Extensions | 7 | None | N/A |

---

## Recommendations

### Keep As-Is
- Keyword conflicts (trailing underscores) - required
- Builder pattern - provides compile-time safety
- Extensions - valuable Rust ergonomics

### Consider Standardizing
1. **emit() positioning** - Add pre-repeat emit support for Gremlin compatibility
2. **Overload naming** - Document clearly, consider macro-based unified API
3. **Direction booleans** - Consider `Order::Asc`/`Order::Desc` enum

### Documentation Improvements
1. Add migration guide for Gremlin users
2. Document each deviation in API docs with Gremlin equivalent
3. Add examples showing both Interstellar and Gremlin syntax

---

## References

- [Apache TinkerPop Gremlin Reference](https://tinkerpop.apache.org/docs/current/reference/)
- [Interstellar API Documentation](../docs/api/gremlin.md)
- [emit_first() Compatibility Issue](./emit-first-gremlin-compatibility.md)
