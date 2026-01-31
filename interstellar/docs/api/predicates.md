# Predicates Reference

Predicates are functions that test values against conditions. They're used with `has_where()`, `is_()`, and other filter steps to create flexible queries.

## Usage

```rust
use interstellar::traversal::p;

// Filter vertices where age > 30
g.v().has_where("age", p::gt(30))

// Filter values directly
g.v().values("age").is_(p::between(20, 40))
```

---

## Comparison Predicates

### eq / neq

Test for equality or inequality.

| Rust | Description |
|------|-------------|
| `p::eq(value)` | Equal to value |
| `p::neq(value)` | Not equal to value |

```rust
g.v().has_where("status", p::eq("active"))
g.v().has_where("type", p::neq("deleted"))
```

### lt / lte / gt / gte

Numeric and string comparisons.

| Rust | Description |
|------|-------------|
| `p::lt(value)` | Less than |
| `p::lte(value)` | Less than or equal |
| `p::gt(value)` | Greater than |
| `p::gte(value)` | Greater than or equal |

```rust
g.v().has_where("age", p::gt(21))
g.v().has_where("score", p::lte(100))
```

### between / inside / outside

Range predicates for numeric comparisons.

| Rust | Description |
|------|-------------|
| `p::between(start, end)` | `start <= x < end` (half-open) |
| `p::inside(start, end)` | `start < x < end` (exclusive) |
| `p::outside(start, end)` | `x < start OR x > end` |

```rust
// age between 18 and 65 (inclusive start, exclusive end)
g.v().has_where("age", p::between(18, 65))

// Strictly between (exclusive both ends)
g.v().has_where("score", p::inside(0, 100))

// Outside the range
g.v().has_where("value", p::outside(10, 20))
```

**Range Semantics:**

| Predicate | 10 | 15 | 20 | 25 |
|-----------|----|----|----|----|
| `between(10, 20)` | Yes | Yes | No | No |
| `inside(10, 20)` | No | Yes | No | No |
| `outside(10, 20)` | No | No | No | Yes |

### within / without

Set membership predicates.

| Rust | Description |
|------|-------------|
| `p::within(&[values])` | Value in set |
| `p::without(&[values])` | Value not in set |

```rust
g.v().has_where("status", p::within(&["active", "pending"]))
g.v().has_where("role", p::without(&["admin", "superuser"]))
```

---

## Text Predicates

### containing / not_containing

Substring matching.

| Rust | Description |
|------|-------------|
| `p::containing(str)` | Contains substring |
| `p::not_containing(str)` | Does not contain |

```rust
g.v().has_where("name", p::containing("son"))
g.v().has_where("email", p::not_containing("spam"))
```

### starting_with / not_starting_with

Prefix matching.

| Rust | Description |
|------|-------------|
| `p::starting_with(str)` | Starts with prefix |
| `p::not_starting_with(str)` | Does not start with |

```rust
g.v().has_where("name", p::starting_with("Dr."))
g.v().has_where("code", p::not_starting_with("TEST_"))
```

### ending_with / not_ending_with

Suffix matching.

| Rust | Description |
|------|-------------|
| `p::ending_with(str)` | Ends with suffix |
| `p::not_ending_with(str)` | Does not end with |

```rust
g.v().has_where("email", p::ending_with("@gmail.com"))
g.v().has_where("file", p::not_ending_with(".tmp"))
```

### regex

Regular expression matching.

| Rust | Description |
|------|-------------|
| `p::regex(pattern)` | Matches regex pattern |

```rust
g.v().has_where("phone", p::regex(r"^\d{3}-\d{3}-\d{4}$"))
g.v().has_where("email", p::regex(r".*@(gmail|yahoo)\.com$"))
```

**Common Regex Patterns:**

| Pattern | Matches | Example |
|---------|---------|---------|
| `^prefix` | Starts with "prefix" | `^Dr\.` |
| `suffix$` | Ends with "suffix" | `\.com$` |
| `.*` | Any characters | `.*test.*` |
| `\d+` | One or more digits | `\d{3}` |
| `[a-z]+` | One or more lowercase | `[a-z]+` |
| `(?i)text` | Case-insensitive | `(?i)john` |

---

## Logical Predicates

Combine predicates with logical operators.

### and

Both predicates must match.

```rust
// age 18-65
g.v().has_where("age", p::and(p::gte(18), p::lt(65)))
```

### or

Either predicate must match.

```rust
// status is active or pending
g.v().has_where("status", p::or(p::eq("active"), p::eq("pending")))
```

### not

Negate a predicate.

```rust
// age is not 0
g.v().has_where("age", p::not(p::eq(0)))
```

### Complex Combinations

```rust
// (age >= 18 AND age < 65) OR role == "admin"
g.v().has_where("age", p::or(
    p::and(p::gte(18), p::lt(65)),
    p::eq("admin")  // This would be on "role", shown for syntax
))

// Better: combine with filter steps
g.v()
    .or_([
        __.has_where("age", p::and(p::gte(18), p::lt(65))),
        __.has_value("role", "admin"),
    ])
```

---

## Using Predicates with Steps

### has_where

Filter elements by property value.

```rust
g.v().has_where("age", p::gt(30))
g.e().has_where("weight", p::between(1.0, 10.0))
```

### is_

Filter the current value directly (not a property).

```rust
g.v().values("age").is_(p::gt(30))
g.v().count().is_(p::gt(0))
```

---

## Type Coercion

Predicates handle type coercion sensibly:

| Comparison | Behavior |
|------------|----------|
| Int vs Float | Both converted to Float |
| String vs number | No match (different types) |
| Null vs anything | No match (except `eq(null)`) |

```rust
// These match:
p::gt(30).test(&Value::Int(40))      // true
p::gt(30.0).test(&Value::Int(40))    // true (Int promoted to Float)
p::gt(30).test(&Value::Float(40.0))  // true

// These don't match:
p::gt(30).test(&Value::String("40")) // false (type mismatch)
p::gt(30).test(&Value::Null)         // false
```

---

## Quick Reference

| Category | Predicates |
|----------|------------|
| Equality | `eq`, `neq` |
| Comparison | `lt`, `lte`, `gt`, `gte` |
| Ranges | `between`, `inside`, `outside` |
| Sets | `within`, `without` |
| Text | `containing`, `starting_with`, `ending_with`, `regex` |
| Negated text | `not_containing`, `not_starting_with`, `not_ending_with` |
| Logical | `and`, `or`, `not` |

---

## See Also

- [Gremlin API](gremlin.md) - Filter steps using predicates
- [Querying Guide](../guides/querying.md) - Query patterns
