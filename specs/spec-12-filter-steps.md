# Spec 12: Missing Filter Steps

## Overview

This specification defines the implementation of missing Gremlin filter steps for Interstellar. These steps extend the existing filter functionality to provide more comprehensive graph traversal filtering capabilities.

## Goals

1. Implement `tail()` / `tail(n)` - Get last n elements
2. Implement `dedup(by)` - Deduplicate by property key or traversal
3. Implement `coin(probability)` - Probabilistic filtering
4. Implement `sample(n)` - Random sampling
5. Implement `hasKey()` - Filter property traversers by key
6. Implement `hasValue()` - Filter property traversers by value
7. Implement `where(predicate)` - Filter with standalone predicate (P.eq, etc.)

## Non-Goals

- `timeLimit()` - Requires async runtime integration (future work)
- Complex barrier-based operations
- Custom sampling strategies

---

## 1. Filter Steps

### 1.1 `tail()` / `tail(n)` - Get Last Elements

Returns only the last n elements from the traversal stream.

**Gremlin Syntax:**
```groovy
g.V().tail()        // Last element
g.V().tail(3)       // Last 3 elements
g.V().values("age").order().tail(5)  // Top 5 ages
```

**Rust API:**
```rust
// Get last element
let last = g.v().tail().to_list();

// Get last n elements
let last_three = g.v().tail_n(3).to_list();

// Combined with ordering
let top_ages = g.v()
    .values("age")
    .order().by_value_asc().build()
    .tail_n(5)
    .to_list();
```

**Behavior:**
- `tail()` returns only the last element (equivalent to `tail_n(1)`)
- `tail_n(n)` returns the last n elements in order
- If fewer than n elements exist, returns all elements
- Empty traversal returns empty result
- **Important:** This is a barrier step - must collect all elements to determine the last n

**Return Type:** Same as input type

**Implementation Notes:**
- Requires buffering all traversers to determine the end
- Use a `VecDeque` with bounded capacity for memory efficiency
- Consider implementing with `Iterator::collect()` + slice access

### 1.2 `dedup(by)` - Deduplicate by Key

Removes duplicate traversers based on a specific property or derived value.

**Gremlin Syntax:**
```groovy
g.V().dedup()                    // Dedup by whole element (already implemented)
g.V().out().dedup().by("name")   // Dedup by name property
g.V().out().dedup().by(__.outE().count())  // Dedup by out-edge count
g.V().dedup().by(label)          // Dedup by label
```

**Rust API:**
```rust
// Dedup by property key
let unique_names = g.v()
    .out()
    .dedup_by_key("name")
    .to_list();

// Dedup by label
let unique_labels = g.v()
    .dedup_by_label()
    .to_list();

// Dedup by traversal
let unique_degrees = g.v()
    .dedup_by(__::out_e().count())
    .to_list();
```

**Behavior:**
- Keeps the first traverser for each unique key value
- Elements without the specified property are treated as having `null` key
- For traversal-based dedup, evaluates the traversal for each element
- The dedup key is computed lazily during iteration

**Return Type:** Same as input type

**Implementation Notes:**
- `DedupByKeyStep` - dedup by property key string
- `DedupByLabelStep` - dedup by element label
- `DedupByTraversalStep` - dedup by anonymous traversal result
- Use `HashMap<Value, ()>` to track seen keys

### 1.3 `coin(probability)` - Probabilistic Filter

Randomly filters elements with a given probability.

**Gremlin Syntax:**
```groovy
g.V().coin(0.5)    // ~50% of vertices pass through
g.V().coin(0.1)    // ~10% of vertices pass through
```

**Rust API:**
```rust
// 50% chance each element passes
let sample = g.v().coin(0.5).to_list();

// 10% sampling
let sparse_sample = g.v().coin(0.1).to_list();
```

**Behavior:**
- Each traverser has `probability` chance of passing through
- `probability` must be in range `[0.0, 1.0]`
- `coin(0.0)` filters everything (empty result)
- `coin(1.0)` passes everything (identity)
- Uses thread-local RNG for reproducibility in tests

**Return Type:** Same as input type

**Implementation Notes:**
- Use `rand` crate with `thread_rng()`
- Stateless filter - no buffering needed
- Consider optional seed parameter for reproducible tests

### 1.4 `sample(n)` - Random Sample

Returns a random sample of n elements from the traversal.

**Gremlin Syntax:**
```groovy
g.V().sample(3)                    // 3 random vertices
g.V().out().sample(10)             // 10 random neighbors
g.V().sample(5).by("weight")       // Weighted sampling (not implemented)
```

**Rust API:**
```rust
// Get 3 random vertices
let sample = g.v().sample(3).to_list();

// Random sample of neighbors
let neighbor_sample = g.v()
    .out()
    .sample(10)
    .to_list();
```

**Behavior:**
- Returns exactly n random elements (or all if fewer than n exist)
- Each element has equal probability of selection (uniform sampling)
- Order of returned elements is not guaranteed
- **Important:** This is a barrier step - must see all elements for fair sampling

**Return Type:** Same as input type

**Implementation Notes:**
- Use reservoir sampling algorithm for memory efficiency
- Algorithm: Keep first n elements, then for element k > n, replace random element with probability n/k
- Requires `rand` crate

### 1.5 `hasKey(key...)` - Filter Properties by Key

Filters property traversers to only those with matching keys.

**Gremlin Syntax:**
```groovy
g.V().properties().hasKey("name")           // Properties with key "name"
g.V().properties().hasKey("name", "age")    // Properties with key "name" OR "age"
```

**Rust API:**
```rust
// Filter to name properties
let name_props = g.v()
    .properties()
    .has_key("name")
    .to_list();

// Filter to multiple keys
let name_or_age = g.v()
    .properties()
    .has_key_any(&["name", "age"])
    .to_list();
```

**Behavior:**
- Only applicable to property traversers (from `.properties()` step)
- Filters to properties matching any of the specified keys
- Non-property values are filtered out

**Return Type:** `Traversal<..., Property>`

**Implementation Notes:**
- Works on `Value::Property(key, value)` variants
- Need to add `Value::Property` variant or use a different approach
- Alternative: Have `properties()` return a `PropertyTraverser` type

### 1.6 `hasValue(value...)` - Filter Properties by Value

Filters property traversers to only those with matching values.

**Gremlin Syntax:**
```groovy
g.V().properties().hasValue("Alice")        // Properties with value "Alice"
g.V().properties().hasValue(30, 31, 32)     // Properties with value 30, 31, or 32
```

**Rust API:**
```rust
// Filter to properties with specific value
let alice_props = g.v()
    .properties()
    .has_prop_value("Alice")
    .to_list();

// Filter to multiple values
let age_range = g.v()
    .properties()
    .has_prop_value_any(&[30i64, 31i64, 32i64])
    .to_list();
```

**Behavior:**
- Only applicable to property traversers (from `.properties()` step)
- Filters to properties matching any of the specified values
- Non-property values are filtered out

**Return Type:** `Traversal<..., Property>`

**Implementation Notes:**
- Named `has_prop_value` to avoid conflict with existing `has_value()` which checks vertex/edge properties
- Works on property values extracted by `.properties()` step

### 1.7 `where(predicate)` - Predicate-Based Filter

Filters traversers using a standalone predicate without a traversal.

**Gremlin Syntax:**
```groovy
g.V().values("age").where(P.gt(30))         // Ages > 30
g.V().values("name").where(P.within("Alice", "Bob"))  // Names in set
g.V().as("a").out().where(P.neq("a"))       // Not equal to labeled step
```

**Rust API:**
```rust
// Filter values by predicate
let over_30 = g.v()
    .values("age")
    .where_p(p::gt(30))
    .to_list();

// With set predicate
let specific_names = g.v()
    .values("name")
    .where_p(p::within(["Alice", "Bob"]))
    .to_list();

// Reference labeled step (already supported via where_(traversal))
let not_self = g.v()
    .as_("a")
    .out()
    .where_p(p::neq_label("a"))
    .to_list();
```

**Behavior:**
- Tests the current traverser's value against the predicate
- For labeled references, retrieves the value from the path
- Predicates: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `within`, `without`, `between`, etc.

**Return Type:** Same as input type

**Implementation Notes:**
- Similar to `is_()` but named `where_p` for Gremlin alignment
- The `neq_label("a")` variant references a labeled step in the path
- May merge functionality with existing `is_()` step

---

## 2. Anonymous Traversal Support

### 2.1 Add to `__` Module

All new filter steps should be available in the anonymous traversal factory:

```rust
// Tail
__::tail()
__::tail_n(3)

// Dedup variants
__::dedup_by_key("name")
__::dedup_by_label()
__::dedup_by(traversal)

// Probabilistic
__::coin(0.5)
__::sample(5)

// Property filters
__::has_key("name")
__::has_key_any(&["name", "age"])
__::has_prop_value("Alice")

// Predicate filter
__::where_p(p::gt(30))
```

---

## 3. Implementation Details

### 3.1 TailStep

```rust
#[derive(Clone, Debug)]
pub struct TailStep {
    count: usize,
}

impl TailStep {
    pub fn new(count: usize) -> Self {
        Self { count }
    }
    
    pub fn last() -> Self {
        Self { count: 1 }
    }
}

impl AnyStep for TailStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all, take last n
        let all: Vec<_> = input.collect();
        let start = all.len().saturating_sub(self.count);
        Box::new(all.into_iter().skip(start))
    }
    
    fn name(&self) -> &'static str {
        "tail"
    }
}
```

### 3.2 DedupByKeyStep

```rust
#[derive(Clone, Debug)]
pub struct DedupByKeyStep {
    key: String,
}

impl DedupByKeyStep {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl AnyStep for DedupByKeyStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone();
        let mut seen = std::collections::HashSet::new();
        
        Box::new(input.filter(move |t| {
            let dedup_key = match &t.value {
                Value::Vertex(id) => {
                    ctx.snapshot().storage().get_vertex(*id)
                        .and_then(|v| v.properties.get(&key).cloned())
                        .unwrap_or(Value::Null)
                }
                Value::Edge(id) => {
                    ctx.snapshot().storage().get_edge(*id)
                        .and_then(|e| e.properties.get(&key).cloned())
                        .unwrap_or(Value::Null)
                }
                _ => Value::Null,
            };
            seen.insert(dedup_key)
        }))
    }
}
```

### 3.3 CoinStep

```rust
use rand::Rng;

#[derive(Clone, Debug)]
pub struct CoinStep {
    probability: f64,
}

impl CoinStep {
    pub fn new(probability: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability),
            "probability must be between 0.0 and 1.0"
        );
        Self { probability }
    }
}

impl AnyStep for CoinStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let prob = self.probability;
        Box::new(input.filter(move |_| {
            rand::thread_rng().gen::<f64>() < prob
        }))
    }
}
```

### 3.4 SampleStep (Reservoir Sampling)

```rust
use rand::Rng;

#[derive(Clone, Debug)]
pub struct SampleStep {
    count: usize,
}

impl SampleStep {
    pub fn new(count: usize) -> Self {
        Self { count }
    }
}

impl AnyStep for SampleStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let n = self.count;
        let mut reservoir: Vec<Traverser> = Vec::with_capacity(n);
        let mut rng = rand::thread_rng();
        
        for (i, item) in input.enumerate() {
            if i < n {
                reservoir.push(item);
            } else {
                // Replace with probability n/(i+1)
                let j = rng.gen_range(0..=i);
                if j < n {
                    reservoir[j] = item;
                }
            }
        }
        
        Box::new(reservoir.into_iter())
    }
}
```

---

## 4. Error Handling

### 4.1 Error Types

```rust
#[derive(Debug, Error)]
pub enum FilterError {
    #[error("invalid probability: {0} (must be between 0.0 and 1.0)")]
    InvalidProbability(f64),
    
    #[error("invalid sample count: {0} (must be > 0)")]
    InvalidSampleCount(usize),
}
```

### 4.2 Validation

- `coin()` probability must be in `[0.0, 1.0]`
- `sample()` count should be > 0 (0 returns empty)
- `tail()` count of 0 returns empty result

---

## 5. Dependencies

### 5.1 New Dependencies

Add to `Cargo.toml`:

```toml
[dependencies]
rand = "0.8"
```

The `rand` crate is needed for:
- `coin()` - Random number generation for probability check
- `sample()` - Reservoir sampling algorithm

---

## 6. Testing Requirements

### 6.1 Unit Tests

**TailStep:**
- `tail()` returns last element
- `tail_n(3)` returns last 3 elements
- `tail_n(10)` on 5 elements returns all 5
- Empty input returns empty output
- Preserves traverser metadata

**DedupByKeyStep:**
- Dedup by property keeps first occurrence
- Missing property treated as null
- Works with vertices and edges
- Empty input returns empty output

**DedupByLabelStep:**
- Dedup by label works correctly
- Mixed element types handled

**CoinStep:**
- `coin(0.0)` returns empty
- `coin(1.0)` returns all
- `coin(0.5)` returns approximately half (statistical test)
- Deterministic with seeded RNG

**SampleStep:**
- `sample(n)` on m < n elements returns all m
- `sample(n)` on m > n elements returns exactly n
- Distribution is approximately uniform (statistical test)
- Empty input returns empty output

**HasKeyStep:**
- Filters properties by key
- Multiple keys work as OR

**HasPropValueStep:**
- Filters properties by value
- Multiple values work as OR

**WherePStep:**
- Works with comparison predicates
- Works with set predicates

### 6.2 Integration Tests

- Chaining: `g.v().out().dedup_by_key("name").tail_n(5)`
- With ordering: `g.v().values("age").order().tail_n(3)`
- With other filters: `g.v().has_label("person").sample(10)`
- Anonymous traversal: `g.v().where_(__::out().dedup_by_key("name").count().is_(p::gt(3)))`

### 6.3 Property-Based Tests

- Reservoir sampling produces uniform distribution
- Coin with p produces ~p fraction of elements

---

## 7. Example Usage

```rust
use interstellar::prelude::*;

fn main() {
    let graph = create_sample_graph();
    let g = graph.traversal();
    
    // Get last 5 vertices
    let last_five = g.v().tail_n(5).to_list();
    
    // Get unique people by name
    let unique_people = g.v()
        .has_label("person")
        .dedup_by_key("name")
        .to_list();
    
    // Random 10% sample
    let sample = g.v().coin(0.1).to_list();
    
    // Get exactly 5 random vertices
    let random_five = g.v().sample(5).to_list();
    
    // Filter values by predicate
    let adults = g.v()
        .values("age")
        .where_p(p::gte(18))
        .to_list();
    
    // Complex query: unique labels among top 10 connected vertices
    let top_connected = g.v()
        .order()
        .by_traversal(__::both_e().count(), Order::Desc)
        .build()
        .tail_n(10)
        .dedup_by_label()
        .label()
        .to_list();
}
```

---

## 8. API Reference Update

After implementation, update `Gremlin_api.md`:

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `tail()` | `tail()` | `traversal::filter` |
| `tail(n)` | `tail_n(n)` | `traversal::filter` |
| `dedup(by)` | `dedup_by_key(key)` | `traversal::filter` |
| `dedup().by(label)` | `dedup_by_label()` | `traversal::filter` |
| `dedup().by(traversal)` | `dedup_by(traversal)` | `traversal::filter` |
| `coin(probability)` | `coin(probability)` | `traversal::filter` |
| `sample(n)` | `sample(n)` | `traversal::filter` |
| `hasKey()` | `has_key(key)` | `traversal::filter` |
| `hasValue()` | `has_prop_value(value)` | `traversal::filter` |
| `where(predicate)` | `where_p(predicate)` | `traversal::filter` |

---

## 9. Future Enhancements

- `timeLimit(ms)` - Requires async runtime integration
- `sample(n).by(weight)` - Weighted sampling
- `dedup(scope)` - Dedup with scope (global vs local)
- Seeded RNG for reproducible `coin()` and `sample()`
