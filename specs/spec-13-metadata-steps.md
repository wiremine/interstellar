# Spec 13: Metadata Transform Steps

## Overview

This specification defines the implementation of Gremlin metadata/transform steps for RustGremlin that extract metadata from traversers and property objects. These steps provide access to property keys/values, stream position indices, and loop depth in repeat operations.

## Goals

1. Implement `propertyMap()` - Extract properties as a map (alternative representation)
2. Implement `key()` - Extract property key from property objects
3. Implement `value()` - Extract property value from property objects
4. Implement `index()` - Annotate/extract stream position index
5. Implement `loops()` - Get current loop depth in repeat operations

## Non-Goals

- `loops(loopName)` - Named loop tracking (future work)
- `index().with(Indexer.map)` - Custom indexer options
- First-class `Property` type in `Value` enum (use existing Map representation)

---

## 1. PropertyMap Step

### 1.1 `propertyMap()` - Get Properties as Map of Property Objects

Returns a map where keys are property names and values are lists of property objects (not just values).

**Gremlin Syntax:**
```groovy
g.V().propertyMap()              // All properties as property objects
g.V().propertyMap("name", "age") // Specific properties only
```

**Rust API:**
```rust
// Get all properties as a map of property objects
let props = g.v().property_map().to_list();
// Returns: [{"name": [{"key": "name", "value": "Alice"}], "age": [{"key": "age", "value": 30}]}]

// Get specific properties only
let props = g.v().property_map_keys(&["name"]).to_list();
```

**Behavior:**
- Each input element produces exactly one output `Value::Map`
- Keys are property names, values are lists of property objects
- Property objects are represented as `Value::Map` with "key" and "value" entries
- Designed to work with multi-properties (values wrapped in lists)
- Non-elements (non-vertex/edge) return empty map

**Difference from `valueMap()`:**
- `valueMap()`: Returns `{name: ["Alice"], age: [30]}` (just values in lists)
- `propertyMap()`: Returns `{name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}` (full property objects)

**Return Type:** `Traversal<..., Value::Map>`

**Implementation Notes:**
- Similar to `ValueMapStep` but wraps values in property object format
- Uses existing `PropertiesStep::make_property_map()` helper pattern

---

## 2. Key Step

### 2.1 `key()` - Extract Property Key

Extracts the key from property objects (typically from `.properties()` step output).

**Gremlin Syntax:**
```groovy
g.V().properties().key()         // Get all property keys
g.V().properties("name").key()   // Just "name" keys
```

**Rust API:**
```rust
// Get property keys
let keys = g.v()
    .properties()
    .key()
    .to_list();
// Returns: ["name", "age", "name", "age", ...]

// Unique keys per vertex
let keys = g.v()
    .properties()
    .key()
    .dedup()
    .to_list();
```

**Behavior:**
- Input: `Value::Map` with "key" and "value" entries (property object)
- Output: `Value::String` (the property key)
- Non-property-map values are filtered out
- 1:1 mapping (one key per input property)

**Return Type:** `Traversal<..., String>`

**Implementation Notes:**
- Works on output from `properties()` step
- Extract `map.get("key")` and unwrap to string
- Filter out invalid inputs (non-maps, maps without "key")

---

## 3. Value Step

### 3.1 `value()` - Extract Property Value

Extracts the value from property objects (typically from `.properties()` step output).

**Gremlin Syntax:**
```groovy
g.V().properties().value()         // Get all property values
g.V().properties("age").value()    // Just age values
```

**Rust API:**
```rust
// Get property values
let values = g.v()
    .properties()
    .value()
    .to_list();
// Returns: ["Alice", 30, "Bob", 25, ...]

// Same as .values() shortcut
let values = g.v().values("name").to_list();
```

**Behavior:**
- Input: `Value::Map` with "key" and "value" entries (property object)
- Output: The value portion (any `Value` type)
- Non-property-map values are filtered out
- 1:1 mapping (one value per input property)

**Return Type:** `Traversal<..., Value>`

**Implementation Notes:**
- Works on output from `properties()` step
- Extract `map.get("value")` and clone
- Filter out invalid inputs (non-maps, maps without "value")
- Note: `.values(key)` is essentially `.properties(key).value()` shorthand

---

## 4. Index Step

### 4.1 `index()` - Annotate Stream with Position Index

Annotates each traverser with its position in the stream, or extracts the index.

**Gremlin Syntax:**
```groovy
g.V().index()                    // Returns [value, index] pairs
g.V().index().unfold()           // Unfold to separate values and indices
g.V().values("name").index()     // Names with indices
```

**Rust API:**
```rust
// Get elements with their indices
let indexed = g.v()
    .index()
    .to_list();
// Returns: [[v[0], 0], [v[1], 1], [v[2], 2], ...]

// Extract just the index
let indices = g.v()
    .values("name")
    .index()
    .unfold()
    .skip(1)  // Skip value, keep index
    .to_list();

// Practical: Get nth element information
let third = g.v()
    .index()
    .has_where(1, p::eq(2))  // Where index == 2
    .to_list();
```

**Behavior:**
- Wraps each value in a `Value::List` with `[value, index]`
- Index is 0-based `Value::Int`
- Stateful step: tracks position counter across iteration
- Preserves all traverser metadata (path, loops, bulk)

**Return Type:** `Traversal<..., List>` where list is `[original_value, index]`

**Implementation Notes:**
- Requires stateful iteration (counter)
- Use `Cell<usize>` or atomic for thread safety
- Each traverser gets wrapped: `[traverser.value, index]`
- Consider alternative: store index in traverser metadata (but this changes API contract)

---

## 5. Loops Step

### 5.1 `loops()` - Get Current Loop Depth

Returns the current loop depth for traversers inside a `repeat()` step.

**Gremlin Syntax:**
```groovy
g.V().repeat(__.out()).times(3).emit().loops()  // Loop count at each emit
g.V().repeat(__.out()).until(__.loops().is(3))  // Stop at depth 3
```

**Rust API:**
```rust
// Get loop depth at each emit
let depths = g.v()
    .has_label("person")
    .repeat(__::out())
    .times(3)
    .emit()
    .loops()
    .to_list();
// Returns: [1, 1, 1, 2, 2, 3, ...] (loop depths when emitted)

// Use in until condition
let vertices = g.v()
    .has_label("person")
    .repeat(__::out())
    .until(__::loops().is_(p::gte(3)))
    .to_list();

// Conditional emit based on loop depth
let deep = g.v()
    .repeat(__::out())
    .times(5)
    .emit_if(__::loops().is_(p::gt(2)))
    .to_list();
```

**Behavior:**
- Returns `Value::Int` representing current loop iteration count
- 0-based or 1-based? Gremlin uses 1-based (first iteration = 1)
- Outside of repeat: returns 0
- Reads from `traverser.loops` field (already tracked)
- 1:1 mapping, no filtering

**Return Type:** `Traversal<..., Int>`

**Implementation Notes:**
- Trivial implementation: just read `traverser.loops`
- The repeat step already increments `traverser.loops` via `inc_loops()`
- Current implementation uses 0-based; consider aligning with Gremlin's 1-based

**Gremlin vs Current Implementation:**
- Gremlin: `loops()` returns 1 on first iteration
- Current `traverser.loops`: starts at 0, incremented each iteration
- Recommend: Return `traverser.loops` as-is (0-based) for Rust idiom, document difference

---

## 6. Anonymous Traversal Support

### 6.1 Add to `__` Module

All new steps should be available in the anonymous traversal factory:

```rust
// PropertyMap
__::property_map()
__::property_map_keys(&["name", "age"])

// Key/Value
__::key()
__::value()

// Index
__::index()

// Loops
__::loops()
```

---

## 7. Implementation Details

### 7.1 PropertyMapStep

```rust
#[derive(Clone, Debug)]
pub struct PropertyMapStep {
    keys: Option<Vec<String>>,
}

impl PropertyMapStep {
    pub fn new() -> Self {
        Self { keys: None }
    }

    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut map = HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    let props = match &self.keys {
                        None => &vertex.properties,
                        Some(keys) => // filter to keys
                    };
                    
                    for (key, value) in props {
                        let prop_obj = PropertiesStep::make_property_map(key.clone(), value.clone());
                        map.insert(key.clone(), Value::List(vec![prop_obj]));
                    }
                }
            }
            Value::Edge(id) => { /* similar */ }
            _ => {}
        }

        Value::Map(map)
    }
}

impl AnyStep for PropertyMapStep {
    fn apply<'a>(...) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let result = self.transform(ctx, &t);
            t.with_value(result)
        }))
    }
    
    fn name(&self) -> &'static str { "propertyMap" }
}
```

### 7.2 KeyStep

```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyStep;

impl AnyStep for KeyStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(|t| {
            match &t.value {
                Value::Map(map) => {
                    map.get("key").cloned().map(|key| t.with_value(key))
                }
                _ => None,
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(*self) }
    fn name(&self) -> &'static str { "key" }
}
```

### 7.3 ValueStep

```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct ValueStep;

impl AnyStep for ValueStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(|t| {
            match &t.value {
                Value::Map(map) => {
                    map.get("value").cloned().map(|v| t.with_value(v))
                }
                _ => None,
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(*self) }
    fn name(&self) -> &'static str { "value" }
}
```

### 7.4 IndexStep

```rust
use std::cell::Cell;

#[derive(Clone, Debug, Default)]
pub struct IndexStep;

impl AnyStep for IndexStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let counter = Cell::new(0usize);
        
        Box::new(input.map(move |t| {
            let idx = counter.get();
            counter.set(idx + 1);
            
            let indexed = Value::List(vec![
                t.value.clone(),
                Value::Int(idx as i64),
            ]);
            t.with_value(indexed)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(Self) }
    fn name(&self) -> &'static str { "index" }
}
```

### 7.5 LoopsStep

```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct LoopsStep;

impl AnyStep for LoopsStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(|t| {
            let loops = t.loops as i64;
            t.with_value(Value::Int(loops))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(*self) }
    fn name(&self) -> &'static str { "loops" }
}
```

---

## 8. Error Handling

### 8.1 Error Conditions

| Step | Error Condition | Behavior |
|------|-----------------|----------|
| `key()` | Non-map input | Filter out (no error) |
| `key()` | Map without "key" | Filter out (no error) |
| `value()` | Non-map input | Filter out (no error) |
| `value()` | Map without "value" | Filter out (no error) |
| `propertyMap()` | Non-element input | Return empty map |
| `index()` | None | Always succeeds |
| `loops()` | None | Always succeeds (0 if not in repeat) |

### 8.2 Design Decision: Filter vs Error

For `key()` and `value()`, filtering invalid inputs rather than erroring:
- Consistent with Gremlin behavior
- Allows graceful handling of mixed-type streams
- Users can validate inputs with preceding filter steps if needed

---

## 9. Testing Requirements

### 9.1 Unit Tests

**PropertyMapStep:**
- Returns property objects for all properties
- Returns property objects for specific keys only
- Works with vertices and edges
- Returns empty map for non-elements
- Preserves traverser metadata

**KeyStep:**
- Extracts key from property map
- Filters out non-map values
- Filters out maps without "key"
- Preserves traverser metadata (path, loops)
- Works in pipeline: `properties().key()`

**ValueStep:**
- Extracts value from property map
- Filters out non-map values
- Filters out maps without "value"
- Handles all value types (String, Int, Float, etc.)
- Preserves traverser metadata

**IndexStep:**
- Returns `[value, 0]` for first element
- Indices increment correctly
- Works with empty input
- Works with various value types
- Preserves traverser metadata
- Index resets per traversal execution

**LoopsStep:**
- Returns 0 outside repeat
- Returns correct depth inside repeat
- Works with emit
- Works with until condition
- Preserves traverser metadata (except value changes)

### 9.2 Integration Tests

```rust
// Key/value pipeline
let keys = g.v()
    .properties()
    .key()
    .dedup()
    .to_list();

// Index with filter
let third = g.v()
    .index()
    .filter(|t| /* index == 2 */)
    .unfold()
    .limit(1)
    .to_list();

// Loops in repeat
let depths = g.v()
    .repeat(__::out())
    .times(3)
    .emit()
    .loops()
    .to_list();

// Loops in until condition
let at_depth_3 = g.v()
    .repeat(__::out())
    .until(__::loops().is_(p::eq(3)))
    .to_list();
```

### 9.3 Property-Based Tests

- `index()` always produces sequential indices starting at 0
- `key()` output count <= input count
- `value()` output count <= input count
- `loops()` inside repeat always >= 0

---

## 10. Example Usage

```rust
use rustgremlin::prelude::*;

fn main() {
    let graph = create_sample_graph();
    let g = graph.traversal();
    
    // Get all property keys across all vertices
    let all_keys = g.v()
        .properties()
        .key()
        .dedup()
        .to_list();
    println!("Property keys: {:?}", all_keys);
    
    // Get property values for "name"
    let names = g.v()
        .properties_keys(&["name"])
        .value()
        .to_list();
    println!("Names: {:?}", names);
    
    // Get vertices with their stream index
    let indexed = g.v()
        .index()
        .to_list();
    for item in indexed {
        if let Value::List(pair) = item {
            println!("Index {}: {:?}", pair[1], pair[0]);
        }
    }
    
    // BFS with depth tracking
    let by_depth = g.v()
        .has_label("person")
        .repeat(__::out())
        .times(3)
        .emit()
        .project(&["vertex", "depth"])
            .by(__::identity())
            .by(__::loops())
            .build()
        .to_list();
    
    // Terminate repeat at specific depth
    let at_depth_2 = g.v()
        .repeat(__::out())
        .until(__::loops().is_(p::eq(2)))
        .to_list();
}
```

---

## 11. API Reference Update

After implementation, update `Gremlin_api.md`:

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `propertyMap()` | `property_map()` | `traversal::transform` |
| `propertyMap(key...)` | `property_map_keys(keys)` | `traversal::transform` |
| `key()` | `key()` | `traversal::transform` |
| `value()` | `value()` | `traversal::transform` |
| `index()` | `index()` | `traversal::transform` |
| `loops()` | `loops()` | `traversal::transform` |

---

## 12. Future Enhancements

- `loops(loopName)` - Named loop tracking for nested repeats
- `index().with(Indexer.map)` - Custom indexer producing maps
- First-class `Property` type in `Value` enum for richer property metadata
- `withIndex()` modifier for other steps (like `unfold().withIndex()`)
