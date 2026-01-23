# Rhai Scripting API

Interstellar includes an embedded [Rhai](https://rhai.rs/) scripting engine for dynamic graph queries. This enables runtime query construction without recompiling your Rust code.

## Requirements

Enable the `rhai` feature in your `Cargo.toml`:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["rhai"] }
```

## Quick Start

```rust
use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create engine and graph
    let engine = RhaiEngine::new();
    let graph = create_sample_graph(); // Your graph setup
    
    // Execute a script
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").values("name").to_list()
    "#;
    
    let result = engine.eval_with_graph(graph, script)?;
    println!("Result: {:?}", result);
    
    Ok(())
}
```

---

## Engine Setup

### Creating the Engine

```rust
use interstellar::rhai::RhaiEngine;

let engine = RhaiEngine::new();
```

### Executing Scripts

**With in-memory graph:**

```rust
let graph = Arc::new(Graph::new());
let result = engine.eval_with_graph(graph, script)?;
```

**With persistent mmap graph:**

```rust
#[cfg(feature = "mmap")]
{
    let mmap_graph = Arc::new(CowMmapGraph::open("data.db")?);
    let result = engine.eval_with_mmap_graph(mmap_graph, script)?;
}
```

---

## Rhai API Reference

### Getting a Traversal Source

```javascript
let g = graph.gremlin();
```

This returns a traversal source bound to the graph. All traversal steps are available on `g`.

### Source Steps

| Rhai | Description |
|------|-------------|
| `g.v()` | All vertices |
| `g.v_id(id)` | Vertex by ID |
| `g.v_ids([id1, id2])` | Vertices by IDs |
| `g.e()` | All edges |
| `g.e_id(id)` | Edge by ID |
| `g.e_ids([id1, id2])` | Edges by IDs |
| `g.add_v("label")` | Add vertex |
| `g.add_e("label")` | Add edge |
| `g.inject([values])` | Inject values |

### Navigation Steps

```javascript
// Vertex to vertex
.out()              .out_labels(["knows"])
.in_()              .in_labels(["knows"])
.both()             .both_labels(["knows"])

// Vertex to edge
.out_e()            .out_e_labels(["knows"])
.in_e()             .in_e_labels(["knows"])
.both_e()           .both_e_labels(["knows"])

// Edge to vertex
.out_v()
.in_v()
.both_v()
.other_v()
```

### Filter Steps

```javascript
.has("key")                    // Has property
.has_not("key")                // Missing property
.has_value("key", value)       // Property equals value
.has_where("key", predicate)   // Property matches predicate
.has_label("label")            // Filter by label
.has_label_any(["a", "b"])     // Filter by any label
.has_id(id)                    // Filter by ID
.has_ids([id1, id2])           // Filter by IDs
.dedup()                       // Remove duplicates
.dedup_by_key("key")           // Dedup by property
.dedup_by_label()              // Dedup by label
.limit(n)                      // Take first n
.skip(n)                       // Skip first n
.range(start, end)             // Range slice
.tail()                        // Last element
.tail_n(n)                     // Last n elements
.coin(probability)             // Random filter
.sample(n)                     // Random sample
.simple_path()                 // Non-cyclic paths only
.cyclic_path()                 // Cyclic paths only
.where_(traversal)             // Filter by sub-traversal
.not(traversal)                // Filter by non-existence
.and_([traversals])            // All must match
.or_([traversals])             // Any must match
.is_eq(value)                  // Value equals
.is_(predicate)                // Value matches predicate
```

### Transform Steps

```javascript
.id()                          // Get element ID
.label()                       // Get element label
.values("key")                 // Get property value
.values_multi(["k1", "k2"])    // Get multiple properties
.properties()                  // Get all properties
.properties_keys(["keys"])     // Get specific properties
.value_map()                   // Properties as map
.value_map_keys(["keys"])      // Selected properties as map
.value_map_with_tokens()       // Include id/label in map
.element_map()                 // Full element as map
.key()                         // Property key
.prop_value()                  // Property value
.identity()                    // Pass through unchanged
.constant(value)               // Replace with constant
.path()                        // Get traversal path
.select(["labels"])            // Select labeled values
.select_one("label")           // Select single label
.unfold()                      // Flatten lists
.fold()                        // Collect to list
.count_step()                  // Count as step (not terminal)
.sum()                         // Sum values
.max()                         // Maximum value
.min()                         // Minimum value
.mean()                        // Average value
.index()                       // Add index to elements
.order_asc()                   // Sort ascending
.order_desc()                  // Sort descending
.order_by("key")               // Sort by property
.order_by_desc("key")          // Sort by property descending
.math("expression")            // Math expression
```

### Branch Steps

```javascript
.union([traversals])           // Merge multiple traversals
.coalesce([traversals])        // First non-empty
.optional(traversal)           // Include if exists
.local(traversal)              // Apply per-element
.choose_binary(cond, true_t, false_t)  // If/else
.choose_options(key_t, options, default) // Switch/case
```

### Repeat Steps

```javascript
.repeat_times(traversal, n)           // Fixed iterations
.repeat_until(traversal, until)       // Until condition
.repeat_emit(traversal, n)            // Emit each iteration
.repeat_emit_until(traversal, until)  // Emit until condition
```

### Side Effect Steps

```javascript
.as_("label")                  // Label current position
.store("key")                  // Store in collection
.aggregate("key")              // Aggregate into collection
.cap("key")                    // Retrieve collection
.cap_multi(["keys"])           // Retrieve multiple collections
.side_effect(traversal)        // Execute side effect
```

### Mutation Steps

```javascript
.add_v("label")                // Add vertex
.add_e("label")                // Add edge
.property("key", value)        // Set property
.from_v(id)                    // Edge from vertex
.from_label("label")           // Edge from labeled
.to_v(id)                      // Edge to vertex
.to_label("label")             // Edge to labeled
.drop_()                       // Delete element
```

**Note:** `drop` is `drop_()` in Rhai (reserved word).

### Terminal Steps

```javascript
.to_list()                     // Collect all results
.to_set()                      // Collect unique results
.to_rich_list()                // Results with metadata
.first()                       // First result
.one()                         // Exactly one result
.take(n)                       // First n results
.has_next()                    // Check if results exist
.iterate()                     // Execute without collecting
```

---

## Predicates

Predicates filter values in `has_where()` and `is_()` steps.

### Comparison Predicates

```javascript
eq(value)                      // Equals
neq(value)                     // Not equals
lt(value)                      // Less than
lte(value)                     // Less than or equal
gt(value)                      // Greater than
gte(value)                     // Greater than or equal
between(start, end)            // In range [start, end)
inside(start, end)             // In range (start, end)
outside(start, end)            // Outside range
within([values])               // In set
without([values])              // Not in set
```

### Text Predicates

```javascript
containing("substring")        // Contains text
starting_with("prefix")        // Starts with
ending_with("suffix")          // Ends with
not_containing("substring")    // Does not contain
not_starting_with("prefix")    // Does not start with
not_ending_with("suffix")      // Does not end with
regex("pattern")               // Regex match
```

### Logical Predicates

```javascript
pred_and(p1, p2)               // Both must match
pred_or(p1, p2)                // Either must match
pred_not(p)                    // Negation
```

### Examples

```javascript
// Age between 18 and 65
g.v().has_where("age", pred_and(gte(18), lt(65)))

// Name contains "son" or starts with "J"
g.v().has_where("name", pred_or(containing("son"), starting_with("J")))

// Email matches pattern
g.v().has_where("email", regex(".*@gmail\\.com"))
```

---

## Anonymous Traversals

Use `A.` (not `__::`) for anonymous traversal fragments in Rhai:

```javascript
// Filter with anonymous traversal
g.v().where_(A.out("knows").has_label("person"))

// Union of anonymous traversals
g.v().union([A.out("knows"), A.out("created")])

// Repeat with anonymous traversal
g.v().repeat_times(A.out(), 3)
```

### Available Anonymous Steps

```javascript
A.identity()
A.out()                  A.out("label")
A.in_()                  A.in_("label")
A.both()                 A.both("label")
A.out_e()                A.in_e()              A.both_e()
A.out_v()                A.in_v()              A.other_v()
A.has_label("label")     A.has("key")          A.has_not("key")
A.has_value("key", val)
A.id()                   A.label()
A.values("key")          A.value_map()
A.path()                 A.constant(val)
A.fold()                 A.unfold()
A.dedup()                A.limit(n)
A.as_("label")
```

---

## Value Constructors

Create `Value` instances explicitly:

```javascript
let v = value_int(42);
let v = value_float(3.14);
let v = value_string("hello");
let v = value_bool(true);
```

---

## Complete Examples

### Social Network Query

```javascript
// Find friends of Alice who are over 25
let g = graph.gremlin();
g.v()
  .has_value("name", "Alice")
  .out_labels(["knows"])
  .has_where("age", gt(25))
  .values("name")
  .to_list()
```

### Path Finding

```javascript
// Find all paths of length 2
let g = graph.gremlin();
g.v()
  .with_path()
  .out().out()
  .path()
  .to_list()
```

### Grouping and Counting

```javascript
// Count vertices by label
let g = graph.gremlin();
g.v().group_count_by_label().to_list()

// Group by property
g.v().has_label("person").group_by_key("city").to_list()
```

### Mutations

```javascript
// Create a person and query immediately
let g = graph.gremlin();
let id = g.add_v("person")
  .property("name", "Dave")
  .property("age", 28)
  .first();

// Query the new vertex
g.v_id(id).values("name").first()
```

---

## API Differences from Rust

| Aspect | Rust | Rhai | Reason |
|--------|------|------|--------|
| Anonymous traversal factory | `__::` | `A.` | Rhai doesn't allow `_` prefix |
| `in()` step | `in_()` | `in_()` | Reserved keyword |
| `drop()` step | `drop()` | `drop_()` | Reserved function |
| `value()` (property) | `value()` | `prop_value()` | Avoids type conflict |
| `count()` terminal | `count()` | `count_step()` + `first()` | Separate step/terminal |

---

## See Also

- [Gremlin API](gremlin.md) - Full Rust Gremlin reference
- [Predicates](predicates.md) - Predicate reference
- [Examples](../getting-started/examples.md) - Scripting example
