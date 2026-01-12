# Spec 07: Completing the Gremlin API

**Phase 7 of Intersteller Implementation**

## Overview

This specification details the implementation of the remaining Gremlin-compatible API steps. A comprehensive comparison between the current Intersteller implementation and the Gremlin reference API (`guiding-documents/gremlin.md`) identified 15 steps that need implementation to achieve full API coverage.

**Duration**: 2-3 weeks  
**Priority**: High  
**Dependencies**: Phase 4 (Anonymous Traversals and Predicates)

---

## Current Implementation Status

### Fully Implemented (Phase 3-4)

| Category | Steps |
|----------|-------|
| **Source** | `V()`, `E()`, `addV()`, `addE()` |
| **Navigation** | `out()`, `in_()`, `both()`, `outE()`, `inE()`, `bothE()`, `outV()`, `inV()`, `bothV()` |
| **Filter** | `hasLabel()`, `has()`, `hasId()`, `dedup()`, `limit()`, `skip()`, `range()` |
| **Transform** | `values()`, `id()`, `label()`, `map()`, `flatMap()`, `constant()`, `path()`, `as_()`, `select()` |
| **Branch/Logic** | `where_()`, `not()`, `and_()`, `or_()`, `union()`, `coalesce()`, `choose()`, `optional()`, `local()` |
| **Repeat** | `repeat()` with `times()`, `until()`, `emit()`, `emit_if()` |
| **Predicates** | Full `p::` module: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `between`, `inside`, `outside`, `within`, `without`, `containing`, `starting_with`, `ending_with`, `regex`, `and`, `or`, `not` |
| **Terminal** | `to_list()`, `next()`, `count()`, `fold()`, `has_next()`, `iterate()`, `to_set()`, `first()`, `last()` |

### Missing Steps (This Phase)

| Category | Steps | Count |
|----------|-------|-------|
| **Filter** | `hasNot()`, `is()`, `simplePath()`, `cyclicPath()` | 4 |
| **Navigation** | `otherV()` | 1 |
| **Transform** | `properties()`, `valueMap()`, `elementMap()`, `unfold()`, `project()`, `math()`, `order()`, `mean()` | 8 |
| **Aggregation** | `group()`, `groupCount()` | 2 |
| **Total** | | **15** |

---

## Goals

1. Implement all 15 missing Gremlin API steps
2. Maintain consistency with existing step patterns (AnyStep trait, type-erased execution)
3. Provide comprehensive test coverage for all new steps
4. Update the `__` factory module with new anonymous traversal methods
5. Update `Traversal<In, Out>` with chainable methods for all new steps

---

## Deliverables

| File | Description |
|------|-------------|
| `src/traversal/filter.rs` | Add `HasNotStep`, `IsStep`, `SimplePathStep`, `CyclicPathStep` |
| `src/traversal/navigation.rs` | Add `OtherVStep` |
| `src/traversal/transform.rs` | Add `PropertiesStep`, `ValueMapStep`, `ElementMapStep`, `UnfoldStep`, `ProjectStep`, `MathStep`, `OrderStep`, `MeanStep` |
| `src/traversal/aggregate.rs` | New file: `GroupStep`, `GroupCountStep` |
| `src/traversal/mod.rs` | Update exports and `__` factory module |

---

## Architecture Notes

### Step Implementation Pattern

All steps implement the `AnyStep` trait:

```rust
pub trait AnyStep: Send + Sync {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;

    fn clone_box(&self) -> Box<dyn AnyStep>;
    fn name(&self) -> &'static str;
}
```

### Step Categories

1. **Filter Steps** (1:0 or 1:1): Use `impl_filter_step!` macro, implement `matches()` method
2. **Transform Steps** (1:1): Map each traverser to a new value
3. **FlatMap Steps** (1:N): Use `impl_flatmap_step!` macro, implement `flat_map()` method
4. **Reducing Steps** (N:1): Collect all inputs, produce single output (terminal-like)

### Traverser Structure

```rust
pub struct Traverser {
    pub value: Value,
    pub path: Path,
    pub loops: usize,
    pub sack: Option<Box<dyn CloneSack>>,
    pub bulk: u64,
}
```

---

## Section 1: Filter Steps

Filter steps pass through or reject traversers based on predicates. They are 1:0 or 1:1 operations.

### 1.1 HasNotStep

**Gremlin**: `hasNot(key)`  
**Semantics**: Filters to elements that do NOT have the specified property.

#### Gremlin Examples

```groovy
// Find people without an email property
g.V().hasLabel('person').hasNot('email')

// Find edges without a weight
g.E().hasNot('weight')
```

#### Rust API

```rust
// Method signature on Traversal<In, Out>
pub fn has_not(self, key: impl Into<String>) -> Traversal<In, Value>

// Anonymous traversal
__::has_not("email")
```

#### Implementation

```rust
/// Filter step that keeps only elements WITHOUT a specific property.
///
/// Inverse of `HasStep`. Works with vertices and edges.
/// Non-element values (integers, strings, etc.) pass through since they
/// don't have properties.
#[derive(Clone, Debug)]
pub struct HasNotStep {
    key: String,
}

impl HasNotStep {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot()
                    .storage()
                    .get_vertex(*id)
                    .map(|v| !v.properties.contains_key(&self.key))
                    .unwrap_or(true) // Vertex not found = no property
            }
            Value::Edge(id) => {
                ctx.snapshot()
                    .storage()
                    .get_edge(*id)
                    .map(|e| !e.properties.contains_key(&self.key))
                    .unwrap_or(true)
            }
            // Non-element values don't have properties, so they pass
            _ => true,
        }
    }
}

impl_filter_step!(HasNotStep, "hasNot");
```

#### Test Cases

```rust
#[test]
fn test_has_not_filters_vertices_with_property() {
    let graph = create_test_graph(); // vertices with/without email
    let g = graph.traversal();
    
    let without_email: Vec<_> = g.v()
        .has_label("person")
        .has_not("email")
        .to_list();
    
    // Verify none have email property
    for v in &without_email {
        assert!(!v.properties.contains_key("email"));
    }
}

#[test]
fn test_has_not_passes_non_elements() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // values() produces strings, which should pass has_not
    let names: Vec<_> = g.v()
        .values("name")
        .has_not("anything")
        .to_list();
    
    assert!(!names.is_empty());
}
```

---

### 1.2 IsStep

**Gremlin**: `is(value)`, `is(predicate)`  
**Semantics**: Filters the current value using equality or a predicate. Unlike `has()` which filters by property, `is()` filters the traverser's current value directly.

#### Gremlin Examples

```groovy
// Filter to vertices with age exactly 30
g.V().values('age').is(30)

// Filter to ages greater than 25
g.V().values('age').is(gt(25))

// Filter using between predicate
g.V().values('age').is(between(20, 40))
```

#### Rust API

```rust
// Equality form
pub fn is_eq(self, value: impl Into<Value>) -> Traversal<In, Value>

// Predicate form (using p:: module)
pub fn is_(self, predicate: impl Predicate + Clone + Send + Sync + 'static) -> Traversal<In, Value>

// Anonymous traversals
__::is_eq(30)
__::is_(p::gt(25))
__::is_(p::between(20, 40))
```

**Note**: We use `is_eq` for value equality and `is_` for predicates to avoid Rust's `is` keyword issues and provide clarity.

#### Implementation

```rust
/// Filter step that tests the current value against a predicate.
///
/// Unlike `HasStep` which checks properties, `IsStep` tests the traverser's
/// current value directly. Commonly used after `values()` to filter property values.
#[derive(Clone)]
pub struct IsStep {
    predicate: Box<dyn Predicate>,
}

impl IsStep {
    pub fn new(predicate: impl Predicate + Clone + Send + Sync + 'static) -> Self {
        Self {
            predicate: Box::new(predicate),
        }
    }

    /// Create an IsStep for exact value equality.
    pub fn eq(value: impl Into<Value>) -> Self {
        Self::new(p::eq(value))
    }

    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        self.predicate.test(&traverser.value)
    }
}

impl_filter_step!(IsStep, "is");
```

#### Test Cases

```rust
#[test]
fn test_is_eq_filters_values() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let ages: Vec<_> = g.v()
        .values("age")
        .is_eq(29)
        .to_list();
    
    assert_eq!(ages.len(), 1);
    assert_eq!(ages[0], Value::Integer(29));
}

#[test]
fn test_is_with_predicate() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let ages: Vec<_> = g.v()
        .values("age")
        .is_(p::gt(30))
        .to_list();
    
    for age in &ages {
        if let Value::Integer(n) = age {
            assert!(*n > 30);
        }
    }
}

#[test]
fn test_is_with_between() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let ages: Vec<_> = g.v()
        .values("age")
        .is_(p::between(25, 35))
        .to_list();
    
    for age in &ages {
        if let Value::Integer(n) = age {
            assert!(*n >= 25 && *n < 35);
        }
    }
}
```

---

### 1.3 SimplePathStep

**Gremlin**: `simplePath()`  
**Semantics**: Filters to traversers whose path has NO repeated elements. A "simple path" visits each element at most once.

#### Gremlin Examples

```groovy
// Find all simple paths from marko
g.V().has('name', 'marko').repeat(both()).times(3).simplePath().path()

// Find friends-of-friends without cycles
g.V().has('name', 'marko').out('knows').out('knows').simplePath()
```

#### Rust API

```rust
pub fn simple_path(self) -> Traversal<In, Value>

// Anonymous traversal
__::simple_path()
```

#### Implementation

```rust
/// Filter step that keeps only traversers with simple (non-cyclic) paths.
///
/// A simple path contains no repeated elements. This is determined by checking
/// if all PathValue elements in the traverser's path are unique.
#[derive(Clone, Debug, Default)]
pub struct SimplePathStep;

impl SimplePathStep {
    pub fn new() -> Self {
        Self
    }

    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        let elements = traverser.path.elements();
        let mut seen = std::collections::HashSet::new();
        
        for element in elements {
            // Check if we've seen this value before
            if !seen.insert(element.value.clone()) {
                return false; // Duplicate found
            }
        }
        true
    }
}

impl_filter_step!(SimplePathStep, "simplePath");
```

**Note**: This requires `PathValue` to implement `Hash` and `Eq`, or we use a different comparison approach.

#### Test Cases

```rust
#[test]
fn test_simple_path_filters_cycles() {
    let graph = create_cycle_graph(); // A -> B -> C -> A
    let g = graph.traversal();
    
    // Without simplePath, we'd get cycles
    let all_paths = g.v()
        .has("name", "A")
        .repeat(__::out())
        .times(4)
        .path()
        .to_list();
    
    // With simplePath, no cycles
    let simple_paths = g.v()
        .has("name", "A")
        .repeat(__::out())
        .times(4)
        .simple_path()
        .path()
        .to_list();
    
    // Simple paths should have no repeated vertices
    for path in &simple_paths {
        let vertices: Vec<_> = path.objects();
        let unique: HashSet<_> = vertices.iter().collect();
        assert_eq!(vertices.len(), unique.len());
    }
}

#[test]
fn test_simple_path_allows_unique_paths() {
    let graph = create_linear_graph(); // A -> B -> C -> D
    let g = graph.traversal();
    
    let paths = g.v()
        .has("name", "A")
        .repeat(__::out())
        .times(3)
        .simple_path()
        .path()
        .to_list();
    
    assert!(!paths.is_empty()); // Linear path has no cycles
}
```

---

### 1.4 CyclicPathStep

**Gremlin**: `cyclicPath()`  
**Semantics**: Filters to traversers whose path HAS repeated elements. Inverse of `simplePath()`.

#### Gremlin Examples

```groovy
// Find all paths that contain cycles
g.V().repeat(both()).times(4).cyclicPath().path()

// Detect cycles in relationships
g.V().has('name', 'marko').repeat(out()).until(cyclicPath()).path()
```

#### Rust API

```rust
pub fn cyclic_path(self) -> Traversal<In, Value>

// Anonymous traversal
__::cyclic_path()
```

#### Implementation

```rust
/// Filter step that keeps only traversers with cyclic paths.
///
/// A cyclic path contains at least one repeated element. This is the
/// inverse of `SimplePathStep`.
#[derive(Clone, Debug, Default)]
pub struct CyclicPathStep;

impl CyclicPathStep {
    pub fn new() -> Self {
        Self
    }

    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        let elements = traverser.path.elements();
        let mut seen = std::collections::HashSet::new();
        
        for element in elements {
            if !seen.insert(element.value.clone()) {
                return true; // Duplicate found = cyclic
            }
        }
        false
    }
}

impl_filter_step!(CyclicPathStep, "cyclicPath");
```

#### Test Cases

```rust
#[test]
fn test_cyclic_path_detects_cycles() {
    let graph = create_cycle_graph(); // A -> B -> C -> A
    let g = graph.traversal();
    
    let cyclic = g.v()
        .has("name", "A")
        .repeat(__::out())
        .times(4)
        .cyclic_path()
        .path()
        .to_list();
    
    // All returned paths should have at least one repeated vertex
    for path in &cyclic {
        let vertices: Vec<_> = path.objects();
        let unique: HashSet<_> = vertices.iter().collect();
        assert!(vertices.len() > unique.len()); // Has duplicates
    }
}

#[test]
fn test_cyclic_path_filters_linear_paths() {
    let graph = create_linear_graph(); // A -> B -> C -> D
    let g = graph.traversal();
    
    let cyclic = g.v()
        .has("name", "A")
        .repeat(__::out())
        .times(3)
        .cyclic_path()
        .to_list();
    
    assert!(cyclic.is_empty()); // Linear path has no cycles
}
```

---

## Section 2: Navigation Steps

Navigation steps move the traversal through the graph structure.

### 2.1 OtherVStep

**Gremlin**: `otherV()`  
**Semantics**: When traversing from an edge, returns the vertex on the "other" side from where you came. This is context-dependent: if you arrived at the edge from vertex A, `otherV()` returns vertex B, and vice versa.

This is a powerful step because it doesn't require knowing the edge direction—it simply gives you the vertex at the opposite end of the edge from your current position.

#### Gremlin Examples

```groovy
// Get the "other" vertex for each edge from marko
g.V().has('name', 'marko').bothE().otherV()

// Traverse edges and get the opposite endpoint
g.V().has('name', 'marko').outE('knows').otherV().values('name')
// Returns: ["vadas", "josh"]

// Alternative to out() when you need edge properties too
g.V().has('name', 'marko').outE('created').otherV()
// Equivalent to: g.V().has('name', 'marko').out('created')
```

#### Rust API

```rust
pub fn other_v(self) -> Traversal<In, Value>

// Anonymous traversal
__::other_v()
```

#### Implementation Approach

The key challenge is tracking which vertex we "came from" when traversing to an edge. Options:

1. **Store source vertex in traverser metadata**: When `outE()`, `inE()`, or `bothE()` produces an edge, store the source vertex ID in the traverser's sack or a custom field.

2. **Use the path**: Look at the path to find the previous vertex, then return the other endpoint.

3. **Dedicated tracking field**: Add an optional `from_vertex: Option<VertexId>` to `Traverser`.

**Recommended**: Option 2 (use path) for simplicity, falling back to examining the edge endpoints.

```rust
/// Navigation step that returns the "other" vertex from an edge.
///
/// When the traverser is on an edge, returns the vertex at the opposite
/// end from where the traverser came from. Requires that the previous
/// step was at a vertex.
///
/// If the current value is not an edge, or if the previous path element
/// cannot be determined, the traverser is filtered out.
#[derive(Clone, Debug, Default)]
pub struct OtherVStep;

impl OtherVStep {
    pub fn new() -> Self {
        Self
    }

    fn get_other_vertex(
        &self,
        ctx: &ExecutionContext,
        traverser: &Traverser,
    ) -> Option<VertexId> {
        // Current value must be an edge
        let edge_id = match &traverser.value {
            Value::Edge(id) => *id,
            _ => return None,
        };

        // Get the edge to find its endpoints
        let edge = ctx.snapshot().storage().get_edge(edge_id)?;
        
        // Find the vertex we came from by looking at the path
        // The second-to-last element should be the source vertex
        let path_elements = traverser.path.elements();
        if path_elements.len() < 2 {
            // No previous element in path, can't determine source
            // Fall back: arbitrarily return out_vertex (or could filter out)
            return Some(edge.out_vertex);
        }

        let prev_element = &path_elements[path_elements.len() - 2];
        match &prev_element.value {
            PathValue::Vertex(prev_id) => {
                // Return the OTHER vertex
                if *prev_id == edge.out_vertex {
                    Some(edge.in_vertex)
                } else if *prev_id == edge.in_vertex {
                    Some(edge.out_vertex)
                } else {
                    // Previous vertex isn't an endpoint of this edge
                    // This shouldn't happen in normal traversals
                    None
                }
            }
            _ => None,
        }
    }
}

impl AnyStep for OtherVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            self.get_other_vertex(ctx, &t).map(|vid| {
                t.with_value(Value::Vertex(vid))
            })
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "otherV"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_other_v_from_out_edge() {
    let graph = create_knows_graph(); // marko -knows-> vadas, marko -knows-> josh
    let g = graph.traversal();
    
    // From outE, otherV should give us the in-vertex
    let others: Vec<_> = g.v()
        .has("name", "marko")
        .out_e("knows")
        .other_v()
        .values("name")
        .to_list();
    
    assert!(others.contains(&Value::String("vadas".into())));
    assert!(others.contains(&Value::String("josh".into())));
}

#[test]
fn test_other_v_from_in_edge() {
    let graph = create_knows_graph();
    let g = graph.traversal();
    
    // From inE, otherV should give us the out-vertex
    let others: Vec<_> = g.v()
        .has("name", "vadas")
        .in_e("knows")
        .other_v()
        .values("name")
        .to_list();
    
    assert_eq!(others, vec![Value::String("marko".into())]);
}

#[test]
fn test_other_v_from_both_e() {
    let graph = create_knows_graph();
    let g = graph.traversal();
    
    // From bothE, otherV should give us the opposite vertex
    let others: Vec<_> = g.v()
        .has("name", "marko")
        .both_e("knows")
        .other_v()
        .values("name")
        .to_list();
    
    // Should not include marko himself
    for name in &others {
        assert_ne!(name, &Value::String("marko".into()));
    }
}

#[test]
fn test_other_v_filters_non_edges() {
    let graph = create_knows_graph();
    let g = graph.traversal();
    
    // otherV on vertices should filter them out
    let result: Vec<_> = g.v()
        .has("name", "marko")
        .other_v()
        .to_list();
    
    assert!(result.is_empty());
}
```

---

## Section 3: Transform Steps

Transform steps modify traverser values. They can be 1:1 (map) or 1:N (flatMap).

### 3.1 PropertiesStep

**Gremlin**: `properties()`, `properties(key...)`  
**Semantics**: Returns property objects (key-value pairs) from elements. Unlike `values()` which returns just the values, `properties()` returns the full property including its key.

#### Gremlin Examples

```groovy
// Get all properties of a vertex
g.V().has('name', 'marko').properties()
// Returns: [vp[name->marko], vp[age->29]]

// Get specific properties
g.V().has('name', 'marko').properties('name', 'age')

// Chain to get property keys
g.V().has('name', 'marko').properties().key()
```

#### Rust API

```rust
// All properties
pub fn properties(self) -> Traversal<In, Value>

// Specific properties
pub fn properties_keys(self, keys: &[&str]) -> Traversal<In, Value>

// Anonymous traversals
__::properties()
__::properties_keys(&["name", "age"])
```

#### Implementation

We need a `Property` value type to represent key-value pairs:

```rust
/// A property representation for the properties() step.
/// Stored as Value::Map with "key" and "value" entries.
pub fn make_property_value(key: &str, value: &Value) -> Value {
    let mut map = HashMap::new();
    map.insert("key".to_string(), Value::String(key.to_string()));
    map.insert("value".to_string(), value.clone());
    Value::Map(map)
}

/// Transform step that returns property objects from elements.
///
/// For vertices and edges, returns their properties as Property values.
/// Non-element values produce no output.
#[derive(Clone, Debug)]
pub struct PropertiesStep {
    keys: Option<Vec<String>>,
}

impl PropertiesStep {
    pub fn new() -> Self {
        Self { keys: None }
    }

    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    fn get_properties(
        &self,
        ctx: &ExecutionContext,
        traverser: &Traverser,
    ) -> Vec<Value> {
        let props = match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot()
                    .storage()
                    .get_vertex(*id)
                    .map(|v| &v.properties)
            }
            Value::Edge(id) => {
                ctx.snapshot()
                    .storage()
                    .get_edge(*id)
                    .map(|e| &e.properties)
            }
            _ => None,
        };

        let Some(props) = props else {
            return vec![];
        };

        match &self.keys {
            Some(keys) => {
                // Only requested keys
                keys.iter()
                    .filter_map(|k| {
                        props.get(k).map(|v| make_property_value(k, v))
                    })
                    .collect()
            }
            None => {
                // All properties
                props.iter()
                    .map(|(k, v)| make_property_value(k, v))
                    .collect()
            }
        }
    }
}

impl_flatmap_step!(PropertiesStep, "properties", get_properties);
```

#### Test Cases

```rust
#[test]
fn test_properties_returns_all() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let props: Vec<_> = g.v()
        .has("name", "marko")
        .properties()
        .to_list();
    
    // Should have name and age properties
    assert!(props.len() >= 2);
    
    // Check structure
    for prop in &props {
        if let Value::Map(m) = prop {
            assert!(m.contains_key("key"));
            assert!(m.contains_key("value"));
        } else {
            panic!("Expected Map value");
        }
    }
}

#[test]
fn test_properties_with_keys() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let props: Vec<_> = g.v()
        .has("name", "marko")
        .properties_keys(&["name"])
        .to_list();
    
    assert_eq!(props.len(), 1);
    if let Value::Map(m) = &props[0] {
        assert_eq!(m.get("key"), Some(&Value::String("name".into())));
    }
}
```

---

### 3.2 ValueMapStep

**Gremlin**: `valueMap()`, `valueMap(keys...)`  
**Semantics**: Returns a map of all property key-values for an element. Each key maps to a list of values (for multi-properties).

#### Gremlin Examples

```groovy
// Get all properties as a map
g.V().has('name', 'marko').valueMap()
// Returns: {name: [marko], age: [29]}

// Get specific keys
g.V().valueMap('name', 'age')

// With tokens (id, label)
g.V().valueMap(true)  // includes id and label
g.V().valueMap().with(WithOptions.tokens)
```

#### Rust API

```rust
// All properties
pub fn value_map(self) -> Traversal<In, Value>

// Specific keys
pub fn value_map_keys(self, keys: &[&str]) -> Traversal<In, Value>

// With tokens (id and label)
pub fn value_map_with_tokens(self) -> Traversal<In, Value>

// Anonymous traversals
__::value_map()
__::value_map_keys(&["name", "age"])
```

#### Implementation

```rust
/// Transform step that returns a map of property key-values.
///
/// Returns Value::Map where each key maps to Value::List (for multi-property support).
/// Non-element values produce empty maps.
#[derive(Clone, Debug)]
pub struct ValueMapStep {
    keys: Option<Vec<String>>,
    include_tokens: bool,
}

impl ValueMapStep {
    pub fn new() -> Self {
        Self {
            keys: None,
            include_tokens: false,
        }
    }

    pub fn with_keys(keys: Vec<String>) -> Self {
        Self {
            keys: Some(keys),
            include_tokens: false,
        }
    }

    pub fn with_tokens(mut self) -> Self {
        self.include_tokens = true;
        self
    }

    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut result: HashMap<String, Value> = HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    // Add tokens if requested
                    if self.include_tokens {
                        result.insert("id".to_string(), Value::Integer(id.0 as i64));
                        result.insert("label".to_string(), Value::String(vertex.label.clone()));
                    }

                    // Add properties
                    self.add_properties(&vertex.properties, &mut result);
                }
            }
            Value::Edge(id) => {
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    if self.include_tokens {
                        result.insert("id".to_string(), Value::Integer(id.0 as i64));
                        result.insert("label".to_string(), Value::String(edge.label.clone()));
                    }
                    self.add_properties(&edge.properties, &mut result);
                }
            }
            _ => {}
        }

        Value::Map(result)
    }

    fn add_properties(
        &self,
        props: &HashMap<String, Value>,
        result: &mut HashMap<String, Value>,
    ) {
        let keys_to_include: Box<dyn Iterator<Item = &String>> = match &self.keys {
            Some(keys) => Box::new(keys.iter()),
            None => Box::new(props.keys()),
        };

        for key in keys_to_include {
            if let Some(value) = props.get(key) {
                // Wrap in list for multi-property compatibility
                result.insert(key.clone(), Value::List(vec![value.clone()]));
            }
        }
    }
}

impl AnyStep for ValueMapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let value = self.transform(ctx, &t);
            t.with_value(value)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "valueMap"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_value_map_all_properties() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let maps: Vec<_> = g.v()
        .has("name", "marko")
        .value_map()
        .to_list();
    
    assert_eq!(maps.len(), 1);
    if let Value::Map(m) = &maps[0] {
        assert!(m.contains_key("name"));
        assert!(m.contains_key("age"));
        // Values should be lists
        if let Some(Value::List(names)) = m.get("name") {
            assert_eq!(names[0], Value::String("marko".into()));
        }
    }
}

#[test]
fn test_value_map_with_tokens() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let maps: Vec<_> = g.v()
        .has("name", "marko")
        .value_map_with_tokens()
        .to_list();
    
    if let Value::Map(m) = &maps[0] {
        assert!(m.contains_key("id"));
        assert!(m.contains_key("label"));
        assert!(m.contains_key("name"));
    }
}
```

---

### 3.3 ElementMapStep

**Gremlin**: `elementMap()`, `elementMap(keys...)`  
**Semantics**: Returns a complete representation of an element including id, label, and properties. Unlike `valueMap()`, values are NOT wrapped in lists.

#### Gremlin Examples

```groovy
// Get full element representation
g.V().has('name', 'marko').elementMap()
// Returns: {id: 1, label: person, name: marko, age: 29}

// For edges, includes IN and OUT vertices
g.E().elementMap()
// Returns: {id: 7, label: knows, IN: {id: 2, label: person}, OUT: {id: 1, label: person}, weight: 0.5}
```

#### Rust API

```rust
pub fn element_map(self) -> Traversal<In, Value>
pub fn element_map_keys(self, keys: &[&str]) -> Traversal<In, Value>

// Anonymous
__::element_map()
```

#### Implementation

```rust
/// Transform step returning complete element representation.
///
/// Always includes id and label. For edges, also includes IN and OUT vertex info.
/// Property values are NOT wrapped in lists (unlike valueMap).
#[derive(Clone, Debug)]
pub struct ElementMapStep {
    keys: Option<Vec<String>>,
}

impl ElementMapStep {
    pub fn new() -> Self {
        Self { keys: None }
    }

    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut result: HashMap<String, Value> = HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    result.insert("id".to_string(), Value::Integer(id.0 as i64));
                    result.insert("label".to_string(), Value::String(vertex.label.clone()));
                    self.add_properties_unwrapped(&vertex.properties, &mut result);
                }
            }
            Value::Edge(id) => {
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    result.insert("id".to_string(), Value::Integer(id.0 as i64));
                    result.insert("label".to_string(), Value::String(edge.label.clone()));

                    // Add IN/OUT vertex references
                    let in_ref = self.make_vertex_ref(ctx, edge.in_vertex);
                    let out_ref = self.make_vertex_ref(ctx, edge.out_vertex);
                    result.insert("IN".to_string(), in_ref);
                    result.insert("OUT".to_string(), out_ref);

                    self.add_properties_unwrapped(&edge.properties, &mut result);
                }
            }
            _ => {}
        }

        Value::Map(result)
    }

    fn make_vertex_ref(&self, ctx: &ExecutionContext, vid: VertexId) -> Value {
        let mut ref_map = HashMap::new();
        ref_map.insert("id".to_string(), Value::Integer(vid.0 as i64));
        if let Some(v) = ctx.snapshot().storage().get_vertex(vid) {
            ref_map.insert("label".to_string(), Value::String(v.label.clone()));
        }
        Value::Map(ref_map)
    }

    fn add_properties_unwrapped(
        &self,
        props: &HashMap<String, Value>,
        result: &mut HashMap<String, Value>,
    ) {
        match &self.keys {
            Some(keys) => {
                for key in keys {
                    if let Some(value) = props.get(key) {
                        result.insert(key.clone(), value.clone());
                    }
                }
            }
            None => {
                for (key, value) in props {
                    result.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

impl AnyStep for ElementMapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let value = self.transform(ctx, &t);
            t.with_value(value)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "elementMap"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_element_map_vertex() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let maps: Vec<_> = g.v()
        .has("name", "marko")
        .element_map()
        .to_list();
    
    if let Value::Map(m) = &maps[0] {
        assert!(m.contains_key("id"));
        assert!(m.contains_key("label"));
        // Values NOT in lists
        assert_eq!(m.get("name"), Some(&Value::String("marko".into())));
    }
}

#[test]
fn test_element_map_edge() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let maps: Vec<_> = g.e()
        .has_label("knows")
        .element_map()
        .to_list();
    
    if let Value::Map(m) = &maps[0] {
        assert!(m.contains_key("IN"));
        assert!(m.contains_key("OUT"));
        // IN/OUT should be vertex refs
        if let Some(Value::Map(in_ref)) = m.get("IN") {
            assert!(in_ref.contains_key("id"));
            assert!(in_ref.contains_key("label"));
        }
    }
}
```

---

### 3.4 UnfoldStep

**Gremlin**: `unfold()`  
**Semantics**: Unrolls a collection (list, map) into individual elements. Each element in the collection becomes a separate traverser.

#### Gremlin Examples

```groovy
// Unfold a list
g.V().values('skills').unfold()  // If skills is a list

// Unfold results of fold
g.V().fold().unfold()

// Unfold a map into entries
g.V().valueMap().unfold()
// Returns: [name=[marko]], [age=[29]], ...
```

#### Rust API

```rust
pub fn unfold(self) -> Traversal<In, Value>

// Anonymous
__::unfold()
```

#### Implementation

```rust
/// Transform step that unrolls collections into individual elements.
///
/// - Lists: each element becomes a traverser
/// - Maps: each key-value pair becomes a traverser (as a 2-element list or map)
/// - Other values: pass through unchanged
#[derive(Clone, Debug, Default)]
pub struct UnfoldStep;

impl UnfoldStep {
    pub fn new() -> Self {
        Self
    }

    fn unfold(&self, traverser: &Traverser) -> Vec<Value> {
        match &traverser.value {
            Value::List(items) => items.clone(),
            Value::Map(map) => {
                // Each entry becomes a single-entry map
                map.iter()
                    .map(|(k, v)| {
                        let mut entry = HashMap::new();
                        entry.insert(k.clone(), v.clone());
                        Value::Map(entry)
                    })
                    .collect()
            }
            // Non-collections pass through
            other => vec![other.clone()],
        }
    }
}

impl_flatmap_step!(UnfoldStep, "unfold", unfold);
```

#### Test Cases

```rust
#[test]
fn test_unfold_list() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // First fold vertices, then unfold
    let results: Vec<_> = g.v()
        .fold()
        .unfold()
        .to_list();
    
    // Should get back individual vertices
    let direct: Vec<_> = g.v().to_list();
    assert_eq!(results.len(), direct.len());
}

#[test]
fn test_unfold_map() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let entries: Vec<_> = g.v()
        .has("name", "marko")
        .value_map()
        .unfold()
        .to_list();
    
    // Each property becomes a separate entry
    assert!(entries.len() >= 2); // name, age at minimum
}
```

---

### 3.5 ProjectStep

**Gremlin**: `project(keys...).by(...).by(...)`  
**Semantics**: Creates a map projection with named keys, where each key's value is computed by a `by()` modulator (sub-traversal).

#### Gremlin Examples

```groovy
// Project person data
g.V().hasLabel('person').project('name', 'age', 'friends')
    .by('name')
    .by('age')
    .by(out('knows').count())

// Returns: [{name: marko, age: 29, friends: 2}, ...]
```

#### Rust API

```rust
// Builder pattern
pub fn project(self, keys: &[&str]) -> ProjectBuilder<In>

impl ProjectBuilder<In> {
    pub fn by(self, traversal: impl Into<Projection>) -> Self
    pub fn by_key(self, key: &str) -> Self  // shorthand for values(key)
    pub fn build(self) -> Traversal<In, Value>
}

// Usage
g.v().has_label("person")
    .project(&["name", "age", "friends"])
    .by_key("name")
    .by_key("age")
    .by(__::out("knows").count())
    .build()
```

#### Implementation

```rust
/// Projection specification for a single key in project()
#[derive(Clone)]
pub enum Projection {
    /// Use a property value directly
    Key(String),
    /// Use a sub-traversal
    Traversal(Traversal<Value, Value>),
}

impl From<&str> for Projection {
    fn from(key: &str) -> Self {
        Projection::Key(key.to_string())
    }
}

impl<In, Out> From<Traversal<In, Out>> for Projection {
    fn from(t: Traversal<In, Out>) -> Self {
        Projection::Traversal(t.into_value_traversal())
    }
}

/// Transform step that creates named projections.
#[derive(Clone)]
pub struct ProjectStep {
    keys: Vec<String>,
    projections: Vec<Projection>,
}

impl ProjectStep {
    pub fn new(keys: Vec<String>, projections: Vec<Projection>) -> Self {
        Self { keys, projections }
    }

    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut result = HashMap::new();

        for (key, proj) in self.keys.iter().zip(self.projections.iter()) {
            let value = match proj {
                Projection::Key(prop_key) => {
                    // Get property value from element
                    self.get_property(ctx, traverser, prop_key)
                }
                Projection::Traversal(sub) => {
                    // Execute sub-traversal and get first result
                    let results: Vec<_> = execute_traversal_from(
                        ctx,
                        std::iter::once(traverser.clone()),
                        sub,
                    ).collect();
                    
                    if results.len() == 1 {
                        results.into_iter().next().map(|t| t.value)
                    } else if results.is_empty() {
                        None
                    } else {
                        // Multiple results -> return as list
                        Some(Value::List(results.into_iter().map(|t| t.value).collect()))
                    }
                }
            };

            result.insert(key.clone(), value.unwrap_or(Value::Null));
        }

        Value::Map(result)
    }

    fn get_property(&self, ctx: &ExecutionContext, t: &Traverser, key: &str) -> Option<Value> {
        match &t.value {
            Value::Vertex(id) => {
                ctx.snapshot().storage().get_vertex(*id)
                    .and_then(|v| v.properties.get(key).cloned())
            }
            Value::Edge(id) => {
                ctx.snapshot().storage().get_edge(*id)
                    .and_then(|e| e.properties.get(key).cloned())
            }
            _ => None,
        }
    }
}

impl AnyStep for ProjectStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let value = self.transform(ctx, &t);
            t.with_value(value)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "project"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_project_with_keys() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let results: Vec<_> = g.v()
        .has_label("person")
        .project(&["name", "age"])
        .by_key("name")
        .by_key("age")
        .build()
        .to_list();
    
    for result in &results {
        if let Value::Map(m) = result {
            assert!(m.contains_key("name"));
            assert!(m.contains_key("age"));
        }
    }
}

#[test]
fn test_project_with_traversal() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let results: Vec<_> = g.v()
        .has("name", "marko")
        .project(&["name", "friend_count"])
        .by_key("name")
        .by(__::out("knows").count())
        .build()
        .to_list();
    
    if let Value::Map(m) = &results[0] {
        assert_eq!(m.get("name"), Some(&Value::String("marko".into())));
        assert!(m.contains_key("friend_count"));
    }
}
```

---

### 3.6 MathStep

**Gremlin**: `math(expression)`  
**Semantics**: Evaluates a mathematical expression. Variables in the expression reference labeled values from `as()` steps.

#### Gremlin Examples

```groovy
// Calculate age difference
g.V().as('a').out('knows').as('b')
    .math('a - b')
    .by('age')

// Use constants
g.V().values('age').math('_ * 2')  // _ is the current value
```

#### Rust API

```rust
pub fn math(self, expression: &str) -> MathBuilder<In>

impl MathBuilder<In> {
    pub fn by(self, key: &str) -> Self
    pub fn build(self) -> Traversal<In, Value>
}

// Usage
g.v().as_("a").out("knows").as_("b")
    .math("a - b")
    .by("age")
    .by("age")
    .build()
```

#### Implementation

```rust
/// Mathematical expression evaluator step.
///
/// Supports basic operations: +, -, *, /, %, ^
/// Variables reference select() labels or `_` for current value.
#[derive(Clone)]
pub struct MathStep {
    expression: String,
    variable_keys: Vec<(String, String)>, // (variable, property_key)
}

impl MathStep {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            variable_keys: vec![],
        }
    }

    pub fn with_by(mut self, variable: &str, key: &str) -> Self {
        self.variable_keys.push((variable.to_string(), key.to_string()));
        self
    }

    fn evaluate(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        // Parse and evaluate expression
        // This requires an expression parser (could use a crate like `meval`)
        
        // Build variable bindings
        let mut bindings = HashMap::new();
        
        // `_` is the current value
        if let Value::Integer(n) = &traverser.value {
            bindings.insert("_".to_string(), *n as f64);
        } else if let Value::Float(f) = &traverser.value {
            bindings.insert("_".to_string(), *f);
        }

        // Get labeled values from path
        for (var, key) in &self.variable_keys {
            if let Some(value) = traverser.path.get_labeled(var) {
                let num = self.extract_number(ctx, &value.clone().into(), key)?;
                bindings.insert(var.clone(), num);
            }
        }

        // Evaluate expression with bindings
        let result = self.evaluate_expr(&self.expression, &bindings)?;
        
        Some(Value::Float(result))
    }

    fn extract_number(&self, ctx: &ExecutionContext, value: &Value, key: &str) -> Option<f64> {
        match value {
            Value::Integer(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            Value::Vertex(id) => {
                ctx.snapshot().storage().get_vertex(*id)
                    .and_then(|v| v.properties.get(key))
                    .and_then(|v| match v {
                        Value::Integer(n) => Some(*n as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
            }
            _ => None,
        }
    }

    fn evaluate_expr(&self, expr: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
        // Simple expression evaluator (production would use meval or similar)
        // For now, this is a placeholder that handles simple cases
        // Full implementation would parse and evaluate the expression tree
        
        // Example: handle "a - b" pattern
        if let Some(pos) = expr.find(" - ") {
            let left = expr[..pos].trim();
            let right = expr[pos + 3..].trim();
            let l = bindings.get(left)?;
            let r = bindings.get(right)?;
            return Some(l - r);
        }
        
        // Handle "_ * N" pattern
        if let Some(pos) = expr.find(" * ") {
            let left = expr[..pos].trim();
            let right = expr[pos + 3..].trim();
            let l = if left == "_" {
                *bindings.get("_")?
            } else {
                bindings.get(left).copied().or_else(|| left.parse().ok())?
            };
            let r = if right == "_" {
                *bindings.get("_")?
            } else {
                bindings.get(right).copied().or_else(|| right.parse().ok())?
            };
            return Some(l * r);
        }

        None
    }
}
```

**Note**: A production implementation would use a proper expression parser like `meval` crate.

#### Test Cases

```rust
#[test]
fn test_math_simple_multiply() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let results: Vec<_> = g.v()
        .values("age")
        .math("_ * 2")
        .build()
        .to_list();
    
    // Ages doubled
    for result in &results {
        if let Value::Float(f) = result {
            assert!(*f > 0.0);
        }
    }
}

#[test]
fn test_math_with_labels() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let results: Vec<_> = g.v()
        .has("name", "marko")
        .as_("a")
        .out("knows")
        .as_("b")
        .math("a - b")
        .by("age")
        .by("age")
        .build()
        .to_list();
    
    // marko's age minus friend's age
    assert!(!results.is_empty());
}
```

---

### 3.7 OrderStep

**Gremlin**: `order()`, `order().by(key)`, `order().by(key, Order.desc)`  
**Semantics**: Sorts traversers by value or by a specified property/traversal. Collects all input traversers, sorts them, and emits in sorted order.

#### Gremlin Examples

```groovy
// Sort by natural order
g.V().values('name').order()

// Sort by property
g.V().order().by('age')

// Sort descending
g.V().order().by('age', desc)

// Sort by traversal result
g.V().order().by(out().count(), desc)
```

#### Rust API

```rust
pub fn order(self) -> OrderBuilder<In>

impl OrderBuilder<In> {
    pub fn by_asc(self) -> Self  // natural ascending (default)
    pub fn by_desc(self) -> Self  // natural descending
    pub fn by_key_asc(self, key: &str) -> Self
    pub fn by_key_desc(self, key: &str) -> Self
    pub fn by_traversal(self, t: Traversal<Value, Value>, desc: bool) -> Self
    pub fn build(self) -> Traversal<In, Value>
}

// Usage
g.v().order().by_key_desc("age").build()
g.v().order().by_traversal(__::out().count(), true).build()
```

#### Implementation

```rust
/// Sort order
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Order {
    Asc,
    Desc,
}

/// Sort key specification
#[derive(Clone)]
pub enum OrderKey {
    /// Sort by the traverser's current value
    Natural(Order),
    /// Sort by a property key
    Property(String, Order),
    /// Sort by traversal result
    Traversal(Traversal<Value, Value>, Order),
}

/// Barrier step that sorts all traversers.
///
/// This is a "barrier" step that collects all input before producing output.
#[derive(Clone)]
pub struct OrderStep {
    keys: Vec<OrderKey>,
}

impl OrderStep {
    pub fn new() -> Self {
        Self {
            keys: vec![OrderKey::Natural(Order::Asc)],
        }
    }

    pub fn by_natural(order: Order) -> Self {
        Self {
            keys: vec![OrderKey::Natural(order)],
        }
    }

    pub fn by_property(key: impl Into<String>, order: Order) -> Self {
        Self {
            keys: vec![OrderKey::Property(key.into(), order)],
        }
    }

    fn compare(
        &self,
        ctx: &ExecutionContext,
        a: &Traverser,
        b: &Traverser,
    ) -> std::cmp::Ordering {
        for key in &self.keys {
            let ord = match key {
                OrderKey::Natural(order) => {
                    let cmp = Self::compare_values(&a.value, &b.value);
                    Self::apply_order(cmp, *order)
                }
                OrderKey::Property(prop, order) => {
                    let va = self.get_property(ctx, a, prop);
                    let vb = self.get_property(ctx, b, prop);
                    let cmp = Self::compare_option_values(&va, &vb);
                    Self::apply_order(cmp, *order)
                }
                OrderKey::Traversal(sub, order) => {
                    let va = self.execute_for_sort(ctx, a, sub);
                    let vb = self.execute_for_sort(ctx, b, sub);
                    let cmp = Self::compare_option_values(&va, &vb);
                    Self::apply_order(cmp, *order)
                }
            };

            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }

        std::cmp::Ordering::Equal
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        // Implement value comparison
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn compare_option_values(a: &Option<Value>, b: &Option<Value>) -> std::cmp::Ordering {
        match (a, b) {
            (Some(a), Some(b)) => Self::compare_values(a, b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }

    fn apply_order(ord: std::cmp::Ordering, order: Order) -> std::cmp::Ordering {
        match order {
            Order::Asc => ord,
            Order::Desc => ord.reverse(),
        }
    }

    fn get_property(&self, ctx: &ExecutionContext, t: &Traverser, key: &str) -> Option<Value> {
        match &t.value {
            Value::Vertex(id) => {
                ctx.snapshot().storage().get_vertex(*id)
                    .and_then(|v| v.properties.get(key).cloned())
            }
            Value::Edge(id) => {
                ctx.snapshot().storage().get_edge(*id)
                    .and_then(|e| e.properties.get(key).cloned())
            }
            _ => None,
        }
    }

    fn execute_for_sort(
        &self,
        ctx: &ExecutionContext,
        t: &Traverser,
        sub: &Traversal<Value, Value>,
    ) -> Option<Value> {
        execute_traversal_from(ctx, std::iter::once(t.clone()), sub)
            .next()
            .map(|t| t.value)
    }
}

impl AnyStep for OrderStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all input (barrier)
        let mut traversers: Vec<_> = input.collect();
        
        // Sort
        traversers.sort_by(|a, b| self.compare(ctx, a, b));
        
        Box::new(traversers.into_iter())
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "order"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_order_natural_ascending() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let names: Vec<_> = g.v()
        .values("name")
        .order()
        .build()
        .to_list();
    
    // Check sorted order
    let strings: Vec<String> = names.iter()
        .filter_map(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
        .collect();
    
    let mut sorted = strings.clone();
    sorted.sort();
    assert_eq!(strings, sorted);
}

#[test]
fn test_order_by_property_desc() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let ages: Vec<_> = g.v()
        .has_label("person")
        .order()
        .by_key_desc("age")
        .build()
        .values("age")
        .to_list();
    
    // Should be descending
    let nums: Vec<i64> = ages.iter()
        .filter_map(|v| if let Value::Integer(n) = v { Some(*n) } else { None })
        .collect();
    
    for i in 1..nums.len() {
        assert!(nums[i-1] >= nums[i]);
    }
}
```

---

### 3.8 MeanStep

**Gremlin**: `mean()`  
**Semantics**: Calculates the arithmetic mean (average) of numeric values. This is a reducing/terminal step that consumes all input and produces a single value.

#### Gremlin Examples

```groovy
// Average age
g.V().hasLabel('person').values('age').mean()
// Returns: 30.75

// Mean of edge weights
g.E().values('weight').mean()
```

#### Rust API

```rust
pub fn mean(self) -> Traversal<In, Value>

// Anonymous
__::mean()
```

#### Implementation

```rust
/// Reducing step that calculates the arithmetic mean.
///
/// Collects all numeric values and returns their average.
/// Non-numeric values are ignored. Returns null if no numeric values.
#[derive(Clone, Debug, Default)]
pub struct MeanStep;

impl MeanStep {
    pub fn new() -> Self {
        Self
    }
}

impl AnyStep for MeanStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let mut sum = 0.0_f64;
        let mut count = 0_u64;
        let mut last_path = None;

        for t in input {
            last_path = Some(t.path.clone());
            
            match &t.value {
                Value::Integer(n) => {
                    sum += *n as f64;
                    count += 1;
                }
                Value::Float(f) => {
                    sum += *f;
                    count += 1;
                }
                _ => {} // Ignore non-numeric
            }
        }

        if count == 0 {
            Box::new(std::iter::empty())
        } else {
            let mean = sum / count as f64;
            let traverser = Traverser {
                value: Value::Float(mean),
                path: last_path.unwrap_or_default(),
                loops: 0,
                sack: None,
                bulk: 1,
            };
            Box::new(std::iter::once(traverser))
        }
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "mean"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_mean_integers() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let results: Vec<_> = g.v()
        .has_label("person")
        .values("age")
        .mean()
        .to_list();
    
    assert_eq!(results.len(), 1);
    if let Value::Float(mean) = &results[0] {
        // Average of test ages
        assert!(*mean > 0.0);
    }
}

#[test]
fn test_mean_empty() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    // No matches -> no mean
    let results: Vec<_> = g.v()
        .has_label("nonexistent")
        .values("age")
        .mean()
        .to_list();
    
    assert!(results.is_empty());
}

#[test]
fn test_mean_ignores_non_numeric() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    // Mix of numeric and string properties
    let results: Vec<_> = g.v()
        .values("age")  // numeric
        .mean()
        .to_list();
    
    assert_eq!(results.len(), 1);
}
```

---

## Section 4: Aggregation Steps

Aggregation steps group and summarize traversers. They are barrier steps that collect all input before producing output.

### 4.1 GroupStep

**Gremlin**: `group()`, `group().by(key).by(value)`  
**Semantics**: Groups traversers by a key, where each group contains a list of values. Uses `by()` modulators to specify the grouping key and the value to collect.

#### Gremlin Examples

```groovy
// Group by label
g.V().group().by(label)
// Returns: {person: [v[1], v[2], v[4], v[6]], software: [v[3], v[5]]}

// Group by property, collect names
g.V().group().by('age').by('name')
// Returns: {29: [marko], 27: [vadas], 32: [josh], 35: [peter]}

// Group with count aggregation
g.V().group().by(label).by(count())
// Returns: {person: 4, software: 2}
```

#### Rust API

```rust
pub fn group(self) -> GroupBuilder<In>

impl GroupBuilder<In> {
    // Key selector (first by())
    pub fn by_label(self) -> Self
    pub fn by_key(self, key: &str) -> Self
    pub fn by_traversal(self, t: Traversal<Value, Value>) -> Self
    
    // Value collector (second by())
    pub fn by_value(self) -> Self  // collect the elements themselves
    pub fn by_value_key(self, key: &str) -> Self  // collect property values
    pub fn by_value_traversal(self, t: Traversal<Value, Value>) -> Self
    
    pub fn build(self) -> Traversal<In, Value>
}

// Usage
g.v().group().by_label().by_value().build()
g.v().group().by_key("age").by_value_key("name").build()
```

#### Implementation

```rust
/// Key selector for group()
#[derive(Clone)]
pub enum GroupKey {
    Label,
    Property(String),
    Traversal(Traversal<Value, Value>),
}

/// Value collector for group()
#[derive(Clone)]
pub enum GroupValue {
    /// Collect the elements themselves
    Identity,
    /// Collect a property value
    Property(String),
    /// Apply a traversal and collect results
    Traversal(Traversal<Value, Value>),
}

/// Aggregation step that groups traversers by key.
///
/// Produces a single Map where keys are group keys and values are lists.
#[derive(Clone)]
pub struct GroupStep {
    key_selector: GroupKey,
    value_collector: GroupValue,
}

impl GroupStep {
    pub fn new(key_selector: GroupKey, value_collector: GroupValue) -> Self {
        Self {
            key_selector,
            value_collector,
        }
    }

    fn get_key(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        match &self.key_selector {
            GroupKey::Label => {
                match &traverser.value {
                    Value::Vertex(id) => {
                        ctx.snapshot().storage().get_vertex(*id)
                            .map(|v| Value::String(v.label.clone()))
                    }
                    Value::Edge(id) => {
                        ctx.snapshot().storage().get_edge(*id)
                            .map(|e| Value::String(e.label.clone()))
                    }
                    _ => None,
                }
            }
            GroupKey::Property(key) => {
                match &traverser.value {
                    Value::Vertex(id) => {
                        ctx.snapshot().storage().get_vertex(*id)
                            .and_then(|v| v.properties.get(key).cloned())
                    }
                    Value::Edge(id) => {
                        ctx.snapshot().storage().get_edge(*id)
                            .and_then(|e| e.properties.get(key).cloned())
                    }
                    _ => None,
                }
            }
            GroupKey::Traversal(sub) => {
                execute_traversal_from(ctx, std::iter::once(traverser.clone()), sub)
                    .next()
                    .map(|t| t.value)
            }
        }
    }

    fn get_value(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        match &self.value_collector {
            GroupValue::Identity => traverser.value.clone(),
            GroupValue::Property(key) => {
                match &traverser.value {
                    Value::Vertex(id) => {
                        ctx.snapshot().storage().get_vertex(*id)
                            .and_then(|v| v.properties.get(key).cloned())
                            .unwrap_or(Value::Null)
                    }
                    Value::Edge(id) => {
                        ctx.snapshot().storage().get_edge(*id)
                            .and_then(|e| e.properties.get(key).cloned())
                            .unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }
            }
            GroupValue::Traversal(sub) => {
                let results: Vec<Value> = execute_traversal_from(
                    ctx,
                    std::iter::once(traverser.clone()),
                    sub,
                ).map(|t| t.value).collect();

                if results.len() == 1 {
                    results.into_iter().next().unwrap()
                } else {
                    Value::List(results)
                }
            }
        }
    }
}

impl AnyStep for GroupStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all traversers and group them
        let mut groups: HashMap<String, Vec<Value>> = HashMap::new();

        for t in input {
            if let Some(key) = self.get_key(ctx, &t) {
                let key_str = Self::value_to_key(&key);
                let value = self.get_value(ctx, &t);
                groups.entry(key_str).or_default().push(value);
            }
        }

        // Convert to result map
        let result_map: HashMap<String, Value> = groups
            .into_iter()
            .map(|(k, v)| (k, Value::List(v)))
            .collect();

        let traverser = Traverser {
            value: Value::Map(result_map),
            path: Path::default(),
            loops: 0,
            sack: None,
            bulk: 1,
        };

        Box::new(std::iter::once(traverser))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "group"
    }
}

impl GroupStep {
    fn value_to_key(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Integer(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Vertex(id) => format!("v[{}]", id.0),
            Value::Edge(id) => format!("e[{}]", id.0),
            _ => "null".to_string(),
        }
    }
}
```

#### Test Cases

```rust
#[test]
fn test_group_by_label() {
    let graph = create_modern_graph();
    let g = graph.traversal();

    let results: Vec<_> = g.v()
        .group()
        .by_label()
        .by_value()
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(groups) = &results[0] {
        assert!(groups.contains_key("person"));
        assert!(groups.contains_key("software"));
    }
}

#[test]
fn test_group_by_property() {
    let graph = create_modern_graph();
    let g = graph.traversal();

    let results: Vec<_> = g.v()
        .has_label("person")
        .group()
        .by_key("age")
        .by_value_key("name")
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(groups) = &results[0] {
        // Each age maps to names
        for (_, names) in groups {
            if let Value::List(list) = names {
                assert!(!list.is_empty());
            }
        }
    }
}

#[test]
fn test_group_with_count() {
    let graph = create_modern_graph();
    let g = graph.traversal();

    let results: Vec<_> = g.v()
        .group()
        .by_label()
        .by_value_traversal(__::count())
        .build()
        .to_list();

    if let Value::Map(groups) = &results[0] {
        // Values should be counts
        for (_, count) in groups {
            assert!(matches!(count, Value::Integer(_) | Value::List(_)));
        }
    }
}
```

---

### 4.2 GroupCountStep

**Gremlin**: `groupCount()`, `groupCount().by(key)`  
**Semantics**: Groups traversers by key and counts elements in each group. A specialized form of `group()` that counts instead of collecting.

#### Gremlin Examples

```groovy
// Count by label
g.V().groupCount().by(label)
// Returns: {person: 4, software: 2}

// Count by property
g.V().groupCount().by('age')
// Returns: {29: 1, 27: 1, 32: 1, 35: 1}

// Count outgoing edges by label
g.V().out().groupCount().by(label)
```

#### Rust API

```rust
pub fn group_count(self) -> GroupCountBuilder<In>

impl GroupCountBuilder<In> {
    pub fn by_label(self) -> Self
    pub fn by_key(self, key: &str) -> Self
    pub fn by_traversal(self, t: Traversal<Value, Value>) -> Self
    pub fn build(self) -> Traversal<In, Value>
}

// Usage
g.v().group_count().by_label().build()
g.v().out().group_count().by_key("name").build()
```

#### Implementation

```rust
/// Aggregation step that groups and counts traversers by key.
///
/// Produces a single Map where keys are group keys and values are counts.
#[derive(Clone)]
pub struct GroupCountStep {
    key_selector: GroupKey,
}

impl GroupCountStep {
    pub fn new(key_selector: GroupKey) -> Self {
        Self { key_selector }
    }

    /// Default: group by the value itself
    pub fn identity() -> Self {
        Self {
            key_selector: GroupKey::Traversal(Traversal::identity()),
        }
    }

    fn get_key(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        // Same logic as GroupStep::get_key
        match &self.key_selector {
            GroupKey::Label => {
                match &traverser.value {
                    Value::Vertex(id) => {
                        ctx.snapshot().storage().get_vertex(*id)
                            .map(|v| Value::String(v.label.clone()))
                    }
                    Value::Edge(id) => {
                        ctx.snapshot().storage().get_edge(*id)
                            .map(|e| Value::String(e.label.clone()))
                    }
                    _ => Some(traverser.value.clone()),
                }
            }
            GroupKey::Property(key) => {
                match &traverser.value {
                    Value::Vertex(id) => {
                        ctx.snapshot().storage().get_vertex(*id)
                            .and_then(|v| v.properties.get(key).cloned())
                    }
                    Value::Edge(id) => {
                        ctx.snapshot().storage().get_edge(*id)
                            .and_then(|e| e.properties.get(key).cloned())
                    }
                    _ => None,
                }
            }
            GroupKey::Traversal(sub) => {
                execute_traversal_from(ctx, std::iter::once(traverser.clone()), sub)
                    .next()
                    .map(|t| t.value)
            }
        }
    }
}

impl AnyStep for GroupCountStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all traversers and count by key
        let mut counts: HashMap<String, i64> = HashMap::new();

        for t in input {
            if let Some(key) = self.get_key(ctx, &t) {
                let key_str = GroupStep::value_to_key(&key);
                *counts.entry(key_str).or_insert(0) += t.bulk as i64;
            }
        }

        // Convert to result map
        let result_map: HashMap<String, Value> = counts
            .into_iter()
            .map(|(k, v)| (k, Value::Integer(v)))
            .collect();

        let traverser = Traverser {
            value: Value::Map(result_map),
            path: Path::default(),
            loops: 0,
            sack: None,
            bulk: 1,
        };

        Box::new(std::iter::once(traverser))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "groupCount"
    }
}
```

#### Test Cases

```rust
#[test]
fn test_group_count_by_label() {
    let graph = create_modern_graph();
    let g = graph.traversal();

    let results: Vec<_> = g.v()
        .group_count()
        .by_label()
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(counts) = &results[0] {
        assert!(counts.contains_key("person"));
        assert!(counts.contains_key("software"));
        
        if let Some(Value::Integer(n)) = counts.get("person") {
            assert_eq!(*n, 4); // 4 people in modern graph
        }
    }
}

#[test]
fn test_group_count_by_property() {
    let graph = create_modern_graph();
    let g = graph.traversal();

    let results: Vec<_> = g.v()
        .has_label("person")
        .group_count()
        .by_key("age")
        .build()
        .to_list();

    if let Value::Map(counts) = &results[0] {
        // Each age should have count 1 (unique ages)
        for (_, count) in counts {
            if let Value::Integer(n) = count {
                assert_eq!(*n, 1);
            }
        }
    }
}

#[test]
fn test_group_count_with_bulk() {
    let graph = create_modern_graph();
    let g = graph.traversal();

    // Multiple paths to same vertex should be counted
    let results: Vec<_> = g.v()
        .out()
        .group_count()
        .by_label()
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    // Counts should reflect multiple traversers reaching same elements
}
```

---

## Section 5: Implementation Plan

### 5.1 Implementation Order

Steps should be implemented in dependency order, grouped by complexity:

#### Phase 1: Simple Filter Steps (Week 1, Days 1-2)

| Step | Complexity | Dependencies |
|------|------------|--------------|
| `HasNotStep` | Low | None (inverse of existing HasStep) |
| `IsStep` | Low | Existing predicate system |
| `SimplePathStep` | Medium | Path tracking (already implemented) |
| `CyclicPathStep` | Medium | Same as SimplePathStep |

**Rationale**: These are straightforward filter steps that use existing infrastructure.

#### Phase 2: Navigation Step (Week 1, Day 3)

| Step | Complexity | Dependencies |
|------|------------|--------------|
| `OtherVStep` | Medium | Path tracking for source vertex |

**Rationale**: Single step, requires path inspection.

#### Phase 3: Basic Transform Steps (Week 1, Days 4-5)

| Step | Complexity | Dependencies |
|------|------------|--------------|
| `PropertiesStep` | Low | Property access (existing) |
| `ValueMapStep` | Low | Property access |
| `ElementMapStep` | Low | Property access + edge endpoints |
| `UnfoldStep` | Low | None |

**Rationale**: These are simple 1:1 or 1:N transforms with no complex dependencies.

#### Phase 4: Complex Transform Steps (Week 2, Days 1-3)

| Step | Complexity | Dependencies |
|------|------------|--------------|
| `OrderStep` | Medium | Value comparison, barrier pattern |
| `MeanStep` | Low | Reducing barrier pattern |
| `ProjectStep` | High | Sub-traversal execution, builder pattern |
| `MathStep` | High | Expression parsing (consider `meval` crate) |

**Rationale**: These require more complex patterns like barriers and sub-traversal execution.

#### Phase 5: Aggregation Steps (Week 2, Days 4-5)

| Step | Complexity | Dependencies |
|------|------------|--------------|
| `GroupStep` | High | Sub-traversal execution, builder pattern |
| `GroupCountStep` | Medium | Shares infrastructure with GroupStep |

**Rationale**: Most complex steps, requiring grouping logic and sub-traversals.

#### Phase 6: API Integration (Week 3)

1. Add methods to `Traversal<In, Out>` for all new steps
2. Add factory functions to `__` module for anonymous traversals
3. Update `mod.rs` exports
4. Create builder types for complex steps (`GroupBuilder`, `ProjectBuilder`, `OrderBuilder`, `MathBuilder`)

### 5.2 File Changes Summary

```
src/traversal/
├── filter.rs       # +4 steps: HasNotStep, IsStep, SimplePathStep, CyclicPathStep
├── navigation.rs   # +1 step: OtherVStep
├── transform.rs    # +8 steps: Properties, ValueMap, ElementMap, Unfold, Project, Math, Order, Mean
├── aggregate.rs    # NEW FILE: GroupStep, GroupCountStep
└── mod.rs          # Update exports, add __ methods, add Traversal methods
```

### 5.3 Dependencies to Add

```toml
[dependencies]
# For math() expression parsing
meval = "0.2"  # Optional, can implement simple parser instead
```

---

## Section 6: Testing Strategy

### 6.1 Test Categories

#### Unit Tests (per step)

Each step should have unit tests covering:

1. **Happy path**: Basic functionality works
2. **Edge cases**: Empty input, single element, large input
3. **Type handling**: Correct behavior for different Value types
4. **Error conditions**: Missing properties, invalid inputs

#### Integration Tests

Test step combinations that commonly occur together:

```rust
// Common patterns to test
g.v().has_label("person").value_map().unfold()
g.v().group().by_label().by(__::count())
g.v().order().by_key_desc("age").limit(10)
g.v().repeat(__::out()).until(__::has("name", "target")).simple_path()
```

#### Property-Based Tests

Use `proptest` for steps with numeric or comparison logic:

```rust
proptest! {
    #[test]
    fn test_order_is_stable(values: Vec<i64>) {
        // Verify order() produces sorted output
        let graph = make_graph_with_values(&values);
        let result = g.v().values("num").order().build().to_list();
        assert!(is_sorted(&result));
    }

    #[test]
    fn test_mean_is_correct(values: Vec<f64>) {
        prop_assume!(!values.is_empty());
        let expected = values.iter().sum::<f64>() / values.len() as f64;
        // Test mean() matches expected
    }
}
```

### 6.2 Test Graph Fixtures

Create reusable test graph factories:

```rust
/// The "modern" TinkerPop graph for compatibility testing
fn create_modern_graph() -> InMemoryGraph {
    let mut graph = InMemoryGraph::new();
    
    // Vertices
    let marko = graph.add_vertex("person", props! {
        "name" => "marko",
        "age" => 29
    });
    let vadas = graph.add_vertex("person", props! {
        "name" => "vadas",
        "age" => 27
    });
    // ... etc
    
    graph
}

/// A graph with cycles for simplePath/cyclicPath tests
fn create_cycle_graph() -> InMemoryGraph { ... }

/// A linear graph (no branches) for path tests
fn create_linear_graph() -> InMemoryGraph { ... }
```

### 6.3 Coverage Goals

| Category | Target |
|----------|--------|
| Line coverage | 90%+ |
| Branch coverage | 85%+ |
| New steps | 100% of public methods |
| Edge cases | Documented and tested |

### 6.4 Benchmark Tests

Add benchmarks for barrier steps that collect input:

```rust
// benches/traversal.rs

#[bench]
fn bench_order_1000_elements(b: &mut Bencher) {
    let graph = create_large_graph(1000);
    let g = graph.traversal();
    
    b.iter(|| {
        g.v().order().by_key_asc("value").build().to_list()
    });
}

#[bench]
fn bench_group_by_label(b: &mut Bencher) {
    let graph = create_large_graph(1000);
    let g = graph.traversal();
    
    b.iter(|| {
        g.v().group().by_label().by_value().build().to_list()
    });
}
```

---

## Section 7: API Summary

### New Methods on `Traversal<In, Out>`

```rust
impl<In, Out> Traversal<In, Out> {
    // Filter steps
    pub fn has_not(self, key: impl Into<String>) -> Traversal<In, Value>;
    pub fn is_eq(self, value: impl Into<Value>) -> Traversal<In, Value>;
    pub fn is_(self, predicate: impl Predicate) -> Traversal<In, Value>;
    pub fn simple_path(self) -> Traversal<In, Value>;
    pub fn cyclic_path(self) -> Traversal<In, Value>;
    
    // Navigation steps
    pub fn other_v(self) -> Traversal<In, Value>;
    
    // Transform steps
    pub fn properties(self) -> Traversal<In, Value>;
    pub fn properties_keys(self, keys: &[&str]) -> Traversal<In, Value>;
    pub fn value_map(self) -> Traversal<In, Value>;
    pub fn value_map_keys(self, keys: &[&str]) -> Traversal<In, Value>;
    pub fn value_map_with_tokens(self) -> Traversal<In, Value>;
    pub fn element_map(self) -> Traversal<In, Value>;
    pub fn element_map_keys(self, keys: &[&str]) -> Traversal<In, Value>;
    pub fn unfold(self) -> Traversal<In, Value>;
    pub fn project(self, keys: &[&str]) -> ProjectBuilder<In>;
    pub fn math(self, expr: &str) -> MathBuilder<In>;
    pub fn order(self) -> OrderBuilder<In>;
    pub fn mean(self) -> Traversal<In, Value>;
    
    // Aggregation steps
    pub fn group(self) -> GroupBuilder<In>;
    pub fn group_count(self) -> GroupCountBuilder<In>;
}
```

### New Functions in `__` Module

```rust
pub mod __ {
    // Filter
    pub fn has_not(key: impl Into<String>) -> Traversal<Value, Value>;
    pub fn is_eq(value: impl Into<Value>) -> Traversal<Value, Value>;
    pub fn is_(predicate: impl Predicate) -> Traversal<Value, Value>;
    pub fn simple_path() -> Traversal<Value, Value>;
    pub fn cyclic_path() -> Traversal<Value, Value>;
    
    // Navigation
    pub fn other_v() -> Traversal<Value, Value>;
    
    // Transform
    pub fn properties() -> Traversal<Value, Value>;
    pub fn value_map() -> Traversal<Value, Value>;
    pub fn element_map() -> Traversal<Value, Value>;
    pub fn unfold() -> Traversal<Value, Value>;
    pub fn mean() -> Traversal<Value, Value>;
    
    // Note: project(), math(), order(), group(), groupCount() 
    // return builders that need configuration before use
}
```

---

## Appendix A: PathValue Hash Implementation

For `SimplePathStep` and `CyclicPathStep`, `PathValue` needs to be hashable:

```rust
impl std::hash::Hash for PathValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            PathValue::Vertex(id) => {
                0u8.hash(state);
                id.0.hash(state);
            }
            PathValue::Edge(id) => {
                1u8.hash(state);
                id.0.hash(state);
            }
            PathValue::Property(value) => {
                2u8.hash(state);
                // Hash based on value type and content
                value.hash_value(state);
            }
        }
    }
}

impl Value {
    fn hash_value<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Boolean(b) => { 1u8.hash(state); b.hash(state); }
            Value::Integer(n) => { 2u8.hash(state); n.hash(state); }
            Value::Float(f) => { 3u8.hash(state); f.to_bits().hash(state); }
            Value::String(s) => { 4u8.hash(state); s.hash(state); }
            Value::Vertex(id) => { 5u8.hash(state); id.0.hash(state); }
            Value::Edge(id) => { 6u8.hash(state); id.0.hash(state); }
            Value::List(items) => {
                7u8.hash(state);
                items.len().hash(state);
                for item in items {
                    item.hash_value(state);
                }
            }
            Value::Map(map) => {
                8u8.hash(state);
                // Note: HashMap iteration order is not deterministic
                // For proper hashing, sort keys first
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for key in keys {
                    key.hash(state);
                    map.get(key).unwrap().hash_value(state);
                }
            }
        }
    }
}
```

---

## Appendix B: Gremlin Compatibility Notes

### Differences from TinkerPop Gremlin

1. **Method naming**: Rust keywords require trailing underscore (`is_`, `as_`, `in_`)
2. **`is()` split**: We use `is_eq()` for values and `is_()` for predicates
3. **Builder pattern**: Steps like `project()`, `order()`, `group()` use builders instead of chainable `by()` calls
4. **Type safety**: Return types are `Traversal<In, Value>` instead of untyped
5. **No multi-properties**: Our `valueMap()` wraps in lists for compatibility but single values are common

### Full Gremlin Compatibility Checklist

After this phase, the following Gremlin steps will be supported:

- [x] V(), E() - Source steps
- [x] out(), in(), both() - Vertex navigation
- [x] outE(), inE(), bothE() - Edge navigation  
- [x] outV(), inV(), bothV() - Edge to vertex
- [x] otherV() - **NEW**
- [x] hasLabel(), has(), hasId() - Basic filters
- [x] hasNot() - **NEW**
- [x] is() - **NEW**
- [x] dedup(), limit(), skip(), range() - Limiting filters
- [x] simplePath(), cyclicPath() - **NEW**
- [x] where(), not(), and(), or() - Logical filters
- [x] values(), id(), label() - Property access
- [x] properties(), valueMap(), elementMap() - **NEW**
- [x] map(), flatMap(), constant() - Transforms
- [x] unfold() - **NEW**
- [x] path(), as(), select() - Path operations
- [x] project() - **NEW**
- [x] math() - **NEW**
- [x] order() - **NEW**
- [x] mean() - **NEW**
- [x] group(), groupCount() - **NEW**
- [x] union(), coalesce(), choose(), optional(), local() - Branching
- [x] repeat() with times/until/emit - Looping
- [x] count(), fold(), toList(), etc. - Terminal steps
