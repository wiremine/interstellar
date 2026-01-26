# Spec 45: wasm-bindgen JavaScript API

This specification defines the JavaScript/TypeScript API for Interstellar via `wasm-bindgen`, enabling use in browsers and Node.js environments.

**Prerequisite**: Spec 44 (WASM Support) must be implemented first.

---

## 1. Overview

### 1.1 Motivation

Spec 44 enables Interstellar to compile to WebAssembly, but produces a raw `.wasm` binary without JavaScript bindings. To make Interstellar usable from JavaScript/TypeScript, we need:

1. **JavaScript-friendly API** - Idiomatic method names and types for JS developers
2. **TypeScript support** - Full type definitions for IDE autocomplete and type checking
3. **Method chaining** - Fluent traversal API that mirrors the Rust Gremlin-style API
4. **npm distribution** - Easy installation via `npm install interstellar-graph`

```
+------------------------------------------------------------------+
|              Current: Raw WASM Binary                             |
+------------------------------------------------------------------+
|                                                                   |
|   cargo build --target wasm32-unknown-unknown                     |
|   Output: interstellar.wasm (no JS bindings)                      |
|                                                                   |
|   Issues:                                                         |
|   - Manual WebAssembly.instantiate() required                     |
|   - No TypeScript types                                           |
|   - Raw memory management                                         |
|   - No method chaining                                            |
|                                                                   |
+------------------------------------------------------------------+

+------------------------------------------------------------------+
|              Proposed: wasm-bindgen + npm Package                 |
+------------------------------------------------------------------+
|                                                                   |
|   wasm-pack build --target web                                    |
|   Output: pkg/                                                    |
|     interstellar_graph.js      (JS glue code)                     |
|     interstellar_graph_bg.wasm (optimized WASM)                   |
|     interstellar_graph.d.ts    (TypeScript definitions)           |
|     package.json               (npm metadata)                     |
|                                                                   |
|   Usage:                                                          |
|   import { Graph } from 'interstellar-graph';                     |
|   const g = new Graph();                                          |
|   g.addVertex('person', { name: 'Alice' });                       |
|   const names = g.V().hasLabel('person').values('name').toList(); |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.2 Scope

This specification covers:

- `wasm-bindgen` facade types: `Graph`, `Traversal`, `Vertex`, `Edge`
- Full traversal API with method chaining
- Predicate system (`P.eq()`, `P.gt()`, etc.)
- Anonymous traversal factory (`__`)
- npm package configuration and publishing
- TypeScript type definitions

### 1.3 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Closure-based methods | `filter()`, `map()`, `flatMap()` require `js_sys::Function` - future enhancement |
| Streaming/async iteration | Complex lifetime management - future enhancement |
| Web Workers integration | Separate concern - future spec |
| React/Vue bindings | Application-level concern - left to users |
| GraphSON file I/O | Not available on WASM (per Spec 44) |

### 1.4 Design Principles

| Principle | Description |
|-----------|-------------|
| **Idiomatic JavaScript** | Use camelCase, return JS arrays/objects, throw JS errors |
| **Full type safety** | Complete TypeScript definitions with no `any` types |
| **Mirror Rust API** | Same method names and semantics as Rust traversal API |
| **Zero runtime cost** | Feature-gated, no overhead when not using WASM |
| **Lazy evaluation** | Traversals execute on terminal steps, not intermediate steps |

---

## 2. Architecture

### 2.1 Facade Pattern

The WASM API uses facade types that wrap internal Rust types. This solves several problems:

1. **Lifetime erasure** - WASM types must be `'static`; facades own their data via `Arc`
2. **API surface control** - Only expose what's needed, with JS-friendly names
3. **Type conversion** - Handle `Value` ↔ `JsValue` at boundaries

```
+------------------------------------------------------------------+
|                         JavaScript                                |
+------------------------------------------------------------------+
|   const g = new Graph();                                          |
|   g.addVertex('person', { name: 'Alice' });                       |
|   g.V().hasLabel('person').out('knows').values('name').toList();  |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|                    WASM Facade Layer                              |
+------------------------------------------------------------------+
|   #[wasm_bindgen]                                                 |
|   pub struct Graph { inner: crate::Graph }                        |
|                                                                   |
|   #[wasm_bindgen]                                                 |
|   pub struct Traversal {                                          |
|       snapshot: Arc<GraphSnapshot>,                               |
|       steps: Vec<Box<dyn DynStep>>,                               |
|   }                                                               |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
|                    Rust Core Library                              |
+------------------------------------------------------------------+
|   crate::Graph, crate::GraphSnapshot                              |
|   crate::traversal::BoundTraversal                                |
|   crate::value::Value                                             |
+------------------------------------------------------------------+
```

### 2.2 Traversal Lifetime Solution

Rust's `BoundTraversal<'g, In, Out>` has a lifetime parameter tied to the graph snapshot. WASM types cannot have lifetimes. Solution:

```rust
// Internal: Rust traversal with lifetime
pub struct BoundTraversal<'g, In, Out> {
    source: &'g GraphSnapshot,
    // ...
}

// WASM facade: Owns snapshot via Arc, accumulates steps
#[wasm_bindgen]
pub struct Traversal {
    snapshot: Arc<GraphSnapshot>,
    steps: Vec<Box<dyn DynStep>>,
    current_type: TraversalType, // Vertex, Edge, or Value
}
```

Each chainable method:
1. Clones `self` (Traversal is `Clone`)
2. Appends a boxed step to `steps`
3. Returns the new `Traversal`

Terminal methods (`toList()`, `count()`, etc.) execute all accumulated steps.

### 2.3 Type Conversion Strategy

| Rust Type | JavaScript Type | Conversion |
|-----------|-----------------|------------|
| `VertexId(u64)` | `bigint` | Direct via wasm-bindgen |
| `EdgeId(u64)` | `bigint` | Direct via wasm-bindgen |
| `Value::Null` | `null` | serde-wasm-bindgen |
| `Value::Bool` | `boolean` | serde-wasm-bindgen |
| `Value::Int` | `bigint` | serde-wasm-bindgen |
| `Value::Float` | `number` | serde-wasm-bindgen |
| `Value::String` | `string` | serde-wasm-bindgen |
| `Value::List` | `Array` | serde-wasm-bindgen |
| `Value::Map` | `Object` | serde-wasm-bindgen |
| `Value::Vertex` | `Vertex` object | Custom conversion |
| `Value::Edge` | `Edge` object | Custom conversion |
| `HashMap<String, Value>` | `Record<string, any>` | serde-wasm-bindgen |
| `Result<T, E>` | `T` (throws on error) | JsError conversion |
| `Option<T>` | `T \| undefined` | wasm-bindgen handles |

### 2.4 Module Structure

```
src/
├── lib.rs                    # Add: pub mod wasm (feature-gated)
└── wasm/
    ├── mod.rs                # Module exports, feature gate
    ├── graph.rs              # Graph facade
    ├── traversal.rs          # Traversal facade + all steps
    ├── types.rs              # Vertex, Edge, Value conversion
    ├── predicate.rs          # P namespace (predicate factory)
    └── anonymous.rs          # __ namespace (anonymous traversals)
```

---

## 3. Cargo.toml Changes

### 3.1 Feature Flag

```toml
[features]
default = ["graphson"]
graphson = ["serde_json"]
gql = []
mmap = ["memmap2"]
full-text = ["tantivy"]
full = ["graphson", "gql", "mmap", "full-text"]

# NEW: WASM JavaScript bindings
wasm = ["wasm-bindgen", "serde-wasm-bindgen", "js-sys"]
```

### 3.2 Dependencies

```toml
[dependencies]
# ... existing dependencies ...

# WASM bindings (optional)
wasm-bindgen = { version = "0.2", optional = true }
serde-wasm-bindgen = { version = "0.6", optional = true }
js-sys = { version = "0.3", optional = true }

[dev-dependencies]
# ... existing dev dependencies ...
wasm-bindgen-test = "0.3"
```

### 3.3 Package Metadata for wasm-pack

```toml
[package.metadata.wasm-pack.profile.release]
wasm-opt = ["-Oz"]  # Optimize for size

[lib]
crate-type = ["cdylib", "rlib"]  # cdylib required for WASM
```

---

## 4. Core Type Exports

### 4.1 Graph

The main entry point for creating and manipulating graphs.

```typescript
/**
 * An in-memory property graph database.
 * 
 * @example
 * ```typescript
 * const graph = new Graph();
 * const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
 * const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
 * graph.addEdge(alice, bob, 'knows', { since: 2020n });
 * ```
 */
declare class Graph {
    /**
     * Create a new empty graph.
     */
    constructor();

    /**
     * Create a graph with a predefined schema.
     * @param schema - JSON schema definition
     */
    static withSchema(schema: SchemaDefinition): Graph;

    // --- Vertex Operations ---

    /**
     * Add a vertex with a label and properties.
     * @param label - The vertex label (e.g., 'person', 'product')
     * @param properties - Key-value properties
     * @returns The new vertex's ID
     */
    addVertex(label: string, properties?: Record<string, Value>): bigint;

    /**
     * Get a vertex by ID.
     * @param id - The vertex ID
     * @returns The vertex, or undefined if not found
     */
    getVertex(id: bigint): Vertex | undefined;

    /**
     * Remove a vertex and all its edges.
     * @param id - The vertex ID to remove
     * @returns true if removed, false if not found
     */
    removeVertex(id: bigint): boolean;

    /**
     * Set a property on a vertex.
     * @param id - The vertex ID
     * @param key - Property name
     * @param value - Property value
     * @throws If vertex not found
     */
    setVertexProperty(id: bigint, key: string, value: Value): void;

    /**
     * Remove a property from a vertex.
     * @param id - The vertex ID
     * @param key - Property name to remove
     * @returns true if removed, false if property didn't exist
     */
    removeVertexProperty(id: bigint, key: string): boolean;

    // --- Edge Operations ---

    /**
     * Add an edge between two vertices.
     * @param from - Source vertex ID
     * @param to - Target vertex ID
     * @param label - The edge label (e.g., 'knows', 'purchased')
     * @param properties - Key-value properties
     * @returns The new edge's ID
     * @throws If source or target vertex not found
     */
    addEdge(from: bigint, to: bigint, label: string, properties?: Record<string, Value>): bigint;

    /**
     * Get an edge by ID.
     * @param id - The edge ID
     * @returns The edge, or undefined if not found
     */
    getEdge(id: bigint): Edge | undefined;

    /**
     * Remove an edge.
     * @param id - The edge ID to remove
     * @returns true if removed, false if not found
     */
    removeEdge(id: bigint): boolean;

    /**
     * Set a property on an edge.
     * @param id - The edge ID
     * @param key - Property name
     * @param value - Property value
     * @throws If edge not found
     */
    setEdgeProperty(id: bigint, key: string, value: Value): void;

    /**
     * Remove a property from an edge.
     * @param id - The edge ID
     * @param key - Property name to remove
     * @returns true if removed, false if property didn't exist
     */
    removeEdgeProperty(id: bigint, key: string): boolean;

    // --- Graph Statistics ---

    /**
     * Get the total number of vertices.
     */
    vertexCount(): bigint;

    /**
     * Get the total number of edges.
     */
    edgeCount(): bigint;

    // --- Traversal ---

    /**
     * Start a new graph traversal.
     * @returns A traversal source for building queries
     * 
     * @example
     * ```typescript
     * graph.V().hasLabel('person').out('knows').values('name').toList();
     * ```
     */
    V(): Traversal;

    /**
     * Start a traversal from specific vertex IDs.
     * @param ids - Vertex IDs to start from
     */
    V_(...ids: bigint[]): Traversal;

    /**
     * Start a traversal over all edges.
     */
    E(): Traversal;

    /**
     * Start a traversal from specific edge IDs.
     * @param ids - Edge IDs to start from
     */
    E_(...ids: bigint[]): Traversal;

    /**
     * Inject values into a traversal.
     * @param values - Values to inject
     */
    inject(...values: Value[]): Traversal;

    /**
     * Start an addVertex traversal.
     * @param label - The vertex label
     */
    addV(label: string): Traversal;

    /**
     * Start an addEdge traversal.
     * @param label - The edge label
     */
    addE(label: string): Traversal;

    // --- Query Language ---

    /**
     * Execute a GQL query string.
     * @param query - GQL query string
     * @returns Query results as an array
     * @throws If query parsing or execution fails
     * 
     * @example
     * ```typescript
     * const results = graph.gql(`
     *     MATCH (p:person)-[:knows]->(friend)
     *     WHERE p.name = 'Alice'
     *     RETURN friend.name
     * `);
     * ```
     */
    gql(query: string): Value[];

    // --- Serialization ---

    /**
     * Export the graph to a GraphSON JSON string.
     * @returns GraphSON 3.0 formatted JSON string
     */
    toGraphSON(): string;

    /**
     * Import graph data from a GraphSON JSON string.
     * @param json - GraphSON 3.0 formatted JSON string
     * @returns Import statistics
     */
    fromGraphSON(json: string): GraphSONImportResult;

    /**
     * Clear all vertices and edges from the graph.
     */
    clear(): void;
}
```

### 4.2 Vertex

```typescript
/**
 * A vertex (node) in the graph.
 */
interface Vertex {
    /** Unique vertex identifier */
    readonly id: bigint;
    /** Vertex label (e.g., 'person', 'product') */
    readonly label: string;
    /** Vertex properties */
    readonly properties: Record<string, Value>;
}
```

### 4.3 Edge

```typescript
/**
 * An edge (relationship) between two vertices.
 */
interface Edge {
    /** Unique edge identifier */
    readonly id: bigint;
    /** Edge label (e.g., 'knows', 'purchased') */
    readonly label: string;
    /** Source vertex ID */
    readonly from: bigint;
    /** Target vertex ID */
    readonly to: bigint;
    /** Edge properties */
    readonly properties: Record<string, Value>;
}
```

### 4.4 Value

```typescript
/**
 * A property value type.
 * 
 * Note: Integers use `bigint` for 64-bit precision.
 */
type Value = null | boolean | bigint | number | string | Value[] | Record<string, Value>;
```

### 4.5 Supporting Types

```typescript
/**
 * Schema definition for typed graphs.
 */
interface SchemaDefinition {
    vertexLabels?: VertexLabelSchema[];
    edgeLabels?: EdgeLabelSchema[];
}

interface VertexLabelSchema {
    name: string;
    properties?: PropertySchema[];
}

interface EdgeLabelSchema {
    name: string;
    from: string;
    to: string;
    properties?: PropertySchema[];
}

interface PropertySchema {
    name: string;
    type: 'null' | 'bool' | 'int' | 'float' | 'string' | 'list' | 'map';
    required?: boolean;
}

/**
 * Result of a GraphSON import operation.
 */
interface GraphSONImportResult {
    verticesImported: bigint;
    edgesImported: bigint;
    warnings: string[];
}
```

---

## 5. Traversal API

### 5.1 Overview

The `Traversal` class provides a fluent, chainable API for querying the graph. Each method returns a new `Traversal` instance, enabling method chaining:

```typescript
graph.V()                        // Start: all vertices
    .hasLabel('person')          // Filter: only 'person' vertices
    .has('age', P.gte(18n))      // Filter: age >= 18
    .out('knows')                // Navigate: outgoing 'knows' edges
    .dedup()                     // Filter: remove duplicates
    .values('name')              // Transform: extract 'name' property
    .limit(10)                   // Filter: first 10 results
    .toList();                   // Terminal: execute and return array
```

### 5.2 Traversal Class

```typescript
/**
 * A graph traversal that can be chained with various steps.
 * 
 * Traversals are lazy - they only execute when a terminal step is called.
 */
declare class Traversal {
    // --- Source Steps (also on Graph) ---

    /**
     * Start from all vertices.
     */
    V(): Traversal;

    /**
     * Start from specific vertex IDs.
     */
    V_(...ids: bigint[]): Traversal;

    /**
     * Start from all edges.
     */
    E(): Traversal;

    /**
     * Start from specific edge IDs.
     */
    E_(...ids: bigint[]): Traversal;

    /**
     * Inject values into the traversal stream.
     */
    inject(...values: Value[]): Traversal;

    /**
     * Add a vertex with a label.
     */
    addV(label: string): Traversal;

    /**
     * Add an edge with a label (use from()/to() to specify endpoints).
     */
    addE(label: string): Traversal;

    // ------------------------------------------------------------------
    // FILTER STEPS
    // ------------------------------------------------------------------

    /**
     * Filter to elements with a specific label.
     * @param label - The label to match
     */
    hasLabel(label: string): Traversal;

    /**
     * Filter to elements with any of the specified labels.
     * @param labels - Labels to match (OR logic)
     */
    hasLabelAny(...labels: string[]): Traversal;

    /**
     * Filter to elements that have a property (any value).
     * @param key - Property name
     */
    has(key: string): Traversal;

    /**
     * Filter to elements that have a property with a specific value.
     * @param key - Property name
     * @param value - Exact value to match
     */
    hasValue(key: string, value: Value): Traversal;

    /**
     * Filter to elements where property matches a predicate.
     * @param key - Property name
     * @param predicate - Predicate to test (e.g., P.gt(10n))
     */
    hasWhere(key: string, predicate: Predicate): Traversal;

    /**
     * Filter to elements that do NOT have a property.
     * @param key - Property name that must be absent
     */
    hasNot(key: string): Traversal;

    /**
     * Filter to elements with a specific ID.
     * @param id - The element ID
     */
    hasId(id: bigint): Traversal;

    /**
     * Filter to elements with any of the specified IDs.
     * @param ids - Element IDs to match
     */
    hasIds(...ids: bigint[]): Traversal;

    /**
     * Filter values matching a predicate.
     * @param predicate - Predicate to test
     */
    is(predicate: Predicate): Traversal;

    /**
     * Filter values equal to a specific value.
     * @param value - Value to match
     */
    isEq(value: Value): Traversal;

    /**
     * Remove duplicate elements from the traversal.
     */
    dedup(): Traversal;

    /**
     * Remove duplicates based on a property key.
     * @param key - Property to deduplicate by
     */
    dedupByKey(key: string): Traversal;

    /**
     * Remove duplicates based on element label.
     */
    dedupByLabel(): Traversal;

    /**
     * Remove duplicates based on the result of a traversal.
     * @param traversal - Anonymous traversal to compute dedup key
     */
    dedupBy(traversal: Traversal): Traversal;

    /**
     * Limit results to the first n elements.
     * @param n - Maximum number of elements
     */
    limit(n: bigint): Traversal;

    /**
     * Skip the first n elements.
     * @param n - Number of elements to skip
     */
    skip(n: bigint): Traversal;

    /**
     * Take elements in a range [start, end).
     * @param start - Start index (inclusive)
     * @param end - End index (exclusive)
     */
    range(start: bigint, end: bigint): Traversal;

    /**
     * Get the last element.
     */
    tail(): Traversal;

    /**
     * Get the last n elements.
     * @param n - Number of elements from end
     */
    tailN(n: bigint): Traversal;

    /**
     * Randomly filter elements with a given probability.
     * @param probability - Probability (0.0 to 1.0) of keeping each element
     */
    coin(probability: number): Traversal;

    /**
     * Randomly sample n elements.
     * @param n - Number of elements to sample
     */
    sample(n: bigint): Traversal;

    /**
     * Filter to paths that don't repeat vertices.
     */
    simplePath(): Traversal;

    /**
     * Filter to paths that do repeat vertices.
     */
    cyclicPath(): Traversal;

    /**
     * Filter based on the result of a traversal (must produce results).
     * @param traversal - Anonymous traversal to test
     */
    where(traversal: Traversal): Traversal;

    /**
     * Filter to elements where the traversal produces NO results.
     * @param traversal - Anonymous traversal that must be empty
     */
    not(traversal: Traversal): Traversal;

    /**
     * Filter where ALL traversals produce results.
     * @param traversals - Anonymous traversals (AND logic)
     */
    and(...traversals: Traversal[]): Traversal;

    /**
     * Filter where ANY traversal produces results.
     * @param traversals - Anonymous traversals (OR logic)
     */
    or(...traversals: Traversal[]): Traversal;

    // ------------------------------------------------------------------
    // NAVIGATION STEPS
    // ------------------------------------------------------------------

    /**
     * Navigate to outgoing adjacent vertices (via all edge labels).
     */
    out(): Traversal;

    /**
     * Navigate to outgoing adjacent vertices via specific edge labels.
     * @param labels - Edge labels to traverse
     */
    outLabels(...labels: string[]): Traversal;

    /**
     * Navigate to incoming adjacent vertices (via all edge labels).
     */
    in_(): Traversal;

    /**
     * Navigate to incoming adjacent vertices via specific edge labels.
     * @param labels - Edge labels to traverse
     */
    inLabels(...labels: string[]): Traversal;

    /**
     * Navigate to adjacent vertices in both directions.
     */
    both(): Traversal;

    /**
     * Navigate to adjacent vertices in both directions via specific labels.
     * @param labels - Edge labels to traverse
     */
    bothLabels(...labels: string[]): Traversal;

    /**
     * Navigate to outgoing edges.
     */
    outE(): Traversal;

    /**
     * Navigate to outgoing edges with specific labels.
     * @param labels - Edge labels to match
     */
    outELabels(...labels: string[]): Traversal;

    /**
     * Navigate to incoming edges.
     */
    inE(): Traversal;

    /**
     * Navigate to incoming edges with specific labels.
     * @param labels - Edge labels to match
     */
    inELabels(...labels: string[]): Traversal;

    /**
     * Navigate to edges in both directions.
     */
    bothE(): Traversal;

    /**
     * Navigate to edges in both directions with specific labels.
     * @param labels - Edge labels to match
     */
    bothELabels(...labels: string[]): Traversal;

    /**
     * Navigate from an edge to its outgoing (source) vertex.
     */
    outV(): Traversal;

    /**
     * Navigate from an edge to its incoming (target) vertex.
     */
    inV(): Traversal;

    /**
     * Navigate from an edge to both endpoints.
     */
    bothV(): Traversal;

    /**
     * Navigate from an edge to the vertex that was NOT the previous step.
     */
    otherV(): Traversal;

    // ------------------------------------------------------------------
    // TRANSFORM STEPS
    // ------------------------------------------------------------------

    /**
     * Extract a single property value.
     * @param key - Property name
     */
    values(key: string): Traversal;

    /**
     * Extract multiple property values (as a list per element).
     * @param keys - Property names
     */
    valuesMulti(...keys: string[]): Traversal;

    /**
     * Get all properties as Property objects.
     */
    properties(): Traversal;

    /**
     * Get specific properties as Property objects.
     * @param keys - Property names
     */
    propertiesKeys(...keys: string[]): Traversal;

    /**
     * Get a map of property name to value.
     */
    valueMap(): Traversal;

    /**
     * Get a map of specific property names to values.
     * @param keys - Property names to include
     */
    valueMapKeys(...keys: string[]): Traversal;

    /**
     * Get a value map including id and label tokens.
     */
    valueMapWithTokens(): Traversal;

    /**
     * Get a complete element map (id, label, and all properties).
     */
    elementMap(): Traversal;

    /**
     * Get an element map with specific property keys.
     * @param keys - Property names to include
     */
    elementMapKeys(...keys: string[]): Traversal;

    /**
     * Get a map of property name to Property objects.
     */
    propertyMap(): Traversal;

    /**
     * Get a property map with specific keys.
     * @param keys - Property names to include
     */
    propertyMapKeys(...keys: string[]): Traversal;

    /**
     * Extract the element ID.
     */
    id(): Traversal;

    /**
     * Extract the element label.
     */
    label(): Traversal;

    /**
     * Replace each element with a constant value.
     * @param value - Constant value to emit
     */
    constant(value: Value): Traversal;

    /**
     * Flatten lists/iterables in the stream.
     */
    unfold(): Traversal;

    /**
     * Collect all elements into a single list.
     */
    fold(): Traversal;

    /**
     * Get the traversal path (history of elements visited).
     */
    path(): Traversal;

    /**
     * Label the current step for later reference.
     * @param label - Step label
     */
    as(label: string): Traversal;

    /**
     * Select labeled steps from the path.
     * @param labels - Step labels to select
     */
    select(...labels: string[]): Traversal;

    /**
     * Select a single labeled step from the path.
     * @param label - Step label
     */
    selectOne(label: string): Traversal;

    /**
     * Calculate the arithmetic mean of numeric values.
     */
    mean(): Traversal;

    /**
     * Calculate the sum of numeric values.
     */
    sum(): Traversal;

    /**
     * Get the minimum value.
     */
    min(): Traversal;

    /**
     * Get the maximum value.
     */
    max(): Traversal;

    /**
     * Count the number of elements.
     */
    count(): Traversal;

    // ------------------------------------------------------------------
    // ORDER STEP (Builder Pattern)
    // ------------------------------------------------------------------

    /**
     * Start an order operation.
     * @returns OrderBuilder for specifying sort criteria
     */
    order(): OrderBuilder;

    // ------------------------------------------------------------------
    // PROJECT STEP (Builder Pattern)
    // ------------------------------------------------------------------

    /**
     * Project each element into a map with named keys.
     * @param keys - Output keys
     * @returns ProjectBuilder for specifying projections
     */
    project(...keys: string[]): ProjectBuilder;

    // ------------------------------------------------------------------
    // GROUP STEPS (Builder Pattern)
    // ------------------------------------------------------------------

    /**
     * Group elements into a map.
     * @returns GroupBuilder for specifying key and value
     */
    group(): GroupBuilder;

    /**
     * Count elements by group.
     * @returns GroupCountBuilder for specifying key
     */
    groupCount(): GroupCountBuilder;

    // ------------------------------------------------------------------
    // BRANCH STEPS
    // ------------------------------------------------------------------

    /**
     * Execute multiple traversals and combine results.
     * @param traversals - Traversals to execute in parallel
     */
    union(...traversals: Traversal[]): Traversal;

    /**
     * Return the result of the first traversal that produces output.
     * @param traversals - Traversals to try in order
     */
    coalesce(...traversals: Traversal[]): Traversal;

    /**
     * Conditional branching.
     * @param condition - Predicate or traversal to test
     * @param ifTrue - Traversal if condition is true
     * @param ifFalse - Traversal if condition is false
     */
    choose(condition: Predicate | Traversal, ifTrue: Traversal, ifFalse?: Traversal): Traversal;

    /**
     * Execute traversal, but pass through original if no results.
     * @param traversal - Optional traversal
     */
    optional(traversal: Traversal): Traversal;

    /**
     * Execute traversal in local scope (per element).
     * @param traversal - Traversal to execute locally
     */
    local(traversal: Traversal): Traversal;

    // ------------------------------------------------------------------
    // REPEAT STEP (Builder Pattern)
    // ------------------------------------------------------------------

    /**
     * Start a repeat loop.
     * @param traversal - Traversal to repeat
     * @returns RepeatBuilder for specifying termination
     */
    repeat(traversal: Traversal): RepeatBuilder;

    // ------------------------------------------------------------------
    // MUTATION STEPS
    // ------------------------------------------------------------------

    /**
     * Set a property on the current element.
     * @param key - Property name
     * @param value - Property value
     */
    property(key: string, value: Value): Traversal;

    /**
     * Set the source vertex for an addE() traversal.
     * @param label - Step label of source vertex
     */
    from(label: string): Traversal;

    /**
     * Set the source vertex for an addE() traversal by ID.
     * @param id - Source vertex ID
     */
    fromId(id: bigint): Traversal;

    /**
     * Set the target vertex for an addE() traversal.
     * @param label - Step label of target vertex
     */
    to(label: string): Traversal;

    /**
     * Set the target vertex for an addE() traversal by ID.
     * @param id - Target vertex ID
     */
    toId(id: bigint): Traversal;

    /**
     * Remove the current element from the graph.
     */
    drop(): Traversal;

    // ------------------------------------------------------------------
    // TERMINAL STEPS
    // ------------------------------------------------------------------

    /**
     * Execute the traversal and return all results as an array.
     * @returns Array of results
     */
    toList(): Value[];

    /**
     * Execute and return the first result, or undefined.
     * @returns First result or undefined
     */
    first(): Value | undefined;

    /**
     * Execute and return exactly one result.
     * @throws If zero or more than one result
     */
    one(): Value;

    /**
     * Execute and return the next result (for iteration).
     * @returns Next result or undefined
     */
    next(): Value | undefined;

    /**
     * Check if the traversal has any results.
     * @returns true if at least one result exists
     */
    hasNext(): boolean;

    /**
     * Execute and return the count of results.
     * @returns Number of results
     */
    toCount(): bigint;

    /**
     * Iterate through all results (for side effects).
     */
    iterate(): void;
}
```

### 5.3 Order Builder

```typescript
/**
 * Builder for order() step configuration.
 */
declare class OrderBuilder {
    /**
     * Order by natural value (ascending).
     */
    byAsc(): OrderBuilder;

    /**
     * Order by natural value (descending).
     */
    byDesc(): OrderBuilder;

    /**
     * Order by a property key (ascending).
     * @param key - Property name
     */
    byKeyAsc(key: string): OrderBuilder;

    /**
     * Order by a property key (descending).
     * @param key - Property name
     */
    byKeyDesc(key: string): OrderBuilder;

    /**
     * Order by the result of a traversal (ascending).
     * @param traversal - Anonymous traversal
     */
    byTraversalAsc(traversal: Traversal): OrderBuilder;

    /**
     * Order by the result of a traversal (descending).
     * @param traversal - Anonymous traversal
     */
    byTraversalDesc(traversal: Traversal): OrderBuilder;

    /**
     * Finalize the order step and return to traversal.
     */
    build(): Traversal;
}
```

### 5.4 Project Builder

```typescript
/**
 * Builder for project() step configuration.
 */
declare class ProjectBuilder {
    /**
     * Project a key using a property value.
     * @param key - Output key (must match one in project())
     * @param propertyKey - Property to extract
     */
    byKey(key: string, propertyKey: string): ProjectBuilder;

    /**
     * Project a key using a traversal result.
     * @param key - Output key
     * @param traversal - Anonymous traversal
     */
    byTraversal(key: string, traversal: Traversal): ProjectBuilder;

    /**
     * Project a key using the element ID.
     * @param key - Output key
     */
    byId(key: string): ProjectBuilder;

    /**
     * Project a key using the element label.
     * @param key - Output key
     */
    byLabel(key: string): ProjectBuilder;

    /**
     * Finalize the project step and return to traversal.
     */
    build(): Traversal;
}
```

### 5.5 Group Builder

```typescript
/**
 * Builder for group() step configuration.
 */
declare class GroupBuilder {
    /**
     * Group by element label.
     */
    byLabel(): GroupBuilder;

    /**
     * Group by a property key.
     * @param key - Property name
     */
    byKey(key: string): GroupBuilder;

    /**
     * Group by the result of a traversal.
     * @param traversal - Anonymous traversal
     */
    byTraversal(traversal: Traversal): GroupBuilder;

    /**
     * Aggregate values using a traversal.
     * @param traversal - Anonymous traversal for values
     */
    valuesByTraversal(traversal: Traversal): GroupBuilder;

    /**
     * Aggregate values using fold (collect into list).
     */
    valuesFold(): GroupBuilder;

    /**
     * Aggregate values using count.
     */
    valuesCount(): GroupBuilder;

    /**
     * Finalize the group step and return to traversal.
     */
    build(): Traversal;
}

/**
 * Builder for groupCount() step configuration.
 */
declare class GroupCountBuilder {
    /**
     * Count by element label.
     */
    byLabel(): GroupCountBuilder;

    /**
     * Count by a property key.
     * @param key - Property name
     */
    byKey(key: string): GroupCountBuilder;

    /**
     * Count by the result of a traversal.
     * @param traversal - Anonymous traversal
     */
    byTraversal(traversal: Traversal): GroupCountBuilder;

    /**
     * Finalize the groupCount step and return to traversal.
     */
    build(): Traversal;
}
```

### 5.6 Repeat Builder

```typescript
/**
 * Builder for repeat() step configuration.
 */
declare class RepeatBuilder {
    /**
     * Repeat a fixed number of times.
     * @param n - Number of iterations
     */
    times(n: bigint): Traversal;

    /**
     * Repeat until a condition is met.
     * @param condition - Anonymous traversal or predicate
     */
    until(condition: Traversal | Predicate): RepeatBuilder;

    /**
     * Emit elements during iteration.
     */
    emit(): RepeatBuilder;

    /**
     * Emit elements that match a condition.
     * @param condition - Anonymous traversal or predicate
     */
    emitIf(condition: Traversal | Predicate): RepeatBuilder;

    /**
     * Finalize the repeat step and return to traversal.
     */
    build(): Traversal;
}
```

---

## 6. Predicate System

### 6.1 Overview

Predicates are used with `hasWhere()`, `is()`, and other filter steps. The `P` namespace provides factory functions:

```typescript
graph.V()
    .hasWhere('age', P.gte(18n))
    .hasWhere('name', P.startingWith('A'))
    .toList();
```

### 6.2 Predicate Interface

```typescript
/**
 * A predicate for filtering values.
 * 
 * Predicates are created via the P namespace factory functions.
 */
interface Predicate {
    readonly _type: 'predicate';
}
```

### 6.3 P Namespace

```typescript
/**
 * Predicate factory functions.
 */
declare namespace P {
    // --- Comparison ---

    /**
     * Equals comparison.
     * @param value - Value to compare against
     */
    function eq(value: Value): Predicate;

    /**
     * Not equals comparison.
     * @param value - Value to compare against
     */
    function neq(value: Value): Predicate;

    /**
     * Less than comparison.
     * @param value - Value to compare against
     */
    function lt(value: Value): Predicate;

    /**
     * Less than or equal comparison.
     * @param value - Value to compare against
     */
    function lte(value: Value): Predicate;

    /**
     * Greater than comparison.
     * @param value - Value to compare against
     */
    function gt(value: Value): Predicate;

    /**
     * Greater than or equal comparison.
     * @param value - Value to compare against
     */
    function gte(value: Value): Predicate;

    // --- Range ---

    /**
     * Value is between start and end (inclusive).
     * @param start - Range start
     * @param end - Range end
     */
    function between(start: Value, end: Value): Predicate;

    /**
     * Value is strictly inside range (exclusive).
     * @param start - Range start
     * @param end - Range end
     */
    function inside(start: Value, end: Value): Predicate;

    /**
     * Value is outside range.
     * @param start - Range start
     * @param end - Range end
     */
    function outside(start: Value, end: Value): Predicate;

    // --- Collection ---

    /**
     * Value is within the given set.
     * @param values - Values to check membership
     */
    function within(...values: Value[]): Predicate;

    /**
     * Value is NOT within the given set.
     * @param values - Values to exclude
     */
    function without(...values: Value[]): Predicate;

    // --- String ---

    /**
     * String contains substring.
     * @param substring - Substring to find
     */
    function containing(substring: string): Predicate;

    /**
     * String does NOT contain substring.
     * @param substring - Substring that must be absent
     */
    function notContaining(substring: string): Predicate;

    /**
     * String starts with prefix.
     * @param prefix - Required prefix
     */
    function startingWith(prefix: string): Predicate;

    /**
     * String does NOT start with prefix.
     * @param prefix - Forbidden prefix
     */
    function notStartingWith(prefix: string): Predicate;

    /**
     * String ends with suffix.
     * @param suffix - Required suffix
     */
    function endingWith(suffix: string): Predicate;

    /**
     * String does NOT end with suffix.
     * @param suffix - Forbidden suffix
     */
    function notEndingWith(suffix: string): Predicate;

    /**
     * String matches regular expression.
     * @param pattern - Regex pattern
     */
    function regex(pattern: string): Predicate;

    // --- Logical ---

    /**
     * Logical AND of two predicates.
     * @param p1 - First predicate
     * @param p2 - Second predicate
     */
    function and(p1: Predicate, p2: Predicate): Predicate;

    /**
     * Logical OR of two predicates.
     * @param p1 - First predicate
     * @param p2 - Second predicate
     */
    function or(p1: Predicate, p2: Predicate): Predicate;

    /**
     * Logical NOT of a predicate.
     * @param p - Predicate to negate
     */
    function not(p: Predicate): Predicate;
}
```

---

## 7. Anonymous Traversals

### 7.1 Overview

The `__` (double underscore) namespace provides factory functions for creating anonymous traversals. These are used with steps like `where()`, `union()`, `repeat()`, etc.

```typescript
// Find people who know someone older than themselves
graph.V()
    .hasLabel('person')
    .as('person')
    .out('knows')
    .where(__.as('person').values('age').is(P.lt(__.values('age'))))
    .toList();
```

### 7.2 __ Namespace

```typescript
/**
 * Anonymous traversal factory.
 * 
 * Creates traversal fragments for use in branch/filter steps.
 */
declare namespace __ {
    /**
     * Start an anonymous traversal (identity).
     */
    function start(): Traversal;

    /**
     * Reference a labeled step.
     */
    function as(label: string): Traversal;

    // --- Navigation ---
    function out(): Traversal;
    function outLabels(...labels: string[]): Traversal;
    function in_(): Traversal;
    function inLabels(...labels: string[]): Traversal;
    function both(): Traversal;
    function bothLabels(...labels: string[]): Traversal;
    function outE(): Traversal;
    function outELabels(...labels: string[]): Traversal;
    function inE(): Traversal;
    function inELabels(...labels: string[]): Traversal;
    function bothE(): Traversal;
    function bothELabels(...labels: string[]): Traversal;
    function outV(): Traversal;
    function inV(): Traversal;
    function bothV(): Traversal;
    function otherV(): Traversal;

    // --- Filter ---
    function hasLabel(label: string): Traversal;
    function hasLabelAny(...labels: string[]): Traversal;
    function has(key: string): Traversal;
    function hasValue(key: string, value: Value): Traversal;
    function hasWhere(key: string, predicate: Predicate): Traversal;
    function hasNot(key: string): Traversal;
    function hasId(id: bigint): Traversal;
    function hasIds(...ids: bigint[]): Traversal;
    function is(predicate: Predicate): Traversal;
    function isEq(value: Value): Traversal;
    function dedup(): Traversal;
    function dedupByKey(key: string): Traversal;
    function dedupByLabel(): Traversal;
    function limit(n: bigint): Traversal;
    function skip(n: bigint): Traversal;
    function range(start: bigint, end: bigint): Traversal;
    function tail(): Traversal;
    function tailN(n: bigint): Traversal;
    function simplePath(): Traversal;
    function cyclicPath(): Traversal;

    // --- Transform ---
    function values(key: string): Traversal;
    function valuesMulti(...keys: string[]): Traversal;
    function properties(): Traversal;
    function propertiesKeys(...keys: string[]): Traversal;
    function valueMap(): Traversal;
    function valueMapKeys(...keys: string[]): Traversal;
    function valueMapWithTokens(): Traversal;
    function elementMap(): Traversal;
    function elementMapKeys(...keys: string[]): Traversal;
    function propertyMap(): Traversal;
    function propertyMapKeys(...keys: string[]): Traversal;
    function id(): Traversal;
    function label(): Traversal;
    function constant(value: Value): Traversal;
    function unfold(): Traversal;
    function fold(): Traversal;
    function path(): Traversal;
    function select(...labels: string[]): Traversal;
    function selectOne(label: string): Traversal;
    function mean(): Traversal;
    function sum(): Traversal;
    function min(): Traversal;
    function max(): Traversal;
    function count(): Traversal;

    // --- Order/Project/Group (return builders) ---
    function order(): OrderBuilder;
    function project(...keys: string[]): ProjectBuilder;
    function group(): GroupBuilder;
    function groupCount(): GroupCountBuilder;

    // --- Branch ---
    function union(...traversals: Traversal[]): Traversal;
    function coalesce(...traversals: Traversal[]): Traversal;
    function choose(condition: Predicate | Traversal, ifTrue: Traversal, ifFalse?: Traversal): Traversal;
    function optional(traversal: Traversal): Traversal;
    function local(traversal: Traversal): Traversal;
    function repeat(traversal: Traversal): RepeatBuilder;
    function where(traversal: Traversal): Traversal;
    function not(traversal: Traversal): Traversal;
    function and(...traversals: Traversal[]): Traversal;
    function or(...traversals: Traversal[]): Traversal;

    // --- Mutation ---
    function property(key: string, value: Value): Traversal;
    function addV(label: string): Traversal;
    function addE(label: string): Traversal;
    function drop(): Traversal;
}
```

---

## 8. Error Handling

### 8.1 Error Types

All errors are thrown as JavaScript `Error` objects with descriptive messages:

```typescript
try {
    graph.addEdge(999n, 1n, 'knows', {});
} catch (e) {
    // Error: Vertex not found: 999
    console.error(e.message);
}
```

### 8.2 Error Categories

| Rust Error | JavaScript Error Message |
|------------|-------------------------|
| `StorageError::VertexNotFound(id)` | `"Vertex not found: {id}"` |
| `StorageError::EdgeNotFound(id)` | `"Edge not found: {id}"` |
| `StorageError::SchemaViolation(msg)` | `"Schema violation: {msg}"` |
| `TraversalError::NoResults` | `"Traversal returned no results"` |
| `TraversalError::MultipleResults` | `"Traversal returned multiple results, expected one"` |
| `GqlError::ParseError(msg)` | `"GQL parse error: {msg}"` |
| `GqlError::ExecutionError(msg)` | `"GQL execution error: {msg}"` |
| `GraphSONError::ParseError(msg)` | `"GraphSON parse error: {msg}"` |
| Serde errors | `"Serialization error: {msg}"` |

### 8.3 Rust Implementation Pattern

```rust
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
impl Graph {
    pub fn add_edge(
        &self,
        from: u64,
        to: u64,
        label: &str,
        properties: JsValue,
    ) -> Result<u64, JsError> {
        let props = serde_wasm_bindgen::from_value(properties)
            .map_err(|e| JsError::new(&format!("Invalid properties: {}", e)))?;
        
        self.inner
            .add_edge(VertexId(from), VertexId(to), label, props)
            .map(|id| id.0)
            .map_err(|e| JsError::new(&e.to_string()))
    }
}
```

---

## 9. npm Package Configuration

### 9.1 wasm-pack Build Commands

```bash
# Browser (ES modules)
wasm-pack build --target web --features wasm

# Node.js (CommonJS)
wasm-pack build --target nodejs --features wasm

# Bundler (webpack, rollup, etc.)
wasm-pack build --target bundler --features wasm

# All targets for publishing
wasm-pack build --target web --features wasm --out-dir pkg/web
wasm-pack build --target nodejs --features wasm --out-dir pkg/nodejs
wasm-pack build --target bundler --features wasm --out-dir pkg/bundler
```

### 9.2 package.json Template

The generated `package.json` should be customized:

```json
{
  "name": "interstellar-graph",
  "version": "0.1.0",
  "description": "High-performance graph database for JavaScript/TypeScript",
  "repository": {
    "type": "git",
    "url": "https://github.com/your-org/interstellar"
  },
  "keywords": [
    "graph",
    "database",
    "gremlin",
    "traversal",
    "wasm",
    "webassembly"
  ],
  "license": "MIT OR Apache-2.0",
  "main": "interstellar_graph.js",
  "types": "interstellar_graph.d.ts",
  "files": [
    "interstellar_graph_bg.wasm",
    "interstellar_graph.js",
    "interstellar_graph.d.ts"
  ],
  "sideEffects": [
    "./snippets/*"
  ]
}
```

### 9.3 Publishing Workflow

```bash
# 1. Build for bundler target (most common for npm)
wasm-pack build --target bundler --features wasm --release

# 2. Test locally
cd pkg
npm link
cd ../test-project
npm link interstellar-graph

# 3. Publish to npm
cd pkg
npm publish --access public
```

### 9.4 Multi-Target Publishing

For supporting all environments, create a wrapper package:

```
interstellar-graph/
├── package.json
├── index.js           # Entry point, detects environment
├── web/               # wasm-pack --target web output
├── nodejs/            # wasm-pack --target nodejs output
└── bundler/           # wasm-pack --target bundler output
```

**index.js:**
```javascript
// Detect environment and export appropriate module
if (typeof window !== 'undefined') {
    module.exports = require('./web/interstellar_graph.js');
} else if (typeof process !== 'undefined' && process.versions?.node) {
    module.exports = require('./nodejs/interstellar_graph.js');
} else {
    module.exports = require('./bundler/interstellar_graph.js');
}
```

---

## 10. TypeScript Integration

### 10.1 Auto-Generated Definitions

wasm-bindgen automatically generates `.d.ts` files. Use attributes to customize:

```rust
#[wasm_bindgen(typescript_custom_section)]
const TS_TYPES: &'static str = r#"
/**
 * A property value type.
 */
export type Value = null | boolean | bigint | number | string | Value[] | Record<string, Value>;
"#;

#[wasm_bindgen]
impl Graph {
    /// Add a vertex with a label and properties.
    #[wasm_bindgen(js_name = "addVertex")]
    pub fn add_vertex(&self, label: &str, properties: JsValue) -> Result<u64, JsError> {
        // ...
    }
}
```

### 10.2 JSDoc Comments

All public methods should have JSDoc comments for IDE documentation:

```rust
#[wasm_bindgen]
impl Traversal {
    /// Filter to elements with a specific label.
    /// 
    /// @param label - The label to match
    /// @returns A new traversal with the filter applied
    /// 
    /// @example
    /// ```typescript
    /// graph.V().hasLabel('person').toList();
    /// ```
    #[wasm_bindgen(js_name = "hasLabel")]
    pub fn has_label(self, label: &str) -> Traversal {
        // ...
    }
}
```

### 10.3 Type Exports

Ensure all types are properly exported:

```rust
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "Record<string, Value>")]
    pub type Properties;
}
```

---

## 11. Usage Examples

### 11.1 Browser (ES Modules)

```html
<!DOCTYPE html>
<html>
<head>
    <title>Interstellar Graph Demo</title>
</head>
<body>
    <script type="module">
        import init, { Graph, P, __ } from './pkg/interstellar_graph.js';

        async function main() {
            // Initialize WASM module
            await init();

            // Create a graph
            const graph = new Graph();

            // Add vertices
            const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
            const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
            const charlie = graph.addVertex('person', { name: 'Charlie', age: 35n });

            // Add edges
            graph.addEdge(alice, bob, 'knows', { since: 2020n });
            graph.addEdge(alice, charlie, 'knows', { since: 2019n });
            graph.addEdge(bob, charlie, 'knows', { since: 2021n });

            // Query: Find names of people Alice knows
            const friends = graph.V_(alice)
                .out('knows')
                .values('name')
                .toList();
            console.log('Alice knows:', friends); // ['Bob', 'Charlie']

            // Query: Find people over 28 who know someone
            const adults = graph.V()
                .hasLabel('person')
                .hasWhere('age', P.gt(28n))
                .where(__.out('knows'))
                .values('name')
                .toList();
            console.log('Adults with friends:', adults); // ['Alice', 'Charlie']

            // GQL query
            const results = graph.gql(`
                MATCH (p:person)-[:knows]->(friend:person)
                WHERE p.name = 'Alice'
                RETURN friend.name
            `);
            console.log('GQL results:', results);
        }

        main().catch(console.error);
    </script>
</body>
</html>
```

### 11.2 Node.js

```javascript
const { Graph, P, __ } = require('interstellar-graph');

// Create a social network graph
const graph = new Graph();

// Batch add users
const users = [
    { name: 'Alice', age: 30n, city: 'NYC' },
    { name: 'Bob', age: 25n, city: 'LA' },
    { name: 'Charlie', age: 35n, city: 'NYC' },
    { name: 'Diana', age: 28n, city: 'Chicago' },
];

const userIds = users.map(user => 
    graph.addVertex('person', user)
);

// Add relationships
graph.addEdge(userIds[0], userIds[1], 'knows', {});
graph.addEdge(userIds[0], userIds[2], 'knows', {});
graph.addEdge(userIds[1], userIds[3], 'knows', {});

// Complex traversal: Find friends-of-friends in NYC
const fofInNYC = graph.V_(userIds[0])
    .out('knows')
    .out('knows')
    .dedup()
    .hasValue('city', 'NYC')
    .not(__.hasId(userIds[0])) // Exclude original person
    .values('name')
    .toList();

console.log('Friends of friends in NYC:', fofInNYC);

// Aggregation: Count users by city
const byCity = graph.V()
    .hasLabel('person')
    .groupCount()
    .byKey('city')
    .build()
    .toList();

console.log('Users by city:', byCity[0]);
// { NYC: 2n, LA: 1n, Chicago: 1n }
```

### 11.3 TypeScript with Bundler

```typescript
import { Graph, P, __, Vertex, Edge, Value } from 'interstellar-graph';

interface Person {
    name: string;
    age: bigint;
    email?: string;
}

class SocialGraph {
    private graph: Graph;

    constructor() {
        this.graph = new Graph();
    }

    addPerson(person: Person): bigint {
        return this.graph.addVertex('person', person as Record<string, Value>);
    }

    addFriendship(person1: bigint, person2: bigint): void {
        this.graph.addEdge(person1, person2, 'friends', {});
        this.graph.addEdge(person2, person1, 'friends', {}); // Bidirectional
    }

    getFriends(personId: bigint): string[] {
        return this.graph.V_(personId)
            .outLabels('friends')
            .values('name')
            .toList() as string[];
    }

    getMutualFriends(person1: bigint, person2: bigint): string[] {
        return this.graph.V_(person1)
            .outLabels('friends')
            .where(__.inLabels('friends').hasId(person2))
            .values('name')
            .toList() as string[];
    }

    findPathBetween(from: bigint, to: bigint, maxDepth: number = 5): Value[] {
        return this.graph.V_(from)
            .repeat(__.out())
            .until(__.hasId(to))
            .times(BigInt(maxDepth))
            .path()
            .limit(1n)
            .toList();
    }
}

// Usage
const social = new SocialGraph();
const alice = social.addPerson({ name: 'Alice', age: 30n });
const bob = social.addPerson({ name: 'Bob', age: 25n });
social.addFriendship(alice, bob);
console.log(social.getFriends(alice)); // ['Bob']
```

### 11.4 React Integration

```tsx
import React, { useEffect, useState } from 'react';
import init, { Graph } from 'interstellar-graph';

function useGraph() {
    const [graph, setGraph] = useState<Graph | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        init().then(() => {
            setGraph(new Graph());
            setLoading(false);
        });
    }, []);

    return { graph, loading };
}

function GraphExplorer() {
    const { graph, loading } = useGraph();
    const [vertices, setVertices] = useState<any[]>([]);

    useEffect(() => {
        if (graph) {
            // Load initial data
            const v = graph.V().elementMap().toList();
            setVertices(v);
        }
    }, [graph]);

    if (loading) return <div>Loading WASM...</div>;

    return (
        <div>
            <h1>Graph Explorer</h1>
            <ul>
                {vertices.map((v, i) => (
                    <li key={i}>{JSON.stringify(v)}</li>
                ))}
            </ul>
        </div>
    );
}
```

---

## 12. Implementation Phases

### Phase 1: Core Infrastructure (Estimated: 2-3 days)

- [ ] Add `wasm` feature flag to Cargo.toml
- [ ] Add wasm-bindgen, serde-wasm-bindgen, js-sys dependencies
- [ ] Create `src/wasm/mod.rs` with feature gate
- [ ] Create `src/wasm/types.rs` with Value conversion
- [ ] Create `src/wasm/graph.rs` with Graph facade
- [ ] Implement Graph constructor and basic getters
- [ ] Set up wasm-pack build workflow

**Deliverable:** `new Graph()` works, `vertexCount()`/`edgeCount()` work

### Phase 2: Graph CRUD Operations (Estimated: 2 days)

- [ ] Implement `addVertex()` with JsValue properties
- [ ] Implement `getVertex()` returning Vertex interface
- [ ] Implement `removeVertex()`
- [ ] Implement `setVertexProperty()`, `removeVertexProperty()`
- [ ] Implement `addEdge()`, `getEdge()`, `removeEdge()`
- [ ] Implement `setEdgeProperty()`, `removeEdgeProperty()`
- [ ] Add error handling with JsError

**Deliverable:** Full CRUD operations work

### Phase 3: Traversal Source Steps (Estimated: 2 days)

- [ ] Create `src/wasm/traversal.rs` with Traversal facade
- [ ] Implement Arc-based snapshot ownership
- [ ] Implement step accumulation pattern
- [ ] Implement `V()`, `V_(ids)`, `E()`, `E_(ids)`
- [ ] Implement `inject()`
- [ ] Implement `addV()`, `addE()` source steps

**Deliverable:** `graph.V().toList()` works

### Phase 4: Filter and Navigation Steps (Estimated: 3-4 days)

- [ ] Implement all filter steps (hasLabel, has, dedup, limit, etc.)
- [ ] Implement all navigation steps (out, in_, both, outE, etc.)
- [ ] Implement `where()`, `not()`, `and()`, `or()`
- [ ] Create `src/wasm/predicate.rs`
- [ ] Implement P namespace with all predicates

**Deliverable:** Complex filter queries work

### Phase 5: Transform and Terminal Steps (Estimated: 2-3 days)

- [ ] Implement transform steps (values, id, label, elementMap, etc.)
- [ ] Implement terminal steps (toList, first, one, toCount, etc.)
- [ ] Implement fold, unfold, path, as, select
- [ ] Implement aggregation (mean, sum, min, max)

**Deliverable:** `graph.V().values('name').toList()` works

### Phase 6: Builder Steps and Branching (Estimated: 2-3 days)

- [ ] Implement OrderBuilder and order() step
- [ ] Implement ProjectBuilder and project() step
- [ ] Implement GroupBuilder, GroupCountBuilder
- [ ] Implement RepeatBuilder and repeat() step
- [ ] Implement union, coalesce, choose, optional, local

**Deliverable:** Complex aggregations and loops work

### Phase 7: Anonymous Traversals (Estimated: 1-2 days)

- [ ] Create `src/wasm/anonymous.rs`
- [ ] Implement __ namespace with all factory functions
- [ ] Test anonymous traversals with where(), union(), repeat()

**Deliverable:** `graph.V().where(__.out()).toList()` works

### Phase 8: GQL and Serialization (Estimated: 1 day)

- [ ] Implement `gql()` method
- [ ] Implement `toGraphSON()` and `fromGraphSON()`
- [ ] Implement `clear()`

**Deliverable:** GQL queries work

### Phase 9: npm Package and Documentation (Estimated: 2 days)

- [ ] Configure wasm-pack for npm publishing
- [ ] Create package.json with proper metadata
- [ ] Test all three targets (web, nodejs, bundler)
- [ ] Write README.md with usage examples
- [ ] Publish to npm as `interstellar-graph`

**Deliverable:** `npm install interstellar-graph` works

---

## 13. Testing Strategy

### 13.1 Rust Unit Tests

Standard Rust tests for WASM facade logic:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_graph_crud() {
        let graph = Graph::new();
        let id = graph.add_vertex("person", JsValue::NULL).unwrap();
        assert!(graph.get_vertex(id).is_some());
    }
}
```

### 13.2 wasm-pack Tests

```bash
# Run in headless browser
wasm-pack test --headless --firefox --features wasm
wasm-pack test --headless --chrome --features wasm

# Run in Node.js
wasm-pack test --node --features wasm
```

### 13.3 Integration Tests (JavaScript)

Create a separate test project:

```
tests/wasm-integration/
├── package.json
├── test.js
└── test.html
```

**test.js:**
```javascript
const assert = require('assert');
const { Graph, P, __ } = require('../../pkg');

describe('Graph', () => {
    it('should create vertices', () => {
        const g = new Graph();
        const id = g.addVertex('person', { name: 'Alice' });
        assert(typeof id === 'bigint');
    });

    it('should traverse', () => {
        const g = new Graph();
        const a = g.addVertex('person', { name: 'Alice' });
        const b = g.addVertex('person', { name: 'Bob' });
        g.addEdge(a, b, 'knows', {});
        
        const names = g.V_(a).out('knows').values('name').toList();
        assert.deepStrictEqual(names, ['Bob']);
    });
});
```

### 13.4 Browser Manual Testing

Provide an HTML test page:

```html
<!DOCTYPE html>
<html>
<head>
    <title>Interstellar WASM Test</title>
</head>
<body>
    <h1>Interstellar WASM Test</h1>
    <pre id="output"></pre>
    <script type="module">
        import init, { Graph, P } from './pkg/interstellar_graph.js';
        
        const output = document.getElementById('output');
        
        async function runTests() {
            await init();
            
            const tests = [];
            
            // Test 1: Basic CRUD
            try {
                const g = new Graph();
                const id = g.addVertex('test', {});
                tests.push(`✅ addVertex: ${id}`);
            } catch (e) {
                tests.push(`❌ addVertex: ${e.message}`);
            }
            
            // Test 2: Traversal
            try {
                const g = new Graph();
                g.addVertex('person', { name: 'Alice' });
                const names = g.V().values('name').toList();
                tests.push(`✅ traversal: ${JSON.stringify(names)}`);
            } catch (e) {
                tests.push(`❌ traversal: ${e.message}`);
            }
            
            output.textContent = tests.join('\n');
        }
        
        runTests();
    </script>
</body>
</html>
```

---

## 14. Future Enhancements

### 14.1 Closure Support via js_sys::Function

```rust
use js_sys::Function;

#[wasm_bindgen]
impl Traversal {
    /// Filter elements using a JavaScript function.
    /// @param fn - Function that receives an element and returns boolean
    pub fn filter(self, func: &Function) -> Traversal {
        // Call func.call1() for each element
    }
}
```

Usage:
```javascript
graph.V().filter(v => v.properties.age > 18n).toList();
```

### 14.2 Async/Streaming Iteration

```typescript
// Future: async iteration
for await (const vertex of graph.V().stream()) {
    console.log(vertex);
}
```

### 14.3 Web Workers

```typescript
// Future: run graph operations in a Web Worker
const worker = new InterstellarWorker();
const results = await worker.query(graph.V().hasLabel('person'));
```

### 14.4 IndexedDB Persistence

```typescript
// Future: persist graph to IndexedDB
await graph.persist('my-graph');
const loaded = await Graph.load('my-graph');
```

---

## 15. Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `Cargo.toml` | Modify | Add `wasm` feature, dependencies, crate-type |
| `src/lib.rs` | Modify | Add `pub mod wasm` with feature gate |
| `src/wasm/mod.rs` | Create | Module exports, feature gate |
| `src/wasm/graph.rs` | Create | Graph facade implementation |
| `src/wasm/traversal.rs` | Create | Traversal facade, all steps |
| `src/wasm/types.rs` | Create | Vertex, Edge, Value conversion |
| `src/wasm/predicate.rs` | Create | P namespace implementation |
| `src/wasm/anonymous.rs` | Create | __ namespace implementation |
| `src/wasm/error.rs` | Create | Error conversion to JsError |
| `pkg/package.json` | Create | npm package metadata (via wasm-pack) |
| `README.md` | Modify | Add WASM/npm usage instructions |

---

## 16. Acceptance Criteria

### Minimum Viable Product (MVP)

- [ ] `npm install interstellar-graph` works
- [ ] `new Graph()` creates an empty graph
- [ ] CRUD operations work (addVertex, addEdge, etc.)
- [ ] Basic traversal works: `graph.V().hasLabel('x').out('y').values('z').toList()`
- [ ] Predicates work: `P.eq()`, `P.gt()`, `P.within()`
- [ ] TypeScript types are complete (no `any` types)
- [ ] Works in both browser and Node.js
- [ ] GQL queries work

### Full Release

- [ ] All traversal steps documented in this spec are implemented
- [ ] All predicates documented in this spec are implemented
- [ ] Anonymous traversal factory (`__`) is complete
- [ ] Order, project, group, repeat builders work
- [ ] GraphSON import/export works
- [ ] npm package published with proper metadata
- [ ] README with comprehensive examples

---

## Appendix A: Method Name Mapping

Some Rust method names conflict with JavaScript reserved words or conventions:

| Rust Name | JavaScript Name | Reason |
|-----------|----------------|--------|
| `in_()` | `in_()` | `in` is reserved in JS |
| `as_()` | `as()` | OK in JS, but keep underscore for consistency |
| `V()` | `V()` | Keep uppercase for Gremlin convention |
| `E()` | `E()` | Keep uppercase for Gremlin convention |
| `v_ids()` | `V_(...ids)` | Variadic version |
| `e_ids()` | `E_(...ids)` | Variadic version |
| `to_list()` | `toList()` | camelCase convention |
| `has_label()` | `hasLabel()` | camelCase convention |
| `group_count()` | `groupCount()` | camelCase convention |

---

## Appendix B: Performance Considerations

### B.1 Memory Management

- `Graph` instances should be explicitly dropped when no longer needed
- Large traversal results should use streaming (future enhancement)
- Consider `wee_alloc` for smaller WASM binary (tradeoff: slower allocations)

### B.2 Bundle Size Optimization

```bash
# Optimize WASM binary
wasm-opt -Oz -o optimized.wasm target/wasm32-unknown-unknown/release/interstellar.wasm

# Typical sizes:
# - Debug: ~2-5 MB
# - Release: ~500 KB - 1 MB
# - Optimized: ~300-700 KB
```

### B.3 Initialization Time

- First `init()` call loads and compiles WASM (~50-200ms)
- Subsequent operations are near-native speed
- Consider lazy loading for large applications
