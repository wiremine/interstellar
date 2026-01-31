# @interstellar/node

> **Early Development Notice**
>
> Interstellar is in early development and is **not recommended for production use**. APIs may change without notice, and the project has not been audited for security or performance at scale.

Native Node.js bindings for the [Interstellar](../interstellar/) graph database, powered by [napi-rs](https://napi.rs/).

## Features

- **Native Performance**: Rust-powered graph operations via N-API
- **Dual Storage Modes**: In-memory (fast) and persistent (disk-backed) graphs
- **Gremlin-Style Traversals**: Fluent API with chainable steps
- **Rich Predicates**: Filter with `P.gt()`, `P.within()`, `P.regex()`, and more
- **Anonymous Traversals**: Composable query fragments via the `__` factory
- **Full TypeScript Support**: Complete type definitions included

## Installation

```bash
npm install @interstellar/node
```

## Quick Start

```javascript
import { Graph, P, __ } from '@interstellar/node';

// Create an in-memory graph
const graph = new Graph();

// Add vertices
const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
const bob = graph.addVertex('person', { name: 'Bob', age: 25n });

// Add an edge
graph.addEdge(alice, bob, 'knows', { since: 2020n });

// Traverse the graph
const friends = graph.V(alice)
    .out('knows')
    .values('name')
    .toList();

console.log(friends); // ['Bob']
```

## Storage Modes

### In-Memory (Default)

Fast, non-persistent storage. Data is lost when the process exits.

```javascript
const graph = new Graph();           // or Graph.inMemory()
graph.addVertex('person', { name: 'Alice' });
```

### Persistent (Disk-Backed)

Memory-mapped storage with automatic persistence. Data survives restarts.

```javascript
const graph = Graph.open('./my_graph.db');
graph.addVertex('person', { name: 'Alice' });
// Data is automatically persisted to disk
```

Check storage mode:

```javascript
console.log(graph.isPersistent); // true or false
```

## API Reference

### Graph

| Method | Description |
|--------|-------------|
| `new Graph()` | Create in-memory graph |
| `Graph.inMemory()` | Create in-memory graph |
| `Graph.open(path)` | Open/create persistent graph |
| `addVertex(label, props?)` | Add vertex, returns ID (bigint) |
| `getVertex(id)` | Get vertex by ID |
| `removeVertex(id)` | Remove vertex and incident edges |
| `setVertexProperty(id, key, value)` | Set vertex property |
| `addEdge(from, to, label, props?)` | Add edge, returns ID (bigint) |
| `getEdge(id)` | Get edge by ID |
| `removeEdge(id)` | Remove edge |
| `setEdgeProperty(id, key, value)` | Set edge property |
| `vertexCount` | Total vertex count |
| `edgeCount` | Total edge count |
| `version` | Current version/transaction ID |
| `V(ids?)` | Start vertex traversal |
| `E(ids?)` | Start edge traversal |

### Traversal Steps

**Filter Steps:**

```javascript
.hasLabel('person')              // Filter by label
.hasLabelAny(['person', 'user']) // Filter by any of labels
.has('name')                     // Has property (any value)
.hasValue('name', 'Alice')       // Property equals value
.hasWhere('age', P.gt(25n))      // Property matches predicate
.hasNot('deleted')               // Does not have property
.hasId(id)                       // Filter by ID
.dedup()                         // Remove duplicates
.limit(10)                       // Take first N
.skip(5)                         // Skip first N
.range(5, 10)                    // Take elements [5, 10)
```

**Navigation Steps:**

```javascript
.out('knows')                    // Outgoing adjacent vertices
.in('knows')                     // Incoming adjacent vertices
.both('knows')                   // Both directions
.outE('knows')                   // Outgoing edges
.inE('knows')                    // Incoming edges
.bothE('knows')                  // Edges in both directions
.outV()                          // Edge source vertex
.inV()                           // Edge target vertex
.bothV()                         // Both endpoints
.otherV()                        // Opposite vertex from previous
```

**Transform Steps:**

```javascript
.values('name')                  // Extract property value
.id()                            // Get element ID
.label()                         // Get element label
.valueMap()                      // All properties as map
.elementMap()                    // Properties + id + label
.constant('value')               // Replace with constant
.unfold()                        // Flatten collections
.fold()                          // Collect into single list
.path()                          // Get traversal path
```

**Aggregation Steps:**

```javascript
.count_()                        // Count (as traversal step)
.sum()                           // Sum numeric values
.mean()                          // Arithmetic mean
.min()                           // Minimum value
.max()                           // Maximum value
.orderAsc()                      // Sort ascending
.orderDesc()                     // Sort descending
```

**Branch Steps:**

```javascript
.where(traversal)                // Filter by sub-traversal
.not(traversal)                  // Negation
.union([t1, t2])                 // Merge multiple traversals
.coalesce([t1, t2])              // First non-empty result
.optional(traversal)             // Include if exists
.local(traversal)                // Apply per-element
```

**Path & Labels:**

```javascript
.as('a')                         // Label current position
.select(['a', 'b'])              // Retrieve labeled values
```

**Mutation Steps:**

```javascript
.property('key', value)          // Set property
.drop()                          // Remove element
```

**Terminal Steps:**

```javascript
.toList()                        // Collect all results
.first()                         // First result or undefined
.next()                          // Alias for first()
.one()                           // Exactly one result (throws if not)
.hasNext()                       // Check if results exist
.count()                         // Count results (as number)
.iterate()                       // Execute for side effects
```

### Predicates (P)

```javascript
import { P } from '@interstellar/node';

// Comparison
P.eq(value)                      // Equals
P.neq(value)                     // Not equals
P.lt(value)                      // Less than
P.lte(value)                     // Less than or equal
P.gt(value)                      // Greater than
P.gte(value)                     // Greater than or equal

// Range
P.between(start, end)            // In range [start, end)
P.inside(start, end)             // In range (start, end)
P.outside(start, end)            // Outside range

// Membership
P.within([a, b, c])              // In set
P.without([a, b, c])             // Not in set

// String
P.containing('sub')              // Contains substring
P.notContaining('sub')           // Does not contain
P.startingWith('pre')            // Starts with prefix
P.notStartingWith('pre')         // Does not start with
P.endingWith('suf')              // Ends with suffix
P.notEndingWith('suf')           // Does not end with
P.regex('^pattern$')             // Matches regex

// Logical
P.and(p1, p2)                    // Both predicates match
P.or(p1, p2)                     // Either predicate matches
P.not(pred)                      // Negate predicate
```

### Anonymous Traversals (__)

Use `__` to create traversal fragments for branch and filter steps:

```javascript
import { __, P } from '@interstellar/node';

// Filter with sub-traversal
graph.V()
    .hasLabel('person')
    .where(__.out('knows').hasWhere('age', P.gt(30n)))
    .values('name')
    .toList();

// Combine results from multiple paths
graph.V(alice)
    .union([
        __.out('knows'),
        __.out('works_at')
    ])
    .values('name')
    .toList();

// Get first non-empty result
graph.V()
    .coalesce([
        __.out('preferred'),
        __.out('default'),
        __.constant('none')
    ])
    .toList();
```

## Examples

See the [examples/](examples/) directory:

```bash
# Run the social network example
node examples/social-network.mjs

# Run the persistent graph example
node examples/persistent-graph.mjs
```

## Building from Source

Requires Rust toolchain and Node.js 16+.

```bash
# Clone the repository
git clone https://github.com/anthropic/interstellar
cd interstellar/interstellar-node

# Install dependencies
npm install

# Build native module
npm run build

# Run tests
npm test
```

## Type Definitions

Full TypeScript definitions are included. Key types:

```typescript
import { Graph, Traversal, Predicate, P, __ } from '@interstellar/node';

// Graph operations return bigint IDs
const id: bigint = graph.addVertex('label', { prop: 'value' });

// Traversals are chainable
const traversal: Traversal = graph.V().hasLabel('person');

// Terminal steps return results
const results: unknown[] = traversal.toList();
const first: unknown | null = traversal.first();
const count: number = traversal.count();
```

## License

MIT OR Apache-2.0

## Development Approach

This project uses **spec-driven development** with AI assistance. Most code is generated or reviewed by LLMs (primarily Claude Opus 4.5). While we aim for high quality and test coverage, this approach is experimental.
