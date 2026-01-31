# Interstellar Graph Database - Node.js Example

This example demonstrates using the Interstellar graph database WASM bindings in a Node.js environment.

## Requirements

- Node.js 18 or later (uses native test runner and ES modules)
- The WASM package must be built first

## Setup

1. Build the WASM package from the project root:

```bash
./scripts/build-wasm.sh
```

2. Run the example:

```bash
cd examples/wasm-node
npm start
```

3. Run the tests:

```bash
npm test
```

## Usage

### Import the Module

```javascript
// CommonJS require (Node.js)
const { Graph, P } = require('../../pkg-node/interstellar.js');

// Or with ESM using createRequire
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { Graph, P } = require('../../pkg-node/interstellar.js');
```

### Create a Graph

```javascript
const graph = new Graph();

// Add vertices
const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
const bob = graph.addVertex('person', { name: 'Bob', age: 25n });

// Add edges
graph.addEdge(alice, bob, 'knows', { since: 2020n });
```

### Query with Traversals

```javascript
// Get all person names
const names = graph.V()
    .hasLabel('person')
    .values('name')
    .toList();
// ['Alice', 'Bob']

// Find Alice's friends
const friends = graph.V_(alice)
    .outLabels(['knows'])
    .values('name')
    .toList();
// ['Bob']

// Filter with predicates
const adults = graph.V()
    .hasLabel('person')
    .hasWhere('age', P.gte(18n))
    .values('name')
    .toList();
```

### GraphSON Export/Import

```javascript
// Export
const json = graph.toGraphSON();

// Import into new graph
const graph2 = new Graph();
graph2.fromGraphSON(json);
```

## Important Notes

### BigInt for Integers

All integer values in properties should use JavaScript `bigint` to preserve 64-bit precision:

```javascript
// Correct
graph.addVertex('person', { age: 30n });

// Also works (small integers)
graph.addVertex('person', { age: 30 });

// But large integers MUST use bigint
graph.addVertex('data', { timestamp: 1706284800000n });
```

### Vertex/Edge IDs

IDs returned by `addVertex()` and `addEdge()` are `bigint` values:

```javascript
const id = graph.addVertex('test', {});
console.log(typeof id); // 'bigint'
```

## API Reference

See the TypeScript definitions in `pkg-node/interstellar.d.ts` for the complete API.

### Graph Methods

- `addVertex(label, properties)` - Add a vertex
- `addEdge(from, to, label, properties)` - Add an edge
- `getVertex(id)` - Get vertex by ID
- `getEdge(id)` - Get edge by ID
- `removeVertex(id)` - Remove a vertex
- `removeEdge(id)` - Remove an edge
- `setVertexProperty(id, key, value)` - Set vertex property
- `setEdgeProperty(id, key, value)` - Set edge property
- `vertexCount()` - Count vertices
- `edgeCount()` - Count edges
- `V()` - Start traversal from all vertices
- `V_(ids)` - Start traversal from specific vertices
- `E()` - Start traversal from all edges
- `E_(ids)` - Start traversal from specific edges
- `toGraphSON()` - Export to GraphSON
- `fromGraphSON(json)` - Import from GraphSON

### Traversal Steps

**Filter:** `hasLabel`, `has`, `hasValue`, `hasWhere`, `hasId`, `limit`, `skip`, `dedup`, `simplePath`

**Navigation:** `out`, `in_`, `both`, `outE`, `inE`, `bothE`, `outV`, `inV`, `bothV`, `otherV`

**Transform:** `values`, `valueMap`, `elementMap`, `id`, `label`, `constant`, `unfold`, `path`

**Terminal:** `toList`, `first`, `one`, `toCount`, `hasNext`, `iterate`

### Predicates (P)

- `P.eq(value)`, `P.neq(value)` - Equality
- `P.gt(value)`, `P.gte(value)`, `P.lt(value)`, `P.lte(value)` - Comparison
- `P.between(start, end)`, `P.inside(start, end)`, `P.outside(start, end)` - Range
- `P.within(values)`, `P.without(values)` - Set membership
- `P.containing(str)`, `P.startingWith(str)`, `P.endingWith(str)` - String
- `P.regex(pattern)` - Regular expression
- `P.and(p1, p2)`, `P.or(p1, p2)`, `P.not(p)` - Logical
