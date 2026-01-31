# Interstellar WASM Browser Example

This example demonstrates using the Interstellar Graph Database in the browser via WebAssembly.

## Quick Start

1. **Serve the example** (requires a local HTTP server due to WASM/ES modules):

   ```bash
   # Using Python 3
   cd examples/wasm-web
   python3 -m http.server 8080
   
   # Or using Node.js (npx)
   npx serve .
   
   # Or using PHP
   php -S localhost:8080
   ```

2. **Open in browser**: Navigate to [http://localhost:8080](http://localhost:8080)

## What's Included

- `index.html` - Main HTML page with UI
- `app.js` - JavaScript code demonstrating the API
- `pkg/` - WASM package built with wasm-pack

## Features Demonstrated

### Graph Operations
- Creating vertices with labels and properties
- Adding edges between vertices
- Querying vertex/edge counts

### Traversal Queries
- `graph.V()` - Start traversal from all vertices
- `.hasLabel('person')` - Filter by label
- `.values('name')` - Extract property values
- `.toList()` / `.toCount()` - Terminal operations

### Predicate Filters
- `P.gte(25n)` - Greater than or equal
- `P.startingWith('A')` - String prefix matching
- `.hasWhere('age', predicate)` - Filter with predicates

### Edge Navigation
- `.out()` / `.outLabels(['knows'])` - Outgoing edges
- `.in_()` / `.inLabels(['knows'])` - Incoming edges  
- `.both()` / `.bothLabels(['knows'])` - Both directions

## API Overview

```javascript
import init, { Graph, P } from './pkg/interstellar.js';

// Initialize WASM
await init();

// Create a graph
const graph = new Graph();

// Add vertices (returns BigInt ID)
const alice = graph.addVertex('person', { 
    name: 'Alice', 
    age: 30n  // Use BigInt for integers
});

// Add edges
graph.addEdge(alice, bob, 'knows', { since: 2020n });

// Query with traversals
const names = graph.V()
    .hasLabel('person')
    .values('name')
    .toList();

// Use predicates for filtering
const adults = graph.V()
    .hasLabel('person')
    .hasWhere('age', P.gte(18n))
    .values('name')
    .toList();
```

## Notes

- Integer values should use BigInt (`30n` not `30`)
- Vertex/Edge IDs are returned as BigInt
- WASM must be loaded with `await init()` before using

## Rebuilding the WASM Package

From the project root:

```bash
wasm-pack build --target web --features wasm
cp -r pkg examples/wasm-web/
```
