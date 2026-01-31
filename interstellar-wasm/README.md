# interstellar-wasm

> **Early Development Notice**
>
> Interstellar is in early development and is **not recommended for production use**. APIs may change without notice, and the project has not been audited for security or performance at scale.

WebAssembly bindings for the [Interstellar](../interstellar/) graph database.

This crate provides a thin WASM entry point that re-exports the wasm-bindgen types from the core `interstellar` crate.

## Building

Requires [wasm-pack](https://rustwasm.github.io/wasm-pack/):

```bash
# Install wasm-pack (one-time setup)
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Build for web
wasm-pack build interstellar-wasm --target web

# Build for Node.js
wasm-pack build interstellar-wasm --target nodejs

# Build for bundlers (webpack, etc.)
wasm-pack build interstellar-wasm --target bundler
```

## Usage

### Browser (ES Modules)

```javascript
import init, { Graph, P, __ } from 'interstellar-wasm';

async function main() {
    await init();

    const graph = new Graph();
    const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
    const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
    graph.addEdge(alice, bob, 'knows', { since: 2020n });

    const friends = graph.V_(alice)
        .outLabels('knows')
        .values('name')
        .toList();
    console.log(friends); // ['Bob']
}

main();
```

### Node.js

```javascript
const { Graph, P, __ } = require('interstellar-wasm');

const graph = new Graph();
const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
// ... same API as browser
```

## Features

| Feature | Description | Default |
|---------|-------------|---------|
| `gql` | GQL query language support | No |

Enable features during build:

```bash
wasm-pack build interstellar-wasm --target web -- --features gql
```

## API

The WASM bindings expose the same API as the core Interstellar library:

- **Graph**: Create graphs, add/remove vertices and edges
- **Traversal**: Gremlin-style fluent traversal API
- **P**: Predicate factory for filters (`P.gt()`, `P.within()`, etc.)
- **__**: Anonymous traversal factory for composable queries

For full API documentation, see:
- [Interstellar README](../interstellar/README.md)
- [WASM Bindgen Spec](../specs/spec-45-wasm-bindgen.md)

## Testing

```bash
# Run tests in headless browser
wasm-pack test interstellar-wasm --headless --firefox
wasm-pack test interstellar-wasm --headless --chrome

# Run tests in Node.js
wasm-pack test interstellar-wasm --node
```

## Limitations

- **In-memory only**: WASM builds do not support persistent storage (no `mmap` feature)
- **No file I/O**: Cannot read/write files directly from WASM
- **BigInt required**: Integer properties should use JavaScript BigInt (`30n` not `30`)

## License

MIT OR Apache-2.0

## Development Approach

This project uses **spec-driven development** with AI assistance. Most code is generated or reviewed by LLMs (primarily Claude Opus 4.5). While we aim for high quality and test coverage, this approach is experimental.
