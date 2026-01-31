<p align="center">
  <img src="assets/interstellar-white-logo.png" alt="Interstellar Logo" width="400">
</p>

# Interstellar

A high-performance graph database with multiple language bindings.

## Packages

| Package | Description | Language |
|---------|-------------|----------|
| [interstellar](./interstellar/) | Core graph database library | Rust |
| [interstellar-cli](./interstellar-cli/) | Command-line interface with REPL | Rust |
| [interstellar-node](./interstellar-node/) | Native Node.js bindings via napi-rs | JavaScript/TypeScript |
| [interstellar-wasm](./interstellar-wasm/) | WebAssembly bindings | JavaScript/TypeScript |

## Features

- **Dual Query APIs**: Gremlin-style fluent traversals and GQL (Graph Query Language)
- **Dual Storage Backends**: In-memory (HashMap-based) and memory-mapped (persistent)
- **Multiple Language Bindings**: Rust, Node.js (native), WebAssembly (browser/Node)
- **Zero-cost Abstractions**: Monomorphized traversal pipelines
- **Thread-Safe**: All backends support concurrent read access

## Quick Start

### CLI

```bash
# Install
cargo install --path interstellar-cli

# Create a database with sample data
interstellar create ./my-graph.db --with-sample marvel

# Query with GQL
interstellar query ./my-graph.db --gql "MATCH (n:Character) RETURN n.name"

# Interactive REPL
interstellar query ./my-graph.db
```

### Rust

```toml
[dependencies]
interstellar = "0.1"
```

```rust
use interstellar::prelude::*;

let graph = Graph::new();
let alice = graph.add_vertex("person", props! { "name" => "Alice" });
let bob = graph.add_vertex("person", props! { "name" => "Bob" });
graph.add_edge(alice, bob, "knows", props! {}).unwrap();

let g = graph.snapshot().traversal();
let friends = g.v_ids([alice]).out("knows").values("name").to_list();
// ["Bob"]
```

### Node.js (Native)

```bash
npm install @interstellar/node
```

```javascript
const { Graph } = require('@interstellar/node');

const graph = new Graph();
const alice = graph.addVertex('person', { name: 'Alice' });
const bob = graph.addVertex('person', { name: 'Bob' });
graph.addEdge(alice, bob, 'knows', {});

const friends = graph.V(alice).out('knows').values('name').toList();
// ['Bob']
```

### WebAssembly (Browser)

```javascript
import init, { Graph } from 'interstellar-wasm';

await init();

const graph = new Graph();
const alice = graph.addVertex('person', { name: 'Alice' });
const bob = graph.addVertex('person', { name: 'Bob' });
graph.addEdge(alice, bob, 'knows', {});

const friends = graph.V([alice]).out('knows').values('name').toList();
// ['Bob']
```

## Building

```bash
# Build all packages
cargo build

# Build specific package
cargo build -p interstellar
cargo build -p interstellar-cli
cargo build -p interstellar-node
cargo build -p interstellar-wasm

# Build with features
cargo build -p interstellar --features "gremlin,gql,mmap"
```

## Testing

```bash
# All workspace tests
cargo test --workspace --features "gremlin,gql,mmap"

# Rust tests (core library)
cargo test -p interstellar --features "gremlin,gql,mmap"

# CLI tests
cargo test -p interstellar-cli

# Node.js tests
cd interstellar-node && npm test

# WASM build verification
wasm-pack build interstellar-wasm --target web
```

## Repository Structure

```
./
├── interstellar/           # Core Rust library
│   ├── src/                # Source code
│   ├── tests/              # Integration tests
│   ├── benches/            # Benchmarks
│   ├── examples/           # Example programs
│   └── docs/               # User documentation
│
├── interstellar-cli/       # Command-line interface
│   ├── src/                # CLI source code
│   └── specs/              # CLI specifications
│
├── interstellar-node/      # Node.js native bindings (napi-rs)
│   ├── src/                # Rust bindings code
│   ├── __test__/           # JavaScript tests (vitest)
│   └── examples/           # Node.js examples
│
├── interstellar-wasm/      # WebAssembly bindings
│   └── src/                # Thin wrapper re-exporting from core
│
├── specs/                  # Implementation specifications
│   ├── plans/              # Development plans
│   └── archive/            # Completed specs
│
├── guiding-documents/      # Architecture & design documents
│
└── todos/                  # Task tracking
    ├── code-reviews/       # Code review notes
    └── perf-improvements/  # Performance work
```

## Documentation

- **Core Library**: See [interstellar/README.md](./interstellar/README.md) for detailed Rust API docs
- **CLI**: See [interstellar-cli/README.md](./interstellar-cli/README.md) for command reference
- **Node.js Bindings**: See [interstellar-node/](./interstellar-node/) for JavaScript API
- **User Guides**: See [interstellar/docs/](./interstellar/docs/) for comprehensive documentation

## License

MIT
