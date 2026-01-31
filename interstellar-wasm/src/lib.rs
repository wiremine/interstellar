//! WebAssembly bindings for Interstellar graph database.
//!
//! This crate provides the WASM entry point for the Interstellar graph database.
//! It re-exports the wasm-bindgen types from the core `interstellar` crate.
//!
//! # Building
//!
//! ```bash
//! wasm-pack build interstellar-wasm --target web
//! ```
//!
//! # Usage
//!
//! ```javascript
//! import init, { Graph, P, __ } from 'interstellar-wasm';
//!
//! async function main() {
//!     await init();
//!
//!     const graph = new Graph();
//!     const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
//!     const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
//!     graph.addEdge(alice, bob, 'knows', { since: 2020n });
//!
//!     const friends = graph.V_(alice)
//!         .outLabels('knows')
//!         .values('name')
//!         .toList();
//!     console.log(friends); // ['Bob']
//! }
//! ```

// Re-export all WASM types from the core interstellar crate
pub use interstellar::wasm::*;
