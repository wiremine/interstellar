//! WebAssembly bindings for Interstellar graph database.
//!
//! This module provides JavaScript/TypeScript bindings via `wasm-bindgen`,
//! enabling use of Interstellar in browsers and Node.js environments.
//!
//! # Usage
//!
//! ```javascript
//! import init, { Graph, P, __ } from 'interstellar-graph';
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
//!
//! # Features
//!
//! - Full graph CRUD operations
//! - Gremlin-style traversal API with method chaining
//! - Predicate system (`P.eq()`, `P.gt()`, etc.)
//! - Anonymous traversal factory (`__`)
//! - GQL query language support (when `gql` feature is enabled)
//! - GraphSON serialization (string-based)

mod error;
mod graph;
mod predicate;
mod traversal;
mod types;

// Re-export main types for wasm-bindgen
pub use graph::Graph;
pub use predicate::P;
pub use traversal::Traversal;

use wasm_bindgen::prelude::*;

// Custom TypeScript type definitions
#[wasm_bindgen(typescript_custom_section)]
const TS_TYPES: &'static str = r#"
/**
 * A property value type.
 *
 * Note: Integers use `bigint` for 64-bit precision.
 */
export type Value = null | boolean | bigint | number | string | Value[] | Record<string, Value>;

/**
 * A vertex (node) in the graph.
 */
export interface Vertex {
    /** Unique vertex identifier */
    readonly id: bigint;
    /** Vertex label (e.g., 'person', 'product') */
    readonly label: string;
    /** Vertex properties */
    readonly properties: Record<string, Value>;
}

/**
 * An edge (relationship) between two vertices.
 */
export interface Edge {
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

/**
 * Result of a GraphSON import operation.
 */
export interface GraphSONImportResult {
    verticesImported: bigint;
    edgesImported: bigint;
    warnings: string[];
}
"#;

/// Initialize the WASM module (called automatically by wasm-bindgen).
#[wasm_bindgen(start)]
pub fn start() {
    // Set panic hook for better error messages in browser console
    // Note: Requires adding `console_error_panic_hook` as a feature and dependency
    // #[cfg(feature = "console_error_panic_hook")]
    // console_error_panic_hook::set_once();
}
