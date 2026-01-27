# WASM IndexedDB Persistence Spec

**Status**: Draft
**Author**: AI Assistant
**Date**: 2026-01-26

## Overview

This spec defines how Interstellar graphs can be persisted to browser IndexedDB storage when compiled to WASM, enabling graphs to survive page refreshes without reconstruction.

## Goals

1. **Transparent persistence**: Save/load graphs with minimal API surface
2. **Performance**: Sub-second save/load for graphs up to 100K elements
3. **Compatibility**: Work across all modern browsers
4. **Size efficiency**: Compact binary format using bincode

## Non-Goals

- Real-time sync between tabs (future work)
- Server-side persistence (use mmap backend instead)
- Partial graph loading (load entire graph or nothing)
- Schema migration (v1 only)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Browser                                  │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  Rust Graph  │───▶│ Serializable │───▶│     bincode      │  │
│  │   (WASM)     │    │    Graph     │    │     bytes        │  │
│  └──────────────┘    └──────────────┘    └────────┬─────────┘  │
│                                                    │            │
│                                          ┌────────▼─────────┐  │
│                                          │   IndexedDB      │  │
│                                          │  (via JS glue)   │  │
│                                          └──────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Format

### SerializableGraph

A dedicated struct optimized for serialization, separate from internal `GraphState`:

```rust
// src/wasm/persistence.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::value::Value;

/// Format version for future compatibility
pub const FORMAT_VERSION: u32 = 1;

/// Magic bytes to identify Interstellar graph files
pub const MAGIC: [u8; 4] = [0x49, 0x53, 0x47, 0x52]; // "ISGR"

/// Top-level serializable graph structure
#[derive(Serialize, Deserialize)]
pub struct SerializableGraph {
    /// Format version (currently 1)
    pub version: u32,
    
    /// All vertices in the graph
    pub vertices: Vec<SerializableVertex>,
    
    /// All edges in the graph
    pub edges: Vec<SerializableEdge>,
}

#[derive(Serialize, Deserialize)]
pub struct SerializableVertex {
    pub id: u64,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub struct SerializableEdge {
    pub id: u64,
    pub src: u64,
    pub dst: u64,
    pub label: String,
    pub properties: HashMap<String, Value>,
}
```

### Binary Layout

```
┌─────────────────────────────────────────────────────┐
│ Offset │ Size    │ Field                            │
├────────┼─────────┼──────────────────────────────────┤
│ 0      │ 4 bytes │ Magic "ISGR"                     │
│ 4      │ 4 bytes │ Format version (u32 LE)          │
│ 8      │ N bytes │ bincode-encoded SerializableGraph│
└─────────────────────────────────────────────────────┘
```

The magic bytes and version are prepended before bincode serialization to allow:
1. Quick format detection without full deserialization
2. Version checking before attempting to load

## Rust API

### Conversion Functions

```rust
// src/wasm/persistence.rs

impl SerializableGraph {
    /// Create from a graph snapshot (O(V + E))
    pub fn from_snapshot(snapshot: &GraphSnapshot) -> Self {
        let vertices = snapshot
            .all_vertices()
            .map(|v| SerializableVertex {
                id: v.id.0,
                label: v.label,
                properties: v.properties,
            })
            .collect();

        let edges = snapshot
            .all_edges()
            .map(|e| SerializableEdge {
                id: e.id.0,
                src: e.src.0,
                dst: e.dst.0,
                label: e.label,
                properties: e.properties,
            })
            .collect();

        Self {
            version: FORMAT_VERSION,
            vertices,
            edges,
        }
    }

    /// Load into an existing graph, returning ID mappings
    /// 
    /// Note: Clears the graph before loading
    pub fn load_into(&self, graph: &Graph) -> Result<IdMappings, PersistenceError> {
        // Implementation adds vertices/edges and tracks old->new ID mappings
        // in case IDs need to be remapped
    }
}

/// Maps old IDs to new IDs (in case of ID conflicts)
pub struct IdMappings {
    pub vertices: HashMap<u64, VertexId>,
    pub edges: HashMap<u64, EdgeId>,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    
    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u32),
    
    #[error("deserialization failed: {0}")]
    DeserializationFailed(String),
    
    #[error("serialization failed: {0}")]
    SerializationFailed(String),
    
    #[error("graph integrity error: {0}")]
    IntegrityError(String),
}
```

### WASM Bindings

```rust
// src/wasm/mod.rs (extend existing WasmGraph)

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl WasmGraph {
    /// Serialize graph to bytes for storage
    /// 
    /// Returns a Uint8Array suitable for IndexedDB
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Result<Vec<u8>, JsValue> {
        let snapshot = self.graph.snapshot();
        let serializable = SerializableGraph::from_snapshot(&snapshot);
        
        let mut bytes = Vec::with_capacity(8 + 1024); // magic + version + estimate
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        
        bincode::serialize_into(&mut bytes, &serializable)
            .map_err(|e| JsValue::from_str(&format!("Serialization failed: {}", e)))?;
        
        Ok(bytes)
    }

    /// Create a graph from previously saved bytes
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<WasmGraph, JsValue> {
        // Verify magic
        if bytes.len() < 8 {
            return Err(JsValue::from_str("Data too short"));
        }
        if &bytes[0..4] != MAGIC {
            return Err(JsValue::from_str("Invalid format: not an Interstellar graph"));
        }
        
        // Check version
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(JsValue::from_str(&format!(
                "Unsupported version: {} (expected {})", 
                version, FORMAT_VERSION
            )));
        }
        
        // Deserialize
        let serializable: SerializableGraph = bincode::deserialize(&bytes[8..])
            .map_err(|e| JsValue::from_str(&format!("Deserialization failed: {}", e)))?;
        
        // Build graph
        let graph = Graph::new();
        serializable.load_into(&graph)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(WasmGraph { 
            graph: Arc::new(graph) 
        })
    }
    
    /// Get the serialized size in bytes (without actually serializing)
    /// Useful for progress estimation
    #[wasm_bindgen(js_name = estimatedSize)]
    pub fn estimated_size(&self) -> usize {
        let snapshot = self.graph.snapshot();
        // Rough estimate: 80 bytes per vertex, 60 bytes per edge
        let v_count = snapshot.vertex_count() as usize;
        let e_count = snapshot.edge_count() as usize;
        8 + (v_count * 80) + (e_count * 60)
    }
}
```

## JavaScript API

### IndexedDB Helper Module

```typescript
// interstellar-persistence.ts

const DB_NAME = 'interstellar-graphs';
const DB_VERSION = 1;
const STORE_NAME = 'graphs';

interface GraphMetadata {
  name: string;
  savedAt: Date;
  vertexCount: number;
  edgeCount: number;
  sizeBytes: number;
}

/**
 * Initialize the IndexedDB database
 */
async function initDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
    
    request.onupgradeneeded = (event) => {
      const db = (event.target as IDBOpenDBRequest).result;
      
      // Main graph data store
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
      
      // Metadata store for listing saved graphs
      if (!db.objectStoreNames.contains('metadata')) {
        const metaStore = db.createObjectStore('metadata', { keyPath: 'name' });
        metaStore.createIndex('savedAt', 'savedAt');
      }
    };
  });
}

/**
 * Save a graph to IndexedDB
 */
async function saveGraph(
  graph: WasmGraph, 
  name: string
): Promise<void> {
  const db = await initDB();
  const bytes = graph.toBytes();
  
  const tx = db.transaction([STORE_NAME, 'metadata'], 'readwrite');
  
  // Save graph data
  tx.objectStore(STORE_NAME).put(bytes, name);
  
  // Save metadata
  const metadata: GraphMetadata = {
    name,
    savedAt: new Date(),
    vertexCount: graph.vertexCount(),
    edgeCount: graph.edgeCount(),
    sizeBytes: bytes.length,
  };
  tx.objectStore('metadata').put(metadata);
  
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

/**
 * Load a graph from IndexedDB
 */
async function loadGraph(name: string): Promise<WasmGraph | null> {
  const db = await initDB();
  
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readonly');
    const request = tx.objectStore(STORE_NAME).get(name);
    
    request.onsuccess = () => {
      const bytes = request.result;
      if (!bytes) {
        resolve(null);
        return;
      }
      
      try {
        const graph = WasmGraph.fromBytes(new Uint8Array(bytes));
        resolve(graph);
      } catch (e) {
        reject(e);
      }
    };
    
    request.onerror = () => reject(request.error);
  });
}

/**
 * Delete a saved graph
 */
async function deleteGraph(name: string): Promise<void> {
  const db = await initDB();
  const tx = db.transaction([STORE_NAME, 'metadata'], 'readwrite');
  
  tx.objectStore(STORE_NAME).delete(name);
  tx.objectStore('metadata').delete(name);
  
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

/**
 * List all saved graphs
 */
async function listGraphs(): Promise<GraphMetadata[]> {
  const db = await initDB();
  
  return new Promise((resolve, reject) => {
    const tx = db.transaction('metadata', 'readonly');
    const request = tx.objectStore('metadata').getAll();
    
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Check if a graph exists
 */
async function graphExists(name: string): Promise<boolean> {
  const db = await initDB();
  
  return new Promise((resolve, reject) => {
    const tx = db.transaction('metadata', 'readonly');
    const request = tx.objectStore('metadata').get(name);
    
    request.onsuccess = () => resolve(request.result !== undefined);
    request.onerror = () => reject(request.error);
  });
}

export {
  initDB,
  saveGraph,
  loadGraph,
  deleteGraph,
  listGraphs,
  graphExists,
  GraphMetadata,
};
```

### Usage Example

```typescript
import init, { WasmGraph } from 'interstellar-wasm';
import { saveGraph, loadGraph, listGraphs } from './interstellar-persistence';

async function main() {
  await init();
  
  // Try to load existing graph
  let graph = await loadGraph('my-graph');
  
  if (!graph) {
    // Create new graph if none exists
    console.log('Creating new graph...');
    graph = new WasmGraph();
    
    // Add some data
    const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
    const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
    graph.addEdge(alice, bob, 'knows', { since: 2020 });
    
    // Save for next time
    await saveGraph(graph, 'my-graph');
    console.log('Graph saved!');
  } else {
    console.log('Loaded existing graph');
    console.log(`Vertices: ${graph.vertexCount()}, Edges: ${graph.edgeCount()}`);
  }
  
  // List all saved graphs
  const saved = await listGraphs();
  console.log('Saved graphs:', saved);
}

main();
```

## Performance Considerations

### Expected Performance

| Graph Size | Serialize | Deserialize | IndexedDB Write | IndexedDB Read | Total Save | Total Load |
|------------|-----------|-------------|-----------------|----------------|------------|------------|
| 1K V, 5K E | <5ms | <10ms | <20ms | <10ms | <25ms | <20ms |
| 10K V, 50K E | <20ms | <50ms | <50ms | <30ms | <70ms | <80ms |
| 100K V, 500K E | <150ms | <300ms | <200ms | <100ms | <350ms | <400ms |

### Optimization: Web Worker

For graphs > 10K elements, run persistence in a Web Worker to avoid blocking the main thread:

```typescript
// persistence-worker.ts
import init, { WasmGraph } from 'interstellar-wasm';

self.onmessage = async (e: MessageEvent) => {
  await init();
  
  const { action, name, bytes } = e.data;
  
  if (action === 'save') {
    const graph = WasmGraph.fromBytes(bytes);
    // ... save to IndexedDB
    self.postMessage({ success: true });
  } else if (action === 'load') {
    // ... load from IndexedDB
    const graph = await loadFromDB(name);
    const bytes = graph.toBytes();
    self.postMessage({ success: true, bytes });
  }
};
```

### Optimization: Compression (Future)

For large graphs, add optional LZ4 compression:

```rust
#[wasm_bindgen(js_name = toBytesCompressed)]
pub fn to_bytes_compressed(&self) -> Result<Vec<u8>, JsValue> {
    let bytes = self.to_bytes()?;
    // LZ4 compress
    let compressed = lz4_flex::compress_prepend_size(&bytes);
    Ok(compressed)
}
```

Typical compression ratios: 2-4x for property-heavy graphs.

## Storage Limits

| Browser | IndexedDB Limit | Notes |
|---------|-----------------|-------|
| Chrome | 60% of disk | Evicted under storage pressure |
| Firefox | 50% of disk | Up to 2GB per origin |
| Safari | 1GB | Prompts user after 200MB |

For persistent storage that won't be evicted:

```typescript
if (navigator.storage && navigator.storage.persist) {
  const granted = await navigator.storage.persist();
  if (granted) {
    console.log('Storage will not be evicted');
  }
}
```

## Testing Strategy

### Unit Tests (Rust)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn roundtrip_empty_graph() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let serializable = SerializableGraph::from_snapshot(&snapshot);
        
        let bytes = serialize(&serializable);
        let restored: SerializableGraph = deserialize(&bytes);
        
        assert_eq!(restored.vertices.len(), 0);
        assert_eq!(restored.edges.len(), 0);
    }
    
    #[test]
    fn roundtrip_with_properties() {
        let graph = Graph::new();
        let id = graph.add_vertex("person", HashMap::from([
            ("name".to_string(), Value::String("Alice".into())),
            ("age".to_string(), Value::Int(30)),
            ("scores".to_string(), Value::List(vec![
                Value::Int(95), Value::Int(87), Value::Int(92)
            ])),
        ]));
        
        // Serialize and restore
        let snapshot = graph.snapshot();
        let serializable = SerializableGraph::from_snapshot(&snapshot);
        let bytes = serialize(&serializable);
        
        let graph2 = Graph::new();
        let restored: SerializableGraph = deserialize(&bytes);
        restored.load_into(&graph2).unwrap();
        
        let v = graph2.snapshot().get_vertex(VertexId(0)).unwrap();
        assert_eq!(v.properties.get("name"), Some(&Value::String("Alice".into())));
    }
    
    #[test]
    fn rejects_invalid_magic() {
        let bad_bytes = vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];
        let result = WasmGraph::from_bytes(&bad_bytes);
        assert!(result.is_err());
    }
    
    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&99u32.to_le_bytes()); // Future version
        
        let result = WasmGraph::from_bytes(&bytes);
        assert!(result.is_err());
    }
}
```

### Integration Tests (wasm-bindgen-test)

```rust
#[cfg(target_arch = "wasm32")]
mod wasm_tests {
    use wasm_bindgen_test::*;
    
    wasm_bindgen_test_configure!(run_in_browser);
    
    #[wasm_bindgen_test]
    fn test_to_bytes_from_bytes_roundtrip() {
        let graph = WasmGraph::new();
        graph.add_vertex("test", JsValue::from_str("{}"));
        
        let bytes = graph.to_bytes().unwrap();
        let restored = WasmGraph::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.vertex_count(), 1);
    }
}
```

### Browser Tests (JavaScript)

```typescript
describe('IndexedDB Persistence', () => {
  beforeEach(async () => {
    // Clear test database
    await deleteGraph('test-graph');
  });
  
  it('saves and loads a graph', async () => {
    const graph = new WasmGraph();
    const v1 = graph.addVertex('person', { name: 'Alice' });
    const v2 = graph.addVertex('person', { name: 'Bob' });
    graph.addEdge(v1, v2, 'knows', {});
    
    await saveGraph(graph, 'test-graph');
    
    const loaded = await loadGraph('test-graph');
    expect(loaded).not.toBeNull();
    expect(loaded!.vertexCount()).toBe(2);
    expect(loaded!.edgeCount()).toBe(1);
  });
  
  it('returns null for non-existent graph', async () => {
    const loaded = await loadGraph('does-not-exist');
    expect(loaded).toBeNull();
  });
  
  it('lists saved graphs', async () => {
    const graph = new WasmGraph();
    await saveGraph(graph, 'test-graph');
    
    const list = await listGraphs();
    expect(list.some(g => g.name === 'test-graph')).toBe(true);
  });
});
```

## File Structure

```
src/
├── wasm/
│   ├── mod.rs              # Existing WASM bindings
│   └── persistence.rs      # NEW: Serialization types and impl
│
pkg/                        # wasm-pack output
│   └── interstellar_wasm.js
│
examples/
│   └── wasm-indexeddb/     # NEW: Example project
│       ├── index.html
│       ├── main.ts
│       └── interstellar-persistence.ts
```

## Implementation Phases

### Phase 1: Core Serialization
- [ ] Add `SerializableGraph`, `SerializableVertex`, `SerializableEdge` types
- [ ] Implement `from_snapshot()` conversion
- [ ] Implement `load_into()` restoration
- [ ] Add `PersistenceError` type
- [ ] Unit tests for roundtrip

### Phase 2: WASM Bindings
- [ ] Add `to_bytes()` to `WasmGraph`
- [ ] Add `from_bytes()` to `WasmGraph`
- [ ] Add `estimated_size()` helper
- [ ] wasm-bindgen-test integration tests

### Phase 3: JavaScript Helpers
- [ ] Create `interstellar-persistence.ts` module
- [ ] Implement `saveGraph`, `loadGraph`, `deleteGraph`, `listGraphs`
- [ ] Add TypeScript types
- [ ] Browser tests

### Phase 4: Example & Documentation
- [ ] Create `examples/wasm-indexeddb/` example project
- [ ] Add documentation to main README
- [ ] Add to wasm-pack package

## Future Enhancements

1. **Incremental saves**: Only persist changes (deltas)
2. **Compression**: LZ4 for large graphs
3. **Encryption**: Encrypt before storing (for sensitive data)
4. **Multi-tab sync**: BroadcastChannel to sync across tabs
5. **Export formats**: JSON, GraphML, GraphSON export options
