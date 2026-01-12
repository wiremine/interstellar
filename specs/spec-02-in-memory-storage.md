# Spec 02: In-Memory Storage (Phase 2)

**Status**: ✅ **IMPLEMENTED** (with deviations from original spec)

**Implementation Notes**:
- Label indexing is implemented **inline** within `InMemoryGraph` using `HashMap<u32, RoaringBitmap>`
- No separate `src/index/` module was created (that abstraction was removed as unnecessary)
- The `try_mutate()` method was added to `Graph` for non-blocking write lock acquisition
- `traversal()` method was moved from `Graph` to `GraphSnapshot` for proper lock semantics

See commit history for actual implementation details.

---

Implements the fast, non-persistent HashMap-based storage backend. Builds on Phase 1 core types and provides a complete `GraphStorage` implementation.

---

## Goals

- Implement `InMemoryGraph` as a complete `GraphStorage` backend
- Provide O(1) vertex/edge lookup by ID
- Maintain label indexes using `RoaringBitmap` for efficient label-based queries
- Support adjacency list traversal with O(degree) complexity
- Enable mutation operations (add/remove vertices and edges)
- Integrate string interning for labels

## Scope

| File | Action | Description |
|------|--------|-------------|
| `src/storage/inmemory.rs` | **Create** | `InMemoryGraph` implementation |
| `src/index/mod.rs` | **Update** | Add `LabelIndexTrait` and re-export `LabelIndex` |
| `src/index/label.rs` | **Create** | `RoaringBitmap`-based label index |
| `src/graph.rs` | **Update** | Add `Graph::in_memory()` and `Graph::storage()` |
| `src/storage/mod.rs` | **Update** | Add `pub mod inmemory` and re-export `InMemoryGraph` |
| `src/storage/interner.rs` | **Update** | Add `lookup()`, `len()`, `is_empty()`, `Default` impl |

## Dependencies

Requires these crates (add to `Cargo.toml` if not present):

```toml
[dependencies]
roaring = "0.10"
hashbrown = "0.14"
parking_lot = "0.12"
```

---

## File Specifications

**Note on existing code**: Phase 1 (spec-01) established the following which this spec builds upon:
- `src/value.rs`: `VertexId`, `EdgeId`, `ElementId`, `Value` types
- `src/error.rs`: `StorageError`, `TraversalError` enums
- `src/storage/mod.rs`: `Vertex`, `Edge` structs and `GraphStorage` trait
- `src/storage/interner.rs`: `StringInterner` with `new()`, `intern()`, `resolve()`
- `src/graph.rs`: `Graph`, `GraphSnapshot`, `GraphMut` types with `new()`, `traversal()`, `snapshot()`, `mutate()`

### `src/storage/inmemory.rs` (Create)

**Purpose**: HashMap-based in-memory graph storage with O(1) lookups.

#### Data Structures

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use roaring::RoaringBitmap;

use crate::error::StorageError;
use crate::storage::interner::StringInterner;
use crate::storage::{Edge, GraphStorage, Vertex};
use crate::value::{EdgeId, Value, VertexId};

/// In-memory graph storage with HashMap-based lookups
pub struct InMemoryGraph {
    /// Vertex data keyed by ID
    nodes: HashMap<VertexId, NodeData>,
    
    /// Edge data keyed by ID
    edges: HashMap<EdgeId, EdgeData>,
    
    /// Next vertex ID (atomic for future thread-safety)
    next_vertex_id: AtomicU64,
    
    /// Next edge ID (atomic for future thread-safety)
    next_edge_id: AtomicU64,
    
    /// Label ID -> set of vertex IDs with that label
    vertex_labels: HashMap<u32, RoaringBitmap>,
    
    /// Label ID -> set of edge IDs with that label
    edge_labels: HashMap<u32, RoaringBitmap>,
    
    /// String interning for labels
    string_table: StringInterner,
}

/// Internal vertex representation
#[derive(Clone, Debug)]
struct NodeData {
    /// Vertex identifier
    id: VertexId,
    
    /// Interned label string ID
    label_id: u32,
    
    /// Property key-value pairs
    properties: HashMap<String, Value>,
    
    /// Outgoing edge IDs (adjacency list)
    out_edges: Vec<EdgeId>,
    
    /// Incoming edge IDs (adjacency list)  
    in_edges: Vec<EdgeId>,
}

/// Internal edge representation
#[derive(Clone, Debug)]
struct EdgeData {
    /// Edge identifier
    id: EdgeId,
    
    /// Interned label string ID
    label_id: u32,
    
    /// Source vertex ID
    src: VertexId,
    
    /// Destination vertex ID
    dst: VertexId,
    
    /// Property key-value pairs
    properties: HashMap<String, Value>,
}
```

#### Constructor

```rust
impl InMemoryGraph {
    /// Create a new empty in-memory graph
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            next_vertex_id: AtomicU64::new(0),
            next_edge_id: AtomicU64::new(0),
            vertex_labels: HashMap::new(),
            edge_labels: HashMap::new(),
            string_table: StringInterner::new(),
        }
    }
}

impl Default for InMemoryGraph {
    fn default() -> Self {
        Self::new()
    }
}
```

#### Mutation Methods

```rust
impl InMemoryGraph {
    /// Add a vertex with the given label and properties
    /// Returns the new vertex's ID
    /// 
    /// Complexity: O(1) amortized
    pub fn add_vertex(
        &mut self, 
        label: &str, 
        properties: HashMap<String, Value>
    ) -> VertexId {
        let id = VertexId(self.next_vertex_id.fetch_add(1, Ordering::Relaxed));
        let label_id = self.string_table.intern(label);
        
        let node = NodeData {
            id,
            label_id,
            properties,
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        };
        
        self.nodes.insert(id, node);
        
        // Update label index
        self.vertex_labels
            .entry(label_id)
            .or_insert_with(RoaringBitmap::new)
            .insert(id.0 as u32);
        
        id
    }
    
    /// Add an edge between two vertices with the given label and properties
    /// Returns the new edge's ID, or error if source/destination vertices don't exist
    /// 
    /// Complexity: O(1)
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        // Validate vertices exist
        if !self.nodes.contains_key(&src) {
            return Err(StorageError::VertexNotFound(src));
        }
        if !self.nodes.contains_key(&dst) {
            return Err(StorageError::VertexNotFound(dst));
        }
        
        let id = EdgeId(self.next_edge_id.fetch_add(1, Ordering::Relaxed));
        let label_id = self.string_table.intern(label);
        
        let edge = EdgeData {
            id,
            label_id,
            src,
            dst,
            properties,
        };
        
        self.edges.insert(id, edge);
        
        // Update adjacency lists
        self.nodes.get_mut(&src).unwrap().out_edges.push(id);
        self.nodes.get_mut(&dst).unwrap().in_edges.push(id);
        
        // Update label index
        self.edge_labels
            .entry(label_id)
            .or_insert_with(RoaringBitmap::new)
            .insert(id.0 as u32);
        
        Ok(id)
    }
    
    /// Remove a vertex and all its incident edges
    /// Returns error if vertex doesn't exist
    /// 
    /// Complexity: O(degree) where degree = in_degree + out_degree
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        let node = self.nodes.remove(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        // Remove from label index
        if let Some(bitmap) = self.vertex_labels.get_mut(&node.label_id) {
            bitmap.remove(id.0 as u32);
        }
        
        // Collect incident edges to remove
        let edges_to_remove: Vec<EdgeId> = node.out_edges
            .iter()
            .chain(node.in_edges.iter())
            .copied()
            .collect();
        
        // Remove all incident edges
        for edge_id in edges_to_remove {
            // Ignore errors (edge may already be processed if self-loop)
            let _ = self.remove_edge_internal(edge_id, Some(id));
        }
        
        Ok(())
    }
    
    /// Remove an edge
    /// Returns error if edge doesn't exist
    /// 
    /// Complexity: O(degree) due to adjacency list removal
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError> {
        self.remove_edge_internal(id, None)
    }
    
    /// Internal edge removal, optionally skipping a vertex being deleted
    fn remove_edge_internal(
        &mut self, 
        id: EdgeId, 
        skip_vertex: Option<VertexId>
    ) -> Result<(), StorageError> {
        let edge = self.edges.remove(&id)
            .ok_or(StorageError::EdgeNotFound(id))?;
        
        // Remove from label index
        if let Some(bitmap) = self.edge_labels.get_mut(&edge.label_id) {
            bitmap.remove(id.0 as u32);
        }
        
        // Remove from source vertex's out_edges (if not being deleted)
        if skip_vertex != Some(edge.src) {
            if let Some(src_node) = self.nodes.get_mut(&edge.src) {
                src_node.out_edges.retain(|&e| e != id);
            }
        }
        
        // Remove from destination vertex's in_edges (if not being deleted)
        if skip_vertex != Some(edge.dst) {
            if let Some(dst_node) = self.nodes.get_mut(&edge.dst) {
                dst_node.in_edges.retain(|&e| e != id);
            }
        }
        
        Ok(())
    }
}
```

#### GraphStorage Implementation

```rust
impl GraphStorage for InMemoryGraph {
    /// O(1) vertex lookup
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.nodes.get(&id)?;
        let label = self.string_table.resolve(node.label_id)?;
        
        Some(Vertex {
            id: node.id,
            label: label.to_string(),
            properties: node.properties.clone(),
        })
    }
    
    /// O(1) count
    fn vertex_count(&self) -> u64 {
        self.nodes.len() as u64
    }
    
    /// O(1) edge lookup
    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let edge = self.edges.get(&id)?;
        let label = self.string_table.resolve(edge.label_id)?;
        
        Some(Edge {
            id: edge.id,
            label: label.to_string(),
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        })
    }
    
    /// O(1) count
    fn edge_count(&self) -> u64 {
        self.edges.len() as u64
    }
    
    /// O(degree) iteration over outgoing edges
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self.nodes
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.out_edges.iter())
            .filter_map(|&edge_id| self.get_edge(edge_id));
        
        Box::new(iter)
    }
    
    /// O(degree) iteration over incoming edges
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self.nodes
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.in_edges.iter())
            .filter_map(|&edge_id| self.get_edge(edge_id));
        
        Box::new(iter)
    }
    
    /// O(n) where n = vertices with label (uses RoaringBitmap)
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Look up label ID without interning (read-only)
        let label_id = self.string_table.lookup(label);
        
        let iter = label_id
            .and_then(|id| self.vertex_labels.get(&id))
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .filter_map(|id| self.get_vertex(VertexId(id as u64)));
        
        Box::new(iter)
    }
    
    /// O(n) where n = edges with label (uses RoaringBitmap)
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Look up label ID without interning (read-only)
        let label_id = self.string_table.lookup(label);
        
        let iter = label_id
            .and_then(|id| self.edge_labels.get(&id))
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .filter_map(|id| self.get_edge(EdgeId(id as u64)));
        
        Box::new(iter)
    }
    
    /// O(n) full vertex scan
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let iter = self.nodes.keys()
            .filter_map(|&id| self.get_vertex(id));
        
        Box::new(iter)
    }
    
    /// O(m) full edge scan
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self.edges.keys()
            .filter_map(|&id| self.get_edge(id));
        
        Box::new(iter)
    }
}
```

#### Thread Safety

```rust
// SAFETY: InMemoryGraph is Send + Sync because:
// - HashMap is Send + Sync when K, V are Send + Sync
// - AtomicU64 is Send + Sync  
// - RoaringBitmap is Send + Sync
// - StringInterner is Send + Sync (HashMap-based)
//
// Note: InMemoryGraph itself does NOT provide interior mutability.
// Thread-safe mutation requires external synchronization (via Graph wrapper).
unsafe impl Send for InMemoryGraph {}
unsafe impl Sync for InMemoryGraph {}
```

---

### `src/index/mod.rs` (Update)

**Purpose**: Add index trait definitions and re-export label index.

Replace the existing placeholder with:

```rust
mod label;

pub use label::LabelIndex;

use roaring::RoaringBitmap;

/// Trait for label-based indexing
pub trait LabelIndexTrait {
    /// Add an element ID to the index for the given label
    fn add(&mut self, label_id: u32, element_id: u64);
    
    /// Remove an element ID from the index
    fn remove(&mut self, label_id: u32, element_id: u64);
    
    /// Get all element IDs for a label
    fn get(&self, label_id: u32) -> Option<&RoaringBitmap>;
    
    /// Check if an element exists for a label
    fn contains(&self, label_id: u32, element_id: u64) -> bool;
    
    /// Count elements for a label
    fn count(&self, label_id: u32) -> u64;
}
```

---

### `src/index/label.rs` (Create)

**Purpose**: RoaringBitmap-based label index implementation.

```rust
use std::collections::HashMap;
use roaring::RoaringBitmap;
use super::LabelIndexTrait;

/// Label index using RoaringBitmap for efficient set operations
/// 
/// Maps label_id -> set of element IDs (as u32, fitting RoaringBitmap)
/// 
/// ## Complexity
/// - add: O(1) amortized
/// - remove: O(1)  
/// - get: O(1)
/// - contains: O(1)
/// - count: O(1)
/// - iteration: O(n) where n = elements with label
pub struct LabelIndex {
    /// label_id -> bitmap of element IDs
    index: HashMap<u32, RoaringBitmap>,
}

impl LabelIndex {
    /// Create a new empty label index
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }
    
    /// Get iterator over all (label_id, bitmap) pairs
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &RoaringBitmap)> {
        self.index.iter()
    }
    
    /// Clear all index entries
    pub fn clear(&mut self) {
        self.index.clear();
    }
}

impl Default for LabelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl LabelIndexTrait for LabelIndex {
    fn add(&mut self, label_id: u32, element_id: u64) {
        self.index
            .entry(label_id)
            .or_insert_with(RoaringBitmap::new)
            .insert(element_id as u32);
    }
    
    fn remove(&mut self, label_id: u32, element_id: u64) {
        if let Some(bitmap) = self.index.get_mut(&label_id) {
            bitmap.remove(element_id as u32);
            
            // Optionally remove empty bitmaps to save memory
            // if bitmap.is_empty() {
            //     self.index.remove(&label_id);
            // }
        }
    }
    
    fn get(&self, label_id: u32) -> Option<&RoaringBitmap> {
        self.index.get(&label_id)
    }
    
    fn contains(&self, label_id: u32, element_id: u64) -> bool {
        self.index
            .get(&label_id)
            .map(|b| b.contains(element_id as u32))
            .unwrap_or(false)
    }
    
    fn count(&self, label_id: u32) -> u64 {
        self.index
            .get(&label_id)
            .map(|b| b.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn add_and_lookup() {
        let mut index = LabelIndex::new();
        index.add(1, 100);
        index.add(1, 200);
        index.add(2, 100);
        
        assert!(index.contains(1, 100));
        assert!(index.contains(1, 200));
        assert!(index.contains(2, 100));
        assert!(!index.contains(2, 200));
        
        assert_eq!(index.count(1), 2);
        assert_eq!(index.count(2), 1);
        assert_eq!(index.count(3), 0);
    }
    
    #[test]
    fn remove_element() {
        let mut index = LabelIndex::new();
        index.add(1, 100);
        index.add(1, 200);
        
        index.remove(1, 100);
        
        assert!(!index.contains(1, 100));
        assert!(index.contains(1, 200));
        assert_eq!(index.count(1), 1);
    }
    
    #[test]
    fn get_bitmap_iteration() {
        let mut index = LabelIndex::new();
        index.add(1, 100);
        index.add(1, 200);
        index.add(1, 300);
        
        let bitmap = index.get(1).unwrap();
        let ids: Vec<u32> = bitmap.iter().collect();
        
        assert_eq!(ids, vec![100, 200, 300]);
    }
}
```

---

### `src/storage/interner.rs` (Update)

**Purpose**: Add `lookup()` method and utility functions for read-only label lookup.

Add the following methods to the existing `StringInterner` (do not change existing fields or methods):

```rust
impl StringInterner {
    // ... existing new(), intern(), resolve() methods remain unchanged ...
    
    /// Look up a string's ID without interning (read-only)
    /// Returns None if the string has not been interned
    pub fn lookup(&self, value: &str) -> Option<u32> {
        self.forward.get(value).copied()
    }
    
    /// Number of interned strings
    pub fn len(&self) -> usize {
        self.forward.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // ... existing tests remain ...
    
    #[test]
    fn lookup_without_interning() {
        let mut interner = StringInterner::new();
        interner.intern("exists");
        
        assert_eq!(interner.lookup("exists"), Some(0));
        assert_eq!(interner.lookup("missing"), None);
    }
    
    #[test]
    fn len_and_is_empty() {
        let mut interner = StringInterner::new();
        assert!(interner.is_empty());
        assert_eq!(interner.len(), 0);
        
        interner.intern("one");
        interner.intern("two");
        interner.intern("one"); // duplicate
        
        assert!(!interner.is_empty());
        assert_eq!(interner.len(), 2);
    }
}
```

**Note**: The `forward` field remains private. The `lookup()` method provides the read-only access needed by `InMemoryGraph::vertices_with_label()` and `edges_with_label()`.

---

### `src/storage/mod.rs` (Update)

**Purpose**: Add `inmemory` module and re-export `InMemoryGraph`.

Add the following to the existing file (existing `Vertex`, `Edge`, `GraphStorage` definitions remain unchanged):

```rust
// Add module declaration at the top
pub mod inmemory;

// Add re-export after module declarations
pub use inmemory::InMemoryGraph;
```

The existing definitions (`Vertex`, `Edge`, `GraphStorage` trait) remain as-is from Phase 1.

---

### `src/graph.rs` (Update)

**Purpose**: Add `Graph::in_memory()` constructor and `Graph::storage()` accessor.

Add the following methods to the existing `Graph` impl block:

```rust
use crate::storage::InMemoryGraph;  // Add to imports

impl Graph {
    // ... existing new(), traversal(), snapshot(), mutate() methods remain unchanged ...
    
    /// Create a new in-memory graph (no persistence)
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryGraph::new()))
    }
    
    /// Get the underlying storage (for advanced use cases)
    pub fn storage(&self) -> &Arc<dyn GraphStorage> {
        &self.storage
    }
}
```

The existing `Graph`, `GraphSnapshot`, and `GraphMut` definitions remain as-is from Phase 1.

---

## Test Specifications

### Unit Tests (`src/storage/inmemory.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn new_graph_is_empty() {
        let graph = InMemoryGraph::new();
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }
    
    #[test]
    fn add_vertex_returns_unique_ids() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("software", HashMap::new());
        
        assert_ne!(v1, v2);
        assert_ne!(v2, v3);
        assert_eq!(graph.vertex_count(), 3);
    }
    
    #[test]
    fn add_vertex_with_properties() {
        let mut graph = InMemoryGraph::new();
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        
        let id = graph.add_vertex("person", props);
        let vertex = graph.get_vertex(id).unwrap();
        
        assert_eq!(vertex.label, "person");
        assert_eq!(vertex.properties.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
    }
    
    #[test]
    fn get_vertex_returns_none_for_missing() {
        let graph = InMemoryGraph::new();
        assert!(graph.get_vertex(VertexId(999)).is_none());
    }
    
    #[test]
    fn add_edge_connects_vertices() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        
        let edge_id = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        let edge = graph.get_edge(edge_id).unwrap();
        
        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v2);
        assert_eq!(edge.label, "knows");
        assert_eq!(graph.edge_count(), 1);
    }
    
    #[test]
    fn add_edge_fails_for_missing_source() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        
        let result = graph.add_edge(VertexId(999), v1, "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }
    
    #[test]
    fn add_edge_fails_for_missing_destination() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        
        let result = graph.add_edge(v1, VertexId(999), "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }
    
    #[test]
    fn out_edges_returns_outgoing() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());
        
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();
        
        let out: Vec<Edge> = graph.out_edges(v1).collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|e| e.src == v1));
    }
    
    #[test]
    fn in_edges_returns_incoming() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());
        
        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v3, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        
        let incoming: Vec<Edge> = graph.in_edges(v1).collect();
        assert_eq!(incoming.len(), 2);
        assert!(incoming.iter().all(|e| e.dst == v1));
    }
    
    #[test]
    fn vertices_with_label_filters_correctly() {
        let mut graph = InMemoryGraph::new();
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("software", HashMap::new());
        
        let people: Vec<Vertex> = graph.vertices_with_label("person").collect();
        let software: Vec<Vertex> = graph.vertices_with_label("software").collect();
        let unknown: Vec<Vertex> = graph.vertices_with_label("unknown").collect();
        
        assert_eq!(people.len(), 2);
        assert_eq!(software.len(), 1);
        assert_eq!(unknown.len(), 0);
    }
    
    #[test]
    fn edges_with_label_filters_correctly() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("software", HashMap::new());
        
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "created", HashMap::new()).unwrap();
        
        let knows: Vec<Edge> = graph.edges_with_label("knows").collect();
        let created: Vec<Edge> = graph.edges_with_label("created").collect();
        
        assert_eq!(knows.len(), 1);
        assert_eq!(created.len(), 1);
    }
    
    #[test]
    fn remove_vertex_removes_incident_edges() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());
        
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v3, v1, "knows", HashMap::new()).unwrap();
        
        graph.remove_vertex(v1).unwrap();
        
        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1); // Only v2->v3 remains
        assert!(graph.get_vertex(v1).is_none());
    }
    
    #[test]
    fn remove_edge_updates_adjacency() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        
        let e1 = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        let e2 = graph.add_edge(v1, v2, "likes", HashMap::new()).unwrap();
        
        graph.remove_edge(e1).unwrap();
        
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.get_edge(e1).is_none());
        assert!(graph.get_edge(e2).is_some());
        
        let out: Vec<Edge> = graph.out_edges(v1).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].label, "likes");
    }
    
    #[test]
    fn all_vertices_iterates_all() {
        let mut graph = InMemoryGraph::new();
        graph.add_vertex("a", HashMap::new());
        graph.add_vertex("b", HashMap::new());
        graph.add_vertex("c", HashMap::new());
        
        let all: Vec<Vertex> = graph.all_vertices().collect();
        assert_eq!(all.len(), 3);
    }
    
    #[test]
    fn all_edges_iterates_all() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("a", HashMap::new());
        let v2 = graph.add_vertex("b", HashMap::new());
        
        graph.add_edge(v1, v2, "e1", HashMap::new()).unwrap();
        graph.add_edge(v2, v1, "e2", HashMap::new()).unwrap();
        
        let all: Vec<Edge> = graph.all_edges().collect();
        assert_eq!(all.len(), 2);
    }
    
    #[test]
    fn self_loop_edge() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        
        let e = graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();
        
        let out: Vec<Edge> = graph.out_edges(v1).collect();
        let in_edges: Vec<Edge> = graph.in_edges(v1).collect();
        
        assert_eq!(out.len(), 1);
        assert_eq!(in_edges.len(), 1);
        assert_eq!(out[0].id, e);
        assert_eq!(in_edges[0].id, e);
    }
    
    #[test]
    fn remove_vertex_with_self_loop() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();
        
        graph.remove_vertex(v1).unwrap();
        
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }
}
```

### Integration Tests (`tests/inmemory.rs`)

```rust
use intersteller::prelude::*;
use std::collections::HashMap;

#[test]
fn graph_in_memory_basic_usage() {
    let graph = Graph::in_memory();
    
    // Verify empty graph
    let snapshot = graph.snapshot();
    assert_eq!(snapshot.graph.storage.vertex_count(), 0);
}

#[test]
fn scale_test_10k_vertices_100k_edges() {
    let mut storage = intersteller::storage::InMemoryGraph::new();
    
    // Add 10,000 vertices
    let vertex_ids: Vec<_> = (0..10_000)
        .map(|i| {
            let mut props = HashMap::new();
            props.insert("index".to_string(), Value::Int(i));
            storage.add_vertex("node", props)
        })
        .collect();
    
    assert_eq!(storage.vertex_count(), 10_000);
    
    // Add 100,000 edges (random connections)
    let mut edge_count = 0;
    for i in 0..10_000 {
        for j in 0..10 {
            let src = vertex_ids[i];
            let dst = vertex_ids[(i + j + 1) % 10_000];
            storage.add_edge(src, dst, "connects", HashMap::new()).unwrap();
            edge_count += 1;
        }
    }
    
    assert_eq!(storage.edge_count(), edge_count);
    
    // Verify lookups work
    let v = storage.get_vertex(vertex_ids[5000]).unwrap();
    assert_eq!(v.properties.get("index"), Some(&Value::Int(5000)));
    
    // Verify adjacency
    let out: Vec<_> = storage.out_edges(vertex_ids[0]).collect();
    assert_eq!(out.len(), 10);
    
    // Verify label scan
    let all_nodes: Vec<_> = storage.vertices_with_label("node").collect();
    assert_eq!(all_nodes.len(), 10_000);
}

#[test]
fn label_index_performance() {
    let mut storage = intersteller::storage::InMemoryGraph::new();
    
    // Add mixed labels
    for _ in 0..1000 {
        storage.add_vertex("person", HashMap::new());
    }
    for _ in 0..500 {
        storage.add_vertex("software", HashMap::new());
    }
    for _ in 0..200 {
        storage.add_vertex("company", HashMap::new());
    }
    
    // Label scans should be efficient
    let people: Vec<_> = storage.vertices_with_label("person").collect();
    let software: Vec<_> = storage.vertices_with_label("software").collect();
    let companies: Vec<_> = storage.vertices_with_label("company").collect();
    
    assert_eq!(people.len(), 1000);
    assert_eq!(software.len(), 500);
    assert_eq!(companies.len(), 200);
}
```

---

## Complexity Guarantees

| Operation | Time Complexity | Space Complexity |
|-----------|-----------------|------------------|
| `get_vertex(id)` | O(1) | O(1) |
| `get_edge(id)` | O(1) | O(1) |
| `add_vertex` | O(1) amortized | O(1) |
| `add_edge` | O(1) | O(1) |
| `remove_vertex` | O(degree) | O(degree) temp |
| `remove_edge` | O(degree) | O(1) |
| `vertex_count` | O(1) | O(1) |
| `edge_count` | O(1) | O(1) |
| `out_edges(v)` | O(out_degree) | O(1) iterator |
| `in_edges(v)` | O(in_degree) | O(1) iterator |
| `vertices_with_label` | O(n) | O(1) iterator |
| `edges_with_label` | O(m) | O(1) iterator |
| `all_vertices` | O(n) | O(1) iterator |
| `all_edges` | O(m) | O(1) iterator |

Where n = vertex count, m = edge count.

---

## Exit Criteria

### Code Deliverables
- [ ] `src/storage/inmemory.rs` created with `InMemoryGraph` implementing `GraphStorage`
- [ ] `src/index/label.rs` created with `LabelIndex` implementing `LabelIndexTrait`
- [ ] `src/index/mod.rs` updated with `LabelIndexTrait` and `LabelIndex` re-export
- [ ] `src/storage/mod.rs` updated with `inmemory` module and `InMemoryGraph` re-export
- [ ] `src/storage/interner.rs` updated with `lookup()`, `len()`, `is_empty()`, `Default`
- [ ] `src/graph.rs` updated with `in_memory()` and `storage()` methods

### Functional Requirements
- [ ] O(1) vertex/edge lookup verified via tests
- [ ] Label indexes correctly filter vertices and edges
- [ ] Add/remove operations correctly update all indexes
- [ ] Self-loop edges handled correctly
- [ ] Vertex removal cascades to incident edge removal
- [ ] `Graph::in_memory()` returns functional graph instance

### Quality Gates
- [ ] All unit tests pass (`cargo test`)
- [ ] Integration test with 10K vertices, 100K edges passes
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo fmt --check` passes

---

## References

- `specs/implementation.md` (Phase 2: In-Memory Storage)
- `guilding-documents/storage.md` (Section 2: In-Memory Storage)
- `guilding-documents/overview.md` (Core Data Structures)
- `specs/spec-01-core-foundation.md` (Phase 1 types this builds on)
