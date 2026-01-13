# Plan 19: Property Index Implementation

Implementation plan for B+ tree and unique property indexes per [spec-18-property-indexes.md](../specs/spec-18-property-indexes.md).

---

## Overview

| Metric | Estimate |
|--------|----------|
| **Total Phases** | 6 |
| **Estimated Effort** | 3-4 days |
| **Files Changed** | ~15 |
| **New Files** | ~8 |
| **Test Coverage Target** | 100% branch |

---

## Phase 1: Core Data Structures (4-6 hours)

### 1.1 Index Types and Traits

Create `src/storage/index/mod.rs`:

```rust
// Module structure
pub mod btree;
pub mod unique;
pub mod error;

// Re-exports
pub use btree::BTreeIndex;
pub use unique::UniqueIndex;
pub use error::IndexError;
```

Create `src/storage/index/error.rs`:

```rust
use thiserror::Error;
use crate::value::Value;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index already exists: {0}")]
    IndexExists(String),
    
    #[error("index not found: {0}")]
    IndexNotFound(String),
    
    #[error("duplicate value in unique index '{index}': {value:?}")]
    DuplicateValue {
        index: String,
        value: Value,
        existing_id: u64,
        new_id: u64,
    },
    
    #[error("missing required property for index builder")]
    MissingProperty,
}
```

Create `src/storage/index/types.rs`:

```rust
/// Index specification
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
    pub element_type: ElementType,
    pub label: Option<String>,
    pub property: String,
    pub index_type: IndexType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementType {
    Vertex,
    Edge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Unique,
}

/// Fluent builder
pub struct IndexBuilder { ... }
```

### 1.2 PropertyIndex Trait

```rust
/// Type-erased index interface
pub trait PropertyIndex: Send + Sync {
    fn name(&self) -> &str;
    fn spec(&self) -> &IndexSpec;
    
    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_>;
    fn lookup_range(
        &self,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_>;
    
    fn insert(&mut self, value: Value, id: u64) -> Result<(), IndexError>;
    fn remove(&mut self, value: &Value, id: u64) -> Result<bool, IndexError>;
    
    fn cardinality(&self) -> u64;
    fn size(&self) -> u64;
    
    fn clone_box(&self) -> Box<dyn PropertyIndex>;
}
```

### 1.3 Tests for Phase 1

- `index_spec_builder_works`
- `index_spec_auto_generates_name`
- `element_type_equality`
- `index_type_equality`

### Deliverables

- [ ] `src/storage/index/mod.rs`
- [ ] `src/storage/index/error.rs`
- [ ] `src/storage/index/types.rs`
- [ ] Unit tests for types

---

## Phase 2: B+ Tree Implementation (8-10 hours)

### 2.1 B+ Tree Core

Create `src/storage/index/btree.rs`:

```rust
use roaring::RoaringBitmap;
use crate::value::Value;

/// B+ tree configuration
pub struct BTreeConfig {
    /// Max keys per node (default: 127)
    pub max_keys: usize,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self { max_keys: 127 }
    }
}

/// B+ tree node
enum Node {
    Internal {
        keys: Vec<Value>,
        children: Vec<Box<Node>>,
    },
    Leaf {
        keys: Vec<Value>,
        values: Vec<RoaringBitmap>,
        next: Option<*mut Node>, // Leaf chain for range scans
    },
}

/// B+ tree index
pub struct BTreeIndex {
    spec: IndexSpec,
    root: Option<Box<Node>>,
    config: BTreeConfig,
    size: u64,
    cardinality: u64,
}
```

### 2.2 B+ Tree Operations

```rust
impl BTreeIndex {
    pub fn new(spec: IndexSpec) -> Self { ... }
    
    /// Insert value -> id mapping
    pub fn insert(&mut self, value: Value, id: u64) -> Result<(), IndexError> {
        // 1. Find leaf node for value
        // 2. Insert into leaf's bitmap (or create new entry)
        // 3. Split if overflow
        // 4. Update cardinality/size
    }
    
    /// Remove id from value's bitmap
    pub fn remove(&mut self, value: &Value, id: u64) -> Result<bool, IndexError> {
        // 1. Find leaf node for value
        // 2. Remove from bitmap
        // 3. If bitmap empty, remove entry
        // 4. Merge if underflow (optional for v1)
    }
    
    /// Exact lookup
    pub fn lookup_eq(&self, value: &Value) -> impl Iterator<Item = u64> + '_ {
        // Binary search to find value, return bitmap iterator
    }
    
    /// Range lookup [start, end)
    pub fn lookup_range(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> impl Iterator<Item = u64> + '_ {
        // 1. Find start leaf
        // 2. Iterate through leaf chain until end
        // 3. Yield all IDs from bitmaps
    }
}
```

### 2.3 B+ Tree Split/Merge

```rust
impl BTreeIndex {
    /// Split a full node
    fn split_node(&mut self, node: &mut Node) -> (Value, Box<Node>) {
        // 1. Find middle key
        // 2. Create new right node with upper half
        // 3. Keep lower half in current node
        // 4. Return (middle_key, right_node) for parent insertion
    }
    
    /// Merge underfull nodes (optional for v1, can skip)
    fn merge_nodes(&mut self, left: &mut Node, right: Node) {
        // Move all entries from right into left
    }
}
```

### 2.4 Value Ordering

Implement `Ord` for `Value` to enable B+ tree comparisons:

```rust
// In src/value.rs
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            
            // Cross-type comparison: use type discriminant
            _ => std::mem::discriminant(self).cmp(&std::mem::discriminant(other)),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
```

### 2.5 Tests for Phase 2

```rust
// Basic operations
#[test] fn btree_new_is_empty() { }
#[test] fn btree_insert_single() { }
#[test] fn btree_insert_multiple_same_value() { }
#[test] fn btree_insert_multiple_different_values() { }
#[test] fn btree_lookup_eq_found() { }
#[test] fn btree_lookup_eq_not_found() { }
#[test] fn btree_remove_existing() { }
#[test] fn btree_remove_nonexistent() { }

// Range queries
#[test] fn btree_range_inclusive_inclusive() { }
#[test] fn btree_range_inclusive_exclusive() { }
#[test] fn btree_range_exclusive_inclusive() { }
#[test] fn btree_range_exclusive_exclusive() { }
#[test] fn btree_range_unbounded_start() { }
#[test] fn btree_range_unbounded_end() { }
#[test] fn btree_range_empty_result() { }

// Split behavior
#[test] fn btree_insert_causes_leaf_split() { }
#[test] fn btree_insert_causes_internal_split() { }
#[test] fn btree_insert_causes_root_split() { }

// Large scale
#[test] fn btree_insert_1000_elements() { }
#[test] fn btree_insert_10000_elements() { }

// Property tests
proptest! {
    #[test] fn btree_insert_lookup_roundtrip(values: Vec<(i64, u64)>) { }
    #[test] fn btree_range_query_correctness(values: Vec<i64>, lo: i64, hi: i64) { }
}
```

### Deliverables

- [ ] `src/storage/index/btree.rs`
- [ ] `Value` ordering implementation
- [ ] Comprehensive B+ tree tests
- [ ] Property-based tests

---

## Phase 3: Unique Index Implementation (2-3 hours)

### 3.1 UniqueIndex Structure

Create `src/storage/index/unique.rs`:

```rust
use std::collections::HashMap;
use crate::value::Value;

/// Hash-based unique index with constraint enforcement
pub struct UniqueIndex {
    spec: IndexSpec,
    map: HashMap<Value, u64>,
}

impl UniqueIndex {
    pub fn new(spec: IndexSpec) -> Self {
        Self {
            spec,
            map: HashMap::new(),
        }
    }
    
    pub fn insert(&mut self, value: Value, id: u64) -> Result<(), IndexError> {
        match self.map.entry(value.clone()) {
            Entry::Occupied(e) => {
                Err(IndexError::DuplicateValue {
                    index: self.spec.name.clone(),
                    value,
                    existing_id: *e.get(),
                    new_id: id,
                })
            }
            Entry::Vacant(e) => {
                e.insert(id);
                Ok(())
            }
        }
    }
    
    pub fn lookup_eq(&self, value: &Value) -> Option<u64> {
        self.map.get(value).copied()
    }
    
    pub fn remove(&mut self, value: &Value, _id: u64) -> Result<bool, IndexError> {
        Ok(self.map.remove(value).is_some())
    }
}

impl PropertyIndex for UniqueIndex {
    // Implement trait methods
}
```

### 3.2 Tests for Phase 3

```rust
#[test] fn unique_insert_success() { }
#[test] fn unique_insert_duplicate_fails() { }
#[test] fn unique_lookup_found() { }
#[test] fn unique_lookup_not_found() { }
#[test] fn unique_remove_existing() { }
#[test] fn unique_remove_nonexistent() { }
#[test] fn unique_cardinality_equals_size() { }
```

### Deliverables

- [ ] `src/storage/index/unique.rs`
- [ ] Unit tests

---

## Phase 4: Storage Integration (4-6 hours)

### 4.1 InMemoryGraph Index Storage

Update `src/storage/inmemory.rs`:

```rust
pub struct InMemoryGraph {
    // ... existing fields ...
    
    /// Property indexes
    indexes: HashMap<String, Box<dyn PropertyIndex>>,
}

impl InMemoryGraph {
    /// Create a new index
    pub fn create_index(&mut self, spec: IndexSpec) -> Result<(), IndexError> {
        if self.indexes.contains_key(&spec.name) {
            return Err(IndexError::IndexExists(spec.name));
        }
        
        let mut index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())),
        };
        
        // Populate from existing data
        self.populate_index(&mut *index, &spec)?;
        
        self.indexes.insert(spec.name.clone(), index);
        Ok(())
    }
    
    /// Populate index with existing graph data
    fn populate_index(
        &self,
        index: &mut dyn PropertyIndex,
        spec: &IndexSpec,
    ) -> Result<(), IndexError> {
        match spec.element_type {
            ElementType::Vertex => {
                for (id, node) in &self.nodes {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let node_label = self.string_table.resolve(node.label_id);
                        if node_label != Some(label.as_str()) {
                            continue;
                        }
                    }
                    // Check property exists
                    if let Some(value) = node.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
            ElementType::Edge => {
                // Similar for edges
            }
        }
        Ok(())
    }
    
    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> Result<(), IndexError> { ... }
    
    /// List indexes
    pub fn list_indexes(&self) -> Vec<&IndexSpec> { ... }
    
    /// Find index for a filter
    pub fn find_index(
        &self,
        element_type: ElementType,
        label: Option<&str>,
        property: &str,
    ) -> Option<&dyn PropertyIndex> { ... }
}
```

### 4.2 Automatic Index Maintenance

Update mutation methods in `InMemoryGraph`:

```rust
impl InMemoryGraph {
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = /* ... existing logic ... */;
        
        // Update indexes
        for (key, value) in &properties {
            self.index_insert(ElementType::Vertex, label, key, value, id.0);
        }
        
        id
    }
    
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        let node = self.nodes.get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        let label = self.string_table.resolve(node.label_id)
            .map(|s| s.to_string());
        
        // Remove from indexes
        if let Some(label) = &label {
            for (key, value) in &node.properties {
                self.index_remove(ElementType::Vertex, label, key, value, id.0);
            }
        }
        
        // ... rest of removal
    }
    
    pub fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        new_value: Value,
    ) -> Result<(), StorageError> {
        let node = self.nodes.get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        let label = self.string_table.resolve(node.label_id)
            .map(|s| s.to_string());
        
        let old_value = node.properties.get(key).cloned();
        
        // Update indexes
        if let Some(label) = &label {
            if let Some(old) = &old_value {
                self.index_remove(ElementType::Vertex, label, key, old, id.0);
            }
            self.index_insert(ElementType::Vertex, label, key, &new_value, id.0);
        }
        
        // ... update property
    }
    
    /// Internal: insert into applicable indexes
    fn index_insert(
        &mut self,
        elem_type: ElementType,
        label: &str,
        property: &str,
        value: &Value,
        id: u64,
    ) {
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if !self.index_applies(spec, elem_type, label, property) {
                continue;
            }
            // Ignore errors for B+ tree, unique errors should have been caught earlier
            let _ = index.insert(value.clone(), id);
        }
    }
    
    fn index_remove(&mut self, ...) { ... }
    
    fn index_applies(&self, spec: &IndexSpec, elem_type: ElementType, label: &str, property: &str) -> bool {
        spec.element_type == elem_type
            && spec.property == property
            && spec.label.as_ref().map_or(true, |l| l == label)
    }
}
```

### 4.3 Tests for Phase 4

```rust
#[test] fn create_index_on_empty_graph() { }
#[test] fn create_index_populates_from_existing() { }
#[test] fn create_duplicate_index_fails() { }
#[test] fn drop_index_succeeds() { }
#[test] fn drop_nonexistent_index_fails() { }
#[test] fn list_indexes_returns_all() { }

#[test] fn add_vertex_updates_btree_index() { }
#[test] fn add_vertex_updates_unique_index() { }
#[test] fn add_vertex_unique_violation_fails() { }
#[test] fn remove_vertex_updates_indexes() { }
#[test] fn set_property_updates_indexes() { }

#[test] fn find_index_by_property() { }
#[test] fn find_index_with_label_filter() { }
#[test] fn find_index_no_match() { }
```

### Deliverables

- [ ] Index storage in `InMemoryGraph`
- [ ] `create_index`, `drop_index`, `list_indexes` methods
- [ ] Automatic index maintenance on mutations
- [ ] Integration tests

---

## Phase 5: Traversal Integration (4-5 hours)

### 5.1 GraphStorage Trait Extension

Add index methods to `GraphStorage` trait:

```rust
pub trait GraphStorage: Send + Sync {
    // ... existing methods ...
    
    /// Find applicable index for a property lookup
    fn find_index(
        &self,
        element_type: ElementType,
        label: Option<&str>,
        property: &str,
    ) -> Option<&dyn PropertyIndex> {
        None // Default: no indexes
    }
    
    /// List all indexes
    fn list_indexes(&self) -> Vec<&IndexSpec> {
        Vec::new()
    }
}
```

### 5.2 Index-Aware Filter Steps

Update `HasValueStep` to use indexes:

```rust
impl HasValueStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Try to use index if input is full vertex/edge scan
        if let Some(index) = self.try_get_index(ctx) {
            return self.apply_indexed(ctx, index);
        }
        
        // Fallback to filter
        self.apply_filter(ctx, input)
    }
    
    fn try_get_index<'a>(&self, ctx: &'a ExecutionContext<'a>) -> Option<&'a dyn PropertyIndex> {
        // Only use index if we're scanning all vertices/edges
        // (i.e., at start of traversal or after has_label)
        ctx.snapshot().storage().find_index(
            ElementType::Vertex, // or Edge depending on context
            None, // label from context
            &self.key,
        )
    }
    
    fn apply_indexed<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        index: &'a dyn PropertyIndex,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let storage = ctx.snapshot().storage();
        let ids = index.lookup_eq(&self.value);
        
        Box::new(ids.filter_map(move |id| {
            let vertex = storage.get_vertex(VertexId(id))?;
            Some(Traverser::new(Value::Vertex(VertexId(id))))
        }))
    }
}
```

### 5.3 Index-Aware HasWhereStep

```rust
impl<P: Predicate> HasWhereStep<P> {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Convert predicate to index range if possible
        if let Some((index, range)) = self.try_get_index_range(ctx) {
            return self.apply_indexed_range(ctx, index, range);
        }
        
        self.apply_filter(ctx, input)
    }
    
    fn predicate_to_range(&self) -> Option<(Bound<Value>, Bound<Value>)> {
        // Convert p::gte(x) to [x, unbounded)
        // Convert p::lt(x) to [unbounded, x)
        // Convert p::between(a, b) to [a, b]
        // etc.
    }
}
```

### 5.4 GraphTraversalSource Index Methods

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Start from vertices matching indexed property value
    pub fn v_by_index(
        self,
        property: &str,
        value: impl Into<Value>,
    ) -> BoundTraversal<'g, (), Value> {
        let value = value.into();
        
        if let Some(index) = self.snapshot.storage().find_index(
            ElementType::Vertex,
            None,
            property,
        ) {
            let ids: Vec<_> = index.lookup_eq(&value).collect();
            return self.inject_vertex_ids(ids);
        }
        
        // Fallback to full scan + filter
        self.v().has_value(property, value)
    }
}
```

### 5.5 Tests for Phase 5

```rust
// Integration tests
#[test] fn has_value_uses_btree_index() {
    let mut g = test_graph_with_index();
    // Verify index is actually used (check query plan or timing)
}

#[test] fn has_where_gte_uses_btree_index() { }
#[test] fn has_where_lt_uses_btree_index() { }
#[test] fn has_where_between_uses_btree_index() { }
#[test] fn has_value_uses_unique_index() { }

#[test] fn v_by_index_returns_correct_results() { }
#[test] fn v_by_index_falls_back_without_index() { }

// Performance tests
#[test] fn indexed_lookup_faster_than_scan() {
    // Create graph with 100k vertices
    // Time has_value with and without index
    // Assert indexed is at least 10x faster
}
```

### Deliverables

- [ ] `GraphStorage::find_index` method
- [ ] Index-aware `HasValueStep`
- [ ] Index-aware `HasWhereStep` (range predicates)
- [ ] `v_by_index` convenience method
- [ ] Integration tests

---

## Phase 6: MmapGraph Support (4-6 hours)

### 6.1 Persistent Index Storage

Add index persistence to `MmapGraph`:

```rust
impl MmapGraph {
    /// Load indexes from disk on open
    fn load_indexes(&mut self) -> Result<(), StorageError> {
        let manifest_path = self.path.join("index_manifest.json");
        if !manifest_path.exists() {
            return Ok(());
        }
        
        let manifest: IndexManifest = serde_json::from_reader(
            File::open(manifest_path)?
        )?;
        
        for entry in manifest.indexes {
            let index = self.load_index(&entry)?;
            self.indexes.insert(entry.name.clone(), index);
        }
        
        Ok(())
    }
    
    /// Save index manifest on close/sync
    fn save_index_manifest(&self) -> Result<(), StorageError> { ... }
}
```

### 6.2 B+ Tree Serialization

```rust
impl BTreeIndex {
    /// Serialize to file
    pub fn save(&self, path: &Path) -> Result<(), io::Error> {
        let mut file = File::create(path)?;
        
        // Write header
        file.write_all(b"BTRE")?;
        file.write_all(&1u32.to_le_bytes())?; // version
        // ... more header fields
        
        // Write nodes in BFS order
        self.write_nodes(&mut file)?;
        
        Ok(())
    }
    
    /// Load from file
    pub fn load(path: &Path, spec: IndexSpec) -> Result<Self, io::Error> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        // Validate header
        // Load nodes
        // Reconstruct tree
    }
}
```

### 6.3 Index WAL Integration

```rust
/// WAL entries for index operations
enum WalEntry {
    // ... existing entries ...
    
    CreateIndex { spec: IndexSpec },
    DropIndex { name: String },
    // Individual index updates are part of vertex/edge mutations
}

impl MmapGraph {
    pub fn create_index(&mut self, spec: IndexSpec) -> Result<(), IndexError> {
        // 1. Log to WAL
        self.wal.log(WalEntry::CreateIndex { spec: spec.clone() })?;
        
        // 2. Create index in memory
        let mut index = /* ... */;
        self.populate_index(&mut index, &spec)?;
        
        // 3. Save to disk
        index.save(&self.index_path(&spec.name))?;
        
        // 4. Update manifest
        self.save_index_manifest()?;
        
        // 5. Commit WAL
        self.wal.commit()?;
        
        self.indexes.insert(spec.name.clone(), index);
        Ok(())
    }
}
```

### 6.4 Tests for Phase 6

```rust
#[test] fn mmap_create_index_persists() {
    let dir = tempdir()?;
    {
        let mut g = MmapGraph::create(dir.path().join("test.db"))?;
        g.add_vertex("person", [("age", 30i64)])?;
        g.create_index(IndexBuilder::vertex().property("age").build()?)?;
    }
    {
        let g = MmapGraph::open(dir.path().join("test.db"))?;
        assert!(g.get_index("idx_all_age").is_some());
    }
}

#[test] fn mmap_index_survives_crash_recovery() { }
#[test] fn mmap_drop_index_persists() { }
#[test] fn mmap_index_updated_through_wal() { }
```

### Deliverables

- [ ] Index persistence for MmapGraph
- [ ] B+ tree serialization
- [ ] WAL integration
- [ ] Crash recovery tests

---

## File Summary

### New Files

| File | Description |
|------|-------------|
| `src/storage/index/mod.rs` | Index module root |
| `src/storage/index/types.rs` | IndexSpec, IndexBuilder, etc. |
| `src/storage/index/error.rs` | IndexError enum |
| `src/storage/index/btree.rs` | B+ tree implementation |
| `src/storage/index/unique.rs` | Unique index implementation |
| `src/storage/index/traits.rs` | PropertyIndex trait |
| `tests/index.rs` | Integration tests |

### Modified Files

| File | Changes |
|------|---------|
| `src/storage/mod.rs` | Export index module |
| `src/storage/inmemory.rs` | Add index storage and methods |
| `src/storage/mmap/mod.rs` | Add index persistence |
| `src/value.rs` | Implement Ord for Value |
| `src/traversal/filter.rs` | Index-aware HasValueStep, HasWhereStep |
| `src/traversal/source.rs` | Add v_by_index method |
| `src/lib.rs` | Export index types |

---

## Testing Strategy

### Unit Test Coverage

| Component | Target |
|-----------|--------|
| B+ tree operations | 100% branch |
| Unique index | 100% branch |
| Index builder | 100% branch |
| Storage integration | 100% branch |

### Integration Tests

1. Index creation on populated graph
2. Automatic index maintenance
3. Query acceleration verification
4. Persistence and recovery

### Performance Tests

```rust
#[test]
#[ignore] // Run with --ignored
fn benchmark_indexed_vs_unindexed() {
    // 100k vertices, compare lookup times
    // Assert 10x+ improvement
}
```

### Property-Based Tests

```rust
proptest! {
    #[test]
    fn btree_operations_consistent(ops: Vec<IndexOp>) {
        // Apply random sequence of insert/remove/lookup
        // Verify consistency
    }
}
```

---

## Dependencies

### New Crates

None required - using existing `roaring` for bitmaps.

### Optional

- `serde` for index manifest serialization (already in deps)

---

## Rollout Plan

1. **Phase 1-3**: Core implementation, internal only
2. **Phase 4**: Storage integration, can be tested independently
3. **Phase 5**: Traversal integration, backwards compatible
4. **Phase 6**: Persistence, feature-flagged initially

---

## Success Criteria

1. [ ] All tests pass with 100% branch coverage
2. [ ] `has_value` with index is 10x+ faster than without on 100k graph
3. [ ] `has_where` range queries work with index
4. [ ] Unique index enforces constraint
5. [ ] Indexes persist and recover correctly (mmap)
6. [ ] No regressions in existing functionality

---

## Future Work (Not This Plan)

1. **Composite indexes** - spec-19
2. **Query planner** - automatic index selection optimization
3. **Index statistics** - histograms for selectivity estimation
4. **Partial indexes** - index subset of elements
5. **Full-text indexes** - tokenized text search
