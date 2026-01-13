# Spec 18: Property Indexes (B+ Tree and Unique)

This specification defines property index support for Intersteller, enabling efficient property-based lookups that avoid full graph scans.

---

## 1. Overview

### 1.1 Problem Statement

Currently, property-based queries require full scans:

```rust
// Current: O(V) where V = vertices with label "person"
g.v().has_label("person").has_value("email", "alice@example.com").next()

// Current: O(V) range scan
g.v().has_label("person").has_where("age", p::gte(65)).to_list()
```

For graphs with millions of vertices, these scans become prohibitively slow.

### 1.2 Solution

Introduce property indexes that transform O(n) scans into O(log n) lookups:

| Index Type | Use Case | Lookup Complexity |
|------------|----------|-------------------|
| **B+ Tree Index** | Range queries, ordered iteration | O(log n) + O(k) |
| **Unique Index** | Exact match on unique values | O(1) |

### 1.3 Goals

1. **10-100x speedup** for indexed property lookups
2. **Transparent integration** - existing traversal API unchanged
3. **Automatic index selection** - query planner chooses best index
4. **Both backends** - support in-memory and mmap storage
5. **ACID compliance** - indexes updated atomically with data

### 1.4 Non-Goals (This Spec)

- Composite indexes (multi-property) - future spec
- Full-text search indexes - future spec
- Spatial indexes - future spec
- Automatic index recommendation - future spec

---

## 2. Index Types

### 2.1 B+ Tree Index

A B+ tree index maps property values to element IDs, supporting:
- Exact match: `has_value("age", 30)`
- Range queries: `has_where("age", p::gte(65))`
- Ordered iteration: values returned in sorted order

```
B+ Tree Structure (branching factor = 4):

                    [30, 50, 70]              <- Internal node
                   /    |    |    \
                  /     |    |     \
    [20,25,28]     [30,35,42]  [51,55,65]  [70,80,90]   <- Leaf nodes
        |              |           |            |
    {v1,v5}→      {v2}→{v6,v9}  {v3,v8}→    {v4}→      <- RoaringBitmaps
    
Leaf nodes are linked for efficient range scans: [20,25,28] → [30,35,42] → ...
```

**Key Properties:**
- Keys: `Value` (the property value)
- Values: `RoaringBitmap` (set of element IDs with that value)
- Leaf linking: enables efficient range iteration
- Configurable branching factor (default: 128 for cache efficiency)

### 2.2 Unique Index

A unique index enforces uniqueness and provides O(1) lookup:

```rust
// HashMap-based for O(1) exact match
unique_index: HashMap<Value, ElementId>

// Enforces: at most one element per value
graph.create_unique_index("user", "email")?;
graph.add_vertex("user", [("email", "alice@example.com")]); // OK
graph.add_vertex("user", [("email", "alice@example.com")]); // Error: duplicate
```

**Key Properties:**
- Keys: `Value` (the property value)
- Values: Single `ElementId` (not a bitmap)
- Constraint enforcement: rejects duplicates on insert
- O(1) lookup: direct hash map access

---

## 3. Data Structures

### 3.1 Index Definition

```rust
/// Specification for creating an index
#[derive(Clone, Debug)]
pub struct IndexSpec {
    /// Unique name for this index
    pub name: String,
    
    /// What element type to index (Vertex or Edge)
    pub element_type: ElementType,
    
    /// Label filter - only index elements with this label
    /// None means index all elements regardless of label
    pub label: Option<String>,
    
    /// Property key to index
    pub property: String,
    
    /// Index type
    pub index_type: IndexType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementType {
    Vertex,
    Edge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexType {
    /// B+ tree for range queries
    BTree,
    /// Hash-based unique index
    Unique,
}
```

### 3.2 Index Handle

```rust
/// Runtime index handle (type-erased for storage in collections)
pub trait PropertyIndex: Send + Sync {
    /// Index name
    fn name(&self) -> &str;
    
    /// Index specification
    fn spec(&self) -> &IndexSpec;
    
    /// Check if this index can accelerate the given filter
    fn covers(&self, filter: &IndexFilter) -> bool;
    
    /// Look up elements matching exact value
    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = ElementId> + '_>;
    
    /// Look up elements in range [start, end)
    fn lookup_range(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = ElementId> + '_>;
    
    /// Insert element into index
    fn insert(&mut self, value: Value, id: ElementId) -> Result<(), IndexError>;
    
    /// Remove element from index
    fn remove(&mut self, value: &Value, id: ElementId) -> Result<(), IndexError>;
    
    /// Number of distinct values in index
    fn cardinality(&self) -> u64;
    
    /// Total number of indexed elements
    fn size(&self) -> u64;
}

/// Filter that can potentially use an index
#[derive(Clone, Debug)]
pub struct IndexFilter {
    pub element_type: ElementType,
    pub label: Option<String>,
    pub property: String,
    pub predicate: IndexPredicate,
}

#[derive(Clone, Debug)]
pub enum IndexPredicate {
    Eq(Value),
    Neq(Value),
    Lt(Value),
    Lte(Value),
    Gt(Value),
    Gte(Value),
    Between { start: Value, end: Value, inclusive: bool },
    Within(Vec<Value>),
}
```

### 3.3 B+ Tree Implementation

```rust
/// B+ tree index for range queries
pub struct BTreeIndex {
    /// Index specification
    spec: IndexSpec,
    
    /// The B+ tree structure
    tree: BPlusTree<Value, RoaringBitmap>,
    
    /// Statistics for query planning
    stats: IndexStatistics,
}

/// B+ tree node
enum BPlusNode<K, V> {
    /// Internal node with keys and child pointers
    Internal {
        keys: Vec<K>,
        children: Vec<Box<BPlusNode<K, V>>>,
    },
    /// Leaf node with keys, values, and sibling link
    Leaf {
        keys: Vec<K>,
        values: Vec<V>,
        next: Option<Box<BPlusNode<K, V>>>,
    },
}

/// B+ tree parameters
pub struct BPlusTreeConfig {
    /// Maximum keys per node (branching factor - 1)
    /// Default: 127 (128 children per internal node)
    pub max_keys: usize,
    
    /// Minimum keys per node (max_keys / 2)
    pub min_keys: usize,
}

impl Default for BPlusTreeConfig {
    fn default() -> Self {
        Self {
            max_keys: 127,
            min_keys: 63,
        }
    }
}
```

### 3.4 Unique Index Implementation

```rust
/// Unique index for O(1) exact match with uniqueness constraint
pub struct UniqueIndex {
    /// Index specification
    spec: IndexSpec,
    
    /// Hash map from value to element ID
    map: HashMap<Value, ElementId>,
}

impl UniqueIndex {
    /// Insert with uniqueness check
    pub fn insert(&mut self, value: Value, id: ElementId) -> Result<(), IndexError> {
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
}
```

### 3.5 Index Statistics

```rust
/// Statistics for query optimization
#[derive(Clone, Debug, Default)]
pub struct IndexStatistics {
    /// Number of distinct values
    pub cardinality: u64,
    
    /// Total indexed elements
    pub total_elements: u64,
    
    /// Min value (if comparable)
    pub min_value: Option<Value>,
    
    /// Max value (if comparable)  
    pub max_value: Option<Value>,
    
    /// Approximate histogram for selectivity estimation
    pub histogram: Option<Histogram>,
    
    /// Last statistics update timestamp
    pub last_updated: u64,
}

/// Histogram for value distribution
#[derive(Clone, Debug)]
pub struct Histogram {
    /// Bucket boundaries (n+1 bounds for n buckets)
    pub bounds: Vec<Value>,
    
    /// Count per bucket
    pub counts: Vec<u64>,
}
```

---

## 4. Index Management API

### 4.1 Creating Indexes

```rust
impl InMemoryGraph {
    /// Create a new property index
    pub fn create_index(&mut self, spec: IndexSpec) -> Result<(), IndexError> {
        // Validate spec
        if self.indexes.contains_key(&spec.name) {
            return Err(IndexError::IndexExists(spec.name));
        }
        
        // Create appropriate index type
        let index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())),
        };
        
        // Populate index from existing data
        self.populate_index(&mut *index)?;
        
        self.indexes.insert(spec.name.clone(), index);
        Ok(())
    }
    
    /// Drop an index
    pub fn drop_index(&mut self, name: &str) -> Result<(), IndexError> {
        self.indexes
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| IndexError::IndexNotFound(name.to_string()))
    }
    
    /// List all indexes
    pub fn list_indexes(&self) -> Vec<&IndexSpec> {
        self.indexes.values().map(|idx| idx.spec()).collect()
    }
    
    /// Get index by name
    pub fn get_index(&self, name: &str) -> Option<&dyn PropertyIndex> {
        self.indexes.get(name).map(|b| b.as_ref())
    }
}
```

### 4.2 Builder Pattern for Index Creation

```rust
/// Fluent builder for index creation
pub struct IndexBuilder {
    name: Option<String>,
    element_type: ElementType,
    label: Option<String>,
    property: Option<String>,
    index_type: IndexType,
}

impl IndexBuilder {
    /// Start building a vertex index
    pub fn vertex() -> Self {
        Self {
            name: None,
            element_type: ElementType::Vertex,
            label: None,
            property: None,
            index_type: IndexType::BTree,
        }
    }
    
    /// Start building an edge index
    pub fn edge() -> Self {
        Self {
            name: None,
            element_type: ElementType::Edge,
            label: None,
            property: None,
            index_type: IndexType::BTree,
        }
    }
    
    /// Set the label filter
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
    
    /// Set the property to index
    pub fn property(mut self, property: impl Into<String>) -> Self {
        self.property = Some(property.into());
        self
    }
    
    /// Make this a unique index
    pub fn unique(mut self) -> Self {
        self.index_type = IndexType::Unique;
        self
    }
    
    /// Set explicit index name (default: auto-generated)
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    
    /// Build the index specification
    pub fn build(self) -> Result<IndexSpec, IndexError> {
        let property = self.property
            .ok_or(IndexError::MissingProperty)?;
        
        let name = self.name.unwrap_or_else(|| {
            // Auto-generate name: "idx_person_email" or "idx_vertex_email"
            let label_part = self.label.as_deref().unwrap_or("all");
            let type_prefix = match self.index_type {
                IndexType::BTree => "idx",
                IndexType::Unique => "uniq",
            };
            format!("{}_{}_{}", type_prefix, label_part, property)
        });
        
        Ok(IndexSpec {
            name,
            element_type: self.element_type,
            label: self.label,
            property,
            index_type: self.index_type,
        })
    }
}

// Usage:
graph.create_index(
    IndexBuilder::vertex()
        .label("person")
        .property("age")
        .build()?
)?;

graph.create_index(
    IndexBuilder::vertex()
        .label("user")
        .property("email")
        .unique()
        .build()?
)?;
```

---

## 5. Query Integration

### 5.1 Index-Aware Source Steps

The traversal source (`g.v()`, `g.e()`) should use indexes when possible:

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Start traversal from vertices, potentially using index
    pub fn v(self) -> VertexTraversal<'g> {
        VertexTraversal::new(self.snapshot)
    }
    
    /// Start traversal from vertices matching property value (uses index if available)
    pub fn v_by_index(
        self,
        property: &str,
        value: impl Into<Value>,
    ) -> VertexTraversal<'g> {
        let value = value.into();
        
        // Check for applicable index
        if let Some(index) = self.find_index(ElementType::Vertex, None, property) {
            // Use index lookup
            let ids = index.lookup_eq(&value);
            return VertexTraversal::from_ids(self.snapshot, ids);
        }
        
        // Fallback to scan + filter
        self.v().has_value(property, value)
    }
}
```

### 5.2 Index Selection in Filter Steps

Filter steps should check for applicable indexes:

```rust
impl HasValueStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Check if we're at the start of traversal (can use index)
        if ctx.can_use_index() {
            if let Some(index) = ctx.find_index(&self.to_filter()) {
                // Use index for initial lookup
                return self.apply_with_index(ctx, index);
            }
        }
        
        // Standard filter implementation
        self.apply_filter(ctx, input)
    }
}
```

### 5.3 Query Planner

A simple query planner selects the best index:

```rust
/// Simple query planner for index selection
pub struct QueryPlanner<'g> {
    snapshot: &'g GraphSnapshot,
}

impl<'g> QueryPlanner<'g> {
    /// Find the best index for a filter, if any
    pub fn find_best_index(&self, filter: &IndexFilter) -> Option<&dyn PropertyIndex> {
        let candidates: Vec<_> = self.snapshot
            .indexes()
            .filter(|idx| idx.covers(filter))
            .collect();
        
        if candidates.is_empty() {
            return None;
        }
        
        // Prefer unique index for equality (O(1) vs O(log n))
        if matches!(filter.predicate, IndexPredicate::Eq(_)) {
            if let Some(unique) = candidates.iter()
                .find(|idx| idx.spec().index_type == IndexType::Unique)
            {
                return Some(*unique);
            }
        }
        
        // Otherwise pick index with best selectivity
        candidates.into_iter()
            .min_by_key(|idx| self.estimate_cost(idx, filter))
    }
    
    /// Estimate cost of using an index for a filter
    fn estimate_cost(&self, index: &dyn PropertyIndex, filter: &IndexFilter) -> u64 {
        let stats = index.statistics();
        
        match &filter.predicate {
            IndexPredicate::Eq(_) => {
                // Expected results = total / cardinality
                stats.total_elements / stats.cardinality.max(1)
            }
            IndexPredicate::Between { .. } => {
                // Estimate from histogram if available
                stats.estimate_range_selectivity(filter)
            }
            _ => stats.total_elements / 10, // Rough estimate
        }
    }
}
```

---

## 6. Automatic Index Maintenance

### 6.1 Insert Operations

```rust
impl InMemoryGraph {
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = self.allocate_vertex_id();
        let label_id = self.string_table.intern(label);
        
        // Store vertex data
        let node = NodeData { id, label_id, properties: properties.clone(), .. };
        self.nodes.insert(id, node);
        
        // Update label index
        self.vertex_labels.entry(label_id).or_default().insert(id.0 as u32);
        
        // Update property indexes
        for (key, value) in &properties {
            self.update_indexes_on_insert(ElementType::Vertex, label, key, value, id.into());
        }
        
        id
    }
    
    fn update_indexes_on_insert(
        &mut self,
        elem_type: ElementType,
        label: &str,
        property: &str,
        value: &Value,
        id: ElementId,
    ) {
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            
            // Check if index applies
            if spec.element_type != elem_type {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            
            // Update index (ignore errors for non-unique, propagate for unique)
            if let Err(e) = index.insert(value.clone(), id) {
                if spec.index_type == IndexType::Unique {
                    // For unique indexes, this is an error
                    // In real impl, this would be handled transactionally
                    panic!("Unique constraint violation: {}", e);
                }
            }
        }
    }
}
```

### 6.2 Delete Operations

```rust
impl InMemoryGraph {
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        let node = self.nodes.remove(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        let label = self.string_table.resolve(node.label_id)
            .map(|s| s.to_string());
        
        // Update property indexes
        for (key, value) in &node.properties {
            if let Some(ref label) = label {
                self.update_indexes_on_remove(
                    ElementType::Vertex,
                    label,
                    key,
                    value,
                    id.into(),
                );
            }
        }
        
        // ... rest of removal logic
        Ok(())
    }
}
```

### 6.3 Update Operations

```rust
impl InMemoryGraph {
    pub fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        new_value: Value,
    ) -> Result<(), StorageError> {
        let node = self.nodes.get_mut(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        let label = self.string_table.resolve(node.label_id)
            .map(|s| s.to_string());
        
        // Get old value for index removal
        let old_value = node.properties.get(key).cloned();
        
        // Update property
        node.properties.insert(key.to_string(), new_value.clone());
        
        // Update indexes
        if let Some(ref label) = label {
            if let Some(ref old) = old_value {
                self.update_indexes_on_remove(
                    ElementType::Vertex,
                    label,
                    key,
                    old,
                    id.into(),
                );
            }
            self.update_indexes_on_insert(
                ElementType::Vertex,
                label,
                key,
                &new_value,
                id.into(),
            );
        }
        
        Ok(())
    }
}
```

---

## 7. Error Handling

```rust
/// Index-related errors
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index already exists: {0}")]
    IndexExists(String),
    
    #[error("index not found: {0}")]
    IndexNotFound(String),
    
    #[error("duplicate value in unique index '{index}': value {value:?} already exists for element {existing_id:?}, cannot add to {new_id:?}")]
    DuplicateValue {
        index: String,
        value: Value,
        existing_id: ElementId,
        new_id: ElementId,
    },
    
    #[error("missing required property for index builder")]
    MissingProperty,
    
    #[error("index corruption detected: {0}")]
    Corruption(String),
    
    #[error("value type not indexable: {0:?}")]
    NotIndexable(Value),
}
```

---

## 8. Memory-Mapped Storage

### 8.1 On-Disk Index Format

For `MmapGraph`, indexes are stored in separate files:

```
my_graph.db              # Main data file
my_graph.wal             # Write-ahead log
my_graph.idx/            # Index directory
├── idx_person_age.btree     # B+ tree index
├── uniq_user_email.hash     # Unique index
└── index_manifest.json      # Index metadata
```

### 8.2 B+ Tree File Format

```
┌────────────────────────────────────────────────────────────────┐
│ Header (64 bytes)                                              │
├────────────────────────────────────────────────────────────────┤
│ magic: u32 = 0x42545245 ("BTRE")                              │
│ version: u32 = 1                                               │
│ page_size: u32 = 4096                                          │
│ root_page: u64                                                 │
│ key_count: u64                                                 │
│ height: u32                                                    │
│ ... (padding to 64 bytes)                                      │
├────────────────────────────────────────────────────────────────┤
│ Page 0 (4096 bytes)                                            │
│ ┌────────────────────────────────────────────────────────────┐ │
│ │ page_type: u8 (0=internal, 1=leaf)                         │ │
│ │ key_count: u16                                              │ │
│ │ keys: [serialized Value; N]                                │ │
│ │ values: [RoaringBitmap or page_id; N] (leaf: bitmap, internal: page_id) │
│ │ next_leaf: u64 (leaf only, 0 if none)                      │ │
│ └────────────────────────────────────────────────────────────┘ │
├────────────────────────────────────────────────────────────────┤
│ Page 1...                                                      │
└────────────────────────────────────────────────────────────────┘
```

### 8.3 Index Manifest

```json
{
  "version": 1,
  "indexes": [
    {
      "name": "idx_person_age",
      "file": "idx_person_age.btree",
      "spec": {
        "element_type": "Vertex",
        "label": "person",
        "property": "age",
        "index_type": "BTree"
      },
      "stats": {
        "cardinality": 100,
        "total_elements": 10000
      }
    }
  ]
}
```

---

## 9. Testing Requirements

### 9.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    // B+ Tree operations
    #[test] fn btree_insert_single() { }
    #[test] fn btree_insert_causes_split() { }
    #[test] fn btree_lookup_eq() { }
    #[test] fn btree_lookup_range() { }
    #[test] fn btree_remove() { }
    #[test] fn btree_remove_causes_merge() { }
    
    // Unique index operations
    #[test] fn unique_insert_success() { }
    #[test] fn unique_insert_duplicate_fails() { }
    #[test] fn unique_lookup() { }
    #[test] fn unique_remove() { }
    
    // Index management
    #[test] fn create_index_populates_existing_data() { }
    #[test] fn drop_index_removes_index() { }
    #[test] fn index_updated_on_vertex_insert() { }
    #[test] fn index_updated_on_vertex_remove() { }
    #[test] fn index_updated_on_property_change() { }
    
    // Query integration
    #[test] fn has_value_uses_btree_index() { }
    #[test] fn has_where_range_uses_btree_index() { }
    #[test] fn has_value_uses_unique_index() { }
    #[test] fn query_planner_prefers_unique_for_eq() { }
}
```

### 9.2 Property-Based Tests

```rust
proptest! {
    #[test]
    fn btree_insert_then_lookup_finds_all(values: Vec<(i64, u64)>) {
        let mut tree = BPlusTree::new();
        for (k, v) in &values {
            tree.insert(Value::Int(*k), *v);
        }
        for (k, v) in &values {
            assert!(tree.lookup_eq(&Value::Int(*k)).any(|id| id == *v));
        }
    }
    
    #[test]
    fn btree_range_query_correct(
        values: Vec<i64>,
        start: i64,
        end: i64,
    ) {
        let mut tree = BPlusTree::new();
        for (i, v) in values.iter().enumerate() {
            tree.insert(Value::Int(*v), i as u64);
        }
        
        let (lo, hi) = if start <= end { (start, end) } else { (end, start) };
        let result: HashSet<_> = tree.lookup_range(
            Bound::Included(&Value::Int(lo)),
            Bound::Excluded(&Value::Int(hi)),
        ).collect();
        
        let expected: HashSet<_> = values.iter()
            .enumerate()
            .filter(|(_, v)| **v >= lo && **v < hi)
            .map(|(i, _)| i as u64)
            .collect();
        
        assert_eq!(result, expected);
    }
}
```

### 9.3 Integration Tests

```rust
#[test]
fn integration_index_accelerates_has_value() {
    let mut graph = InMemoryGraph::new();
    
    // Add 10,000 vertices
    for i in 0..10_000 {
        graph.add_vertex("person", HashMap::from([
            ("name".to_string(), format!("Person{}", i).into()),
            ("age".to_string(), (i % 100).into()),
        ]));
    }
    
    // Time query without index
    let start = Instant::now();
    let _ = graph.traversal().v()
        .has_value("age", 42i64)
        .to_list();
    let without_index = start.elapsed();
    
    // Create index
    graph.create_index(
        IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()?
    )?;
    
    // Time query with index
    let start = Instant::now();
    let _ = graph.traversal().v()
        .has_value("age", 42i64)
        .to_list();
    let with_index = start.elapsed();
    
    // Index should be significantly faster
    assert!(with_index < without_index / 5);
}
```

---

## 10. Performance Targets

| Operation | Target |
|-----------|--------|
| B+ tree point lookup | < 1 us (in-memory) |
| B+ tree range scan (100 results) | < 10 us |
| Unique index lookup | < 100 ns |
| Index insert | < 1 us amortized |
| Index remove | < 1 us amortized |
| Create index (1M elements) | < 5 seconds |

---

## 11. Public API Summary

```rust
// Index creation
graph.create_index(IndexBuilder::vertex().label("person").property("age").build()?)?;
graph.create_index(IndexBuilder::vertex().label("user").property("email").unique().build()?)?;

// Index management
graph.drop_index("idx_person_age")?;
let indexes = graph.list_indexes();

// Queries automatically use indexes
let adults = g.v().has_label("person").has_where("age", p::gte(18)).to_list();
let user = g.v().has_label("user").has_value("email", "alice@example.com").next();

// Explicit index lookup (bypass query planner)
let vertices = g.v_by_index("email", "alice@example.com").to_list();
```

---

## 12. Future Extensions

1. **Composite indexes** - Index multiple properties together
2. **Covering indexes** - Include additional properties in index to avoid vertex lookup
3. **Partial indexes** - Index only elements matching a predicate
4. **Expression indexes** - Index computed values
5. **Full-text indexes** - Text search with tokenization
6. **Spatial indexes** - R-tree for geometric queries

---

## 13. References

- [storage-advanced.md](../guiding-documents/storage-advanced.md) - Original design notes
- [B+ Tree Wikipedia](https://en.wikipedia.org/wiki/B%2B_tree) - Algorithm reference
- [RoaringBitmap](https://roaringbitmap.org/) - Efficient bitmap implementation
