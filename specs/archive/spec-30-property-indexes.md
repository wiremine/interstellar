# Spec 30: Property Indexes

This specification defines property index support for Interstellar, enabling efficient property-based lookups for both the Gremlin-style traversal API and GQL queries.

---

## 1. Overview

### 1.1 Problem Statement

Currently, all property-based queries require O(n) full scans:

```rust
// Gremlin API: O(n) full vertex scan
g.v().has_label("user").has_value("email", "alice@example.com").next()

// Gremlin API: O(n) scan with range predicate  
g.v().has_label("person").has_where("age", p::gte(18)).to_list()
```

```sql
-- GQL: O(n) scan translated from MATCH
MATCH (u:user {email: 'alice@example.com'}) RETURN u

-- GQL: O(n) scan from WHERE clause
MATCH (p:person) WHERE p.age >= 18 RETURN p.name
```

For graphs with millions of elements, these scans are prohibitively slow.

### 1.2 Solution

Introduce property indexes that transform O(n) scans into O(log n) or O(1) lookups:

| Index Type | Use Case | Lookup Complexity |
|------------|----------|-------------------|
| **B+ Tree Index** | Range queries, ordered iteration | O(log n) + O(k) |
| **Unique Index** | Exact match on unique values | O(1) average |

### 1.3 Goals

1. **10-100x speedup** for indexed property lookups on large graphs
2. **Transparent integration** - Existing traversal and GQL APIs unchanged
3. **Automatic index selection** - Filter steps use indexes when applicable
4. **Both backends** - Support `Graph` and `MmapGraph`
5. **Constraint enforcement** - Unique indexes reject duplicate values
6. **Incremental updates** - Indexes maintained on insert/update/delete

### 1.4 Non-Goals (This Spec)

- Composite indexes (multi-property) - future spec
- Full-text search indexes - future spec
- Spatial/geometric indexes - future spec
- Automatic index recommendation - future spec
- Index-only scans (covering indexes) - future optimization

---

## 2. User-Facing API

### 2.1 Index Creation (Rust API)

```rust
use interstellar::index::{IndexBuilder, IndexType};

// Create a B+ tree index on person.age for range queries
graph.create_index(
    IndexBuilder::vertex()
        .label("person")
        .property("age")
        .build()?
)?;

// Create a unique index on user.email for O(1) lookup + uniqueness constraint
graph.create_index(
    IndexBuilder::vertex()
        .label("user")
        .property("email")
        .unique()
        .build()?
)?;

// Create an edge index
graph.create_index(
    IndexBuilder::edge()
        .label("purchased")
        .property("amount")
        .build()?
)?;

// Index all vertices regardless of label
graph.create_index(
    IndexBuilder::vertex()
        .property("created_at")
        .build()?
)?;
```

### 2.2 Index Creation (GQL DDL)

```sql
-- B+ tree index (default)
CREATE INDEX idx_person_age ON :person(age)

-- Unique index
CREATE UNIQUE INDEX idx_user_email ON :user(email)

-- Edge index
CREATE INDEX idx_purchased_amount ON :purchased(amount)

-- Index without label filter
CREATE INDEX idx_created_at ON (created_at)
```

### 2.3 Index Management

```rust
// Drop an index
graph.drop_index("idx_person_age")?;

// List all indexes
for spec in graph.list_indexes() {
    println!("{}: {:?} on {}({})", 
        spec.name, spec.index_type, 
        spec.label.as_deref().unwrap_or("*"), 
        spec.property);
}

// Check if index exists
if graph.has_index("idx_user_email") {
    // ...
}

// Get index statistics
if let Some(stats) = graph.index_stats("idx_person_age") {
    println!("Cardinality: {}", stats.cardinality);
    println!("Total indexed: {}", stats.total_elements);
}
```

### 2.4 Automatic Index Usage

Indexes are used automatically by filter steps - no API changes required:

```rust
// With idx_user_email unique index: O(1) lookup
let user = g.v()
    .has_label("user")
    .has_value("email", "alice@example.com")
    .next();

// With idx_person_age B+ tree index: O(log n) + O(k)
let adults = g.v()
    .has_label("person")
    .has_where("age", p::gte(18))
    .to_list();

// Range query using index
let mid_price = g.v()
    .has_label("product")
    .has_where("price", p::between(10.0, 50.0))
    .to_list();
```

GQL queries also benefit automatically:

```sql
-- Uses idx_user_email if available
MATCH (u:user {email: 'alice@example.com'}) RETURN u

-- Uses idx_person_age if available
MATCH (p:person) WHERE p.age >= 18 RETURN p.name
```

---

## 3. Data Structures

### 3.1 Index Specification

```rust
/// Specification for creating an index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexSpec {
    /// Unique name for this index.
    pub name: String,
    
    /// What element type to index (Vertex or Edge).
    pub element_type: ElementType,
    
    /// Label filter - only index elements with this label.
    /// None means index all elements regardless of label.
    pub label: Option<String>,
    
    /// Property key to index.
    pub property: String,
    
    /// Index type (BTree or Unique).
    pub index_type: IndexType,
}

/// Element type for indexing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ElementType {
    Vertex,
    Edge,
}

/// Type of index structure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// B+ tree for range queries and ordered iteration.
    BTree,
    /// Hash-based index with uniqueness constraint.
    Unique,
}
```

### 3.2 Index Builder

```rust
/// Fluent builder for index creation.
pub struct IndexBuilder {
    element_type: ElementType,
    label: Option<String>,
    property: Option<String>,
    index_type: IndexType,
    name: Option<String>,
}

impl IndexBuilder {
    /// Start building a vertex index.
    pub fn vertex() -> Self;
    
    /// Start building an edge index.
    pub fn edge() -> Self;
    
    /// Set the label filter (only index elements with this label).
    pub fn label(self, label: impl Into<String>) -> Self;
    
    /// Set the property to index (required).
    pub fn property(self, property: impl Into<String>) -> Self;
    
    /// Make this a unique index (default is B+ tree).
    pub fn unique(self) -> Self;
    
    /// Set explicit index name (default: auto-generated).
    pub fn name(self, name: impl Into<String>) -> Self;
    
    /// Build the index specification.
    pub fn build(self) -> Result<IndexSpec, IndexError>;
}
```

### 3.3 Property Index Trait

```rust
/// Trait for property index implementations.
pub trait PropertyIndex: Send + Sync {
    /// Returns the index specification.
    fn spec(&self) -> &IndexSpec;
    
    /// Check if this index covers the given filter.
    fn covers(&self, filter: &IndexFilter) -> bool;
    
    /// Look up elements with exact value match.
    /// Returns element IDs (vertex or edge).
    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_>;
    
    /// Look up elements in a range [start, end).
    fn lookup_range(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_>;
    
    /// Insert an element into the index.
    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError>;
    
    /// Remove an element from the index.
    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError>;
    
    /// Update an element's indexed value.
    fn update(
        &mut self, 
        old_value: &Value, 
        new_value: Value, 
        element_id: u64
    ) -> Result<(), IndexError>;
    
    /// Return index statistics.
    fn statistics(&self) -> &IndexStatistics;
    
    /// Rebuild statistics from current data.
    fn refresh_statistics(&mut self);
}
```

### 3.4 Index Filter (for Query Planning)

```rust
/// A filter that can potentially use an index.
#[derive(Clone, Debug)]
pub struct IndexFilter {
    /// Element type (Vertex or Edge).
    pub element_type: ElementType,
    
    /// Label constraint (None = any label).
    pub label: Option<String>,
    
    /// Property being filtered.
    pub property: String,
    
    /// The predicate to evaluate.
    pub predicate: IndexPredicate,
}

/// Predicates that can use indexes.
#[derive(Clone, Debug)]
pub enum IndexPredicate {
    /// Exact equality: property = value
    Eq(Value),
    
    /// Inequality: property <> value  
    Neq(Value),
    
    /// Less than: property < value
    Lt(Value),
    
    /// Less than or equal: property <= value
    Lte(Value),
    
    /// Greater than: property > value
    Gt(Value),
    
    /// Greater than or equal: property >= value
    Gte(Value),
    
    /// Range: start <= property < end (or inclusive)
    Between { 
        start: Value, 
        end: Value, 
        start_inclusive: bool,
        end_inclusive: bool,
    },
    
    /// Membership: property IN [values]
    Within(Vec<Value>),
}
```

### 3.5 Index Statistics

```rust
/// Statistics for query optimization.
#[derive(Clone, Debug, Default)]
pub struct IndexStatistics {
    /// Number of distinct values in the index.
    pub cardinality: u64,
    
    /// Total number of indexed elements.
    pub total_elements: u64,
    
    /// Minimum value (if values are comparable).
    pub min_value: Option<Value>,
    
    /// Maximum value (if values are comparable).
    pub max_value: Option<Value>,
    
    /// Last time statistics were updated (Unix timestamp).
    pub last_updated: u64,
}
```

---

## 4. B+ Tree Index Implementation

### 4.1 Structure

```rust
/// B+ tree index for range queries.
pub struct BTreeIndex {
    /// Index specification.
    spec: IndexSpec,
    
    /// Underlying B+ tree structure.
    /// Maps property values to sets of element IDs.
    tree: BTreeMap<ComparableValue, RoaringBitmap>,
    
    /// Index statistics.
    stats: IndexStatistics,
}
```

**Design Notes:**
- Use `std::collections::BTreeMap` for simplicity (Rust's BTreeMap is already a B-tree)
- Keys are `ComparableValue` (wrapper enabling `Ord` for `Value`)
- Values are `RoaringBitmap` for efficient ID set storage
- Each key maps to potentially many element IDs (non-unique)

### 4.2 ComparableValue Wrapper

```rust
/// Wrapper that enables ordering for Value types.
/// 
/// Ordering: Null < Bool < Int < Float < String < List < Map < Vertex < Edge
#[derive(Clone, Debug)]
pub struct ComparableValue(pub Value);

impl Ord for ComparableValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Bool(_), _) => Ordering::Less,
            (_, Value::Bool(_)) => Ordering::Greater,
            
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Int(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Int(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Int(_), _) => Ordering::Less,
            (_, Value::Int(_)) => Ordering::Greater,
            (Value::Float(_), _) => Ordering::Less,
            (_, Value::Float(_)) => Ordering::Greater,
            
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::String(_), _) => Ordering::Less,
            (_, Value::String(_)) => Ordering::Greater,
            
            // Lists, Maps, Vertex, Edge - compare by debug representation
            (a, b) => format!("{:?}", a).cmp(&format!("{:?}", b)),
        }
    }
}
```

### 4.3 BTreeIndex Implementation

```rust
impl BTreeIndex {
    pub fn new(spec: IndexSpec) -> Self {
        assert!(spec.index_type == IndexType::BTree);
        Self {
            spec,
            tree: BTreeMap::new(),
            stats: IndexStatistics::default(),
        }
    }
    
    /// Build index from existing data.
    pub fn populate<I>(&mut self, elements: I)
    where
        I: Iterator<Item = (u64, Value)>,  // (element_id, property_value)
    {
        for (id, value) in elements {
            let key = ComparableValue(value);
            self.tree.entry(key).or_insert_with(RoaringBitmap::new).insert(id as u32);
        }
        self.refresh_statistics();
    }
}

impl PropertyIndex for BTreeIndex {
    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_> {
        let key = ComparableValue(value.clone());
        match self.tree.get(&key) {
            Some(bitmap) => Box::new(bitmap.iter().map(|id| id as u64)),
            None => Box::new(std::iter::empty()),
        }
    }
    
    fn lookup_range(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_> {
        let start_bound = match start {
            Bound::Included(v) => Bound::Included(ComparableValue(v.clone())),
            Bound::Excluded(v) => Bound::Excluded(ComparableValue(v.clone())),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match end {
            Bound::Included(v) => Bound::Included(ComparableValue(v.clone())),
            Bound::Excluded(v) => Bound::Excluded(ComparableValue(v.clone())),
            Bound::Unbounded => Bound::Unbounded,
        };
        
        Box::new(
            self.tree
                .range((start_bound, end_bound))
                .flat_map(|(_, bitmap)| bitmap.iter())
                .map(|id| id as u64)
        )
    }
    
    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
        let key = ComparableValue(value);
        self.tree
            .entry(key)
            .or_insert_with(RoaringBitmap::new)
            .insert(element_id as u32);
        self.stats.total_elements += 1;
        Ok(())
    }
    
    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError> {
        let key = ComparableValue(value.clone());
        if let Some(bitmap) = self.tree.get_mut(&key) {
            bitmap.remove(element_id as u32);
            if bitmap.is_empty() {
                self.tree.remove(&key);
            }
            self.stats.total_elements = self.stats.total_elements.saturating_sub(1);
        }
        Ok(())
    }
    
    // ... other methods
}
```

---

## 5. Unique Index Implementation

### 5.1 Structure

```rust
/// Hash-based unique index with O(1) lookup.
pub struct UniqueIndex {
    /// Index specification.
    spec: IndexSpec,
    
    /// Maps property values to single element IDs.
    /// Enforces uniqueness constraint.
    map: HashMap<HashableValue, u64>,
    
    /// Reverse map for efficient removal by element ID.
    reverse: HashMap<u64, HashableValue>,
    
    /// Index statistics.
    stats: IndexStatistics,
}
```

### 5.2 HashableValue Wrapper

```rust
/// Wrapper that enables hashing for Value types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashableValue(pub Value);

impl Hash for HashableValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Discriminant for type
        std::mem::discriminant(&self.0).hash(state);
        
        match &self.0 {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(n) => n.hash(state),
            Value::Float(f) => f.to_bits().hash(state),  // Hash float bits
            Value::String(s) => s.hash(state),
            Value::List(items) => {
                for item in items {
                    HashableValue(item.clone()).hash(state);
                }
            }
            Value::Map(map) => {
                // Sort keys for deterministic hashing
                let mut pairs: Vec<_> = map.iter().collect();
                pairs.sort_by_key(|(k, _)| *k);
                for (k, v) in pairs {
                    k.hash(state);
                    HashableValue(v.clone()).hash(state);
                }
            }
            Value::Vertex(id) => id.0.hash(state),
            Value::Edge(id) => id.0.hash(state),
        }
    }
}
```

### 5.3 UniqueIndex Implementation

```rust
impl UniqueIndex {
    pub fn new(spec: IndexSpec) -> Self {
        assert!(spec.index_type == IndexType::Unique);
        Self {
            spec,
            map: HashMap::new(),
            reverse: HashMap::new(),
            stats: IndexStatistics::default(),
        }
    }
}

impl PropertyIndex for UniqueIndex {
    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_> {
        let key = HashableValue(value.clone());
        match self.map.get(&key) {
            Some(&id) => Box::new(std::iter::once(id)),
            None => Box::new(std::iter::empty()),
        }
    }
    
    fn lookup_range(
        &self,
        _start: Bound<&Value>,
        _end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_> {
        // Unique indexes don't support efficient range queries
        // Could fall back to full scan, but better to not use this index
        Box::new(std::iter::empty())
    }
    
    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
        let key = HashableValue(value.clone());
        
        // Check for duplicate
        if let Some(&existing_id) = self.map.get(&key) {
            if existing_id != element_id {
                return Err(IndexError::DuplicateValue {
                    index_name: self.spec.name.clone(),
                    value,
                    existing_id,
                    new_id: element_id,
                });
            }
            // Same element, no-op
            return Ok(());
        }
        
        // Remove old value if updating
        if let Some(old_key) = self.reverse.remove(&element_id) {
            self.map.remove(&old_key);
        }
        
        // Insert new mapping
        self.map.insert(key.clone(), element_id);
        self.reverse.insert(element_id, key);
        self.stats.total_elements = self.map.len() as u64;
        self.stats.cardinality = self.map.len() as u64;
        
        Ok(())
    }
    
    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError> {
        let key = HashableValue(value.clone());
        
        if let Some(&stored_id) = self.map.get(&key) {
            if stored_id == element_id {
                self.map.remove(&key);
                self.reverse.remove(&element_id);
                self.stats.total_elements = self.map.len() as u64;
                self.stats.cardinality = self.map.len() as u64;
            }
        }
        
        Ok(())
    }
    
    fn covers(&self, filter: &IndexFilter) -> bool {
        // Unique index only covers equality predicates efficiently
        if !matches!(filter.predicate, IndexPredicate::Eq(_)) {
            return false;
        }
        
        // Check element type matches
        if filter.element_type != self.spec.element_type {
            return false;
        }
        
        // Check property matches
        if filter.property != self.spec.property {
            return false;
        }
        
        // Check label matches (if index has label filter)
        match (&self.spec.label, &filter.label) {
            (Some(idx_label), Some(filter_label)) => idx_label == filter_label,
            (Some(_), None) => false,  // Index is label-specific, filter is not
            (None, _) => true,         // Index covers all labels
        }
    }
    
    // ... other methods
}
```

---

## 6. Storage Integration

### 6.1 GraphStorage Trait Extensions

Add new methods to the `GraphStorage` trait for index access:

```rust
/// Extension trait for index-aware storage.
pub trait IndexedStorage: GraphStorage {
    /// Returns an iterator over all indexes.
    fn indexes(&self) -> Box<dyn Iterator<Item = &dyn PropertyIndex> + '_>;
    
    /// Get an index by name.
    fn get_index(&self, name: &str) -> Option<&dyn PropertyIndex>;
    
    /// Find indexes that cover a filter.
    fn find_indexes(&self, filter: &IndexFilter) -> Vec<&dyn PropertyIndex>;
    
    /// Lookup vertices by indexed property value.
    /// Falls back to scan if no index available.
    fn vertices_by_property(
        &self, 
        label: Option<&str>,
        property: &str, 
        value: &Value
    ) -> Box<dyn Iterator<Item = Vertex> + '_>;
    
    /// Lookup vertices by indexed property range.
    fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_>;
}

/// Extension trait for mutable indexed storage.
pub trait IndexedStorageMut: IndexedStorage + GraphStorageMut {
    /// Create a new index.
    fn create_index(&mut self, spec: IndexSpec) -> Result<(), IndexError>;
    
    /// Drop an index by name.
    fn drop_index(&mut self, name: &str) -> Result<(), IndexError>;
}
```

### 6.2 Graph Index Storage

```rust
pub struct Graph {
    // ... existing fields ...
    
    /// Property indexes by name.
    indexes: HashMap<String, Box<dyn PropertyIndex>>,
}

impl Graph {
    /// Create a new index and populate it with existing data.
    pub fn create_index(&mut self, spec: IndexSpec) -> Result<(), IndexError> {
        // Check for duplicate name
        if self.indexes.contains_key(&spec.name) {
            return Err(IndexError::AlreadyExists(spec.name.clone()));
        }
        
        // Create the appropriate index type
        let mut index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())),
        };
        
        // Populate index with existing data
        self.populate_index(&mut *index)?;
        
        self.indexes.insert(spec.name.clone(), index);
        Ok(())
    }
    
    /// Populate an index with existing graph data.
    fn populate_index(&self, index: &mut dyn PropertyIndex) -> Result<(), IndexError> {
        let spec = index.spec();
        
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
                    
                    // Get property value
                    if let Some(value) = node.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
            ElementType::Edge => {
                for (id, edge) in &self.edges {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let edge_label = self.string_table.resolve(edge.label_id);
                        if edge_label != Some(label.as_str()) {
                            continue;
                        }
                    }
                    
                    // Get property value
                    if let Some(value) = edge.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
        }
        
        Ok(())
    }
}
```

### 6.3 Automatic Index Maintenance

Update mutation methods to maintain indexes:

```rust
impl Graph {
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = /* allocate ID */;
        
        // ... create vertex ...
        
        // Update indexes
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                // Ignore errors for non-unique, propagate for unique
                if let Err(e) = index.insert(value.clone(), id.0) {
                    if spec.index_type == IndexType::Unique {
                        // Rollback vertex creation
                        self.nodes.remove(&id);
                        panic!("Unique constraint violation: {}", e);
                        // In production: return Result
                    }
                }
            }
        }
        
        id
    }
    
    pub fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let node = self.nodes.get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        let label = self.string_table.resolve(node.label_id).map(|s| s.to_string());
        let old_value = node.properties.get(key).cloned();
        
        // Update indexes
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != key {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if label.as_ref() != Some(idx_label) {
                    continue;
                }
            }
            
            // Remove old value from index
            if let Some(ref old) = old_value {
                let _ = index.remove(old, id.0);
            }
            
            // Insert new value
            index.insert(value.clone(), id.0)?;
        }
        
        // Update property
        let node = self.nodes.get_mut(&id).unwrap();
        node.properties.insert(key.to_string(), value);
        
        Ok(())
    }
    
    // Similar updates for remove_vertex, add_edge, set_edge_property, remove_edge
}
```

---

## 7. Traversal Integration

### 7.1 Index-Aware Filter Steps

Modify filter steps to check for applicable indexes:

```rust
impl HasValueStep {
    fn apply_with_context<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Check if we can use an index
        // Indexes are only useful at the start of traversal (no prior filters)
        if ctx.is_index_eligible() {
            if let Some(ids) = self.try_index_lookup(ctx) {
                // Use indexed lookup
                return Box::new(
                    ids.into_iter()
                       .filter_map(move |id| {
                           ctx.snapshot().storage().get_vertex(VertexId(id))
                       })
                       .map(|v| Traverser::new(Value::Vertex(v.id)))
                );
            }
        }
        
        // Fall back to standard filter
        Box::new(input.filter(move |t| self.matches(ctx, t)))
    }
    
    fn try_index_lookup(&self, ctx: &ExecutionContext) -> Option<Vec<u64>> {
        let filter = IndexFilter {
            element_type: ElementType::Vertex,  // Determined by traversal context
            label: ctx.current_label_filter().cloned(),
            property: self.key.clone(),
            predicate: IndexPredicate::Eq(self.value.clone()),
        };
        
        // Find a covering index
        let indexes = ctx.snapshot().storage().find_indexes(&filter);
        if let Some(index) = indexes.first() {
            let ids: Vec<u64> = index.lookup_eq(&self.value).collect();
            return Some(ids);
        }
        
        None
    }
}
```

### 7.2 HasWhereStep with Range Index

```rust
impl HasWhereStep {
    fn try_index_lookup(&self, ctx: &ExecutionContext) -> Option<Vec<u64>> {
        // Convert predicate to IndexPredicate
        let index_pred = match &self.predicate {
            Predicate::Eq(v) => IndexPredicate::Eq(v.clone()),
            Predicate::Lt(v) => IndexPredicate::Lt(v.clone()),
            Predicate::Lte(v) => IndexPredicate::Lte(v.clone()),
            Predicate::Gt(v) => IndexPredicate::Gt(v.clone()),
            Predicate::Gte(v) => IndexPredicate::Gte(v.clone()),
            Predicate::Between(start, end) => IndexPredicate::Between {
                start: start.clone(),
                end: end.clone(),
                start_inclusive: true,
                end_inclusive: false,
            },
            _ => return None,  // Predicate not indexable
        };
        
        let filter = IndexFilter {
            element_type: ElementType::Vertex,
            label: ctx.current_label_filter().cloned(),
            property: self.key.clone(),
            predicate: index_pred,
        };
        
        let indexes = ctx.snapshot().storage().find_indexes(&filter);
        for index in indexes {
            // Use range lookup for range predicates
            let ids = match &self.predicate {
                Predicate::Lt(v) => {
                    index.lookup_range(Bound::Unbounded, Bound::Excluded(v))
                }
                Predicate::Lte(v) => {
                    index.lookup_range(Bound::Unbounded, Bound::Included(v))
                }
                Predicate::Gt(v) => {
                    index.lookup_range(Bound::Excluded(v), Bound::Unbounded)
                }
                Predicate::Gte(v) => {
                    index.lookup_range(Bound::Included(v), Bound::Unbounded)
                }
                Predicate::Between(start, end) => {
                    index.lookup_range(Bound::Included(start), Bound::Excluded(end))
                }
                _ => return None,
            };
            
            return Some(ids.collect());
        }
        
        None
    }
}
```

---

## 8. GQL Integration

### 8.1 DDL Parser Extensions

Add grammar rules for index DDL:

```pest
// Index DDL statements
create_index = {
    CREATE ~ UNIQUE? ~ INDEX ~ identifier ~ ON ~ 
    label_filter? ~ "(" ~ identifier ~ ")"
}

drop_index = { DROP_KW ~ INDEX ~ identifier }

// Keywords
INDEX = @{ ^"index" ~ !ASCII_ALPHANUMERIC }
```

### 8.2 GQL Compiler Integration

The GQL compiler should use indexes when compiling MATCH patterns:

```rust
impl QueryCompiler {
    fn compile_node_pattern(&self, node: &NodePattern) -> Result<...> {
        // Check for indexed property in pattern
        for (key, value) in &node.properties {
            let filter = IndexFilter {
                element_type: ElementType::Vertex,
                label: node.labels.first().cloned(),
                property: key.clone(),
                predicate: IndexPredicate::Eq(value.clone().into()),
            };
            
            if let Some(index) = self.find_best_index(&filter) {
                // Use index as start point
                return self.compile_with_index(index, &filter, node);
            }
        }
        
        // Fall back to label scan
        self.compile_with_label_scan(node)
    }
}
```

---

## 9. Error Handling

```rust
/// Index-related errors.
#[derive(Debug, Error)]
pub enum IndexError {
    /// Index with this name already exists.
    #[error("index already exists: {0}")]
    AlreadyExists(String),
    
    /// Index not found.
    #[error("index not found: {0}")]
    NotFound(String),
    
    /// Duplicate value in unique index.
    #[error("unique constraint violation on index '{index_name}': value {value:?} already exists for element {existing_id}, cannot add element {new_id}")]
    DuplicateValue {
        index_name: String,
        value: Value,
        existing_id: u64,
        new_id: u64,
    },
    
    /// Missing required property in IndexBuilder.
    #[error("index builder missing required property")]
    MissingProperty,
    
    /// Value type cannot be indexed.
    #[error("value type not indexable: {0:?}")]
    NotIndexable(Value),
}
```

---

## 10. MmapGraph Integration

For `MmapGraph`, indexes are persisted alongside the data:

### 10.1 File Layout

```
my_graph.db              # Main data file
my_graph.wal             # Write-ahead log
my_graph.idx/            # Index directory
├── idx_person_age.idx   # Serialized B+ tree index
├── uniq_user_email.idx  # Serialized unique index
└── manifest.json        # Index metadata
```

### 10.2 Index Manifest

```json
{
  "version": 1,
  "indexes": [
    {
      "name": "idx_person_age",
      "file": "idx_person_age.idx",
      "element_type": "Vertex",
      "label": "person",
      "property": "age",
      "index_type": "BTree"
    },
    {
      "name": "uniq_user_email",
      "file": "uniq_user_email.idx",
      "element_type": "Vertex",
      "label": "user",
      "property": "email",
      "index_type": "Unique"
    }
  ]
}
```

### 10.3 Index Serialization

Indexes are serialized using `bincode` or similar:

```rust
impl BTreeIndex {
    pub fn serialize<W: Write>(&self, writer: W) -> Result<(), io::Error>;
    pub fn deserialize<R: Read>(reader: R, spec: IndexSpec) -> Result<Self, io::Error>;
}
```

---

## 11. Testing Requirements

### 11.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    // B+ Tree Index
    #[test] fn btree_insert_single() { }
    #[test] fn btree_insert_multiple_same_value() { }
    #[test] fn btree_lookup_eq_found() { }
    #[test] fn btree_lookup_eq_not_found() { }
    #[test] fn btree_lookup_range_inclusive() { }
    #[test] fn btree_lookup_range_exclusive() { }
    #[test] fn btree_lookup_range_unbounded_start() { }
    #[test] fn btree_lookup_range_unbounded_end() { }
    #[test] fn btree_remove_single() { }
    #[test] fn btree_remove_from_multiple() { }
    #[test] fn btree_update_value() { }
    
    // Unique Index
    #[test] fn unique_insert_success() { }
    #[test] fn unique_insert_duplicate_fails() { }
    #[test] fn unique_lookup_found() { }
    #[test] fn unique_lookup_not_found() { }
    #[test] fn unique_remove() { }
    #[test] fn unique_update_same_element() { }
    
    // Index Management
    #[test] fn create_index_populates_existing_data() { }
    #[test] fn create_index_duplicate_name_fails() { }
    #[test] fn drop_index_removes_index() { }
    #[test] fn drop_index_not_found_fails() { }
    
    // Automatic Maintenance
    #[test] fn index_updated_on_vertex_insert() { }
    #[test] fn index_updated_on_vertex_remove() { }
    #[test] fn index_updated_on_property_set() { }
    #[test] fn unique_rejects_duplicate_on_insert() { }
    #[test] fn unique_rejects_duplicate_on_update() { }
    
    // ComparableValue / HashableValue
    #[test] fn comparable_value_ordering() { }
    #[test] fn comparable_value_int_float_comparison() { }
    #[test] fn hashable_value_equality() { }
    #[test] fn hashable_value_hash_consistency() { }
}
```

### 11.2 Integration Tests

```rust
#[test]
fn traversal_uses_btree_index_for_has_value() {
    let mut graph = Graph::new();
    
    // Add 10,000 vertices
    for i in 0..10_000 {
        graph.add_vertex("person", HashMap::from([
            ("age".into(), (i % 100).into()),
        ]));
    }
    
    // Create index
    graph.create_index(
        IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build().unwrap()
    ).unwrap();
    
    // Query should use index
    let g = graph.traversal();
    let results = g.v()
        .has_label("person")
        .has_value("age", 42i64)
        .to_list();
    
    assert_eq!(results.len(), 100);  // 10000 / 100 = 100 with age 42
}

#[test]
fn traversal_uses_btree_index_for_range() {
    // Similar test with has_where and range predicate
}

#[test]
fn traversal_uses_unique_index_for_has_value() {
    let mut graph = Graph::new();
    
    // Add users with unique emails
    for i in 0..1000 {
        graph.add_vertex("user", HashMap::from([
            ("email".into(), format!("user{}@example.com", i).into()),
        ]));
    }
    
    // Create unique index
    graph.create_index(
        IndexBuilder::vertex()
            .label("user")
            .property("email")
            .unique()
            .build().unwrap()
    ).unwrap();
    
    // Query should use unique index - O(1)
    let g = graph.traversal();
    let user = g.v()
        .has_label("user")
        .has_value("email", "user500@example.com")
        .next();
    
    assert!(user.is_some());
}

#[test]
fn unique_index_rejects_duplicate() {
    let mut graph = Graph::new();
    
    graph.create_index(
        IndexBuilder::vertex()
            .label("user")
            .property("email")
            .unique()
            .build().unwrap()
    ).unwrap();
    
    graph.add_vertex("user", HashMap::from([
        ("email".into(), "alice@example.com".into()),
    ]));
    
    // Second insert with same email should fail
    let result = std::panic::catch_unwind(|| {
        graph.add_vertex("user", HashMap::from([
            ("email".into(), "alice@example.com".into()),
        ]));
    });
    
    assert!(result.is_err());
}

#[test]
fn gql_uses_index_for_pattern_property() {
    // Similar test verifying GQL queries use indexes
}
```

### 11.3 Benchmark Tests

```rust
#[bench]
fn bench_has_value_without_index(b: &mut Bencher) {
    let graph = create_graph_with_10k_vertices();
    let snapshot = graph.snapshot();
    
    b.iter(|| {
        snapshot.traversal()
            .v()
            .has_label("person")
            .has_value("age", 42i64)
            .count()
    });
}

#[bench]
fn bench_has_value_with_index(b: &mut Bencher) {
    let mut graph = create_graph_with_10k_vertices();
    graph.create_index(/* ... */);
    let snapshot = graph.snapshot();
    
    b.iter(|| {
        snapshot.traversal()
            .v()
            .has_label("person")
            .has_value("age", 42i64)
            .count()
    });
}
```

---

## 12. Performance Targets

| Operation | Target (10K elements) | Target (1M elements) |
|-----------|----------------------|---------------------|
| B+ tree point lookup | < 1 µs | < 5 µs |
| B+ tree range (100 results) | < 10 µs | < 50 µs |
| Unique index lookup | < 500 ns | < 1 µs |
| Index insert | < 1 µs | < 5 µs |
| Index remove | < 1 µs | < 5 µs |
| Create index (populate) | < 100 ms | < 5 s |

---

## 13. Implementation Plan

### Phase 1: Core Data Structures (2-3 days)
1. Create `src/index/mod.rs` module
2. Implement `IndexSpec`, `IndexBuilder`, `IndexFilter`, `IndexPredicate`
3. Implement `ComparableValue` and `HashableValue` wrappers
4. Implement `PropertyIndex` trait
5. Unit tests for data structures

### Phase 2: BTreeIndex Implementation (2-3 days)
1. Implement `BTreeIndex` using `std::collections::BTreeMap`
2. Implement `lookup_eq`, `lookup_range`, `insert`, `remove`
3. Implement `IndexStatistics` tracking
4. Unit tests for B+ tree operations

### Phase 3: UniqueIndex Implementation (1-2 days)
1. Implement `UniqueIndex` with HashMap
2. Implement uniqueness constraint enforcement
3. Implement reverse lookup for efficient removal
4. Unit tests for unique index operations

### Phase 4: Graph Integration (2-3 days)
1. Add `indexes` field to `Graph`
2. Implement `create_index`, `drop_index`, `list_indexes`
3. Implement `populate_index` for existing data
4. Update `add_vertex`, `add_edge` to maintain indexes
5. Update `set_vertex_property`, `set_edge_property` to maintain indexes
6. Update `remove_vertex`, `remove_edge` to maintain indexes
7. Integration tests for index maintenance

### Phase 5: Traversal Integration (2-3 days)
1. Add `IndexedStorage` trait extension
2. Modify `HasValueStep` to check for indexes
3. Modify `HasWhereStep` to check for indexes
4. Add `ExecutionContext` tracking for index eligibility
5. Integration tests for index-accelerated traversals

### Phase 6: GQL Integration (1-2 days)
1. Add index DDL grammar rules (`CREATE INDEX`, `DROP INDEX`)
2. Implement DDL statement execution
3. Modify GQL compiler to use indexes for pattern matching
4. Integration tests for GQL index usage

### Phase 7: MmapGraph Integration (2-3 days)
1. Implement index serialization/deserialization
2. Implement index manifest file
3. Integrate index persistence with WAL
4. Recovery: rebuild indexes from WAL if needed
5. Integration tests for persistent indexes

### Phase 8: Documentation & Polish (1-2 days)
1. Document public API
2. Add examples
3. Write benchmarks
4. Performance tuning

**Total Estimated Time: 2-3 weeks**

---

## 14. Future Extensions

1. **Composite Indexes** - Index multiple properties together
2. **Covering Indexes** - Include additional properties to avoid vertex lookup
3. **Partial Indexes** - Index only elements matching a predicate
4. **Expression Indexes** - Index computed values
5. **Full-text Indexes** - Text search with tokenization
6. **Spatial Indexes** - R-tree for geometric queries
7. **Index Hints** - Allow users to force/prevent index usage
8. **Query Plan Explain** - Show which indexes would be used

---

## 15. References

- [spec-18-property-indexes.md](./spec-18-property-indexes.md) - Original design notes
- [storage.md](../guiding-documents/storage.md) - Storage architecture
- [std::collections::BTreeMap](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html) - Rust B-tree
- [RoaringBitmap](https://roaringbitmap.org/) - Efficient bitmap library
