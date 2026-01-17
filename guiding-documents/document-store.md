# Document Store Extension

This document describes the extensions needed to use Interstellar as a document database with graph capabilities.

## Overview

Interstellar's current data model already supports document-style storage through:
- `Value::Map(HashMap<String, Value>)` for nested objects
- `Value::List(Vec<Value>)` for arrays
- Schema-free vertex/edge properties

The goal is to enhance this foundation to provide first-class document database features while retaining graph traversal capabilities—creating a hybrid document-graph database.

## Current State

### What Already Works

| Feature | Implementation | Location |
|---------|---------------|----------|
| Nested documents | `Value::Map` and `Value::List` | `src/value.rs:223-242` |
| Dynamic typing | `Value` enum with JSON-like types | `src/value.rs` |
| Schema-free storage | No schema enforcement | `src/storage/inmemory.rs` |
| Document relationships | Native graph edges | `src/storage/mod.rs:142-154` |
| Label-based collections | `vertices_with_label()` | `src/storage/mod.rs:227-228` |

### Current Limitations

1. **No secondary indexes** - Only label indexes exist; property queries require full scans
2. **No nested field access** - Cannot directly query `document.address.city`
3. **No query DSL** - Must use Gremlin traversal steps for all queries
4. **No aggregation pipeline** - Limited to what Gremlin steps provide
5. **No full-text search** - Only exact property matching

## Required Extensions

### Phase 1: Property Indexing

Add secondary indexes on document properties for efficient queries.

#### 1.1 Index Types

```rust
pub enum IndexType {
    /// Hash index for equality lookups: O(1)
    Hash,
    /// B-tree index for range queries: O(log n)
    BTree,
    /// Full-text index using inverted index
    FullText,
    /// Composite index on multiple fields
    Composite(Vec<String>),
}

pub struct PropertyIndex {
    /// The property path being indexed (e.g., "name" or "address.city")
    path: PropertyPath,
    /// Index type
    index_type: IndexType,
    /// Whether this index is unique
    unique: bool,
    /// Applies to vertices, edges, or both
    target: IndexTarget,
}
```

#### 1.2 Property Path Notation

Support dot-notation for nested field access:

```rust
pub struct PropertyPath {
    segments: Vec<String>,
}

impl PropertyPath {
    /// Parse "address.city" into ["address", "city"]
    pub fn parse(path: &str) -> Self;
    
    /// Extract value from a nested document
    pub fn extract(&self, value: &Value) -> Option<&Value>;
}
```

#### 1.3 Index Storage

```rust
pub trait IndexStorage {
    fn insert(&mut self, key: Value, id: VertexId);
    fn remove(&mut self, key: Value, id: VertexId);
    fn lookup(&self, key: &Value) -> impl Iterator<Item = VertexId>;
    fn range(&self, start: &Value, end: &Value) -> impl Iterator<Item = VertexId>;
}
```

#### 1.4 API Extensions

```rust
impl InMemoryGraph {
    /// Create an index on a property path
    pub fn create_index(
        &mut self,
        name: &str,
        path: &str,
        index_type: IndexType,
    ) -> Result<(), StorageError>;
    
    /// Drop an existing index
    pub fn drop_index(&mut self, name: &str) -> Result<(), StorageError>;
    
    /// List all indexes
    pub fn list_indexes(&self) -> Vec<&PropertyIndex>;
}
```

### Phase 2: Document Query DSL

Provide a more natural document query interface alongside Gremlin.

#### 2.1 Query Builder

```rust
pub struct DocumentQuery {
    collection: String,  // Vertex label
    filter: Option<Filter>,
    projection: Option<Vec<PropertyPath>>,
    sort: Option<Vec<SortField>>,
    skip: Option<usize>,
    limit: Option<usize>,
}

pub enum Filter {
    Eq(PropertyPath, Value),
    Ne(PropertyPath, Value),
    Gt(PropertyPath, Value),
    Gte(PropertyPath, Value),
    Lt(PropertyPath, Value),
    Lte(PropertyPath, Value),
    In(PropertyPath, Vec<Value>),
    Contains(PropertyPath, Value),  // Array contains
    Exists(PropertyPath),
    Regex(PropertyPath, String),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}
```

#### 2.2 Fluent API

```rust
// Example usage
let results = graph
    .collection("users")
    .filter(Filter::And(vec![
        Filter::Gte("age".into(), Value::Int(18)),
        Filter::Eq("status".into(), Value::String("active".into())),
    ]))
    .project(vec!["name", "email"])
    .sort(vec![SortField::desc("created_at")])
    .limit(10)
    .execute()?;
```

#### 2.3 Integration with Gremlin

The document query should compile to Gremlin traversal internally:

```rust
impl DocumentQuery {
    /// Convert to equivalent Gremlin traversal
    pub fn to_traversal<S: GraphStorage>(self, g: &GraphTraversalSource<S>) -> Traversal<...> {
        let mut t = g.v(()).has_label(&self.collection);
        
        if let Some(filter) = self.filter {
            t = self.apply_filter(t, filter);
        }
        // ... apply projection, sort, skip, limit
        t
    }
}
```

### Phase 3: Nested Field Access in Traversals

Extend Gremlin steps to support nested property paths.

#### 3.1 Enhanced `values()` Step

```rust
impl<In, S: GraphStorage> Traversal<In, S> {
    /// Get nested property value using dot notation
    /// g.V().values("address.city")
    pub fn values(self, path: impl Into<PropertyPath>) -> Traversal<In, Value, S>;
}
```

#### 3.2 Enhanced `has()` Predicate

```rust
// Filter by nested property
g.V()
    .has_label("user")
    .has("address.country", "USA")
    .has("profile.settings.theme", "dark")
```

#### 3.3 Implementation

```rust
impl PropertyPath {
    pub fn extract(&self, value: &Value) -> Option<&Value> {
        let mut current = value;
        for segment in &self.segments {
            match current {
                Value::Map(map) => {
                    current = map.get(segment)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }
}
```

### Phase 4: Aggregation Framework

Add aggregation capabilities for analytics on document collections.

#### 4.1 Aggregation Operations

```rust
pub enum AggregateOp {
    Count,
    Sum(PropertyPath),
    Avg(PropertyPath),
    Min(PropertyPath),
    Max(PropertyPath),
    First(PropertyPath),
    Last(PropertyPath),
    Push(PropertyPath),      // Collect into array
    AddToSet(PropertyPath),  // Collect unique values
}

pub struct GroupBy {
    keys: Vec<PropertyPath>,
    aggregates: Vec<(String, AggregateOp)>,
}
```

#### 4.2 Pipeline Stages

```rust
pub enum PipelineStage {
    Match(Filter),
    Project(Vec<(String, ProjectExpr)>),
    Group(GroupBy),
    Sort(Vec<SortField>),
    Skip(usize),
    Limit(usize),
    Unwind(PropertyPath),  // Flatten arrays
    Lookup {               // Join with another collection
        from: String,
        local_field: PropertyPath,
        foreign_field: PropertyPath,
        as_field: String,
    },
}
```

#### 4.3 API

```rust
let results = graph
    .collection("orders")
    .aggregate(vec![
        PipelineStage::Match(Filter::Gte("date".into(), start_date)),
        PipelineStage::Group(GroupBy {
            keys: vec!["customer_id".into()],
            aggregates: vec![
                ("total".into(), AggregateOp::Sum("amount".into())),
                ("count".into(), AggregateOp::Count),
            ],
        }),
        PipelineStage::Sort(vec![SortField::desc("total")]),
        PipelineStage::Limit(10),
    ])
    .execute()?;
```

### Phase 5: Full-Text Search

Add text search capabilities for document content.

#### 5.1 Text Index

```rust
pub struct TextIndex {
    /// Fields to index
    fields: Vec<PropertyPath>,
    /// Language for stemming/stop words
    language: String,
    /// Custom weights per field
    weights: HashMap<PropertyPath, f64>,
}
```

#### 5.2 Search API

```rust
// Create text index
graph.create_text_index(
    "user_search",
    vec!["name", "bio", "tags"],
    TextIndexOptions {
        language: "english",
        weights: [("name", 10.0), ("bio", 5.0), ("tags", 1.0)].into(),
    },
)?;

// Search
let results = graph
    .collection("users")
    .text_search("rust developer")
    .with_score()
    .limit(20)
    .execute()?;
```

#### 5.3 Implementation Options

1. **Built-in**: Implement inverted index with stemming (tantivy crate)
2. **External**: Integrate with Elasticsearch/Meilisearch
3. **Hybrid**: Simple built-in search with optional external integration

### Phase 6: Document Validation (Optional Schema)

Leverage the planned schema system for document validation.

#### 6.1 Collection Schema

```rust
pub struct CollectionSchema {
    /// Collection name (vertex label)
    name: String,
    /// JSON Schema for document validation
    validator: JsonSchema,
    /// Validation mode
    validation: ValidationMode,
}

pub enum ValidationMode {
    /// No validation
    Off,
    /// Log warnings but accept documents
    Warn,
    /// Reject invalid documents
    Strict,
}
```

#### 6.2 API

```rust
graph.create_collection("users", CollectionSchema {
    validator: json_schema!({
        "type": "object",
        "required": ["email", "name"],
        "properties": {
            "email": { "type": "string", "format": "email" },
            "name": { "type": "string", "minLength": 1 },
            "age": { "type": "integer", "minimum": 0 },
            "address": {
                "type": "object",
                "properties": {
                    "city": { "type": "string" },
                    "country": { "type": "string" }
                }
            }
        }
    }),
    validation: ValidationMode::Strict,
})?;
```

## Implementation Priority

| Phase | Feature | Effort | Value | Priority |
|-------|---------|--------|-------|----------|
| 1 | Property Indexing | Medium | High | **P0** |
| 2 | Document Query DSL | Medium | High | **P0** |
| 3 | Nested Field Access | Low | Medium | **P1** |
| 4 | Aggregation Framework | High | Medium | **P2** |
| 5 | Full-Text Search | High | Medium | **P2** |
| 6 | Document Validation | Low | Low | **P3** |

## Data Model Considerations

### Collections as Labels

Document collections map naturally to vertex labels:

```rust
// MongoDB equivalent: db.users.insertOne({...})
graph.add_vertex("users", properties)?;

// MongoDB equivalent: db.users.find({})
graph.collection("users").find_all()?;
// or via Gremlin:
g.V().has_label("users").to_list()?;
```

### Relationships as Edges

The graph model adds value over pure document stores:

```rust
// Create relationship between documents
graph.add_edge(user_id, order_id, "placed", HashMap::new())?;

// Traverse relationships (not possible in pure document DBs)
g.V(user_id)
    .out("placed")           // Orders placed by user
    .out("contains")         // Items in those orders
    .has_label("product")
    .values("name")
    .to_list()?;
```

### Embedding vs. Referencing

Support both patterns:

```rust
// Embedded document (denormalized)
let user = json!({
    "name": "Alice",
    "address": {           // Embedded
        "city": "Seattle",
        "country": "USA"
    }
});

// Referenced document (normalized, uses edges)
let user_id = graph.add_vertex("user", user_props)?;
let addr_id = graph.add_vertex("address", addr_props)?;
graph.add_edge(user_id, addr_id, "lives_at", HashMap::new())?;
```

## Example Usage

### Creating and Querying Documents

```rust
use interstellar::prelude::*;

let mut graph = InMemoryGraph::new();

// Create index for efficient queries
graph.create_index("users_email", "email", IndexType::Hash)?;
graph.create_index("users_age", "age", IndexType::BTree)?;

// Insert documents
let alice = graph.add_vertex("users", hashmap! {
    "name" => "Alice",
    "email" => "alice@example.com",
    "age" => 30,
    "address" => hashmap! {
        "city" => "Seattle",
        "country" => "USA",
    },
})?;

let bob = graph.add_vertex("users", hashmap! {
    "name" => "Bob", 
    "email" => "bob@example.com",
    "age" => 25,
    "address" => hashmap! {
        "city" => "Portland",
        "country" => "USA",
    },
})?;

// Create relationship
graph.add_edge(alice, bob, "knows", hashmap! {
    "since" => 2020,
})?;

// Document-style query
let adults_in_usa = graph
    .collection("users")
    .filter(Filter::And(vec![
        Filter::Gte("age".into(), Value::Int(18)),
        Filter::Eq("address.country".into(), Value::String("USA".into())),
    ]))
    .sort(vec![SortField::asc("name")])
    .execute()?;

// Graph traversal (unique to document-graph hybrid)
let friends_of_alice = g.V(alice)
    .out("knows")
    .values("name")
    .to_list()?;
// => ["Bob"]
```

## Comparison with Other Document Databases

| Feature | MongoDB | CouchDB | Interstellar (proposed) |
|---------|---------|---------|------------------------|
| Nested documents | ✅ | ✅ | ✅ |
| Secondary indexes | ✅ | ✅ | ✅ (Phase 1) |
| Query DSL | ✅ | ✅ | ✅ (Phase 2) |
| Aggregation | ✅ | ✅ (views) | ✅ (Phase 4) |
| Full-text search | ✅ (Atlas) | ✅ (Lucene) | ✅ (Phase 5) |
| Graph traversal | ❌ ($graphLookup limited) | ❌ | ✅ Native |
| ACID transactions | ✅ | ✅ | ⚠️ Planned |
| Sharding | ✅ | ✅ | ❌ Not planned |

## Dependencies

```toml
[dependencies]
# For full-text search (Phase 5)
tantivy = { version = "0.21", optional = true }

# For JSON Schema validation (Phase 6)
jsonschema = { version = "0.17", optional = true }

[features]
full-text = ["tantivy"]
schema-validation = ["jsonschema"]
```

## Conclusion

Interstellar's existing `Value` system provides a solid foundation for document storage. The proposed extensions would create a unique hybrid database that combines:

1. **Document flexibility** - Schema-free nested documents
2. **Graph power** - Native relationship traversal
3. **Query versatility** - Both document queries and Gremlin traversals

This positions Interstellar as a compelling alternative for applications that need both document storage and graph capabilities without running separate databases.
