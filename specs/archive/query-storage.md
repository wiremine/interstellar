# Mmap-Backed Query Storage Specification

## Overview

This specification defines a persistent query library feature for Interstellar that stores named Gremlin and GQL queries within the existing mmap graph file. Queries are validated on save, support typed parameters, and are accessed via methods integrated into `MmapGraph`.

## Goals

1. **Named Query Library**: Store reusable queries by name for later execution
2. **Validation on Save**: Parse and validate query syntax before persisting
3. **Typed Parameters**: Support `$param` syntax with inferred/declared types
4. **Integrated API**: Access queries via `MmapGraph` methods
5. **Dynamic Growth**: Query region grows automatically when capacity is exceeded

## Non-Goals

- Query versioning/history (out of scope for v1)
- Query sharing across databases
- Remote query storage

---

## File Format Extension

### Header Changes

The existing `FileHeader` has 36 bytes of reserved space at offset 156. We use 24 bytes for query storage metadata:

```text
Offset | Size | Field                | Description
-------|------|----------------------|------------------------------------------
156    | 8    | query_store_offset   | Byte offset to query region start
164    | 8    | query_store_end      | Byte offset to query data end (exclusive)
172    | 4    | query_count          | Number of active (non-deleted) queries
176    | 4    | next_query_id        | Next query ID to allocate
180    | 12   | _reserved_queries    | Reserved for future query-related fields
```

**Version Compatibility**:
- Files with `query_store_offset == 0` have no query region (backward compatible)
- When queries are saved, set `min_reader_version = 3` to prevent older readers from corrupting data
- Readers with version < 3 can still open files without queries

### Query Record Structure

Queries are stored as variable-length records in a dedicated region:

```rust
/// Query record header (32 bytes fixed + variable data)
#[repr(C, packed)]
pub struct QueryRecord {
    /// Query ID (unique, never reused)
    pub id: u32,
    
    /// Flags bitfield
    /// - bit 0: deleted flag
    /// - bits 1-2: query type (0=reserved, 1=gremlin, 2=gql)
    pub flags: u16,
    
    /// Number of parameters
    pub param_count: u16,
    
    /// Length of name string (UTF-8 bytes)
    pub name_len: u16,
    
    /// Length of description string (UTF-8 bytes)  
    pub description_len: u16,
    
    /// Length of query text (UTF-8 bytes)
    pub query_len: u32,
    
    /// Total record size including header and all variable data
    /// Used for skipping to next record
    pub record_size: u32,
    
    /// Offset to next record (u64::MAX if last)
    pub next: u64,
    
    /// Offset to previous record (u64::MAX if first) - enables deletion
    pub prev: u64,
}
// Header size: 4 + 2 + 2 + 2 + 2 + 4 + 4 + 8 + 8 = 36 bytes

/// Query record size constant
pub const QUERY_RECORD_HEADER_SIZE: usize = 36;
```

**Variable data layout** (immediately follows header):

```text
Offset              | Size              | Content
--------------------|-------------------|------------------
0                   | name_len          | Query name (UTF-8)
name_len            | description_len   | Description (UTF-8)  
name_len+desc_len   | query_len         | Query text (UTF-8)
...                 | param_count * N   | Parameter entries
```

### Parameter Entry Structure

Each parameter is stored with its name and inferred type:

```rust
/// Parameter entry (variable length)
#[repr(C, packed)]
pub struct ParameterEntry {
    /// Parameter name length
    pub name_len: u16,
    
    /// Expected value type (Value discriminant)
    /// 0xFF = any type (unknown)
    pub value_type: u8,
    
    /// Reserved for future use (alignment)
    pub _reserved: u8,
}
// Header: 4 bytes, followed by name_len bytes of UTF-8 name

pub const PARAMETER_ENTRY_HEADER_SIZE: usize = 4;
```

### Query Region Layout

```text
┌─────────────────────────────────────────────────────────────┐
│ Query Region Header (16 bytes)                              │
│   magic: u32 = 0x51525953 ("QRYS")                         │
│   version: u32 = 1                                          │
│   first_query: u64 = offset to first record (or u64::MAX)  │
├─────────────────────────────────────────────────────────────┤
│ QueryRecord 1 (header + variable data)                      │
├─────────────────────────────────────────────────────────────┤
│ QueryRecord 2 (header + variable data)                      │
├─────────────────────────────────────────────────────────────┤
│ ... more records ...                                        │
├─────────────────────────────────────────────────────────────┤
│ Free space for new queries                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Types

### Public API Types

```rust
// File: interstellar/src/query/mod.rs

/// Query language type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryType {
    /// Gremlin traversal language
    Gremlin = 1,
    /// Graph Query Language (GQL/Cypher-like)
    Gql = 2,
}

/// Expected parameter type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParameterType {
    /// Type not constrained (any Value)
    Any,
    /// Expects a string value
    String,
    /// Expects an integer (i64)
    Integer,
    /// Expects a float (f64)
    Float,
    /// Expects a boolean
    Boolean,
    /// Expects a vertex ID
    VertexId,
    /// Expects an edge ID
    EdgeId,
    /// Expects a list
    List,
    /// Expects a map
    Map,
}

/// A query parameter definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParameter {
    /// Parameter name (without $ prefix)
    pub name: String,
    /// Expected value type
    pub param_type: ParameterType,
}

/// A saved query entry
#[derive(Debug, Clone)]
pub struct SavedQuery {
    /// Unique query ID
    pub id: u32,
    /// Query name (unique across all queries)
    pub name: String,
    /// Query language type
    pub query_type: QueryType,
    /// Human-readable description
    pub description: String,
    /// Query text (may contain $param placeholders)
    pub query: String,
    /// Declared/inferred parameters
    pub parameters: Vec<QueryParameter>,
}

/// Parameter bindings for query execution
pub type QueryParams = HashMap<String, Value>;
```

### Error Types

```rust
// Addition to interstellar/src/error.rs

/// Errors related to query storage and execution
#[derive(Debug, Error)]
pub enum QueryError {
    /// Query with this name already exists
    #[error("query already exists: {0}")]
    AlreadyExists(String),
    
    /// Query not found
    #[error("query not found: {0}")]
    NotFound(String),
    
    /// Query syntax is invalid
    #[error("invalid query syntax: {0}")]
    InvalidSyntax(String),
    
    /// Missing required parameter
    #[error("missing parameter: {0}")]
    MissingParameter(String),
    
    /// Parameter type mismatch
    #[error("parameter type mismatch for '{0}': expected {1:?}, got {2:?}")]
    TypeMismatch(String, ParameterType, String),
    
    /// Unexpected parameter provided
    #[error("unexpected parameter: {0}")]
    UnexpectedParameter(String),
    
    /// Query region is full and cannot grow
    #[error("query storage full")]
    StorageFull,
    
    /// Name validation failed
    #[error("invalid query name: {0}")]
    InvalidName(String),
    
    /// Storage error during query operation
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    
    /// Gremlin parse/compile error
    #[error("gremlin error: {0}")]
    Gremlin(#[from] GremlinError),
    
    /// GQL parse/compile error  
    #[error("gql error: {0}")]
    Gql(#[from] GqlError),
}
```

---

## API Design

### MmapGraph Methods

```rust
impl MmapGraph {
    // =========================================================================
    // Query CRUD Operations
    // =========================================================================
    
    /// Save a new query to the library.
    ///
    /// Validates the query syntax before saving. Extracts parameters from
    /// the query text (variables prefixed with `$`).
    ///
    /// # Arguments
    /// * `name` - Unique query name (alphanumeric, underscores, hyphens)
    /// * `query_type` - Language type (Gremlin or GQL)
    /// * `description` - Human-readable description
    /// * `query` - Query text (may contain $param placeholders)
    ///
    /// # Returns
    /// The assigned query ID on success.
    ///
    /// # Errors
    /// - `QueryError::AlreadyExists` - Name already in use
    /// - `QueryError::InvalidSyntax` - Query failed to parse
    /// - `QueryError::InvalidName` - Name contains invalid characters
    /// - `QueryError::StorageFull` - Cannot allocate space for query
    ///
    /// # Example
    /// ```rust,no_run
    /// use interstellar::query::QueryType;
    /// 
    /// let id = graph.save_query(
    ///     "find_person_by_name",
    ///     QueryType::Gremlin,
    ///     "Find a person vertex by name property",
    ///     "g.V().has('person', 'name', $name)"
    /// )?;
    /// ```
    pub fn save_query(
        &self,
        name: &str,
        query_type: QueryType,
        description: &str,
        query: &str,
    ) -> Result<u32, QueryError>;
    
    /// Get a query by name.
    ///
    /// Returns `None` if no query exists with the given name.
    pub fn get_query(&self, name: &str) -> Option<SavedQuery>;
    
    /// Get a query by ID.
    ///
    /// Returns `None` if no query exists with the given ID.
    pub fn get_query_by_id(&self, id: u32) -> Option<SavedQuery>;
    
    /// List all saved queries.
    ///
    /// Returns queries in insertion order.
    pub fn list_queries(&self) -> Vec<SavedQuery>;
    
    /// Update an existing query.
    ///
    /// The query name cannot be changed. To rename, delete and re-create.
    ///
    /// # Errors
    /// - `QueryError::NotFound` - Query does not exist
    /// - `QueryError::InvalidSyntax` - New query text failed to parse
    pub fn update_query(
        &self,
        name: &str,
        description: Option<&str>,
        query: Option<&str>,
    ) -> Result<(), QueryError>;
    
    /// Delete a query by name.
    ///
    /// # Errors
    /// - `QueryError::NotFound` - Query does not exist
    pub fn delete_query(&self, name: &str) -> Result<(), QueryError>;
    
    // =========================================================================
    // Query Execution
    // =========================================================================
    
    /// Execute a saved query with parameter bindings.
    ///
    /// Parameters are substituted into the query before execution.
    ///
    /// # Arguments
    /// * `name` - Name of the saved query
    /// * `params` - Parameter name -> value bindings
    ///
    /// # Errors
    /// - `QueryError::NotFound` - Query does not exist
    /// - `QueryError::MissingParameter` - Required parameter not provided
    /// - `QueryError::TypeMismatch` - Parameter value has wrong type
    /// - `QueryError::UnexpectedParameter` - Unknown parameter provided
    ///
    /// # Example
    /// ```rust,no_run
    /// use interstellar::Value;
    /// use std::collections::HashMap;
    /// 
    /// let params = HashMap::from([
    ///     ("name".to_string(), Value::String("Alice".to_string())),
    /// ]);
    /// let results = graph.execute_query("find_person_by_name", params)?;
    /// ```
    pub fn execute_query(
        &self,
        name: &str,
        params: QueryParams,
    ) -> Result<Vec<Value>, QueryError>;
    
    /// Execute a saved query by ID with parameter bindings.
    pub fn execute_query_by_id(
        &self,
        id: u32,
        params: QueryParams,
    ) -> Result<Vec<Value>, QueryError>;
}
```

---

## Parameter Extraction

### Gremlin Parameters

Parameters in Gremlin queries use the `$name` syntax in value positions:

```gremlin
g.V().has('person', 'name', $name)           // String parameter
g.V($vertex_id)                               // VertexId parameter
g.V().has('age', gt($min_age))               // Integer parameter (from predicate context)
g.V().limit($count)                           // Integer parameter (from step context)
```

**Type inference rules**:
1. In `g.V($x)` or `g.E($x)` → `VertexId` or `EdgeId`
2. In predicates like `eq($x)`, `gt($x)`, `lt($x)` → infer from property if known, else `Any`
3. In `.limit($x)`, `.skip($x)`, `.range($x, $y)` → `Integer`
4. In `.has('label', 'prop', $x)` → `Any` (property value)
5. Default fallback → `Any`

### GQL Parameters

Parameters in GQL queries also use `$name` syntax:

```gql
MATCH (p:Person {name: $name}) RETURN p      // String parameter (from property)
MATCH (p:Person) WHERE p.age > $min_age      // Integer parameter (from comparison)
MATCH (p:Person) RETURN p LIMIT $count       // Integer parameter
```

**Type inference rules**:
1. In `LIMIT $x`, `SKIP $x` → `Integer`
2. In comparison `prop > $x`, `prop = $x` → infer from property if schema exists, else `Any`
3. In property patterns `{prop: $x}` → `Any`
4. Default fallback → `Any`

### Parameter Name Validation

- Must start with letter or underscore
- May contain letters, digits, underscores
- Case-sensitive
- Max length: 64 characters
- Regex: `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`

---

## Storage Operations

### Region Initialization

When the first query is saved:

1. Calculate initial query region size (16KB default)
2. Grow the file to accommodate query region after string table
3. Initialize query region header
4. Update file header with `query_store_offset` and `query_store_end`
5. Set `min_reader_version = 3`

### Saving a Query

1. Validate query name (format, uniqueness)
2. Parse query to validate syntax
3. Extract parameters and infer types
4. Calculate total record size
5. Check available space; grow region if needed
6. Allocate space and write record
7. Update linked list pointers
8. Update file header (`query_count`, `next_query_id`, `query_store_end`)
9. Log to WAL for durability

### Growing the Query Region

When query region is full:

1. Calculate new size (current * 2, min 16KB growth)
2. Check if string table can be relocated
3. Relocate string table to end of file
4. Extend query region into vacated space
5. Update all header offsets
6. Remap file

**Alternative (simpler)**: Query region is always at end of file, after string table. Growth simply extends the file.

### Deleting a Query

Soft delete with flag:
1. Set deleted flag in record
2. Update prev/next pointers to skip record
3. Decrement `query_count`
4. Space is not reclaimed (fragmentation)

Future: Compaction pass to reclaim deleted space.

---

## WAL Integration

### New WAL Entry Types

```rust
pub enum WalEntry {
    // ... existing entries ...
    
    /// Save a new query
    SaveQuery {
        id: u32,
        name: String,
        query_type: u8,
        description: String,
        query: String,
        parameters: Vec<(String, u8)>, // (name, type discriminant)
    },
    
    /// Update a query
    UpdateQuery {
        id: u32,
        description: Option<String>,
        query: Option<String>,
        parameters: Option<Vec<(String, u8)>>,
    },
    
    /// Delete a query
    DeleteQuery {
        id: u32,
    },
}
```

### Recovery

On recovery, replay query operations in order to rebuild query region state.

---

## Implementation Phases

### Phase 1: Core Infrastructure (~300 LoC)

**Files to modify/create**:
- `interstellar/src/storage/mmap/records.rs` - Add `QueryRecord`, `ParameterEntry`
- `interstellar/src/query/mod.rs` - New module with types
- `interstellar/src/error.rs` - Add `QueryError`
- `interstellar/src/lib.rs` - Export query module

**Tasks**:
1. Define `QueryRecord` and `ParameterEntry` packed structs
2. Add `from_bytes()` / `to_bytes()` methods
3. Create `QueryType`, `ParameterType`, `QueryParameter`, `SavedQuery` types
4. Add `QueryError` enum
5. Extend `FileHeader` to parse query offsets from reserved bytes

### Phase 2: Storage Layer (~400 LoC)

**Files to modify**:
- `interstellar/src/storage/mmap/mod.rs` - Add query methods to `MmapGraph`
- `interstellar/src/storage/mmap/query.rs` - New file for query storage logic

**Tasks**:
1. Add query region initialization in `initialize_new_file()`
2. Add in-memory query index (`HashMap<String, u32>` name→id)
3. Implement `save_query()` - serialize, write, update index
4. Implement `get_query()` / `get_query_by_id()` - lookup, deserialize
5. Implement `list_queries()` - iterate linked list
6. Implement `delete_query()` - soft delete
7. Implement `update_query()` - in-place update or delete+insert

### Phase 3: Validation & Parameters (~250 LoC)

**Files to modify**:
- `interstellar/src/query/params.rs` - New file for parameter extraction

**Tasks**:
1. Implement Gremlin parameter extractor using regex + AST analysis
2. Implement GQL parameter extractor
3. Implement type inference from query context
4. Integrate validation into `save_query()` flow
5. Add name validation logic

### Phase 4: Execution (~200 LoC)

**Files to modify**:
- `interstellar/src/query/execute.rs` - New file for execution logic

**Tasks**:
1. Implement parameter substitution for Gremlin
2. Implement parameter substitution for GQL
3. Implement type checking for parameter values
4. Wire up `execute_query()` to existing execution engines

### Phase 5: WAL & Testing (~350 LoC)

**Files to modify**:
- `interstellar/src/storage/mmap/wal.rs` - Add query WAL entries
- `interstellar/src/storage/mmap/recovery.rs` - Handle query recovery
- `interstellar/tests/query_storage.rs` - Integration tests

**Tasks**:
1. Add `SaveQuery`, `UpdateQuery`, `DeleteQuery` WAL entry variants
2. Implement WAL serialization/deserialization for query entries
3. Implement recovery replay for query operations
4. Write unit tests for record serialization
5. Write integration tests for CRUD operations
6. Write tests for parameter extraction
7. Write tests for query execution with parameters

---

## Test Cases

### Unit Tests

```rust
#[test]
fn test_query_record_roundtrip() { }

#[test]
fn test_parameter_entry_roundtrip() { }

#[test]
fn test_query_name_validation() { }

#[test]
fn test_gremlin_parameter_extraction() { }

#[test]
fn test_gql_parameter_extraction() { }

#[test]
fn test_parameter_type_inference() { }
```

### Integration Tests

```rust
#[test]
fn test_save_and_get_query() { }

#[test]
fn test_duplicate_name_rejected() { }

#[test]
fn test_invalid_syntax_rejected() { }

#[test]
fn test_list_queries() { }

#[test]
fn test_delete_query() { }

#[test]
fn test_update_query() { }

#[test]
fn test_execute_gremlin_with_params() { }

#[test]
fn test_execute_gql_with_params() { }

#[test]
fn test_missing_parameter_error() { }

#[test]
fn test_type_mismatch_error() { }

#[test]
fn test_query_persistence_across_reopen() { }

#[test]
fn test_wal_recovery_for_queries() { }

#[test]
fn test_region_growth() { }
```

---

## Future Enhancements (Out of Scope)

1. **Query versioning**: Track history of query changes
2. **Query categories/tags**: Organize queries into groups
3. **Query statistics**: Track execution count, avg time
4. **Query caching**: Cache compiled queries for faster execution
5. **Query import/export**: JSON/YAML format for sharing
6. **Query dependencies**: Track queries that reference other queries
7. **Region compaction**: Reclaim space from deleted queries

---

## Appendix: Example Usage

```rust
use interstellar::storage::MmapGraph;
use interstellar::query::{QueryType, QueryParams};
use interstellar::Value;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = MmapGraph::open("social.db")?;
    
    // Save some queries
    graph.save_query(
        "find_person",
        QueryType::Gremlin,
        "Find person by name",
        "g.V().has('person', 'name', $name)"
    )?;
    
    graph.save_query(
        "friends_of_friends",
        QueryType::Gremlin,
        "Get friends of friends for a person",
        "g.V($person_id).out('knows').out('knows').dedup()"
    )?;
    
    graph.save_query(
        "recent_posts",
        QueryType::Gql,
        "Get recent posts by a user",
        "MATCH (u:User {id: $user_id})-[:POSTED]->(p:Post)
         WHERE p.created_at > $since
         RETURN p ORDER BY p.created_at DESC LIMIT $limit"
    )?;
    
    // List all queries
    for query in graph.list_queries() {
        println!("{}: {} ({:?})", query.name, query.description, query.query_type);
        for param in &query.parameters {
            println!("  - ${}: {:?}", param.name, param.param_type);
        }
    }
    
    // Execute a query
    let results = graph.execute_query(
        "find_person",
        QueryParams::from([
            ("name".to_string(), Value::String("Alice".to_string())),
        ])
    )?;
    
    println!("Found {} results", results.len());
    
    Ok(())
}
```
