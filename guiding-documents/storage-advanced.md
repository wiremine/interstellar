# Intersteller: Advanced Storage Features

This document covers advanced storage features planned for future phases: **Property Indexes**, **Compaction**, and **Compression**. These features build on top of the core memory-mapped storage architecture defined in [storage.md](./storage.md) and [spec-08-storage.md](../specs/spec-08-storage.md).

---

## Table of Contents

1. [Property Indexes](#1-property-indexes)
2. [Compaction](#2-compaction)
3. [Compression](#3-compression)
4. [Implementation Priorities](#4-implementation-priorities)

---

## 1. Property Indexes

### 1.1 Overview

Property indexes accelerate queries that filter by property values. Without indexes, a query like `g.v().has("age", 30)` must scan all vertices. With a property index, it becomes O(log n) + O(k) where k is the number of matching results.

```
Without index:  Scan all 1M vertices → 50ms
With index:     B+ tree lookup → 0.5ms
```

### 1.2 Index Types

#### 1.2.1 Single-Property Index

Indexes a single property key for elements with a specific label.

```rust
/// Index definition
pub struct PropertyIndex {
    /// Which elements this index covers
    element_type: ElementType,  // Vertex or Edge
    
    /// Label filter (e.g., only "person" vertices)
    label_id: u32,
    
    /// Property key being indexed
    property_key: String,
    
    /// Index data structure
    tree: BPlusTree<Value, RoaringBitmap>,
}

/// Usage
// Create index: "person" vertices by "age"
graph.create_index(IndexSpec {
    element_type: ElementType::Vertex,
    label: "person",
    property: "age",
})?;

// Query now uses index automatically
let seniors = g.v()
    .has_label("person")
    .has_where("age", p::gte(65))
    .to_list();
```

**Index Structure:**

```
B+ Tree: age → vertex IDs
┌─────────────────────────────────────────┐
│ Internal Node [25, 50, 75]              │
├──────┬──────┬──────┬──────┬─────────────┤
│  <25 │25-50 │50-75 │ >=75 │             │
└──┬───┴──┬───┴──┬───┴──┬───┴─────────────┘
   │      │      │      │
   ▼      ▼      ▼      ▼
┌─────┐┌─────┐┌─────┐┌─────┐
│Leaf ││Leaf ││Leaf ││Leaf │
│20→{1,5}│25→{2}│51→{3,8}│80→{4}│
│21→{7}  │30→{6}│65→{9}  │      │
└─────┘└─────┘└─────┘└─────┘
```

#### 1.2.2 Composite Index

Indexes multiple properties together for queries with multiple conditions.

```rust
/// Composite index on multiple properties
pub struct CompositeIndex {
    element_type: ElementType,
    label_id: u32,
    
    /// Properties in order (order matters for prefix queries)
    properties: Vec<String>,
    
    /// B+ tree with concatenated key
    tree: BPlusTree<CompositeKey, RoaringBitmap>,
}

/// Composite key is ordered tuple of values
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub struct CompositeKey(Vec<Value>);

/// Usage
// Index "person" by (country, city, age)
graph.create_composite_index(CompositeIndexSpec {
    element_type: ElementType::Vertex,
    label: "person",
    properties: vec!["country", "city", "age"],
})?;

// Efficient: uses full index
g.v().has_label("person")
    .has_value("country", "USA")
    .has_value("city", "NYC")
    .has_where("age", p::gt(30))
    .to_list();

// Efficient: uses index prefix (country, city)
g.v().has_label("person")
    .has_value("country", "USA")
    .has_value("city", "NYC")
    .to_list();

// Efficient: uses index prefix (country)
g.v().has_label("person")
    .has_value("country", "USA")
    .to_list();

// NOT efficient: can't use index (skips "country")
// Falls back to label scan + filter
g.v().has_label("person")
    .has_value("city", "NYC")
    .to_list();
```

#### 1.2.3 Unique Index

Enforces uniqueness constraint and provides O(1) lookup.

```rust
/// Unique index - enforces constraint + fast lookup
pub struct UniqueIndex {
    element_type: ElementType,
    label_id: u32,
    property_key: String,
    
    /// Hash map for O(1) lookup (value must be unique)
    map: HashMap<Value, ElementId>,
}

/// Usage
graph.create_unique_index(UniqueIndexSpec {
    element_type: ElementType::Vertex,
    label: "user",
    property: "email",
})?;

// O(1) lookup
let user = g.v()
    .has_label("user")
    .has_value("email", "alice@example.com")
    .next();

// Throws error on duplicate
graph.mutate()
    .add_v("user")
    .property("email", "alice@example.com")  // Error: duplicate!
    .build();
```

#### 1.2.4 Full-Text Index

For text search queries on string properties.

```rust
/// Full-text search index using inverted index
pub struct FullTextIndex {
    element_type: ElementType,
    label_id: u32,
    property_key: String,
    
    /// Inverted index: token → document IDs
    inverted: HashMap<String, RoaringBitmap>,
    
    /// Tokenizer configuration
    tokenizer: TokenizerConfig,
}

pub struct TokenizerConfig {
    lowercase: bool,
    stop_words: HashSet<String>,
    stemming: bool,
    min_token_length: usize,
}

/// Usage
graph.create_fulltext_index(FullTextIndexSpec {
    element_type: ElementType::Vertex,
    label: "document",
    property: "content",
    tokenizer: TokenizerConfig::default(),
})?;

// Text search
let results = g.v()
    .has_label("document")
    .has_text("content", "rust graph database")
    .to_list();
```

### 1.3 Index Storage (On-Disk)

Indexes are stored in separate files alongside the main data file.

```
my_graph.db          # Main data file
my_graph.wal         # Write-ahead log
my_graph.idx/        # Index directory
├── person_age.btree       # Single property index
├── person_location.btree  # Composite index
├── user_email.hash        # Unique index
└── document_content.inv   # Full-text index
```

**B+ Tree File Format:**

```
┌────────────────────────────────────────────────────────────────┐
│ Index File Header (64 bytes)                                   │
├────────────────────────────────────────────────────────────────┤
│ magic: u32 = 0x49445854 ("IDXT")                              │
│ version: u32                                                   │
│ index_type: u8 (0=single, 1=composite, 2=unique, 3=fulltext)  │
│ key_count: u64                                                 │
│ root_page: u64                                                 │
│ page_size: u32 (typically 4096)                               │
│ height: u32                                                    │
│ ... (padding)                                                  │
├────────────────────────────────────────────────────────────────┤
│ Page 0: Internal/Leaf Node                                     │
│ ┌──────────────────────────────────────────────────────────┐  │
│ │ page_type: u8 (0=internal, 1=leaf)                       │  │
│ │ key_count: u16                                            │  │
│ │ keys: [Value; N]                                          │  │
│ │ children/values: [u64; N+1] or [RoaringBitmap; N]        │  │
│ │ next_leaf: u64 (for leaf nodes, linked list)             │  │
│ └──────────────────────────────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────┤
│ Page 1...                                                      │
└────────────────────────────────────────────────────────────────┘
```

### 1.4 Index Maintenance

#### Automatic Updates

Indexes are updated automatically when data changes:

```rust
impl MmapGraph {
    fn add_vertex_with_indexes(
        &mut self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<VertexId, StorageError> {
        // 1. Add vertex to main storage
        let id = self.add_vertex(label, properties.clone())?;
        
        // 2. Update all applicable indexes
        let label_id = self.string_table.intern(label);
        
        for index in self.indexes_for_label(label_id) {
            match index {
                Index::Single(idx) => {
                    if let Some(value) = properties.get(&idx.property_key) {
                        idx.insert(value.clone(), id)?;
                    }
                }
                Index::Composite(idx) => {
                    let key = idx.extract_key(&properties);
                    idx.insert(key, id)?;
                }
                Index::Unique(idx) => {
                    if let Some(value) = properties.get(&idx.property_key) {
                        idx.insert_unique(value.clone(), id)?;
                    }
                }
                // ...
            }
        }
        
        Ok(id)
    }
}
```

#### Index Rebuild

Rebuild indexes from scratch (e.g., after corruption or schema change):

```rust
impl MmapGraph {
    /// Rebuild a specific index from source data
    pub fn rebuild_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        let index = self.get_index_mut(index_name)?;
        index.clear()?;
        
        // Scan all elements matching the index criteria
        let label_id = index.label_id();
        
        for vertex in self.vertices_with_label_id(label_id) {
            if let Some(value) = vertex.properties.get(&index.property_key()) {
                index.insert(value.clone(), vertex.id)?;
            }
        }
        
        index.sync()?;
        Ok(())
    }
    
    /// Rebuild all indexes (e.g., after recovery)
    pub fn rebuild_all_indexes(&mut self) -> Result<(), StorageError> {
        for index_name in self.list_indexes() {
            self.rebuild_index(&index_name)?;
        }
        Ok(())
    }
}
```

### 1.5 Query Optimization

The query planner selects the best index for each query:

```rust
/// Query planner selects optimal index
pub struct QueryPlanner<'g> {
    graph: &'g MmapGraph,
}

impl<'g> QueryPlanner<'g> {
    pub fn plan(&self, traversal: &Traversal) -> QueryPlan {
        let mut plan = QueryPlan::new();
        
        // Extract filter conditions
        let filters = self.extract_filters(traversal);
        
        // Find applicable indexes
        for filter in &filters {
            if let Some(index) = self.find_best_index(filter) {
                plan.add_index_scan(index, filter);
            } else {
                plan.add_full_scan(filter);
            }
        }
        
        // Estimate costs and reorder
        plan.optimize();
        
        plan
    }
    
    fn find_best_index(&self, filter: &Filter) -> Option<&Index> {
        let candidates: Vec<_> = self.graph.indexes()
            .filter(|idx| idx.covers(filter))
            .collect();
        
        // Prefer: unique > composite (more specific) > single
        // Consider: selectivity estimates from statistics
        candidates.into_iter()
            .max_by_key(|idx| idx.selectivity_score(filter))
    }
}

/// Cost estimation for index selection
impl Index {
    fn selectivity_score(&self, filter: &Filter) -> u64 {
        match self {
            Index::Unique(_) => 1000,  // Best: single result
            Index::Composite(idx) => {
                // Score based on how many prefix columns are used
                let used = filter.matching_prefix_columns(&idx.properties);
                100 * used as u64
            }
            Index::Single(_) => 10,
        }
    }
}
```

### 1.6 Index Statistics

Maintain statistics for query planning:

```rust
/// Statistics for query optimization
pub struct IndexStatistics {
    /// Total number of indexed elements
    cardinality: u64,
    
    /// Number of distinct values
    distinct_values: u64,
    
    /// Value distribution histogram (for range queries)
    histogram: Histogram,
    
    /// Last update timestamp
    last_updated: u64,
}

pub struct Histogram {
    /// Bucket boundaries
    bounds: Vec<Value>,
    
    /// Count per bucket
    counts: Vec<u64>,
}

impl MmapGraph {
    /// Update statistics for all indexes
    pub fn analyze(&mut self) -> Result<(), StorageError> {
        for index in self.indexes_mut() {
            index.compute_statistics()?;
        }
        Ok(())
    }
}
```

---

## 2. Compaction

### 2.1 Overview

Over time, deletions create "holes" (fragmentation) in the data file. Compaction rewrites the file to reclaim space and improve read performance.

```
Before Compaction:
┌───────┬─────────┬───────┬─────────┬───────┬─────────┐
│Node 0 │ DELETED │Node 2 │ DELETED │ DELETED│Node 5  │
└───────┴─────────┴───────┴─────────┴────────┴────────┘
File size: 288 bytes, Active data: 144 bytes (50% utilization)

After Compaction:
┌───────┬───────┬───────┐
│Node 0 │Node 2 │Node 5 │
└───────┴───────┴───────┘
File size: 144 bytes, Active data: 144 bytes (100% utilization)
```

### 2.2 Compaction Strategies

#### 2.2.1 Full Compaction

Rewrites the entire file. Simple but requires significant I/O and temporary space.

```rust
impl MmapGraph {
    /// Full compaction - rewrite entire database
    pub fn compact(&mut self) -> Result<CompactionStats, StorageError> {
        let start = Instant::now();
        let original_size = self.file_size();
        
        // 1. Create new file
        let temp_path = self.path.with_extension("db.compact");
        let mut new_file = MmapGraph::create_empty(&temp_path)?;
        
        // 2. Copy all active vertices (builds ID mapping)
        let mut vertex_map: HashMap<VertexId, VertexId> = HashMap::new();
        
        for old_vertex in self.all_vertices() {
            let new_id = new_file.add_vertex_raw(
                old_vertex.label_id,
                old_vertex.properties.clone(),
            )?;
            vertex_map.insert(old_vertex.id, new_id);
        }
        
        // 3. Copy all active edges (translating vertex IDs)
        for old_edge in self.all_edges() {
            let new_src = vertex_map[&VertexId(old_edge.src)];
            let new_dst = vertex_map[&VertexId(old_edge.dst)];
            
            new_file.add_edge_raw(
                new_src,
                new_dst,
                old_edge.label_id,
                old_edge.properties.clone(),
            )?;
        }
        
        // 4. Sync and swap files
        new_file.sync()?;
        
        // Atomic swap (Unix: rename, Windows: ReplaceFile)
        std::fs::rename(&temp_path, &self.path)?;
        
        // 5. Reopen and rebuild indexes
        *self = MmapGraph::open(&self.path)?;
        self.rebuild_all_indexes()?;
        
        Ok(CompactionStats {
            duration: start.elapsed(),
            original_size,
            compacted_size: self.file_size(),
            vertices_moved: vertex_map.len() as u64,
        })
    }
}

pub struct CompactionStats {
    pub duration: Duration,
    pub original_size: u64,
    pub compacted_size: u64,
    pub vertices_moved: u64,
}
```

#### 2.2.2 Incremental Compaction

Compacts portions of the file at a time, reducing I/O spikes.

```rust
impl MmapGraph {
    /// Incremental compaction - compact one segment at a time
    pub fn compact_incremental(&mut self, segment_size: usize) -> Result<bool, StorageError> {
        // Find most fragmented segment
        let segment = self.find_most_fragmented_segment(segment_size)?;
        
        if segment.fragmentation_ratio() < 0.3 {
            // Not worth compacting
            return Ok(false);
        }
        
        // Compact just this segment
        self.compact_segment(segment)?;
        
        Ok(true)
    }
    
    fn find_most_fragmented_segment(&self, size: usize) -> Result<Segment, StorageError> {
        let header = self.read_header();
        let total_slots = header.node_capacity as usize;
        
        let mut worst_segment = None;
        let mut worst_ratio = 0.0;
        
        for start in (0..total_slots).step_by(size) {
            let end = (start + size).min(total_slots);
            let segment = Segment { start, end };
            
            let deleted = self.count_deleted_in_range(start, end);
            let ratio = deleted as f64 / (end - start) as f64;
            
            if ratio > worst_ratio {
                worst_ratio = ratio;
                worst_segment = Some(segment);
            }
        }
        
        worst_segment.ok_or(StorageError::NoSegmentFound)
    }
    
    fn compact_segment(&mut self, segment: Segment) -> Result<(), StorageError> {
        // Move active records from segment to free slots elsewhere
        // Update all references (edges pointing to moved vertices)
        // This is complex due to reference updates
        
        // ... implementation details
        Ok(())
    }
}
```

#### 2.2.3 Online Compaction

Compact while allowing concurrent reads (but blocking writes).

```rust
impl MmapGraph {
    /// Online compaction - allows reads during compaction
    pub fn compact_online(&self) -> Result<CompactionStats, StorageError> {
        // 1. Take read lock (blocks new writes, allows reads)
        let _write_guard = self.write_lock.lock();
        
        // 2. Create compacted copy (readers see old data)
        let temp_path = self.path.with_extension("db.compact");
        self.create_compacted_copy(&temp_path)?;
        
        // 3. Brief exclusive lock for swap
        // Readers will be blocked momentarily
        drop(_write_guard);
        let _exclusive = self.exclusive_lock();
        
        // 4. Atomic swap
        std::fs::rename(&temp_path, &self.path)?;
        
        // 5. Reopen (invalidates existing snapshots)
        self.reopen()?;
        
        Ok(CompactionStats::default())
    }
}
```

### 2.3 Compaction Triggers

#### Automatic Compaction

```rust
pub struct CompactionConfig {
    /// Trigger compaction when fragmentation exceeds this ratio
    pub fragmentation_threshold: f64,  // e.g., 0.3 = 30% deleted
    
    /// Minimum file size to consider compaction
    pub min_file_size: u64,  // e.g., 100MB
    
    /// Maximum file size growth before forced compaction
    pub max_growth_ratio: f64,  // e.g., 2.0 = 2x original size
    
    /// Check interval
    pub check_interval: Duration,
    
    /// Preferred compaction time window (e.g., off-peak hours)
    pub preferred_window: Option<TimeWindow>,
}

impl MmapGraph {
    /// Check if compaction is needed
    pub fn should_compact(&self) -> bool {
        let stats = self.storage_stats();
        
        let fragmentation = stats.deleted_bytes as f64 / stats.total_bytes as f64;
        
        fragmentation > self.compaction_config.fragmentation_threshold
            && stats.total_bytes > self.compaction_config.min_file_size
    }
    
    /// Background compaction thread
    pub fn start_compaction_thread(self: Arc<Self>) -> JoinHandle<()> {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(self.compaction_config.check_interval);
                
                if self.should_compact() {
                    if let Some(window) = &self.compaction_config.preferred_window {
                        if !window.is_now() {
                            continue;
                        }
                    }
                    
                    if let Err(e) = self.compact_online() {
                        eprintln!("Compaction failed: {}", e);
                    }
                }
            }
        })
    }
}
```

### 2.4 Compaction and WAL

During compaction, the WAL must be handled carefully:

```rust
impl MmapGraph {
    fn compact_with_wal(&mut self) -> Result<(), StorageError> {
        // 1. Checkpoint WAL (apply all pending transactions)
        self.checkpoint()?;
        
        // 2. Block new transactions
        let _tx_lock = self.transaction_lock.lock();
        
        // 3. Perform compaction
        self.compact()?;
        
        // 4. Clear WAL (all data is now in compacted file)
        self.wal.truncate()?;
        
        // 5. Resume transactions
        drop(_tx_lock);
        
        Ok(())
    }
}
```

### 2.5 ID Remapping

Compaction changes element IDs. External references must be updated:

```rust
/// ID remapping after compaction
pub struct IdRemapping {
    vertex_map: HashMap<VertexId, VertexId>,
    edge_map: HashMap<EdgeId, EdgeId>,
}

impl MmapGraph {
    /// Compact and return ID mapping for external reference updates
    pub fn compact_with_mapping(&mut self) -> Result<IdRemapping, StorageError> {
        let mut vertex_map = HashMap::new();
        let mut edge_map = HashMap::new();
        
        // ... compaction logic that populates maps ...
        
        Ok(IdRemapping { vertex_map, edge_map })
    }
}

// Usage: update application caches
let mapping = graph.compact_with_mapping()?;
for (old_id, new_id) in mapping.vertex_map {
    application_cache.update_vertex_id(old_id, new_id);
}
```

**Alternative: Stable IDs**

Use indirection to maintain stable external IDs:

```rust
/// Stable ID system - external IDs never change
pub struct StableIdGraph {
    /// External stable ID → internal slot ID
    id_table: HashMap<StableId, SlotId>,
    
    /// Internal storage uses slot IDs
    storage: MmapGraph,
}

impl StableIdGraph {
    pub fn get_vertex(&self, stable_id: StableId) -> Option<Vertex> {
        let slot_id = self.id_table.get(&stable_id)?;
        self.storage.get_vertex_by_slot(*slot_id)
    }
    
    /// Compaction only changes internal slot IDs
    /// External StableIds remain valid
    pub fn compact(&mut self) -> Result<(), StorageError> {
        let mapping = self.storage.compact_with_mapping()?;
        
        // Update id_table to point to new slot IDs
        for (stable_id, slot_id) in self.id_table.iter_mut() {
            if let Some(new_slot) = mapping.vertex_map.get(&VertexId(slot_id.0)) {
                *slot_id = SlotId(new_slot.0);
            }
        }
        
        Ok(())
    }
}
```

---

## 3. Compression

### 3.1 Overview

Compression reduces storage size and can improve read performance by reducing I/O. The key is choosing the right compression strategy for different data types.

```
Uncompressed: 1.0 GB
Compressed:   0.3 GB (3.3x ratio)

Read performance:
- Uncompressed: 100 MB/s disk → 100 MB/s effective
- Compressed:   100 MB/s disk × 3.3 decompression = 250 MB/s effective
  (if decompression is fast enough)
```

### 3.2 Compression Strategies

#### 3.2.1 Page-Level Compression

Compress individual pages (typically 4KB-64KB).

```rust
/// Page-level compression configuration
pub struct PageCompressionConfig {
    /// Compression algorithm
    pub algorithm: CompressionAlgorithm,
    
    /// Page size before compression
    pub page_size: usize,  // e.g., 4096
    
    /// Minimum compression ratio to keep compressed
    pub min_ratio: f64,  // e.g., 0.9 (10% savings minimum)
}

#[derive(Clone, Copy)]
pub enum CompressionAlgorithm {
    None,
    Lz4,      // Fast compression/decompression
    Zstd,     // Good balance of ratio and speed
    Snappy,   // Very fast, moderate ratio
}

/// Compressed page on disk
#[repr(C, packed)]
pub struct CompressedPageHeader {
    /// Original (uncompressed) size
    original_size: u32,
    
    /// Compressed size
    compressed_size: u32,
    
    /// Compression algorithm used
    algorithm: u8,
    
    /// Checksum of compressed data
    checksum: u32,
}

impl MmapGraph {
    /// Read a page, decompressing if necessary
    fn read_page(&self, page_id: u64) -> Result<Vec<u8>, StorageError> {
        let header = self.read_page_header(page_id)?;
        
        if header.algorithm == CompressionAlgorithm::None as u8 {
            // Page not compressed
            return self.read_raw_page(page_id);
        }
        
        // Read compressed data
        let compressed = self.read_compressed_page(page_id, header.compressed_size)?;
        
        // Verify checksum
        if crc32(&compressed) != header.checksum {
            return Err(StorageError::ChecksumMismatch);
        }
        
        // Decompress
        let algorithm = CompressionAlgorithm::from_u8(header.algorithm);
        decompress(algorithm, &compressed, header.original_size)
    }
    
    /// Write a page, compressing if beneficial
    fn write_page(&mut self, page_id: u64, data: &[u8]) -> Result<(), StorageError> {
        let compressed = compress(self.compression_config.algorithm, data);
        
        let ratio = compressed.len() as f64 / data.len() as f64;
        
        if ratio < self.compression_config.min_ratio {
            // Compression is beneficial
            self.write_compressed_page(page_id, &compressed)?;
        } else {
            // Store uncompressed
            self.write_raw_page(page_id, data)?;
        }
        
        Ok(())
    }
}
```

#### 3.2.2 Column-Oriented Compression

Store similar data together for better compression ratios.

```
Traditional (row-oriented):
[Node0: id, label, prop1, prop2]
[Node1: id, label, prop1, prop2]
[Node2: id, label, prop1, prop2]

Column-oriented:
[ids:    0, 1, 2, ...]        ← Compress together (similar values)
[labels: 5, 5, 7, ...]        ← Compress together (many duplicates)
[prop1:  "Alice", "Bob", ...] ← Compress together (strings)
[prop2:  30, 35, 28, ...]     ← Compress together (integers)
```

```rust
/// Column-oriented storage for better compression
pub struct ColumnStore {
    /// IDs stored contiguously
    ids: CompressedColumn<u64>,
    
    /// Labels stored contiguously (excellent compression due to repetition)
    label_ids: CompressedColumn<u32>,
    
    /// Flags stored contiguously
    flags: CompressedColumn<u32>,
    
    /// Edge pointers stored contiguously
    first_out_edges: CompressedColumn<u64>,
    first_in_edges: CompressedColumn<u64>,
    
    /// Property pointers stored contiguously
    prop_heads: CompressedColumn<u64>,
}

pub struct CompressedColumn<T> {
    /// Compression method
    encoding: ColumnEncoding,
    
    /// Compressed data
    data: Vec<u8>,
    
    /// Number of elements
    count: usize,
    
    _phantom: PhantomData<T>,
}

#[derive(Clone, Copy)]
pub enum ColumnEncoding {
    /// No compression
    Plain,
    
    /// Run-length encoding (good for repeated values)
    Rle,
    
    /// Dictionary encoding (good for low-cardinality strings)
    Dictionary,
    
    /// Delta encoding (good for sorted/sequential values)
    Delta,
    
    /// Bit-packing (good for small integers)
    BitPacked { bits: u8 },
    
    /// Frame-of-reference + bit-packing
    For { reference: i64, bits: u8 },
}
```

#### 3.2.3 Dictionary Compression for Strings

Labels and common property values benefit from dictionary encoding:

```
Without dictionary:
Node 0: label = "person" (6 bytes)
Node 1: label = "person" (6 bytes)
Node 2: label = "person" (6 bytes)
...
1M nodes × 6 bytes = 6 MB

With dictionary:
Dictionary: { 0: "person", 1: "software", 2: "company" }
Node 0: label_id = 0 (4 bytes)
Node 1: label_id = 0 (4 bytes)
Node 2: label_id = 0 (4 bytes)
...
1M nodes × 4 bytes = 4 MB + dictionary overhead (~100 bytes)
```

This is already implemented via the `StringInterner`, but can be extended to property values:

```rust
/// Extended dictionary encoding for property values
pub struct ValueDictionary {
    /// String values
    strings: StringInterner,
    
    /// Common numeric values
    numbers: HashMap<i64, u32>,
    
    /// Common boolean patterns
    booleans: [u32; 2],  // false=0, true=1
}

impl ValueDictionary {
    /// Encode a value to dictionary ID (if common) or inline
    pub fn encode(&mut self, value: &Value) -> EncodedValue {
        match value {
            Value::String(s) => {
                let id = self.strings.intern(s);
                EncodedValue::DictString(id)
            }
            Value::Int(n) if self.numbers.contains_key(n) => {
                EncodedValue::DictNumber(self.numbers[n])
            }
            Value::Int(n) => {
                EncodedValue::InlineInt(*n)
            }
            // ... other types
        }
    }
}
```

#### 3.2.4 Adjacency List Compression

Compress edge lists for high-degree vertices:

```rust
/// Compressed adjacency list for high-degree vertices
pub enum AdjacencyList {
    /// Small degree: inline linked list (current approach)
    Inline { first_edge: u64 },
    
    /// Medium degree: compressed array of edge IDs
    Compressed {
        offset: u64,      // Offset in compressed adjacency file
        count: u32,       // Number of edges
        encoding: AdjEncoding,
    },
    
    /// Very high degree: multi-level structure
    Tiered {
        offset: u64,
        levels: u8,
    },
}

#[derive(Clone, Copy)]
pub enum AdjEncoding {
    /// Sorted array with delta encoding
    DeltaEncoded,
    
    /// Roaring bitmap (for very dense ranges)
    Bitmap,
    
    /// Variable-length integer encoding
    VarInt,
}

impl MmapGraph {
    /// Compress adjacency lists for vertices exceeding threshold
    pub fn compress_adjacency(&mut self, threshold: usize) -> Result<(), StorageError> {
        for vertex_id in 0..self.vertex_count() {
            let degree = self.out_degree(VertexId(vertex_id));
            
            if degree > threshold {
                self.compress_vertex_adjacency(VertexId(vertex_id))?;
            }
        }
        
        Ok(())
    }
    
    fn compress_vertex_adjacency(&mut self, id: VertexId) -> Result<(), StorageError> {
        // Collect all edge IDs
        let edge_ids: Vec<u64> = self.out_edges(id)
            .map(|e| e.id.0)
            .collect();
        
        // Sort for better delta encoding
        let mut sorted = edge_ids.clone();
        sorted.sort();
        
        // Delta encode
        let deltas: Vec<u64> = sorted.windows(2)
            .map(|w| w[1] - w[0])
            .collect();
        
        // Compress deltas
        let compressed = self.compress_deltas(&deltas)?;
        
        // Store compressed adjacency
        self.store_compressed_adjacency(id, &compressed)?;
        
        Ok(())
    }
}
```

### 3.3 Compression Configuration

```rust
/// Comprehensive compression configuration
pub struct CompressionConfig {
    /// Enable compression
    pub enabled: bool,
    
    /// Page compression settings
    pub page: PageCompressionConfig,
    
    /// Use column-oriented storage for fixed fields
    pub column_storage: bool,
    
    /// Compress adjacency lists above this degree
    pub adjacency_threshold: usize,
    
    /// Dictionary encode property values above this frequency
    pub dictionary_threshold: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            page: PageCompressionConfig {
                algorithm: CompressionAlgorithm::Lz4,
                page_size: 4096,
                min_ratio: 0.9,
            },
            column_storage: false,  // Requires different file format
            adjacency_threshold: 100,
            dictionary_threshold: 10,
        }
    }
}
```

### 3.4 Compression Trade-offs

| Strategy | Compression Ratio | Read Speed | Write Speed | Use Case |
|----------|-------------------|------------|-------------|----------|
| None | 1.0x | Fastest | Fastest | Hot data, low latency |
| LZ4 | 2-3x | Very fast | Very fast | General purpose |
| Zstd | 3-5x | Fast | Medium | Cold data, archival |
| Dictionary | 2-10x | Fast | Fast | Repetitive strings |
| Delta | 2-4x | Fast | Fast | Sequential IDs |
| Column | 5-10x | Medium | Slow | Analytics, bulk reads |

### 3.5 Transparent Compression

Compression should be transparent to the traversal API:

```rust
impl GraphStorage for MmapGraph {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        // Decompression happens automatically inside
        let record = self.get_node_record(id)?;  // May decompress page
        
        let label = self.string_table
            .resolve(record.label_id)?  // Dictionary lookup
            .to_string();
        
        let properties = self.load_properties(record.prop_head)  // May decompress
            .ok()?;
        
        Some(Vertex { id, label, properties })
    }
}

// User code is unaware of compression
let people = g.v()
    .has_label("person")
    .to_list();  // Works the same with or without compression
```

---

## 4. Implementation Priorities

### Priority 1: Single-Property Indexes (High Impact)

Most queries filter by a single property. This provides the biggest performance improvement.

**Effort:** Medium
**Impact:** High (10-100x speedup for filtered queries)

### Priority 2: Full Compaction (Essential for Production)

Without compaction, the database file grows unbounded. Full compaction is simpler than incremental.

**Effort:** Medium
**Impact:** High (required for long-running production use)

### Priority 3: Page Compression with LZ4 (Low Effort, Good ROI)

LZ4 is fast enough that compression often improves read performance by reducing I/O.

**Effort:** Low
**Impact:** Medium (2-3x storage reduction, potentially faster reads)

### Priority 4: Composite Indexes (For Complex Queries)

Important for multi-condition queries, but single-property indexes cover most use cases.

**Effort:** Medium
**Impact:** Medium

### Priority 5: Dictionary Compression for Properties (Easy Win)

Extend existing `StringInterner` pattern to common property values.

**Effort:** Low
**Impact:** Medium (especially for properties with low cardinality)

### Priority 6: Incremental/Online Compaction (For High Availability)

Important for production systems that can't afford downtime, but full compaction works for most cases.

**Effort:** High
**Impact:** Medium

### Priority 7: Column-Oriented Storage (Major Rewrite)

Significant architectural change. Only worthwhile for analytics-heavy workloads.

**Effort:** Very High
**Impact:** High (for specific use cases)

### Priority 8: Full-Text Indexes (Specialized)

Only needed if text search is a core requirement.

**Effort:** High
**Impact:** Low-Medium (depends on use case)

---

## Summary

| Feature | Purpose | Complexity | Priority |
|---------|---------|------------|----------|
| Single-Property Index | Fast property lookups | Medium | 1 |
| Full Compaction | Reclaim deleted space | Medium | 2 |
| LZ4 Compression | Reduce file size | Low | 3 |
| Composite Index | Multi-property queries | Medium | 4 |
| Dictionary Compression | Compress repetitive values | Low | 5 |
| Online Compaction | Zero-downtime compaction | High | 6 |
| Column Storage | Analytics optimization | Very High | 7 |
| Full-Text Index | Text search | High | 8 |

These features build on the core storage architecture and can be implemented incrementally as needed.
