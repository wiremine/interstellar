# Interstellar: Storage Options

This document outlines the dual storage architecture supporting both in-memory and on-disk graph databases, enabling flexibility for different use cases from high-performance analytics to large-scale persistent graph storage.

---

## 1. Storage Architecture Overview

Interstellar supports two storage modes:

```
┌─────────────────────────────────────────────────────────────────┐
│                   Storage Architecture                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐         ┌──────────────────────┐     │
│  │   In-Memory Graph    │         │   On-Disk Graph      │     │
│  ├──────────────────────┤         ├──────────────────────┤     │
│  │ • HashMap-based      │         │ • Memory-mapped      │     │
│  │ • No persistence     │         │ • Persistent         │     │
│  │ • Fastest access     │         │ • Larger capacity    │     │
│  │ • Limited by RAM     │         │ • Page cache assist  │     │
│  └──────────────────────┘         └──────────────────────┘     │
│           │                                 │                   │
│           └────────────┬────────────────────┘                   │
│                        ▼                                        │
│            ┌─────────────────────┐                              │
│            │   Unified Graph API  │                             │
│            │  (same traversal     │                             │
│            │   interface)         │                             │
│            └─────────────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. In-Memory Storage

### 2.1 Architecture

In-memory storage uses native Rust data structures for maximum performance when persistence is not required or when the entire graph fits in RAM.

```
┌─────────────────────────────────────────────────────────────────┐
│                In-Memory Storage Layout                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Nodes: HashMap<VertexId, NodeData>                           │
│  ┌──────────────────────────────────────────────────────┐      │
│  │ VertexId(0) → NodeData {                             │      │
│  │   label_id: u32,  // Interned string ID              │      │
│  │   properties: HashMap<String, Value>,                │      │
│  │   out_edges: Vec<EdgeId>,                            │      │
│  │   in_edges: Vec<EdgeId>,                             │      │
│  │ }                                                    │      │
│  └──────────────────────────────────────────────────────┘      │
│                                                                 │
│  Edges: HashMap<EdgeId, EdgeData>                              │
│  ┌──────────────────────────────────────────────────────┐      │
│  │ EdgeId(0) → EdgeData {                               │      │
│  │   label_id: u32,  // Interned string ID              │      │
│  │   src: VertexId,                                     │      │
│  │   dst: VertexId,                                     │      │
│  │   properties: HashMap<String, Value>,                │      │
│  │ }                                                    │      │
│  └──────────────────────────────────────────────────────┘      │
│                                                                 │
│  Label Indexes: HashMap<u32, RoaringBitmap>  // label_id → vertices with label
│  Property Indexes: BTreeMap<(String, Value), RoaringBitmap>    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Structures

```rust
/// In-memory graph storage
pub struct Graph {
    nodes: HashMap<VertexId, NodeData>,
    edges: HashMap<EdgeId, EdgeData>,
    next_vertex_id: AtomicU64,
    next_edge_id: AtomicU64,
    
    // Indexes
    vertex_labels: HashMap<u32, RoaringBitmap>,  // label_id → vertex IDs
    edge_labels: HashMap<u32, RoaringBitmap>,    // label_id → edge IDs
    property_indexes: HashMap<PropertyIndexKey, BTreeMap<Value, RoaringBitmap>>,
    
    // String interning
    string_table: StringInterner,
}

/// Node data in memory
#[derive(Clone)]
struct NodeData {
    id: VertexId,
    label_id: u32,  // Interned string ID
    properties: HashMap<String, Value>,
    out_edges: Vec<EdgeId>,
    in_edges: Vec<EdgeId>,
}

/// Edge data in memory
#[derive(Clone)]
struct EdgeData {
    id: EdgeId,
    label_id: u32,  // Interned string ID
    src: VertexId,
    dst: VertexId,
    properties: HashMap<String, Value>,
}

#[derive(Hash, Eq, PartialEq)]
struct PropertyIndexKey {
    label_id: u32,       // Interned string ID for label
    property_key: String,
}
```

### 2.3 Operations

```rust
impl Graph {
    /// Create new in-memory graph
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            next_vertex_id: AtomicU64::new(0),
            next_edge_id: AtomicU64::new(0),
            vertex_labels: HashMap::new(),
            edge_labels: HashMap::new(),
            property_indexes: HashMap::new(),
        }
    }
    
    /// O(1) vertex lookup
    #[inline]
    pub fn get_vertex(&self, id: VertexId) -> Option<&NodeData> {
        self.nodes.get(&id)
    }
    
    /// O(1) edge lookup
    #[inline]
    pub fn get_edge(&self, id: EdgeId) -> Option<&EdgeData> {
        self.edges.get(&id)
    }
    
    /// O(1) insertion
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = VertexId(self.next_vertex_id.fetch_add(1, Ordering::Relaxed));
        
        // Intern label string
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
    
    /// O(1) edge insertion
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        if !self.nodes.contains_key(&src) || !self.nodes.contains_key(&dst) {
            return Err(StorageError::VertexNotFound);
        }
        
        let id = EdgeId(self.next_edge_id.fetch_add(1, Ordering::Relaxed));
        
        // Intern label string
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
    
    /// Iterate outgoing edges: O(degree)
    pub fn out_edges(&self, vertex: VertexId) -> impl Iterator<Item = &EdgeData> {
        self.nodes
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.out_edges.iter())
            .filter_map(|&edge_id| self.edges.get(&edge_id))
    }
}
```

### 2.4 Advantages

- **Maximum Performance**: Direct memory access, no I/O overhead
- **Simple Implementation**: Straightforward data structures
- **Fast Mutations**: No WAL or durability overhead
- **Easy Snapshotting**: Clone for consistent read views (Phase 2: full MVCC)
- **Predictable Latency**: No page faults or cache misses

### 2.5 Limitations

- **Memory Bound**: Limited by available RAM
- **No Persistence**: Data lost on shutdown
- **Memory Overhead**: Rust heap allocations per node/edge
- **Large Graph Cost**: Cannot handle graphs larger than RAM

### 2.6 Use Cases

- **Session Graphs**: Temporary graphs for request processing
- **Testing**: Fast unit tests without filesystem I/O
- **Analytics**: Hot data loaded entirely in memory
- **Graph Construction**: Build graph in memory, export when complete
- **Small Datasets**: Graphs with < 10M vertices that fit comfortably in RAM

---

## 3. On-Disk Storage (Memory-Mapped)

### 3.1 Architecture

On-disk storage uses memory-mapped files for persistence while maintaining near-memory performance through OS page cache.

```
┌─────────────────────────────────────────────────────────────────┐
│                    On-Disk File Layout                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  my_graph.db                                                    │
│  ┌──────────────────────────────────────────────────────┐      │
│  │ [0-64]     Header                                    │      │
│  │            - magic: 0x47524D4C ("GRML")              │      │
│  │            - version: u32                             │      │
│  │            - node_count/capacity: u64                 │      │
│  │            - edge_count/capacity: u64                 │      │
│  │            - offsets to sections                      │      │
│  ├──────────────────────────────────────────────────────┤      │
│  │ [64+]      Node Table (fixed 48-byte records)        │      │
│  │            NodeRecord[0..node_capacity]              │      │
│  ├──────────────────────────────────────────────────────┤      │
│  │            Edge Table (fixed 56-byte records)        │      │
│  │            EdgeRecord[0..edge_capacity]              │      │
│  ├──────────────────────────────────────────────────────┤      │
│  │            Property Arena (variable length)          │      │
│  │            Linked list of properties per element     │      │
│  ├──────────────────────────────────────────────────────┤      │
│  │            String Table (interned strings)           │      │
│  │            ID → offset mapping + string data         │      │
│  └──────────────────────────────────────────────────────┘      │
│                                                                 │
│  my_graph.wal (Write-Ahead Log)                                │
│  ┌──────────────────────────────────────────────────────┐      │
│  │ Sequence of transaction log entries                  │      │
│  │ Used for crash recovery and atomic commits           │      │
│  └──────────────────────────────────────────────────────┘      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Memory-Mapped Storage

```rust
/// On-disk graph using memory-mapped files
pub struct MmapGraph {
    mmap: Mmap,
    file: File,
    wal: WriteAheadLog,
    
    // In-memory indexes (rebuilt on load)
    label_index: LabelIndex,
    property_indexes: Vec<PropertyIndex>,
    
    // String interning
    string_table: StringInterner,
    
    // Concurrency
    read_lock: RwLock<()>,
}

/// File header (64 bytes)
#[repr(C, packed)]
struct FileHeader {
    magic: u32,              // 0x47524D4C ("GRML")
    version: u32,            // File format version
    node_count: u64,         // Number of nodes
    node_capacity: u64,      // Allocated node slots
    edge_count: u64,         // Number of edges
    edge_capacity: u64,      // Allocated edge slots
    string_table_offset: u64,
    property_arena_offset: u64,
    free_node_head: u64,     // Free list head (or u64::MAX)
    free_edge_head: u64,
}

/// On-disk node record (48 bytes, cache-line friendly)
#[repr(C, packed)]
struct NodeRecord {
    id: u64,                 // Vertex ID
    label_id: u32,           // String table ID for label
    flags: u32,              // Deleted, indexed, etc.
    first_out_edge: u64,     // First outgoing edge (or u64::MAX)
    first_in_edge: u64,      // First incoming edge (or u64::MAX)
    prop_head: u64,          // Head of property linked list
}

/// On-disk edge record (56 bytes)
#[repr(C, packed)]
struct EdgeRecord {
    id: u64,                 // Edge ID
    label_id: u32,           // String table ID for label
    _padding: u32,           // Alignment
    src: u64,                // Source vertex ID
    dst: u64,                // Destination vertex ID
    next_out: u64,           // Next outgoing edge from src
    next_in: u64,            // Next incoming edge to dst
    prop_head: u64,          // Head of property linked list
}
```

### 3.3 Operations

```rust
impl MmapGraph {
    /// Open existing or create new graph file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        // Initialize or validate file
        let file_size = file.metadata()?.len();
        if file_size == 0 {
            // New file - write initial header
            file.set_len(Self::initial_size())?;
            Self::write_initial_header(&file)?;
        }
        
        // Memory map the file
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        // Open WAL
        let mut wal_path = path.as_ref().to_path_buf();
        wal_path.set_extension("wal");
        let wal = WriteAheadLog::open(wal_path)?;
        
        let mut graph = Self {
            mmap: mmap.make_read_only()?,
            file,
            wal,
            label_index: LabelIndex::new(),
            property_indexes: Vec::new(),
            string_table: StringInterner::new(),
            read_lock: RwLock::new(()),
        };
        
        // Rebuild indexes from disk data
        graph.rebuild_indexes()?;
        
        Ok(graph)
    }
    
    /// O(1) node lookup via direct array access
    #[inline]
    pub fn get_node(&self, id: VertexId) -> Option<&NodeRecord> {
        let header = self.header();
        
        if id.0 >= header.node_count {
            return None;
        }
        
        let offset = size_of::<FileHeader>() + (id.0 as usize * size_of::<NodeRecord>());
        let ptr = unsafe { self.mmap.as_ptr().add(offset) };
        let record = unsafe { &*(ptr as *const NodeRecord) };
        
        // Check if slot is valid (not deleted)
        if record.flags & FLAG_DELETED != 0 {
            return None;
        }
        
        Some(record)
    }
    
    /// O(degree) adjacency traversal via linked list
    pub fn out_edges(&self, vertex: VertexId) -> OutEdgeIterator<'_> {
        let node = self.get_node(vertex);
        let first = node.map(|n| n.first_out_edge).unwrap_or(u64::MAX);
        
        OutEdgeIterator {
            graph: self,
            current: first,
        }
    }
}

/// Iterator for outgoing edges (follows linked list)
pub struct OutEdgeIterator<'g> {
    graph: &'g MmapGraph,
    current: u64,
}

impl<'g> Iterator for OutEdgeIterator<'g> {
    type Item = &'g EdgeRecord;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == u64::MAX {
            return None;
        }
        
        let edge = self.graph.get_edge(EdgeId(self.current))?;
        self.current = edge.next_out;
        Some(edge)
    }
}
```

### 3.4 Write Operations with WAL

```rust
impl MmapGraph {
    /// Insert node with durability via WAL
    pub fn insert_node(
        &mut self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<VertexId, StorageError> {
        // 1. Log to WAL first (write-ahead)
        let tx_id = self.wal.begin_transaction()?;
        
        // 2. Allocate node slot
        let node_id = self.allocate_node_slot()?;
        
        // 3. Intern label string
        let label_id = self.string_table.intern(label);
        
        // 4. Allocate properties in arena
        let prop_head = self.allocate_properties(&properties)?;
        
        // 5. Create record
        let record = NodeRecord {
            id: node_id.0,
            label_id,
            flags: 0,
            first_out_edge: u64::MAX,
            first_in_edge: u64::MAX,
            prop_head,
        };
        
        // 6. Log operation
        self.wal.log(WalEntry::InsertNode {
            id: node_id,
            record: record.clone(),
        })?;
        
        // 7. Write to mmap (requires remapping as mutable)
        self.write_node_record(node_id, &record)?;
        
        // 8. Update indexes
        self.label_index.add_vertex(node_id, label_id);
        
        // 9. Commit transaction
        self.wal.log(WalEntry::CommitTx { tx_id })?;
        self.wal.sync()?;
        
        Ok(node_id)
    }
    
    /// Insert edge atomically
    pub fn insert_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        // Similar to insert_node with additional adjacency list updates
        let tx_id = self.wal.begin_transaction()?;
        
        let edge_id = self.allocate_edge_slot()?;
        let label_id = self.string_table.intern(label);
        let prop_head = self.allocate_properties(&properties)?;
        
        // Get current first edges
        let src_node = self.get_node_mut(src)?;
        let old_first_out = src_node.first_out_edge;
        
        let dst_node = self.get_node_mut(dst)?;
        let old_first_in = dst_node.first_in_edge;
        
        // Create edge record
        let record = EdgeRecord {
            id: edge_id.0,
            label_id,
            _padding: 0,
            src: src.0,
            dst: dst.0,
            next_out: old_first_out,
            next_in: old_first_in,
            prop_head,
        };
        
        // Log and write
        self.wal.log(WalEntry::InsertEdge {
            id: edge_id,
            record: record.clone(),
        })?;
        
        self.write_edge_record(edge_id, &record)?;
        
        // Update vertex adjacency pointers
        self.get_node_mut(src)?.first_out_edge = edge_id.0;
        self.get_node_mut(dst)?.first_in_edge = edge_id.0;
        
        self.wal.log(WalEntry::CommitTx { tx_id })?;
        self.wal.sync()?;
        
        Ok(edge_id)
    }
}
```

### 3.5 Advantages

- **Persistence**: Data survives process restart
- **Large Capacity**: Limited by disk, not RAM
- **Page Cache**: OS caches hot data in memory
- **Portable**: Single file can be copied/moved
- **Memory Efficiency**: Zero-copy reads via mmap
- **Crash Recovery**: WAL enables transaction recovery

### 3.6 Limitations

- **Write Overhead**: WAL synchronization for durability
- **File Growth**: Requires explicit compaction
- **Cold Start**: Initial reads may hit disk
- **Platform Dependent**: mmap behavior varies by OS
- **Fixed Schema**: Record sizes must be known upfront

### 3.7 Use Cases

- **Production Databases**: Persistent graph storage
- **Large Graphs**: Graphs with > 100M vertices
- **Data Warehousing**: Historical graph data
- **Analytics Pipelines**: Process and persist results
- **Multi-Process**: Share graph file across processes

---

## 4. Hybrid Storage Strategy

### 4.1 Tiered Storage

Combine in-memory and on-disk storage for optimal performance:

```rust
/// Hybrid storage with hot/cold separation
pub struct HybridGraph {
    hot: Graph,      // Recently accessed nodes/edges
    cold: MmapGraph,         // Full persistent storage
    cache_size: usize,       // Max hot set size
    eviction_policy: LRU,
}

impl HybridGraph {
    pub fn get_vertex(&mut self, id: VertexId) -> Option<Vertex> {
        // Check hot cache first
        if let Some(node) = self.hot.get_vertex(id) {
            self.eviction_policy.touch(id);
            return Some(node.clone().into());
        }
        
        // Cache miss - fetch from disk
        if let Some(node) = self.cold.get_node(id) {
            let vertex = self.hydrate_vertex(node);
            
            // Promote to hot cache
            if self.hot.node_count() >= self.cache_size {
                let evict_id = self.eviction_policy.evict();
                self.hot.remove_vertex(evict_id);
            }
            
            self.hot.insert_vertex(vertex.clone());
            self.eviction_policy.insert(id);
            
            return Some(vertex);
        }
        
        None
    }
}
```

### 4.2 Write-Back Cache

```rust
/// In-memory cache with lazy write-back
pub struct WriteCachedGraph {
    memory: Graph,
    disk: MmapGraph,
    dirty: HashSet<ElementId>,
    flush_interval: Duration,
}

impl WriteCachedGraph {
    /// Mutations go to memory first
    pub fn add_vertex(&mut self, label: &str) -> VertexId {
        let id = self.memory.add_vertex(label, HashMap::new());
        self.dirty.insert(ElementId::Vertex(id));
        id
    }
    
    /// Periodic flush to disk
    pub fn flush(&mut self) -> Result<(), StorageError> {
        for element_id in self.dirty.drain() {
            match element_id {
                ElementId::Vertex(id) => {
                    let node = self.memory.get_vertex(id).unwrap();
                    self.disk.insert_node(
                        node.label.clone(),
                        node.properties.clone(),
                    )?;
                }
                ElementId::Edge(id) => {
                    // Similar for edges
                }
            }
        }
        
        self.disk.wal.sync()?;
        Ok(())
    }
}
```

---

## 5. Storage Selection Guide

### 5.1 Decision Matrix

| Criteria | In-Memory | Memory-Mapped | Hybrid |
|----------|-----------|---------------|--------|
| **Graph Size** | < 10M nodes | 10M - 1B+ nodes | 10M - 100M nodes |
| **Persistence Needed** | No | Yes | Yes |
| **Read Latency** | Lowest (< 10ns) | Low (< 100ns) | Low-Medium |
| **Write Latency** | Lowest (< 50ns) | Medium (< 10µs) | Low-Medium |
| **Memory Usage** | High | Low | Medium |
| **Startup Time** | Instant | Slow (index rebuild) | Medium |
| **Crash Safety** | None | Full (WAL) | Configurable |

### 5.2 Recommendation Algorithm

```rust
/// Select optimal storage based on requirements
pub fn recommend_storage(req: &StorageRequirements) -> StorageType {
    if !req.persistence_required {
        // No persistence needed
        if req.estimated_nodes < 10_000_000 && req.available_memory > req.estimated_size {
            return StorageType::InMemory;
        }
    }
    
    if req.persistence_required {
        if req.estimated_nodes < 1_000_000 && req.hot_set_ratio > 0.8 {
            // Most data is hot, use hybrid
            return StorageType::Hybrid {
                cache_size: (req.estimated_nodes as f64 * req.hot_set_ratio) as usize,
            };
        }
        
        // Large persistent graph
        return StorageType::MemoryMapped;
    }
    
    // Default to memory-mapped for safety
    StorageType::MemoryMapped
}

pub struct StorageRequirements {
    persistence_required: bool,
    estimated_nodes: u64,
    estimated_edges: u64,
    estimated_size: u64,
    available_memory: u64,
    hot_set_ratio: f64,  // Fraction of graph frequently accessed
}
```

---

## 6. Unified Graph API

Both storage types expose the same traversal interface:

```rust
/// Unified graph trait implemented by both storage types
pub trait GraphStorage {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_>;
}

/// Graph handle (hides storage implementation)
pub struct Graph {
    storage: Box<dyn GraphStorage>,
}

impl Graph {
    /// Create in-memory graph
    pub fn in_memory() -> Self {
        Self {
            storage: Box::new(Graph::new()),
        }
    }
    
    /// Open persistent graph
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        Ok(Self {
            storage: Box::new(MmapGraph::open(path)?),
        })
    }
    
    /// Traversal source (same for both storage types)
    pub fn traversal(&self) -> GraphTraversalSource<'_> {
        GraphTraversalSource::new(self)
    }
}
```

---

## 7. Performance Characteristics

### 7.1 Benchmark Results (Estimated)

| Operation | In-Memory | Memory-Mapped (Hot) | Memory-Mapped (Cold) |
|-----------|-----------|---------------------|----------------------|
| Vertex lookup | 10 ns | 50 ns | 10 µs (page fault) |
| Edge traversal (deg=10) | 100 ns | 500 ns | 100 µs |
| Insert vertex | 50 ns | 5 µs (WAL) | 5 µs |
| Insert edge | 80 ns | 8 µs (WAL) | 8 µs |
| Label scan (1M nodes) | 2 ms | 5 ms | 50 ms |
| BFS (10 hops, 1M nodes) | 50 ms | 100 ms | 500 ms |

### 7.2 Memory Footprint

**In-Memory (per element):**
- Vertex: ~200 bytes (includes HashMap overhead)
- Edge: ~180 bytes
- Total for 1M node, 10M edge graph: ~2.2 GB

**Memory-Mapped (per element):**
- Vertex: 48 bytes on disk
- Edge: 56 bytes on disk
- Total for 1M node, 10M edge graph: ~600 MB (+ property data)

---

## 8. Migration & Import/Export

### 8.1 Export In-Memory to Disk

```rust
impl Graph {
    /// Export to memory-mapped format
    pub fn export_to_disk<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        let mut disk_graph = MmapGraph::create(path)?;
        
        // Export vertices
        for (id, node) in &self.nodes {
            disk_graph.insert_node(node.label.clone(), node.properties.clone())?;
        }
        
        // Export edges
        for (id, edge) in &self.edges {
            disk_graph.insert_edge(
                edge.src,
                edge.dst,
                edge.label.clone(),
                edge.properties.clone(),
            )?;
        }
        
        disk_graph.wal.sync()?;
        Ok(())
    }
}
```

### 8.2 Load Disk to Memory

```rust
impl MmapGraph {
    /// Load entire graph into memory
    pub fn load_into_memory(&self) -> Result<Graph, StorageError> {
        let mut memory_graph = Graph::new();
        
        // Load all vertices
        for i in 0..self.header().node_count {
            if let Some(node) = self.get_node(VertexId(i)) {
                let label = self.string_table.resolve(node.label_id).unwrap();
                let props = self.load_properties(node.prop_head)?;
                memory_graph.add_vertex(label.to_string(), props);
            }
        }
        
        // Load all edges
        for i in 0..self.header().edge_count {
            if let Some(edge) = self.get_edge(EdgeId(i)) {
                let label = self.string_table.resolve(edge.label_id).unwrap();
                let props = self.load_properties(edge.prop_head)?;
                memory_graph.add_edge(
                    VertexId(edge.src),
                    VertexId(edge.dst),
                    label.to_string(),
                    props,
                )?;
            }
        }
        
        Ok(memory_graph)
    }
}
```

---

## 9. Future Enhancements

### 9.1 Compression

- **Edge Compression**: Store adjacency lists in compressed bitmap format
- **Property Compression**: Dictionary encoding for repeated values
- **Delta Encoding**: Store incremental changes efficiently

### 9.2 Partitioning

- **Sharding**: Split large graphs across multiple mmap files
- **Vertical Partitioning**: Separate hot properties from cold
- **Temporal Partitioning**: Time-based graph slices

### 9.3 Advanced Caching

- **Adaptive Cache**: ML-driven cache eviction
- **Prefetching**: Predict and preload traversal paths
- **Compression in Cache**: Compressed in-memory representation

---

## 10. Summary

Interstellar's dual storage architecture provides flexibility:

- **In-Memory**: Maximum performance for graphs that fit in RAM or don't need persistence
- **Memory-Mapped**: Persistent, scalable storage for large graphs with good performance
- **Hybrid**: Best of both worlds for working sets smaller than total graph size

The unified Graph API ensures code portability across storage backends, allowing users to switch storage strategies without changing traversal logic.
