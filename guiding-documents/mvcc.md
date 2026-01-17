# Interstellar: Multi-Version Concurrency Control (MVCC)

This document describes the MVCC implementation for Interstellar, enabling lock-free reads with snapshot isolation while maintaining high write throughput.

---

## 1. Overview and Motivation

### 1.1 Why MVCC?

Interstellar Phase 1 uses a simple `RwLock`-based concurrency model:

```
┌─────────────────────────────────────────────────────────────────┐
│              Phase 1: RwLock Concurrency                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Reader 1 ──┐                                                  │
│   Reader 2 ──┼──▶ RwLock::read()  ──▶ Shared Access             │
│   Reader 3 ──┘                                                  │
│                                                                 │
│   Writer   ────▶ RwLock::write() ──▶ Exclusive Access           │
│                  (blocks all readers)                           │
│                                                                 │
│   Problems:                                                     │
│   • Writers block all readers                                   │
│   • Long traversals block writes                                │
│   • No concurrent write transactions                            │
│   • Lock contention under high load                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

MVCC solves these problems by maintaining multiple versions of data:

```
┌─────────────────────────────────────────────────────────────────┐
│              Phase 2: MVCC Concurrency                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Reader 1 (v=100) ──▶ Sees snapshot at version 100             │
│   Reader 2 (v=105) ──▶ Sees snapshot at version 105             │
│   Reader 3 (v=108) ──▶ Sees snapshot at version 108             │
│                                                                 │
│   Writer (v=110)   ──▶ Creates new versions, doesn't block      │
│                        readers seeing older versions            │
│                                                                 │
│   Benefits:                                                     │
│   • Readers never block writers                                 │
│   • Writers never block readers                                 │
│   • Consistent snapshots without locks                          │
│   • Time-travel queries possible                                │
│   • Better scalability under concurrent load                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 MVCC Concepts

**Version**: A monotonically increasing transaction ID (u64) that orders all changes.

**Snapshot**: A consistent view of the database at a specific version. All reads within a snapshot see the same data, regardless of concurrent writes.

**Visibility**: Rules determining which version of a record is visible to a given snapshot.

**Garbage Collection (GC)**: Process of reclaiming storage from old versions no longer visible to any active snapshot.

### 1.3 Design Goals

| Goal | Description |
|------|-------------|
| Lock-free reads | Readers proceed without acquiring locks |
| Snapshot isolation | Each transaction sees a consistent point-in-time view |
| Serializable writes | Write conflicts detected and resolved |
| Efficient storage | Minimize overhead for version metadata |
| Fast GC | Reclaim old versions without blocking operations |
| Backward compatible | Existing traversal API unchanged |

### 1.4 Isolation Levels

Interstellar MVCC supports two isolation levels:

```rust
/// Supported isolation levels
#[derive(Clone, Copy, Debug)]
pub enum IsolationLevel {
    /// Snapshot Isolation (SI) - Default
    /// - Reads see consistent snapshot at transaction start
    /// - Write-write conflicts detected at commit
    /// - Allows write skew anomaly
    SnapshotIsolation,
    
    /// Serializable Snapshot Isolation (SSI)
    /// - All SI guarantees plus
    /// - Detects read-write conflicts (prevents write skew)
    /// - Slightly higher overhead
    Serializable,
}
```

**Snapshot Isolation** is the default, providing excellent performance for most graph workloads. **Serializable** is available for applications requiring strict consistency.

---

## 2. Version Storage and Record Format

### 2.1 Versioned Record Structure

Each graph element (vertex or edge) maintains a version chain:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Version Chain Structure                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Vertex ID: 42                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Version Chain                         │  │
│  │                                                           │  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │  │
│  │  │ Version 150 │───▶│ Version 120 │───▶│ Version 100 │   │  │
│  │  │ (current)   │    │ (previous)  │    │ (oldest)    │   │  │
│  │  │             │    │             │    │             │   │  │
│  │  │ xmin: 150   │    │ xmin: 120   │    │ xmin: 100   │   │  │
│  │  │ xmax: ∞     │    │ xmax: 150   │    │ xmax: 120   │   │  │
│  │  │ name: "Bob" │    │ name: "Rob" │    │ name: "Bob" │   │  │
│  │  │ age: 31     │    │ age: 30     │    │ age: 30     │   │  │
│  │  └─────────────┘    └─────────────┘    └─────────────┘   │  │
│  │        │                                                  │  │
│  │        └── Head of chain (newest version)                 │  │
│  │                                                           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Visibility for snapshot at version 125:                        │
│  • Version 150: xmin=150 > 125, NOT visible (created after)    │
│  • Version 120: xmin=120 ≤ 125, xmax=150 > 125, VISIBLE ✓      │
│  • Version 100: xmax=120 ≤ 125, NOT visible (superseded)       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Core Data Structures

```rust
/// Transaction ID type - monotonically increasing
pub type TxId = u64;

/// Special transaction IDs
pub mod tx_id {
    use super::TxId;
    
    /// Minimum valid transaction ID
    pub const MIN: TxId = 1;
    
    /// Maximum transaction ID (indicates "infinity" / not yet deleted)
    pub const MAX: TxId = u64::MAX;
    
    /// Invalid/uncommitted transaction marker
    pub const INVALID: TxId = 0;
}

/// Version metadata embedded in each record
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct VersionInfo {
    /// Transaction that created this version
    pub xmin: TxId,
    
    /// Transaction that deleted/superseded this version (MAX if current)
    pub xmax: TxId,
    
    /// Pointer to previous version (0 if none)
    pub prev_version: u64,
    
    /// Transaction status flags
    pub flags: VersionFlags,
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug)]
    pub struct VersionFlags: u8 {
        /// xmin transaction has committed
        const XMIN_COMMITTED = 0b0000_0001;
        /// xmin transaction has aborted
        const XMIN_ABORTED   = 0b0000_0010;
        /// xmax transaction has committed
        const XMAX_COMMITTED = 0b0000_0100;
        /// xmax transaction has aborted
        const XMAX_ABORTED   = 0b0000_1000;
        /// Version is marked for deletion
        const DELETED        = 0b0001_0000;
        /// Version has been garbage collected
        const VACUUMED       = 0b0010_0000;
    }
}

impl VersionInfo {
    /// Create version info for a new record
    pub fn new(creating_tx: TxId) -> Self {
        Self {
            xmin: creating_tx,
            xmax: tx_id::MAX,
            prev_version: 0,
            flags: VersionFlags::empty(),
        }
    }
    
    /// Size of version metadata in bytes
    pub const SIZE: usize = 8 + 8 + 8 + 1; // 25 bytes, padded to 32
}
```

### 2.3 Versioned Node Record

```rust
/// On-disk versioned node record (80 bytes)
/// Extended from Phase 1's 48-byte NodeRecord
#[repr(C, packed)]
pub struct VersionedNodeRecord {
    // === Identity (16 bytes) ===
    /// Node ID
    pub id: u64,
    /// Label ID (interned string)
    pub label_id: u32,
    /// Record flags
    pub flags: u32,
    
    // === Version Info (32 bytes) ===
    pub version: VersionInfo,
    pub _version_pad: [u8; 7],  // Align to 8 bytes
    
    // === Adjacency (16 bytes) ===
    /// First outgoing edge in chain
    pub first_out_edge: u64,
    /// First incoming edge in chain
    pub first_in_edge: u64,
    
    // === Properties (16 bytes) ===
    /// Offset to property data in arena
    pub prop_offset: u64,
    /// Length of property data
    pub prop_len: u32,
    /// Property data checksum
    pub prop_checksum: u32,
}

impl VersionedNodeRecord {
    pub const SIZE: usize = 80;
    
    /// Check if this record is visible to a snapshot
    #[inline]
    pub fn is_visible(&self, snapshot: &Snapshot) -> bool {
        snapshot.can_see(&self.version)
    }
}
```

### 2.4 Versioned Edge Record

```rust
/// On-disk versioned edge record (88 bytes)
/// Extended from Phase 1's 56-byte EdgeRecord
#[repr(C, packed)]
pub struct VersionedEdgeRecord {
    // === Identity (16 bytes) ===
    /// Edge ID
    pub id: u64,
    /// Label ID (interned string)
    pub label_id: u32,
    /// Record flags
    pub flags: u32,
    
    // === Version Info (32 bytes) ===
    pub version: VersionInfo,
    pub _version_pad: [u8; 7],
    
    // === Topology (16 bytes) ===
    /// Source vertex ID
    pub src: u64,
    /// Destination vertex ID
    pub dst: u64,
    
    // === Adjacency Chain (16 bytes) ===
    /// Next outgoing edge from src
    pub next_out: u64,
    /// Next incoming edge to dst
    pub next_in: u64,
    
    // === Properties (8 bytes) ===
    /// Offset to property data
    pub prop_offset: u64,
}

impl VersionedEdgeRecord {
    pub const SIZE: usize = 88;
}
```

### 2.5 Version Chain Storage Strategies

Interstellar supports two version chain storage strategies:

```
┌─────────────────────────────────────────────────────────────────┐
│              Version Chain Storage Strategies                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Strategy 1: Inline Version Chain (Default)                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                                                           │  │
│  │  Main Storage Array:                                      │  │
│  │  ┌────────┬────────┬────────┬────────┬────────┐          │  │
│  │  │ Node 0 │ Node 1 │ Node 2 │ v1(N1) │ v0(N1) │          │  │
│  │  └────────┴───┬────┴────────┴────▲───┴────▲───┘          │  │
│  │               │                   │        │              │  │
│  │               └───────────────────┴────────┘              │  │
│  │                   prev_version pointers                   │  │
│  │                                                           │  │
│  │  Pros: Cache-friendly for recent versions                 │  │
│  │  Cons: Old versions fragment main array                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Strategy 2: Separate Version Store                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                                                           │  │
│  │  Main Storage:           Version Store:                   │  │
│  │  ┌────────┬────────┐    ┌────────┬────────┬────────┐     │  │
│  │  │ Node 0 │ Node 1 │    │ v1(N1) │ v0(N1) │ v0(N0) │     │  │
│  │  └────────┴───┬────┘    └────▲───┴────▲───┴────────┘     │  │
│  │               │              │        │                   │  │
│  │               └──────────────┴────────┘                   │  │
│  │                                                           │  │
│  │  Pros: Main array stays compact, better for GC            │  │
│  │  Cons: Extra indirection for version traversal            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
/// Version store for historical versions (Strategy 2)
pub struct VersionStore {
    /// Memory-mapped file for version data
    mmap: MmapMut,
    
    /// Free list for reclaimed version slots
    free_list: Vec<u64>,
    
    /// Next allocation offset
    next_offset: AtomicU64,
}

impl VersionStore {
    /// Allocate space for a historical version
    pub fn allocate(&self, size: usize) -> u64 {
        // Try free list first
        if let Some(offset) = self.free_list.pop() {
            return offset;
        }
        
        // Allocate from end
        self.next_offset.fetch_add(size as u64, Ordering::SeqCst)
    }
    
    /// Store a historical version, return offset
    pub fn store_version<T: VersionedRecord>(&mut self, record: &T) -> u64 {
        let offset = self.allocate(T::SIZE);
        let dest = &mut self.mmap[offset as usize..][..T::SIZE];
        dest.copy_from_slice(record.as_bytes());
        offset
    }
    
    /// Read a historical version
    pub fn read_version<T: VersionedRecord>(&self, offset: u64) -> &T {
        let src = &self.mmap[offset as usize..][..T::SIZE];
        unsafe { &*(src.as_ptr() as *const T) }
    }
}
```

### 2.6 Version Index

For efficient version lookups, maintain an index from (element_id, version) to record location:

```rust
/// Index for fast version lookups
pub struct VersionIndex {
    /// (element_id, xmin) → record offset
    /// Sorted by (element_id, xmin DESC) for efficient range scans
    index: BTreeMap<(u64, Reverse<TxId>), VersionLocation>,
}

#[derive(Clone, Copy)]
pub struct VersionLocation {
    /// Offset in main storage or version store
    pub offset: u64,
    /// True if in version store, false if in main storage
    pub in_version_store: bool,
}

impl VersionIndex {
    /// Find the visible version for an element at a given snapshot
    pub fn find_visible(
        &self,
        element_id: u64,
        snapshot: &Snapshot,
    ) -> Option<VersionLocation> {
        // Scan versions from newest to oldest
        let start = (element_id, Reverse(tx_id::MAX));
        let end = (element_id, Reverse(tx_id::MIN));
        
        for ((id, Reverse(xmin)), location) in self.index.range(start..=end) {
            if *id != element_id {
                break;
            }
            
            // Check if this version is visible
            if snapshot.is_visible_xmin(*xmin) {
                return Some(*location);
            }
        }
        
        None
    }
    
    /// Add a new version to the index
    pub fn insert(&mut self, element_id: u64, xmin: TxId, location: VersionLocation) {
        self.index.insert((element_id, Reverse(xmin)), location);
    }
    
    /// Remove versions older than the oldest active snapshot
    pub fn gc_versions(&mut self, oldest_visible: TxId) -> Vec<VersionLocation> {
        let mut to_remove = Vec::new();
        
        self.index.retain(|(_, Reverse(xmin)), location| {
            if *xmin < oldest_visible {
                to_remove.push(*location);
                false
            } else {
                true
            }
        });
        
        to_remove
    }
}
```

---

## 3. Snapshot Isolation and Visibility Rules

### 3.1 Snapshot Structure

A snapshot represents a consistent point-in-time view of the database:

```rust
/// Immutable snapshot for consistent reads
#[derive(Clone)]
pub struct Snapshot {
    /// Snapshot version (transaction ID at snapshot creation)
    pub version: TxId,
    
    /// Set of transaction IDs that were active (uncommitted) when snapshot was taken
    /// These transactions' changes are invisible to this snapshot
    pub active_transactions: Arc<HashSet<TxId>>,
    
    /// Minimum active transaction ID (optimization for visibility checks)
    pub xmin: TxId,
    
    /// Maximum committed transaction ID at snapshot time
    pub xmax: TxId,
}

impl Snapshot {
    /// Create a new snapshot from current transaction state
    pub fn new(tx_manager: &TransactionManager) -> Self {
        let (version, active, xmin, xmax) = tx_manager.get_snapshot_info();
        
        Self {
            version,
            active_transactions: Arc::new(active),
            xmin,
            xmax,
        }
    }
    
    /// Check if a version is visible to this snapshot
    #[inline]
    pub fn can_see(&self, version: &VersionInfo) -> bool {
        self.is_visible_xmin(version.xmin) && self.is_visible_xmax(version.xmax)
    }
    
    /// Check if creator transaction is visible
    #[inline]
    pub fn is_visible_xmin(&self, xmin: TxId) -> bool {
        // Version created by committed transaction before our snapshot
        xmin < self.version && !self.active_transactions.contains(&xmin)
    }
    
    /// Check if version hasn't been deleted/superseded
    #[inline]
    pub fn is_visible_xmax(&self, xmax: TxId) -> bool {
        // xmax == MAX means not deleted
        // xmax >= version means deleted after our snapshot
        // xmax in active means deleting tx not yet committed
        xmax == tx_id::MAX 
            || xmax >= self.version 
            || self.active_transactions.contains(&xmax)
    }
}
```

### 3.2 Visibility Rules Flowchart

```
┌─────────────────────────────────────────────────────────────────┐
│                    Visibility Decision Tree                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Is version visible to snapshot S?                              │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 1: Check xmin (creating transaction)               │   │
│  │                                                          │   │
│  │   xmin > S.version?  ──YES──▶ NOT VISIBLE               │   │
│  │         │                     (created after snapshot)   │   │
│  │         NO                                               │   │
│  │         ▼                                                │   │
│  │   xmin ∈ S.active?   ──YES──▶ NOT VISIBLE               │   │
│  │         │                     (creator not committed)    │   │
│  │         NO                                               │   │
│  │         ▼                                                │   │
│  │   xmin committed?    ──NO───▶ Check commit log          │   │
│  │         │                                                │   │
│  │        YES                                               │   │
│  │         ▼                                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 2: Check xmax (deleting transaction)               │   │
│  │                                                          │   │
│  │   xmax == MAX?       ──YES──▶ VISIBLE ✓                 │   │
│  │         │                     (not deleted)              │   │
│  │         NO                                               │   │
│  │         ▼                                                │   │
│  │   xmax > S.version?  ──YES──▶ VISIBLE ✓                 │   │
│  │         │                     (deleted after snapshot)   │   │
│  │         NO                                               │   │
│  │         ▼                                                │   │
│  │   xmax ∈ S.active?   ──YES──▶ VISIBLE ✓                 │   │
│  │         │                     (deleter not committed)    │   │
│  │         NO                                               │   │
│  │         ▼                                                │   │
│  │   xmax committed?    ──YES──▶ NOT VISIBLE               │   │
│  │                               (deleted before snapshot)  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 Transaction Status Cache

To avoid repeated commit log lookups, cache transaction status in version flags:

```rust
impl Snapshot {
    /// Check visibility with hint caching
    pub fn can_see_with_hints(&self, version: &mut VersionInfo) -> bool {
        // Fast path: use cached hints
        if version.flags.contains(VersionFlags::XMIN_COMMITTED) {
            if !self.is_visible_xmin(version.xmin) {
                return false;
            }
        } else if version.flags.contains(VersionFlags::XMIN_ABORTED) {
            return false;
        } else {
            // Slow path: check commit log
            match self.check_tx_status(version.xmin) {
                TxStatus::Committed => {
                    version.flags.insert(VersionFlags::XMIN_COMMITTED);
                    if !self.is_visible_xmin(version.xmin) {
                        return false;
                    }
                }
                TxStatus::Aborted => {
                    version.flags.insert(VersionFlags::XMIN_ABORTED);
                    return false;
                }
                TxStatus::InProgress => {
                    return false;
                }
            }
        }
        
        // Check xmax similarly...
        self.check_xmax_visible(version)
    }
}
```

### 3.4 Visibility Examples

```
┌─────────────────────────────────────────────────────────────────┐
│                    Visibility Examples                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Timeline:                                                      │
│  ─────────────────────────────────────────────────────────────  │
│  T=100    T=110    T=120    T=130    T=140    T=150             │
│    │        │        │        │        │        │               │
│    │      Tx110    Tx120    Tx130    Tx140                       │
│    │      starts   starts   commits  starts                     │
│    │               commits  Tx110                                │
│    ▼                                                            │
│  Vertex v1 created (xmin=100, xmax=MAX)                         │
│           │                                                      │
│           ▼                                                      │
│         v1 updated by Tx110 → v1' (xmin=110, xmax=MAX)          │
│         v1 gets xmax=110                                         │
│                    │                                             │
│                    ▼                                             │
│                  Tx110 commits at T=130                          │
│                                                                 │
│  Snapshot at T=125 (active={Tx110}):                            │
│  • v1  (xmin=100): xmin<125 ✓, not in active ✓                  │
│                    xmax=110, 110 ∈ active ✓ → VISIBLE           │
│  • v1' (xmin=110): 110 ∈ active → NOT VISIBLE                   │
│                                                                 │
│  Snapshot at T=135 (active={Tx140}):                            │
│  • v1  (xmin=100): xmax=110, 110<135, committed → NOT VISIBLE   │
│  • v1' (xmin=110): xmin=110<135, committed ✓                    │
│                    xmax=MAX → VISIBLE                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.5 Read Operations with Visibility

```rust
impl MvccStorage {
    /// Read a vertex with visibility check
    pub fn get_vertex(&self, id: VertexId, snapshot: &Snapshot) -> Option<Vertex> {
        // Get the head of version chain
        let head_offset = self.vertex_index.get(id)?;
        let mut current = self.read_vertex_record(head_offset);
        
        // Walk version chain to find visible version
        loop {
            if snapshot.can_see(&current.version) {
                return Some(Vertex::from_record(&current, &self.properties));
            }
            
            // Move to previous version
            if current.version.prev_version == 0 {
                return None;  // No visible version exists
            }
            
            current = self.read_vertex_record(current.version.prev_version);
        }
    }
    
    /// Iterate vertices with visibility filtering
    pub fn vertices(&self, snapshot: &Snapshot) -> impl Iterator<Item = Vertex> + '_ {
        self.vertex_records()
            .filter(move |record| snapshot.can_see(&record.version))
            .map(move |record| Vertex::from_record(&record, &self.properties))
    }
    
    /// Iterate outgoing edges with visibility filtering
    pub fn out_edges(
        &self,
        vertex: VertexId,
        snapshot: &Snapshot,
    ) -> impl Iterator<Item = Edge> + '_ {
        let vertex_record = self.get_visible_vertex_record(vertex, snapshot);
        
        vertex_record
            .into_iter()
            .flat_map(move |v| {
                self.edge_chain_iter(v.first_out_edge)
                    .filter(move |e| snapshot.can_see(&e.version))
                    .map(move |e| Edge::from_record(&e, &self.properties))
            })
    }
}
```

### 3.6 Index Visibility

Indexes must also respect visibility:

```rust
/// MVCC-aware property index
pub struct MvccPropertyIndex {
    /// Base index: (label, key, value) → Set<(element_id, xmin)>
    inner: PropertyIndex,
}

impl MvccPropertyIndex {
    /// Query index with visibility filtering
    pub fn query(
        &self,
        label: u32,
        key: u32,
        value: &Value,
        snapshot: &Snapshot,
    ) -> impl Iterator<Item = VertexId> + '_ {
        self.inner
            .get(label, key, value)
            .into_iter()
            .flat_map(|entries| entries.iter())
            .filter(move |(_, xmin)| snapshot.is_visible_xmin(*xmin))
            .map(|(id, _)| VertexId(*id))
    }
    
    /// Insert index entry with version
    pub fn insert(
        &mut self,
        label: u32,
        key: u32,
        value: &Value,
        element_id: u64,
        xmin: TxId,
    ) {
        self.inner.insert(label, key, value, (element_id, xmin));
    }
    
    /// Mark index entry as deleted
    pub fn mark_deleted(
        &mut self,
        label: u32,
        key: u32,
        value: &Value,
        element_id: u64,
        xmax: TxId,
    ) {
        // Update the entry's xmax or add deletion marker
        self.inner.set_xmax(label, key, value, element_id, xmax);
    }
}
```

---

## 4. Transaction Lifecycle

### 4.1 Transaction Manager

The transaction manager coordinates all MVCC operations:

```rust
/// Central transaction coordinator
pub struct TransactionManager {
    /// Next transaction ID to assign
    next_tx_id: AtomicU64,
    
    /// Currently active transactions
    active_transactions: RwLock<BTreeMap<TxId, TransactionState>>,
    
    /// Commit log for transaction status lookups
    commit_log: CommitLog,
    
    /// Oldest transaction ID that might still be visible
    /// (used for garbage collection)
    oldest_active: AtomicU64,
}

/// State of an active transaction
#[derive(Clone)]
pub struct TransactionState {
    /// Transaction ID
    pub tx_id: TxId,
    
    /// When transaction started
    pub start_time: Instant,
    
    /// Snapshot for this transaction's reads
    pub snapshot: Snapshot,
    
    /// Write set: elements modified by this transaction
    pub write_set: HashSet<ElementId>,
    
    /// Read set (for SSI): elements read by this transaction
    pub read_set: Option<HashSet<ElementId>>,
    
    /// Isolation level
    pub isolation: IsolationLevel,
}

impl TransactionManager {
    /// Begin a new transaction
    pub fn begin(&self, isolation: IsolationLevel) -> Transaction {
        // Allocate transaction ID
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        
        // Take snapshot of current state
        let snapshot = self.create_snapshot();
        
        // Track read set only for serializable isolation
        let read_set = match isolation {
            IsolationLevel::Serializable => Some(HashSet::new()),
            IsolationLevel::SnapshotIsolation => None,
        };
        
        let state = TransactionState {
            tx_id,
            start_time: Instant::now(),
            snapshot,
            write_set: HashSet::new(),
            read_set,
            isolation,
        };
        
        // Register as active
        self.active_transactions.write().insert(tx_id, state.clone());
        
        Transaction {
            state,
            manager: self,
        }
    }
    
    /// Create a snapshot of current transaction state
    fn create_snapshot(&self) -> Snapshot {
        let active = self.active_transactions.read();
        let active_ids: HashSet<TxId> = active.keys().copied().collect();
        
        let xmin = active_ids.iter().min().copied().unwrap_or(self.next_tx_id.load(Ordering::SeqCst));
        let xmax = self.next_tx_id.load(Ordering::SeqCst);
        
        Snapshot {
            version: xmax,
            active_transactions: Arc::new(active_ids),
            xmin,
            xmax,
        }
    }
    
    /// Get snapshot info for external use
    pub fn get_snapshot_info(&self) -> (TxId, HashSet<TxId>, TxId, TxId) {
        let snapshot = self.create_snapshot();
        (snapshot.version, (*snapshot.active_transactions).clone(), snapshot.xmin, snapshot.xmax)
    }
}
```

### 4.2 Transaction Handle

```rust
/// Active transaction handle
pub struct Transaction<'tm> {
    state: TransactionState,
    manager: &'tm TransactionManager,
}

impl<'tm> Transaction<'tm> {
    /// Get the transaction's snapshot for reads
    pub fn snapshot(&self) -> &Snapshot {
        &self.state.snapshot
    }
    
    /// Get transaction ID
    pub fn id(&self) -> TxId {
        self.state.tx_id
    }
    
    /// Record a read (for SSI)
    pub fn record_read(&mut self, element_id: ElementId) {
        if let Some(ref mut read_set) = self.state.read_set {
            read_set.insert(element_id);
        }
    }
    
    /// Record a write
    pub fn record_write(&mut self, element_id: ElementId) {
        self.state.write_set.insert(element_id);
    }
    
    /// Commit the transaction
    pub fn commit(self) -> Result<(), CommitError> {
        self.manager.commit_transaction(self.state)
    }
    
    /// Abort/rollback the transaction
    pub fn abort(self) {
        self.manager.abort_transaction(self.state.tx_id);
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Auto-abort if not explicitly committed
        // (actual abort happens in manager)
    }
}
```

### 4.3 Commit Protocol

```
┌─────────────────────────────────────────────────────────────────┐
│                    Commit Protocol                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Transaction T wants to commit:                                  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 1: Validate (Optimistic Concurrency Control)       │   │
│  │                                                          │   │
│  │   For each element E in T.write_set:                     │   │
│  │     current_version = get_latest_version(E)              │   │
│  │     if current_version.xmin > T.snapshot.version:        │   │
│  │       → CONFLICT: another tx modified E after T started  │   │
│  │       → ABORT T                                          │   │
│  │                                                          │   │
│  │   For SSI, also check read-write conflicts:              │   │
│  │     For each element E in T.read_set:                    │   │
│  │       if E was modified by concurrent committed tx:      │   │
│  │         → CONFLICT: read-write dependency cycle          │   │
│  │         → ABORT T                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                         │                                       │
│                         ▼ (no conflicts)                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 2: Lock and Write                                   │   │
│  │                                                          │   │
│  │   Acquire commit lock (brief exclusive lock)             │   │
│  │   For each write in T.write_set:                         │   │
│  │     - Set xmax on old version to T.tx_id                 │   │
│  │     - Link new version to chain                          │   │
│  │   Write commit record to WAL                             │   │
│  │   Release commit lock                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 3: Finalize                                         │   │
│  │                                                          │   │
│  │   Mark T as committed in commit log                      │   │
│  │   Remove T from active transactions                      │   │
│  │   Update oldest_active if needed                         │   │
│  │   Signal waiting transactions                            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
impl TransactionManager {
    /// Commit a transaction
    pub fn commit_transaction(&self, state: TransactionState) -> Result<(), CommitError> {
        let tx_id = state.tx_id;
        
        // Step 1: Validate write-write conflicts
        self.validate_writes(&state)?;
        
        // Step 1b: Validate read-write conflicts (SSI only)
        if state.isolation == IsolationLevel::Serializable {
            self.validate_reads(&state)?;
        }
        
        // Step 2: Apply writes under lock
        {
            let _commit_lock = self.commit_lock.lock();
            
            // Apply all writes
            for element_id in &state.write_set {
                self.apply_write(tx_id, *element_id)?;
            }
            
            // Write commit record to WAL
            self.wal.log_commit(tx_id)?;
        }
        
        // Step 3: Finalize
        self.commit_log.mark_committed(tx_id);
        self.active_transactions.write().remove(&tx_id);
        self.update_oldest_active();
        
        Ok(())
    }
    
    /// Validate no write-write conflicts
    fn validate_writes(&self, state: &TransactionState) -> Result<(), CommitError> {
        for element_id in &state.write_set {
            let current = self.storage.get_latest_version(*element_id);
            
            if let Some(version) = current {
                // Check if modified by a transaction that committed after our snapshot
                if version.xmin > state.snapshot.version {
                    return Err(CommitError::WriteConflict {
                        element: *element_id,
                        conflicting_tx: version.xmin,
                    });
                }
                
                // Check if being modified by another active transaction
                if version.xmax != tx_id::MAX && version.xmax != state.tx_id {
                    if self.is_active(version.xmax) {
                        return Err(CommitError::WriteConflict {
                            element: *element_id,
                            conflicting_tx: version.xmax,
                        });
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate no read-write conflicts (SSI)
    fn validate_reads(&self, state: &TransactionState) -> Result<(), CommitError> {
        let read_set = state.read_set.as_ref().unwrap();
        
        for element_id in read_set {
            // Check if any concurrent transaction modified what we read
            let current = self.storage.get_latest_version(*element_id);
            
            if let Some(version) = current {
                // If modified by a committed tx that started after us
                if version.xmin > state.snapshot.version 
                   && self.commit_log.is_committed(version.xmin) 
                {
                    return Err(CommitError::SerializationFailure {
                        element: *element_id,
                        reason: "Read-write conflict detected",
                    });
                }
            }
        }
        
        Ok(())
    }
    
    /// Abort a transaction
    pub fn abort_transaction(&self, tx_id: TxId) {
        // Mark as aborted
        self.commit_log.mark_aborted(tx_id);
        
        // Remove from active transactions
        self.active_transactions.write().remove(&tx_id);
        
        // Update oldest active
        self.update_oldest_active();
        
        // Note: Aborted versions will be cleaned up by GC
    }
}

#[derive(Debug)]
pub enum CommitError {
    WriteConflict {
        element: ElementId,
        conflicting_tx: TxId,
    },
    SerializationFailure {
        element: ElementId,
        reason: &'static str,
    },
    IoError(std::io::Error),
}
```

### 4.4 Write Operations

```rust
impl MvccStorage {
    /// Insert a new vertex within a transaction
    pub fn insert_vertex(
        &mut self,
        tx: &mut Transaction,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> VertexId {
        let tx_id = tx.id();
        
        // Allocate vertex ID
        let vertex_id = self.allocate_vertex_id();
        
        // Create versioned record
        let record = VersionedNodeRecord {
            id: vertex_id.0,
            label_id: self.intern_label(label),
            flags: 0,
            version: VersionInfo::new(tx_id),
            first_out_edge: u64::MAX,
            first_in_edge: u64::MAX,
            prop_offset: self.store_properties(&properties),
            prop_len: 0,
            prop_checksum: 0,
            _version_pad: [0; 7],
        };
        
        // Write record
        self.write_vertex_record(vertex_id, &record);
        
        // Update indexes
        self.label_index.insert(record.label_id, vertex_id, tx_id);
        for (key, value) in &properties {
            self.property_index.insert(record.label_id, self.intern_key(key), value, vertex_id.0, tx_id);
        }
        
        // Record write for conflict detection
        tx.record_write(ElementId::Vertex(vertex_id));
        
        vertex_id
    }
    
    /// Update a vertex within a transaction
    pub fn update_vertex(
        &mut self,
        tx: &mut Transaction,
        vertex_id: VertexId,
        updates: HashMap<String, Value>,
    ) -> Result<(), UpdateError> {
        let tx_id = tx.id();
        
        // Find current visible version
        let current = self.get_visible_vertex_record(vertex_id, tx.snapshot())
            .ok_or(UpdateError::NotFound)?;
        
        // Check for write-write conflict
        if current.version.xmax != tx_id::MAX && current.version.xmax != tx_id {
            return Err(UpdateError::Conflict);
        }
        
        // Create new version with updated properties
        let mut new_props = self.load_properties(current.prop_offset);
        new_props.extend(updates);
        
        let new_record = VersionedNodeRecord {
            version: VersionInfo {
                xmin: tx_id,
                xmax: tx_id::MAX,
                prev_version: self.get_vertex_offset(vertex_id),
                flags: VersionFlags::empty(),
            },
            prop_offset: self.store_properties(&new_props),
            ..current.clone()
        };
        
        // Mark old version as superseded
        self.set_vertex_xmax(vertex_id, tx_id);
        
        // Write new version (at new location or in-place for head)
        let new_offset = self.write_new_vertex_version(&new_record);
        self.update_vertex_head(vertex_id, new_offset);
        
        // Update indexes
        self.update_property_indexes(vertex_id, &current, &new_record, tx_id);
        
        // Record write
        tx.record_write(ElementId::Vertex(vertex_id));
        
        Ok(())
    }
    
    /// Delete a vertex within a transaction
    pub fn delete_vertex(
        &mut self,
        tx: &mut Transaction,
        vertex_id: VertexId,
    ) -> Result<(), DeleteError> {
        let tx_id = tx.id();
        
        // Find current visible version
        let current = self.get_visible_vertex_record(vertex_id, tx.snapshot())
            .ok_or(DeleteError::NotFound)?;
        
        // Check for conflict
        if current.version.xmax != tx_id::MAX && current.version.xmax != tx_id {
            return Err(DeleteError::Conflict);
        }
        
        // Mark as deleted by setting xmax
        self.set_vertex_xmax(vertex_id, tx_id);
        
        // Mark in indexes
        self.label_index.mark_deleted(current.label_id, vertex_id, tx_id);
        
        // Record write
        tx.record_write(ElementId::Vertex(vertex_id));
        
        Ok(())
    }
}
```

### 4.5 Commit Log

```rust
/// Persistent commit log for transaction status
pub struct CommitLog {
    /// Memory-mapped commit status array
    /// Each byte represents status of one transaction
    mmap: MmapMut,
    
    /// Base transaction ID (offset 0 in array)
    base_tx_id: TxId,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
pub enum TxStatus {
    InProgress = 0,
    Committed = 1,
    Aborted = 2,
}

impl CommitLog {
    /// Check if a transaction is committed
    #[inline]
    pub fn is_committed(&self, tx_id: TxId) -> bool {
        self.get_status(tx_id) == TxStatus::Committed
    }
    
    /// Get transaction status
    pub fn get_status(&self, tx_id: TxId) -> TxStatus {
        if tx_id < self.base_tx_id {
            // Very old transaction - assume committed
            return TxStatus::Committed;
        }
        
        let offset = (tx_id - self.base_tx_id) as usize;
        if offset >= self.mmap.len() {
            return TxStatus::InProgress;
        }
        
        match self.mmap[offset] {
            0 => TxStatus::InProgress,
            1 => TxStatus::Committed,
            2 => TxStatus::Aborted,
            _ => TxStatus::InProgress,
        }
    }
    
    /// Mark transaction as committed
    pub fn mark_committed(&self, tx_id: TxId) {
        let offset = (tx_id - self.base_tx_id) as usize;
        // Use atomic write for thread safety
        unsafe {
            std::ptr::write_volatile(&self.mmap[offset] as *const u8 as *mut u8, 1);
        }
    }
    
    /// Mark transaction as aborted
    pub fn mark_aborted(&self, tx_id: TxId) {
        let offset = (tx_id - self.base_tx_id) as usize;
        unsafe {
            std::ptr::write_volatile(&self.mmap[offset] as *const u8 as *mut u8, 2);
        }
    }
}
```

---

## 5. Garbage Collection (Vacuum)

### 5.1 Overview

Old versions accumulate and must be reclaimed. A version is "dead" when no active snapshot can see it:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Garbage Collection                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Active Snapshots: [v=150, v=180, v=200]                        │
│  Oldest Active: v=150                                           │
│                                                                 │
│  Version Chain for Vertex 42:                                   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │ v200 (live) │───▶│ v160 (live) │───▶│ v100 (dead) │         │
│  │ xmin: 200   │    │ xmin: 160   │    │ xmin: 100   │         │
│  │ xmax: ∞     │    │ xmax: 200   │    │ xmax: 160   │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
│        │                  │                  │                   │
│        │                  │                  └── Can be removed: │
│        │                  │                     xmax=160 < 150   │
│        │                  │                     (superseded      │
│        │                  │                      before oldest)  │
│        │                  │                                      │
│        │                  └── Must keep: snapshot v=150 might    │
│        │                     see this version                    │
│        │                                                         │
│        └── Current version, always keep                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Vacuum Process

```rust
/// Background vacuum process
pub struct Vacuum {
    storage: Arc<MvccStorage>,
    tx_manager: Arc<TransactionManager>,
    config: VacuumConfig,
}

pub struct VacuumConfig {
    /// Minimum interval between vacuum runs
    pub interval: Duration,
    
    /// Maximum versions to process per run
    pub batch_size: usize,
    
    /// Target: remove versions older than this many transactions
    pub version_retention: u64,
    
    /// Run vacuum in background thread
    pub background: bool,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            batch_size: 10_000,
            version_retention: 1_000_000,
            background: true,
        }
    }
}

impl Vacuum {
    /// Run a vacuum cycle
    pub fn run_cycle(&self) -> VacuumStats {
        let mut stats = VacuumStats::default();
        
        // Get the oldest active transaction
        let oldest_active = self.tx_manager.oldest_active();
        
        // Process vertex versions
        stats.vertices_scanned = self.vacuum_vertices(oldest_active, &mut stats);
        
        // Process edge versions
        stats.edges_scanned = self.vacuum_edges(oldest_active, &mut stats);
        
        // Process index entries
        stats.index_entries_removed = self.vacuum_indexes(oldest_active);
        
        // Reclaim space in version store
        stats.bytes_reclaimed = self.compact_version_store();
        
        stats
    }
    
    /// Vacuum vertex version chains
    fn vacuum_vertices(&self, oldest_active: TxId, stats: &mut VacuumStats) -> u64 {
        let mut scanned = 0;
        
        for vertex_id in self.storage.vertex_ids() {
            scanned += 1;
            
            // Get version chain head
            let head = match self.storage.get_vertex_head(vertex_id) {
                Some(h) => h,
                None => continue,
            };
            
            // Walk chain, find versions to remove
            let mut current_offset = head;
            let mut prev_offset: Option<u64> = None;
            
            while current_offset != 0 {
                let version = self.storage.read_vertex_version(current_offset);
                
                // Check if this version is dead
                if self.is_dead_version(&version.version, oldest_active) {
                    // Unlink from chain
                    if let Some(prev) = prev_offset {
                        self.storage.set_vertex_prev_version(prev, version.version.prev_version);
                    }
                    
                    // Add to free list
                    self.storage.free_vertex_version(current_offset);
                    stats.versions_removed += 1;
                    
                    current_offset = version.version.prev_version;
                } else {
                    prev_offset = Some(current_offset);
                    current_offset = version.version.prev_version;
                }
            }
        }
        
        scanned
    }
    
    /// Check if a version is dead (invisible to all active snapshots)
    fn is_dead_version(&self, version: &VersionInfo, oldest_active: TxId) -> bool {
        // Version is dead if:
        // 1. It was superseded (xmax set) by a committed transaction
        // 2. That transaction committed before the oldest active snapshot
        
        if version.xmax == tx_id::MAX {
            return false;  // Current version, not dead
        }
        
        // Check if superseding transaction committed
        if !self.tx_manager.is_committed(version.xmax) {
            return false;  // Superseding tx not committed, might rollback
        }
        
        // Check if committed before oldest active
        version.xmax < oldest_active
    }
}

#[derive(Default)]
pub struct VacuumStats {
    pub vertices_scanned: u64,
    pub edges_scanned: u64,
    pub versions_removed: u64,
    pub index_entries_removed: u64,
    pub bytes_reclaimed: u64,
    pub duration: Duration,
}
```

### 5.3 Incremental Vacuum

For large graphs, vacuum in small batches to avoid blocking:

```rust
impl Vacuum {
    /// Incremental vacuum using cursor
    pub fn vacuum_incremental(&self, cursor: &mut VacuumCursor) -> VacuumStats {
        let oldest_active = self.tx_manager.oldest_active();
        let mut stats = VacuumStats::default();
        let mut processed = 0;
        
        // Resume from cursor position
        let vertex_iter = self.storage.vertex_ids_from(cursor.last_vertex);
        
        for vertex_id in vertex_iter {
            if processed >= self.config.batch_size {
                cursor.last_vertex = vertex_id;
                cursor.completed = false;
                return stats;
            }
            
            // Vacuum this vertex's versions
            stats.versions_removed += self.vacuum_single_vertex(vertex_id, oldest_active);
            processed += 1;
        }
        
        cursor.completed = true;
        stats
    }
}

pub struct VacuumCursor {
    pub last_vertex: VertexId,
    pub last_edge: EdgeId,
    pub completed: bool,
}
```

### 5.4 Version Store Compaction

```rust
impl Vacuum {
    /// Compact the version store to reclaim fragmented space
    fn compact_version_store(&self) -> u64 {
        let version_store = self.storage.version_store();
        
        // Strategy 1: Simple free list coalescing
        let coalesced = version_store.coalesce_free_list();
        
        // Strategy 2: If fragmentation > threshold, do full compaction
        let fragmentation = version_store.fragmentation_ratio();
        
        if fragmentation > 0.3 {  // 30% fragmented
            // Full compaction: rewrite all live versions contiguously
            return self.full_compaction();
        }
        
        coalesced
    }
    
    /// Full compaction - expensive but eliminates fragmentation
    fn full_compaction(&self) -> u64 {
        let version_store = self.storage.version_store();
        let mut new_store = VersionStore::new_temp();
        let mut reclaimed = 0;
        
        // Copy all live versions to new store
        for (element_id, versions) in self.storage.all_version_chains() {
            for version in versions {
                if !version.flags.contains(VersionFlags::VACUUMED) {
                    let new_offset = new_store.store_version(&version);
                    self.storage.update_version_pointer(element_id, version.xmin, new_offset);
                }
            }
        }
        
        reclaimed = version_store.size() - new_store.size();
        
        // Swap stores
        self.storage.swap_version_store(new_store);
        
        reclaimed
    }
}
```

### 5.5 Index Cleanup

```rust
impl Vacuum {
    /// Remove dead entries from indexes
    fn vacuum_indexes(&self, oldest_active: TxId) -> u64 {
        let mut removed = 0;
        
        // Vacuum property index
        removed += self.storage.property_index().vacuum(|entry| {
            // Entry is dead if its xmax < oldest_active and xmax tx committed
            entry.xmax < oldest_active && self.tx_manager.is_committed(entry.xmax)
        });
        
        // Vacuum label index
        removed += self.storage.label_index().vacuum(|entry| {
            entry.xmax < oldest_active && self.tx_manager.is_committed(entry.xmax)
        });
        
        removed
    }
}
```

### 5.6 Background Vacuum Thread

```rust
impl Vacuum {
    /// Start background vacuum thread
    pub fn start_background(self: Arc<Self>) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let mut cursor = VacuumCursor::default();
            
            loop {
                // Wait for interval or shutdown signal
                std::thread::sleep(self.config.interval);
                
                // Run incremental vacuum
                let stats = self.vacuum_incremental(&mut cursor);
                
                if stats.versions_removed > 0 {
                    log::info!(
                        "Vacuum: removed {} versions, reclaimed {} bytes",
                        stats.versions_removed,
                        stats.bytes_reclaimed
                    );
                }
                
                // Reset cursor if completed
                if cursor.completed {
                    cursor = VacuumCursor::default();
                }
            }
        })
    }
}
```

---

## 6. Integration with Existing Storage

### 6.1 Unified Graph API

The MVCC layer integrates transparently with the existing fluent API:

```rust
/// Graph with MVCC support
pub struct MvccGraph {
    storage: MvccStorage,
    tx_manager: Arc<TransactionManager>,
    vacuum: Arc<Vacuum>,
}

impl MvccGraph {
    /// Open or create an MVCC-enabled graph
    pub fn open(path: &str) -> Result<Self, StorageError> {
        let storage = MvccStorage::open(path)?;
        let tx_manager = Arc::new(TransactionManager::new(&storage)?);
        let vacuum = Arc::new(Vacuum::new(
            Arc::new(storage.clone()),
            tx_manager.clone(),
            VacuumConfig::default(),
        ));
        
        // Start background vacuum
        vacuum.clone().start_background();
        
        Ok(Self {
            storage,
            tx_manager,
            vacuum,
        })
    }
    
    /// Get a read-only traversal source (auto snapshot)
    pub fn traversal(&self) -> GraphTraversalSource<'_> {
        let snapshot = Snapshot::new(&self.tx_manager);
        GraphTraversalSource::new(&self.storage, snapshot)
    }
    
    /// Begin a read-write transaction
    pub fn begin(&self) -> Transaction<'_> {
        self.tx_manager.begin(IsolationLevel::SnapshotIsolation)
    }
    
    /// Begin a serializable transaction
    pub fn begin_serializable(&self) -> Transaction<'_> {
        self.tx_manager.begin(IsolationLevel::Serializable)
    }
}
```

### 6.2 Traversal Source Integration

```rust
/// MVCC-aware traversal source
pub struct GraphTraversalSource<'g> {
    storage: &'g MvccStorage,
    snapshot: Snapshot,
}

impl<'g> GraphTraversalSource<'g> {
    /// Start traversal from all vertices
    pub fn v(self) -> Traversal<'g, Vertex> {
        let vertices = self.storage.vertices(&self.snapshot);
        Traversal::new(vertices, self.snapshot.clone())
    }
    
    /// Start from specific vertex IDs
    pub fn v_by_ids(self, ids: impl IntoIterator<Item = VertexId>) -> Traversal<'g, Vertex> {
        let vertices = ids.into_iter()
            .filter_map(|id| self.storage.get_vertex(id, &self.snapshot));
        Traversal::new(vertices, self.snapshot.clone())
    }
    
    /// Start traversal from all edges
    pub fn e(self) -> Traversal<'g, Edge> {
        let edges = self.storage.edges(&self.snapshot);
        Traversal::new(edges, self.snapshot.clone())
    }
}
```

### 6.3 Mutation Integration

```rust
impl<'tm> Transaction<'tm> {
    /// Get traversal source within this transaction
    pub fn traversal(&self) -> GraphTraversalSource<'_> {
        GraphTraversalSource::new(self.storage(), self.snapshot().clone())
    }
    
    /// Add a vertex within this transaction
    pub fn add_v(&mut self, label: &str) -> VertexBuilder<'_, 'tm> {
        VertexBuilder::new(self, label)
    }
    
    /// Add an edge within this transaction
    pub fn add_e(&mut self, label: &str) -> EdgeBuilder<'_, 'tm> {
        EdgeBuilder::new(self, label)
    }
}

pub struct VertexBuilder<'a, 'tm> {
    tx: &'a mut Transaction<'tm>,
    label: String,
    properties: HashMap<String, Value>,
}

impl<'a, 'tm> VertexBuilder<'a, 'tm> {
    pub fn property(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.properties.insert(key.to_string(), value.into());
        self
    }
    
    pub fn build(self) -> VertexId {
        self.tx.storage_mut().insert_vertex(
            self.tx,
            &self.label,
            self.properties,
        )
    }
}
```

### 6.4 Backward Compatibility

Existing Phase 1 code continues to work with minimal changes:

```rust
// Phase 1 code (still works)
let graph = Graph::open("my_graph.db")?;
let g = graph.traversal();
let results = g.v().has_label("person").to_list();

// Phase 2 MVCC code (new)
let graph = MvccGraph::open("my_graph.db")?;

// Simple reads (auto-snapshot)
let g = graph.traversal();
let results = g.v().has_label("person").to_list();

// Transactional writes
let mut tx = graph.begin();
let alice = tx.add_v("person").property("name", "Alice").build();
let bob = tx.add_v("person").property("name", "Bob").build();
tx.add_e("knows").from(alice).to(bob).build();
tx.commit()?;
```

### 6.5 Storage Backend Abstraction

```rust
/// Trait for storage backends (supports both Phase 1 and MVCC)
pub trait GraphStorage: Send + Sync {
    /// Get a vertex by ID with visibility check
    fn get_vertex(&self, id: VertexId, visibility: &dyn Visibility) -> Option<Vertex>;
    
    /// Iterate all vertices with visibility filtering
    fn vertices(&self, visibility: &dyn Visibility) -> Box<dyn Iterator<Item = Vertex> + '_>;
    
    /// Get outgoing edges with visibility filtering
    fn out_edges(&self, vertex: VertexId, visibility: &dyn Visibility) 
        -> Box<dyn Iterator<Item = Edge> + '_>;
    
    // ... other methods
}

/// Visibility checker (Phase 1: always visible, Phase 2: snapshot-based)
pub trait Visibility {
    fn can_see(&self, version: &VersionInfo) -> bool;
}

/// Phase 1: Everything is visible
pub struct AlwaysVisible;

impl Visibility for AlwaysVisible {
    fn can_see(&self, _version: &VersionInfo) -> bool {
        true
    }
}

/// Phase 2: Snapshot-based visibility
impl Visibility for Snapshot {
    fn can_see(&self, version: &VersionInfo) -> bool {
        self.is_visible_xmin(version.xmin) && self.is_visible_xmax(version.xmax)
    }
}
```

---

## 7. Performance Considerations

### 7.1 Overhead Analysis

```
┌─────────────────────────────────────────────────────────────────┐
│                    MVCC Overhead Analysis                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Space Overhead:                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Component              │ Phase 1    │ Phase 2 (MVCC)     │  │
│  ├────────────────────────┼────────────┼────────────────────│  │
│  │ Node record            │ 48 bytes   │ 80 bytes (+67%)    │  │
│  │ Edge record            │ 56 bytes   │ 88 bytes (+57%)    │  │
│  │ Version metadata       │ 0 bytes    │ 25 bytes/version   │  │
│  │ Commit log             │ 0 bytes    │ 1 byte/transaction │  │
│  │ Active tx tracking     │ 0 bytes    │ ~100 bytes/tx      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Time Overhead:                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Operation              │ Phase 1    │ Phase 2 (MVCC)     │  │
│  ├────────────────────────┼────────────┼────────────────────│  │
│  │ Read (cache hit)       │ ~50ns      │ ~80ns (+60%)       │  │
│  │ Read (version walk)    │ N/A        │ ~20ns/version      │  │
│  │ Write                  │ ~200ns     │ ~300ns (+50%)      │  │
│  │ Commit (small tx)      │ ~1µs       │ ~5µs (+400%)       │  │
│  │ Snapshot creation      │ N/A        │ ~500ns             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Concurrency Improvement:                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Scenario               │ Phase 1    │ Phase 2 (MVCC)     │  │
│  ├────────────────────────┼────────────┼────────────────────│  │
│  │ Concurrent readers     │ High       │ Higher (no lock)   │  │
│  │ Reader + Writer        │ Blocked    │ No blocking        │  │
│  │ Concurrent writers     │ Serialized │ OCC validation     │  │
│  │ Long read + short write│ Poor       │ Excellent          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 Optimizations

```rust
/// Optimization 1: Inline current version
/// Most reads access the current version. Store it at a fixed location
/// to avoid version chain traversal.
pub struct OptimizedVersionedRecord {
    /// Current (newest) version data - always at this location
    current: VersionedNodeRecord,
    
    /// Pointer to historical versions (in version store)
    history_head: u64,
}

/// Optimization 2: Epoch-based snapshot
/// Instead of tracking individual active transactions, use epochs
pub struct EpochSnapshot {
    epoch: u64,
    // Snapshot sees all commits before this epoch
}

/// Optimization 3: Hint bits caching
/// Cache transaction status in version records to avoid commit log lookups
impl VersionInfo {
    #[inline]
    pub fn set_committed_hint(&mut self) {
        self.flags.insert(VersionFlags::XMIN_COMMITTED);
    }
    
    #[inline]
    pub fn is_definitely_committed(&self) -> bool {
        self.flags.contains(VersionFlags::XMIN_COMMITTED)
    }
}

/// Optimization 4: Batch visibility checks
impl Snapshot {
    /// Check visibility for multiple versions at once (SIMD-friendly)
    pub fn filter_visible<'a>(
        &self,
        versions: &'a [VersionInfo],
    ) -> impl Iterator<Item = (usize, &'a VersionInfo)> {
        versions.iter().enumerate().filter(|(_, v)| self.can_see(v))
    }
}
```

### 7.3 Benchmark Scenarios

```rust
/// Benchmark configuration
pub struct MvccBenchmark {
    /// Number of vertices in test graph
    pub vertex_count: usize,
    /// Number of edges
    pub edge_count: usize,
    /// Number of concurrent readers
    pub reader_threads: usize,
    /// Number of concurrent writers
    pub writer_threads: usize,
    /// Read/write ratio (0.0-1.0, higher = more reads)
    pub read_ratio: f64,
    /// Average transaction size (operations per tx)
    pub tx_size: usize,
}

impl MvccBenchmark {
    /// Benchmark: Pure read throughput
    pub fn bench_read_throughput(&self) -> BenchResult {
        // Multiple threads doing read-only traversals
        // Measure: operations/second, latency percentiles
    }
    
    /// Benchmark: Read/write mix
    pub fn bench_mixed_workload(&self) -> BenchResult {
        // Concurrent readers and writers
        // Measure: throughput, conflict rate, latency
    }
    
    /// Benchmark: Write-heavy with conflicts
    pub fn bench_write_conflicts(&self) -> BenchResult {
        // High contention on small set of vertices
        // Measure: commit success rate, retry overhead
    }
    
    /// Benchmark: Long-running read vs. writes
    pub fn bench_long_read(&self) -> BenchResult {
        // One thread does long traversal
        // Other threads do short writes
        // Measure: both complete without blocking
    }
    
    /// Benchmark: Vacuum overhead
    pub fn bench_vacuum(&self) -> BenchResult {
        // Continuous writes generating versions
        // Background vacuum running
        // Measure: version count, space usage, vacuum impact
    }
}

/// Expected results (targets)
/// 
/// | Scenario          | Phase 1      | Phase 2 Target |
/// |-------------------|--------------|----------------|
/// | Read throughput   | 1M ops/sec   | 900K ops/sec   |
/// | Mixed (80% read)  | 200K ops/sec | 400K ops/sec   |
/// | Writer blocked    | Yes          | No             |
/// | Conflict rate     | N/A          | < 1%           |
/// | Vacuum overhead   | N/A          | < 5% CPU       |
```

### 7.4 Tuning Parameters

```rust
/// MVCC configuration options
pub struct MvccConfig {
    /// Maximum active transactions before blocking new ones
    pub max_active_transactions: usize,  // Default: 1000
    
    /// Snapshot retention period (for time-travel queries)
    pub snapshot_retention: Duration,  // Default: 1 hour
    
    /// Vacuum trigger threshold (versions per element)
    pub vacuum_threshold: usize,  // Default: 10
    
    /// Commit log size before truncation
    pub commit_log_max_size: usize,  // Default: 1GB
    
    /// Use separate version store vs. inline versions
    pub separate_version_store: bool,  // Default: true
    
    /// Enable hint bit caching
    pub hint_bits: bool,  // Default: true
}
```

---

## 8. Implementation Roadmap

### 8.1 Phase 2a: Core MVCC (4-6 weeks)

| Week | Deliverable |
|------|-------------|
| 1-2 | Versioned record formats, VersionInfo, basic visibility checks |
| 3 | Transaction manager, snapshot creation, commit log |
| 4 | Write operations (insert, update, delete) with versioning |
| 5 | Read path integration, visibility filtering |
| 6 | Basic conflict detection, commit validation |

### 8.2 Phase 2b: Garbage Collection (2-3 weeks)

| Week | Deliverable |
|------|-------------|
| 7 | Dead version identification, version chain cleanup |
| 8 | Index vacuum, commit log truncation |
| 9 | Background vacuum thread, incremental processing |

### 8.3 Phase 2c: Integration & Optimization (2-3 weeks)

| Week | Deliverable |
|------|-------------|
| 10 | Traversal API integration, backward compatibility |
| 11 | Hint bits, inline current version optimization |
| 12 | Benchmarking, tuning, documentation |

### 8.4 Testing Strategy

```rust
#[cfg(test)]
mod mvcc_tests {
    /// Test: Basic snapshot isolation
    #[test]
    fn test_snapshot_isolation() {
        // T1 reads vertex, T2 modifies, T1 still sees old value
    }
    
    /// Test: Write-write conflict detection
    #[test]
    fn test_write_conflict() {
        // T1 and T2 both modify same vertex
        // Second to commit should fail
    }
    
    /// Test: Read-write conflict (SSI)
    #[test]
    fn test_serializable_isolation() {
        // Detect write skew anomaly
    }
    
    /// Test: Vacuum correctness
    #[test]
    fn test_vacuum_preserves_visibility() {
        // Old versions visible to active snapshots not collected
    }
    
    /// Test: Recovery after crash
    #[test]
    fn test_crash_recovery() {
        // Uncommitted transactions rolled back
        // Committed transactions visible
    }
    
    /// Test: Concurrent correctness
    #[test]
    fn test_concurrent_operations() {
        // Multiple threads, verify linearizability
    }
}
```

### 8.5 Migration Path

```rust
/// Migrate Phase 1 database to MVCC format
pub fn migrate_to_mvcc(
    phase1_path: &str,
    mvcc_path: &str,
) -> Result<(), MigrationError> {
    let phase1 = Phase1Graph::open(phase1_path)?;
    let mvcc = MvccGraph::create(mvcc_path)?;
    
    let mut tx = mvcc.begin();
    
    // Copy all vertices with initial version (xmin=1, xmax=MAX)
    for vertex in phase1.vertices() {
        tx.add_v(vertex.label())
            .properties(vertex.properties())
            .with_id(vertex.id())  // Preserve IDs
            .build();
    }
    
    // Copy all edges
    for edge in phase1.edges() {
        tx.add_e(edge.label())
            .from(edge.out_v())
            .to(edge.in_v())
            .properties(edge.properties())
            .build();
    }
    
    tx.commit()?;
    
    // Verify migration
    assert_eq!(phase1.vertex_count(), mvcc.vertex_count());
    assert_eq!(phase1.edge_count(), mvcc.edge_count());
    
    Ok(())
}
```

---

## 9. Summary

### Key Design Decisions

1. **Version Chain Storage**: Separate version store for historical versions, inline current version for fast access

2. **Visibility Rule**: Standard xmin/xmax model with hint bit caching for performance

3. **Isolation Levels**: Snapshot Isolation (default) with optional Serializable

4. **Conflict Detection**: Optimistic concurrency control with validation at commit time

5. **Garbage Collection**: Incremental background vacuum to avoid blocking operations

6. **Backward Compatibility**: Same traversal API, optional transaction boundaries

### Complexity Summary

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Visibility check | O(1) | With hint bits |
| Version chain walk | O(v) | v = versions for element |
| Snapshot creation | O(a) | a = active transactions |
| Commit validation | O(w) | w = write set size |
| Vacuum (incremental) | O(b) | b = batch size |

### Dependencies

```toml
[dependencies]
# Existing
memmap2 = "0.9"
parking_lot = "0.12"

# New for MVCC
crossbeam-epoch = "0.9"  # Epoch-based memory reclamation
bitflags = "2.4"         # Version flags
```

