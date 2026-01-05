//! Memory-mapped persistent graph storage.
//!
//! This module implements the `GraphStorage` trait using memory-mapped files,
//! providing durable storage with write-ahead logging for crash recovery.

use hashbrown::HashMap;
use memmap2::{Mmap, MmapOptions};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use crate::error::StorageError;
use crate::storage::{Edge, GraphStorage, StringInterner, Vertex};

pub mod arena;
pub mod freelist;
pub mod records;
pub mod recovery;
pub mod wal;

use records::{
    EdgeRecord, FileHeader, NodeRecord, EDGE_RECORD_SIZE, HEADER_SIZE, MAGIC, NODE_RECORD_SIZE,
    VERSION,
};

use crate::value::{EdgeId, VertexId};

/// Memory-mapped graph storage backend.
///
/// This backend provides persistent storage using memory-mapped files with
/// write-ahead logging for durability and crash recovery.
///
/// # File Structure
///
/// The database consists of a main data file and a write-ahead log (WAL):
///
/// ```text
/// my_graph.db        - Main data file (mmap'd for reads)
/// my_graph.wal       - Write-ahead log (not implemented yet)
/// ```
///
/// # Thread Safety
///
/// All mutable state is protected by `RwLock`, making the graph safe to share
/// across threads. However, concurrent write operations require external
/// coordination.
pub struct MmapGraph {
    /// Memory-mapped file (read-only view of data)
    mmap: Arc<RwLock<Mmap>>,

    /// File handle for writes
    #[allow(dead_code)] // Will be used in Phase 4.2 and beyond
    file: Arc<RwLock<File>>,

    /// String interner (in-memory, rebuilt on load)
    /// Note: Uses RwLock for interior mutability during writes, but reads
    /// are lock-free via the interner() accessor using parking_lot's RwLock.
    string_table: Arc<RwLock<StringInterner>>,

    /// Label indexes (in-memory, rebuilt on load)
    vertex_labels: Arc<RwLock<HashMap<u32, RoaringBitmap>>>,
    edge_labels: Arc<RwLock<HashMap<u32, RoaringBitmap>>>,
}

impl MmapGraph {
    /// Open existing database or create new one.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file (`.db` extension recommended)
    ///
    /// # Returns
    ///
    /// A new `MmapGraph` instance connected to the file.
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - File exists but has invalid header
    /// - [`StorageError::Io`] - I/O error opening or creating file
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rustgremlin::storage::MmapGraph;
    ///
    /// // Create or open a database
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let file_exists = path.exists();

        // Open or create main data file
        // Note: We don't use truncate(true) because we want to preserve existing data
        // when reopening a database. New files are initialized separately.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        if !file_exists {
            // New database - initialize with default structure
            Self::initialize_new_file(&file)?;
        }

        // Memory-map the file
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        // Validate header
        Self::validate_header(&mmap)?;

        let graph = Self {
            mmap: Arc::new(RwLock::new(mmap)),
            file: Arc::new(RwLock::new(file)),
            string_table: Arc::new(RwLock::new(StringInterner::new())),
            vertex_labels: Arc::new(RwLock::new(HashMap::new())),
            edge_labels: Arc::new(RwLock::new(HashMap::new())),
        };

        // Rebuild in-memory indexes from disk data
        graph.rebuild_indexes()?;

        Ok(graph)
    }

    /// Initialize a new database file with header and initial structure.
    ///
    /// Creates a file with:
    /// - 64-byte header
    /// - Space for 1000 initial node records
    /// - Space for 10000 initial edge records
    /// - 64KB for properties and strings
    ///
    /// # Safety
    ///
    /// This function assumes the file is empty and newly created.
    fn initialize_new_file(file: &File) -> Result<(), StorageError> {
        // Initial capacities
        const INITIAL_NODE_CAPACITY: u64 = 1000;
        const INITIAL_EDGE_CAPACITY: u64 = 10000;
        const INITIAL_ARENA_SIZE: u64 = 64 * 1024; // 64KB

        // Calculate file size
        let node_table_size = INITIAL_NODE_CAPACITY * NODE_RECORD_SIZE as u64;
        let edge_table_size = INITIAL_EDGE_CAPACITY * records::EDGE_RECORD_SIZE as u64;
        let initial_size =
            HEADER_SIZE as u64 + node_table_size + edge_table_size + INITIAL_ARENA_SIZE;

        // Set file size
        file.set_len(initial_size)?;

        // Calculate offsets
        let property_arena_offset = HEADER_SIZE as u64 + node_table_size + edge_table_size;
        let string_table_offset = initial_size - 32 * 1024; // Last 32KB for strings

        // Create initial header
        let mut header = FileHeader::new();
        header.node_capacity = INITIAL_NODE_CAPACITY;
        header.edge_capacity = INITIAL_EDGE_CAPACITY;
        header.property_arena_offset = property_arena_offset;
        header.string_table_offset = string_table_offset;

        // Write header
        Self::write_header(file, &header)?;

        Ok(())
    }

    /// Validate file header for correct magic and version.
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - File is too small or has wrong magic number
    /// - [`StorageError::InvalidFormat`] - File has unsupported version
    fn validate_header(mmap: &[u8]) -> Result<(), StorageError> {
        if mmap.len() < HEADER_SIZE {
            return Err(StorageError::InvalidFormat);
        }

        let header = Self::read_header(mmap);

        // Check magic number
        let magic = header.magic;
        if magic != MAGIC {
            return Err(StorageError::InvalidFormat);
        }

        // Check version
        let version = header.version;
        if version != VERSION {
            return Err(StorageError::InvalidFormat);
        }

        Ok(())
    }

    /// Read header from memory-mapped bytes.
    ///
    /// # Safety
    ///
    /// This uses `read_unaligned` since FileHeader is `#[repr(C, packed)]`.
    /// Caller must ensure mmap has at least HEADER_SIZE bytes.
    fn read_header(mmap: &[u8]) -> FileHeader {
        assert!(mmap.len() >= HEADER_SIZE, "mmap too small to read header");

        FileHeader::from_bytes(mmap)
    }

    /// Write header to file at offset 0.
    ///
    /// # Arguments
    ///
    /// * `file` - File to write to
    /// * `header` - Header to write
    ///
    /// # Platform Notes
    ///
    /// On Unix, uses `write_all_at` for positioned writes.
    /// On other platforms, uses seek + write_all.
    fn write_header(file: &File, header: &FileHeader) -> Result<(), StorageError> {
        let bytes = header.to_bytes();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, 0)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = file;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&bytes)?;
        }

        file.sync_data()?;

        Ok(())
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Get node record by vertex ID (O(1) lookup).
    ///
    /// Returns `None` if:
    /// - ID is out of bounds (>= node_capacity)
    /// - Node is marked as deleted
    /// - Record cannot be read from mmap
    ///
    /// # Arguments
    ///
    /// * `id` - Vertex ID to look up
    ///
    /// # Returns
    ///
    /// The node record if found and not deleted, otherwise `None`.
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` to read the packed struct from memory. This is safe
    /// because we verify the offset is within the mmap bounds before reading.
    #[inline]
    pub(crate) fn get_node_record(&self, id: VertexId) -> Option<NodeRecord> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Check bounds
        if id.0 >= header.node_capacity {
            return None;
        }

        // Calculate offset: header + (node_id * record_size)
        let offset = HEADER_SIZE + (id.0 as usize * NODE_RECORD_SIZE);

        // Verify we can read the full record
        if offset + NODE_RECORD_SIZE > mmap.len() {
            return None;
        }

        // Read record (using read_unaligned for packed struct)
        let record = unsafe {
            let ptr = mmap.as_ptr().add(offset) as *const NodeRecord;
            ptr.read_unaligned()
        };

        // Check deleted flag
        if record.is_deleted() {
            return None;
        }

        // Check if this is a valid initialized record by verifying the ID matches
        // Uninitialized records have all zeros, so id=0 might be valid for VertexId(0)
        // but for higher IDs, the record ID must match
        if record.id != id.0 {
            return None;
        }

        Some(record)
    }

    /// Get edge record by edge ID (O(1) lookup).
    ///
    /// Returns `None` if:
    /// - ID is out of bounds (>= edge_capacity)
    /// - Edge is marked as deleted
    /// - Record cannot be read from mmap
    ///
    /// # Arguments
    ///
    /// * `id` - Edge ID to look up
    ///
    /// # Returns
    ///
    /// The edge record if found and not deleted, otherwise `None`.
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` to read the packed struct from memory. This is safe
    /// because we verify the offset is within the mmap bounds before reading.
    #[inline]
    pub(crate) fn get_edge_record(&self, id: EdgeId) -> Option<EdgeRecord> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Check bounds
        if id.0 >= header.edge_capacity {
            return None;
        }

        // Calculate edge table offset: header + (node_capacity * node_record_size)
        let edge_table_offset = HEADER_SIZE + (header.node_capacity as usize * NODE_RECORD_SIZE);

        // Calculate record offset: edge_table_offset + (edge_id * record_size)
        let offset = edge_table_offset + (id.0 as usize * EDGE_RECORD_SIZE);

        // Verify we can read the full record
        if offset + EDGE_RECORD_SIZE > mmap.len() {
            return None;
        }

        // Read record (using read_unaligned for packed struct)
        let record = unsafe {
            let ptr = mmap.as_ptr().add(offset) as *const EdgeRecord;
            ptr.read_unaligned()
        };

        // Check deleted flag
        if record.is_deleted() {
            return None;
        }

        // Check if this is a valid initialized record by verifying the ID matches
        if record.id != id.0 {
            return None;
        }

        Some(record)
    }

    /// Helper: Calculate offset to the edge table.
    ///
    /// The edge table starts immediately after the node table.
    /// Returns the byte offset from the beginning of the file.
    #[inline]
    #[allow(dead_code)] // Will be used in Phase 3+ for write operations
    fn edge_table_offset(&self) -> usize {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);
        HEADER_SIZE + (header.node_capacity as usize * NODE_RECORD_SIZE)
    }

    /// Helper: Read a u32 value from mmap at the given offset.
    ///
    /// # Safety
    ///
    /// This performs bounds checking before reading to ensure the offset
    /// is valid within the mmap.
    #[inline]
    fn read_u32(&self, offset: usize) -> Result<u32, StorageError> {
        let mmap = self.mmap.read();

        if offset + 4 > mmap.len() {
            return Err(StorageError::CorruptedData);
        }

        let bytes: [u8; 4] = mmap[offset..offset + 4].try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    /// Helper: Read a u64 value from mmap at the given offset.
    ///
    /// # Safety
    ///
    /// This performs bounds checking before reading to ensure the offset
    /// is valid within the mmap.
    #[inline]
    fn read_u64(&self, offset: usize) -> Result<u64, StorageError> {
        let mmap = self.mmap.read();

        if offset + 8 > mmap.len() {
            return Err(StorageError::CorruptedData);
        }

        let bytes: [u8; 8] = mmap[offset..offset + 8].try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }

    /// Helper: Read a u8 value from mmap at the given offset.
    ///
    /// # Safety
    ///
    /// This performs bounds checking before reading to ensure the offset
    /// is valid within the mmap.
    #[inline]
    fn read_u8(&self, offset: usize) -> Result<u8, StorageError> {
        let mmap = self.mmap.read();

        if offset >= mmap.len() {
            return Err(StorageError::CorruptedData);
        }

        Ok(mmap[offset])
    }

    // =========================================================================
    // Phase 2.5: Index Rebuilding
    // =========================================================================

    /// Rebuild in-memory indexes from on-disk data.
    ///
    /// This method scans all node and edge records in the database and rebuilds
    /// the label bitmap indexes. It is called automatically when opening an
    /// existing database to restore the in-memory indexes.
    ///
    /// # Process
    ///
    /// 1. Scan all node records from 0 to node_capacity
    /// 2. For each non-deleted node with matching ID, add its ID to the vertex_labels bitmap
    /// 3. Scan all edge records from 0 to edge_capacity
    /// 4. For each non-deleted edge with matching ID, add its ID to the edge_labels bitmap
    ///
    /// Records are considered valid only if:
    /// - The deleted flag is not set
    /// - The record's ID field matches its position (filters out uninitialized zero records)
    ///
    /// # Errors
    ///
    /// This method does not return errors currently, but could in the future if
    /// record corruption is detected during scanning.
    ///
    /// # Performance
    ///
    /// This is an O(V + E) operation where V is node_capacity and E is edge_capacity.
    /// For large databases, this can take several seconds. The operation is performed
    /// on startup to rebuild the indexes.
    pub(crate) fn rebuild_indexes(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Use node_count/edge_count rather than capacity to avoid scanning
        // uninitialized slots. Zero-initialized memory at slot 0 would otherwise
        // appear as a valid record (id=0 matches VertexId(0)/EdgeId(0)).
        let node_count = header.node_count;
        let edge_count = header.edge_count;

        drop(mmap); // Release mmap lock before taking index locks

        // Rebuild vertex label indexes
        // Scan slots up to node_count. get_node_record filters out:
        // - Deleted records
        // - Records with ID mismatch (shouldn't happen in valid DB)
        {
            let mut vertex_labels = self.vertex_labels.write();
            vertex_labels.clear();

            for node_id in 0..node_count {
                if let Some(node) = self.get_node_record(VertexId(node_id)) {
                    let label_id = node.label_id;
                    vertex_labels
                        .entry(label_id)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(node_id as u32);
                }
            }
        }

        // Rebuild edge label indexes
        // Scan slots up to edge_count. get_edge_record filters out:
        // - Deleted records
        // - Records with ID mismatch (shouldn't happen in valid DB)
        {
            let mut edge_labels = self.edge_labels.write();
            edge_labels.clear();

            for edge_id in 0..edge_count {
                if let Some(edge) = self.get_edge_record(EdgeId(edge_id)) {
                    let label_id = edge.label_id;
                    edge_labels
                        .entry(label_id)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(edge_id as u32);
                }
            }
        }

        Ok(())
    }

    // =========================================================================
    // Phase 2.3: Property Loading
    // =========================================================================

    /// Load properties for a node or edge from the property arena.
    ///
    /// Properties are stored as a linked list in the property arena. This method
    /// follows the chain starting at `prop_head`, deserializing each property
    /// entry and resolving property keys via the string interner.
    ///
    /// # Arguments
    ///
    /// * `prop_head` - Offset to the first property entry, or `u64::MAX` if no properties
    ///
    /// # Returns
    ///
    /// A `HashMap` containing all properties for the element. Returns an empty
    /// map if `prop_head == u64::MAX` (no properties).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::CorruptedData`] if:
    /// - Property offsets are out of bounds
    /// - Value data is malformed
    /// - String IDs cannot be resolved
    ///
    /// # Safety
    ///
    /// This method reads from the memory-mapped file using validated offsets.
    /// All reads are bounds-checked before accessing memory.
    pub(crate) fn load_properties(
        &self,
        prop_head: u64,
    ) -> Result<hashbrown::HashMap<String, crate::value::Value>, StorageError> {
        use crate::value::Value;
        use records::PROPERTY_ENTRY_HEADER_SIZE;

        let mut properties = hashbrown::HashMap::new();

        // Empty property list
        if prop_head == u64::MAX {
            return Ok(properties);
        }

        let mut current_offset = prop_head as usize;

        // Follow the linked list of properties
        loop {
            let mmap = self.mmap.read();

            // Verify we can read the property entry header
            if current_offset + PROPERTY_ENTRY_HEADER_SIZE > mmap.len() {
                return Err(StorageError::CorruptedData);
            }

            // Read property entry header fields
            let key_id = self.read_u32(current_offset)?;
            let _value_type = self.read_u8(current_offset + 4)?;
            let value_len = self.read_u32(current_offset + 5)?;
            let next = self.read_u64(current_offset + 9)?;

            // Move past the header
            let value_data_offset = current_offset + PROPERTY_ENTRY_HEADER_SIZE;

            // Verify we can read the value data
            if value_data_offset + value_len as usize > mmap.len() {
                return Err(StorageError::CorruptedData);
            }

            // Get value data slice
            let value_bytes = &mmap[value_data_offset..value_data_offset + value_len as usize];

            // Deserialize the value
            let mut pos = 0;
            let value =
                Value::deserialize(value_bytes, &mut pos).ok_or(StorageError::CorruptedData)?;

            // Resolve the property key from the string interner
            let string_table = self.string_table.read();
            let key = string_table
                .resolve(key_id)
                .ok_or(StorageError::CorruptedData)?
                .to_string();

            // Insert property into map
            properties.insert(key, value);

            // Check if this is the last property in the list
            if next == u64::MAX {
                break;
            }

            // Move to next property
            current_offset = next as usize;
        }

        Ok(properties)
    }

    /// Get the header from the mmap.
    ///
    /// This is a helper method for GraphStorage implementations that need
    /// to read counts from the header.
    fn get_header(&self) -> FileHeader {
        let mmap = self.mmap.read();
        Self::read_header(&mmap)
    }
}

// =========================================================================
// GraphStorage Trait Implementation
// =========================================================================

impl GraphStorage for MmapGraph {
    /// O(1) vertex lookup.
    ///
    /// Retrieves a vertex by ID, constructing the full `Vertex` struct with
    /// resolved label and loaded properties.
    ///
    /// # Returns
    ///
    /// - `Some(Vertex)` if the vertex exists and is not deleted
    /// - `None` if the vertex doesn't exist or is deleted
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        // Get the node record
        let record = self.get_node_record(id)?;

        // Resolve label from string table
        let label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id)?.to_string()
        };

        // Load properties
        let properties = self.load_properties(record.prop_head).ok()?;

        // Convert hashbrown HashMap to std HashMap for Vertex
        let properties: std::collections::HashMap<String, crate::value::Value> =
            properties.into_iter().collect();

        Some(Vertex {
            id,
            label,
            properties,
        })
    }

    /// O(1) edge lookup.
    ///
    /// Retrieves an edge by ID, constructing the full `Edge` struct with
    /// resolved label and loaded properties.
    ///
    /// # Returns
    ///
    /// - `Some(Edge)` if the edge exists and is not deleted
    /// - `None` if the edge doesn't exist or is deleted
    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        // Get the edge record
        let record = self.get_edge_record(id)?;

        // Resolve label from string table
        let label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id)?.to_string()
        };

        // Load properties
        let properties = self.load_properties(record.prop_head).ok()?;

        // Convert hashbrown HashMap to std HashMap for Edge
        let properties: std::collections::HashMap<String, crate::value::Value> =
            properties.into_iter().collect();

        Some(Edge {
            id,
            label,
            src: VertexId(record.src),
            dst: VertexId(record.dst),
            properties,
        })
    }

    /// O(1) vertex count from header.
    fn vertex_count(&self) -> u64 {
        self.get_header().node_count
    }

    /// O(1) edge count from header.
    fn edge_count(&self) -> u64 {
        self.get_header().edge_count
    }

    /// Returns iterator over all outgoing edges from a vertex.
    ///
    /// Follows the linked list starting at the vertex's `first_out_edge` pointer.
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Get the starting edge ID from the node record
        let first_edge = match self.get_node_record(vertex) {
            Some(record) => record.first_out_edge,
            None => return Box::new(std::iter::empty()),
        };

        Box::new(OutEdgeIterator {
            graph: self,
            current: first_edge,
        })
    }

    /// Returns iterator over all incoming edges to a vertex.
    ///
    /// Follows the linked list starting at the vertex's `first_in_edge` pointer.
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Get the starting edge ID from the node record
        let first_edge = match self.get_node_record(vertex) {
            Some(record) => record.first_in_edge,
            None => return Box::new(std::iter::empty()),
        };

        Box::new(InEdgeIterator {
            graph: self,
            current: first_edge,
        })
    }

    /// Returns iterator over all vertices with a given label.
    ///
    /// Uses the bitmap index for efficient label filtering.
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Look up label ID without interning (read-only)
        let label_id = {
            let string_table = self.string_table.read();
            string_table.lookup(label)
        };

        // Get the bitmap for this label, if any
        let bitmap = label_id.and_then(|id| {
            let vertex_labels = self.vertex_labels.read();
            vertex_labels.get(&id).cloned()
        });

        match bitmap {
            Some(bitmap) => Box::new(
                bitmap
                    .into_iter()
                    .filter_map(move |id| self.get_vertex(VertexId(id as u64))),
            ),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Returns iterator over all edges with a given label.
    ///
    /// Uses the bitmap index for efficient label filtering.
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Look up label ID without interning (read-only)
        let label_id = {
            let string_table = self.string_table.read();
            string_table.lookup(label)
        };

        // Get the bitmap for this label, if any
        let bitmap = label_id.and_then(|id| {
            let edge_labels = self.edge_labels.read();
            edge_labels.get(&id).cloned()
        });

        match bitmap {
            Some(bitmap) => Box::new(
                bitmap
                    .into_iter()
                    .filter_map(move |id| self.get_edge(EdgeId(id as u64))),
            ),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Returns iterator over all vertices in the graph.
    ///
    /// Scans all node slots from 0 to node_count, skipping deleted nodes.
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let node_count = self.get_header().node_count;

        Box::new((0..node_count).filter_map(move |id| self.get_vertex(VertexId(id))))
    }

    /// Returns iterator over all edges in the graph.
    ///
    /// Scans all edge slots from 0 to edge_count, skipping deleted edges.
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let edge_count = self.get_header().edge_count;

        Box::new((0..edge_count).filter_map(move |id| self.get_edge(EdgeId(id))))
    }

    /// Returns a reference to the string interner.
    ///
    /// # Implementation Note
    ///
    /// This uses `parking_lot::RwLockReadGuard::leak` to return a static reference.
    /// This is safe because:
    /// 1. The lock is held for the lifetime of the reference
    /// 2. MmapGraph is designed for single-threaded write access with concurrent reads
    /// 3. The leaked guard will be reclaimed when MmapGraph is dropped
    ///
    /// # Safety
    ///
    /// This method uses unsafe code to convert the guard into a static reference.
    /// The caller must ensure that the returned reference does not outlive the MmapGraph.
    fn interner(&self) -> &StringInterner {
        // SAFETY: We leak the read guard to get a 'static lifetime reference.
        // This is safe because:
        // 1. The StringInterner lives as long as MmapGraph
        // 2. We're holding a read lock, allowing concurrent reads
        // 3. The Arc ensures the data won't be deallocated while we hold a reference
        //
        // Note: This does leak the RwLockReadGuard, which means the read lock
        // is held until MmapGraph is dropped. For read-heavy workloads this is
        // acceptable. For write operations, we use a separate mutex pattern.
        let guard = self.string_table.read();
        let ptr = &*guard as *const StringInterner;
        std::mem::forget(guard);
        // SAFETY: The pointer is valid for the lifetime of MmapGraph
        unsafe { &*ptr }
    }
}

// =========================================================================
// Edge Iterators for Adjacency List Traversal
// =========================================================================

/// Iterator over outgoing edges from a vertex.
///
/// Follows the linked list of edges via the `next_out` pointer in each edge record.
struct OutEdgeIterator<'g> {
    graph: &'g MmapGraph,
    current: u64,
}

impl<'g> Iterator for OutEdgeIterator<'g> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        // u64::MAX indicates end of list
        if self.current == u64::MAX {
            return None;
        }

        // Get the edge record
        let record = self.graph.get_edge_record(EdgeId(self.current))?;

        // Move to next edge in the linked list
        self.current = record.next_out;

        // Construct and return the Edge
        self.graph.get_edge(EdgeId(record.id))
    }
}

/// Iterator over incoming edges to a vertex.
///
/// Follows the linked list of edges via the `next_in` pointer in each edge record.
struct InEdgeIterator<'g> {
    graph: &'g MmapGraph,
    current: u64,
}

impl<'g> Iterator for InEdgeIterator<'g> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        // u64::MAX indicates end of list
        if self.current == u64::MAX {
            return None;
        }

        // Get the edge record
        let record = self.graph.get_edge_record(EdgeId(self.current))?;

        // Move to next edge in the linked list
        self.current = record.next_in;

        // Construct and return the Edge
        self.graph.get_edge(EdgeId(record.id))
    }
}

// SAFETY: MmapGraph is Send + Sync because:
// - Arc<RwLock<_>> is Send + Sync
// - All interior data is protected by RwLocks
// - Memory-mapped regions are thread-safe for reads
unsafe impl Send for MmapGraph {}
unsafe impl Sync for MmapGraph {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::value::{EdgeId, VertexId};

    #[test]
    fn test_create_new_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Verify file was created
        assert!(path.exists());

        // Verify header
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        // Copy values to avoid unaligned reference errors
        let magic = header.magic;
        let version = header.version;
        let node_count = header.node_count;
        let node_capacity = header.node_capacity;
        let edge_count = header.edge_count;
        let edge_capacity = header.edge_capacity;

        assert_eq!(magic, MAGIC);
        assert_eq!(version, VERSION);
        assert_eq!(node_count, 0);
        assert_eq!(node_capacity, 1000);
        assert_eq!(edge_count, 0);
        assert_eq!(edge_capacity, 10000);
    }

    #[test]
    fn test_validate_header_rejects_invalid_magic() {
        // Create a buffer with wrong magic number
        let mut bytes = [0u8; HEADER_SIZE];
        let mut header = FileHeader::new();
        header.magic = 0xDEADBEEF; // Wrong magic
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        assert!(matches!(result, Err(StorageError::InvalidFormat)));
    }

    #[test]
    fn test_validate_header_rejects_invalid_version() {
        // Create a buffer with wrong version
        let mut bytes = [0u8; HEADER_SIZE];
        let mut header = FileHeader::new();
        header.version = 999; // Unsupported version
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        assert!(matches!(result, Err(StorageError::InvalidFormat)));
    }

    #[test]
    fn test_validate_header_accepts_valid_header() {
        // Create a buffer with valid header
        let mut bytes = [0u8; HEADER_SIZE];
        let header = FileHeader::new();
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_initial_file_has_correct_structure() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Verify file size
        let file = graph.file.read();
        let metadata = file.metadata().unwrap();
        let file_size = metadata.len();

        // Size should be: header + nodes + edges + arena
        // 64 + (1000 * 48) + (10000 * 56) + (64 * 1024)
        let expected_size = 64 + (1000 * 48) + (10000 * 56) + (64 * 1024);
        assert_eq!(file_size, expected_size);

        // Verify header fields
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        let property_arena_offset = header.property_arena_offset;
        let string_table_offset = header.string_table_offset;
        let free_node_head = header.free_node_head;

        // Property arena should start after node and edge tables
        let expected_arena_offset = 64 + (1000 * 48) + (10000 * 56);
        assert_eq!(property_arena_offset, expected_arena_offset as u64);

        // String table should be in last 32KB
        assert_eq!(string_table_offset, file_size - 32 * 1024);

        // Free list should be empty
        assert_eq!(free_node_head, u64::MAX);
    }

    #[test]
    fn test_reopen_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create and close database
        {
            let _graph = MmapGraph::open(&path).unwrap();
        }

        // Reopen database
        let graph = MmapGraph::open(&path).unwrap();

        // Verify header is still valid
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        let magic = header.magic;
        let version = header.version;

        assert_eq!(magic, MAGIC);
        assert_eq!(version, VERSION);
    }

    #[test]
    fn test_header_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Read initial header
        let mmap = graph.mmap.read();
        let header1 = MmapGraph::read_header(&mmap);

        let node_capacity1 = header1.node_capacity;
        let edge_capacity1 = header1.edge_capacity;

        drop(mmap);

        // Write modified header
        let mut header2 = FileHeader::new();
        header2.node_capacity = 2000;
        header2.edge_capacity = 20000;
        header2.node_count = 10;
        header2.edge_count = 50;

        let file = graph.file.read();
        MmapGraph::write_header(&file, &header2).unwrap();
        drop(file);

        // Remap to see changes
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        drop(file);

        let header3 = MmapGraph::read_header(&new_mmap);

        let node_capacity3 = header3.node_capacity;
        let edge_capacity3 = header3.edge_capacity;
        let node_count3 = header3.node_count;
        let edge_count3 = header3.edge_count;

        assert_eq!(node_capacity3, 2000);
        assert_eq!(edge_capacity3, 20000);
        assert_eq!(node_count3, 10);
        assert_eq!(edge_count3, 50);

        // Original values should have been different
        assert_ne!(node_capacity1, node_capacity3);
        assert_ne!(edge_capacity1, edge_capacity3);
    }

    // =========================================================================
    // Phase 2.2: Read Operations Tests
    // =========================================================================

    #[test]
    fn test_get_node_record_returns_none_for_out_of_bounds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Capacity is 1000, so ID 1000 should be out of bounds
        let result = graph.get_node_record(VertexId(1000));
        assert!(result.is_none(), "Should return None for out-of-bounds ID");

        let result = graph.get_node_record(VertexId(9999));
        assert!(
            result.is_none(),
            "Should return None for way out-of-bounds ID"
        );
    }

    #[test]
    fn test_get_node_record_reads_valid_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Manually write a node record to the file
        let node_id = VertexId(0);
        let mut record = records::NodeRecord::new(node_id.0, 42);
        record.first_out_edge = 100;
        record.first_in_edge = 200;
        record.prop_head = 300;

        // Write record to file
        let offset = HEADER_SIZE;
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap to see the changes
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read the record back
        let result = graph.get_node_record(node_id);
        assert!(result.is_some(), "Should find the written record");

        let retrieved = result.unwrap();
        // Copy fields to avoid unaligned reference errors with packed structs
        let id = retrieved.id;
        let label_id = retrieved.label_id;
        let first_out_edge = retrieved.first_out_edge;
        let first_in_edge = retrieved.first_in_edge;
        let prop_head = retrieved.prop_head;

        assert_eq!(id, 0);
        assert_eq!(label_id, 42);
        assert_eq!(first_out_edge, 100);
        assert_eq!(first_in_edge, 200);
        assert_eq!(prop_head, 300);
    }

    #[test]
    fn test_get_node_record_returns_none_for_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a deleted node record
        let node_id = VertexId(5);
        let mut record = records::NodeRecord::new(node_id.0, 7);
        record.mark_deleted();

        let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Should return None for deleted node
        let result = graph.get_node_record(node_id);
        assert!(
            result.is_none(),
            "Should return None for deleted node record"
        );
    }

    #[test]
    fn test_get_edge_record_returns_none_for_out_of_bounds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Capacity is 10000, so ID 10000 should be out of bounds
        let result = graph.get_edge_record(EdgeId(10000));
        assert!(result.is_none(), "Should return None for out-of-bounds ID");

        let result = graph.get_edge_record(EdgeId(99999));
        assert!(
            result.is_none(),
            "Should return None for way out-of-bounds ID"
        );
    }

    #[test]
    fn test_get_edge_record_reads_valid_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Manually write an edge record to the file
        let edge_id = EdgeId(0);
        let mut record = records::EdgeRecord::new(edge_id.0, 99, 10, 20);
        record.next_out = 500;
        record.next_in = 600;
        record.prop_head = 700;

        // Calculate offset: header + node_table + edge_id * edge_record_size
        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);
        let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap to see the changes
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read the record back
        let result = graph.get_edge_record(edge_id);
        assert!(result.is_some(), "Should find the written record");

        let retrieved = result.unwrap();
        // Copy fields to avoid unaligned reference errors with packed structs
        let id = retrieved.id;
        let label_id = retrieved.label_id;
        let src = retrieved.src;
        let dst = retrieved.dst;
        let next_out = retrieved.next_out;
        let next_in = retrieved.next_in;
        let prop_head = retrieved.prop_head;

        assert_eq!(id, 0);
        assert_eq!(label_id, 99);
        assert_eq!(src, 10);
        assert_eq!(dst, 20);
        assert_eq!(next_out, 500);
        assert_eq!(next_in, 600);
        assert_eq!(prop_head, 700);
    }

    #[test]
    fn test_get_edge_record_returns_none_for_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a deleted edge record
        let edge_id = EdgeId(42);
        let mut record = records::EdgeRecord::new(edge_id.0, 13, 100, 200);
        record.mark_deleted();

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);
        let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Should return None for deleted edge
        let result = graph.get_edge_record(edge_id);
        assert!(
            result.is_none(),
            "Should return None for deleted edge record"
        );
    }

    #[test]
    fn test_edge_table_offset_calculation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let offset = graph.edge_table_offset();

        // Should be: header (64) + node_table (1000 * 48)
        let expected = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);
        assert_eq!(offset, expected);
    }

    #[test]
    fn test_read_u32_helper() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a known u32 value at a specific offset
        let test_offset = 100;
        let test_value = 0x12345678u32;
        let bytes = test_value.to_le_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(test_offset)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read it back using the helper
        let result = graph.read_u32(test_offset as usize);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_value);
    }

    #[test]
    fn test_read_u32_bounds_checking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mmap = graph.mmap.read();
        let file_size = mmap.len();
        drop(mmap);

        // Try to read beyond the end of the file
        let result = graph.read_u32(file_size);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::CorruptedData)));

        // Try to read where we'd overflow
        let result = graph.read_u32(file_size - 2); // Only 2 bytes left, need 4
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u64_helper() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a known u64 value at a specific offset
        let test_offset = 200;
        let test_value = 0x123456789ABCDEF0u64;
        let bytes = test_value.to_le_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(test_offset)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read it back using the helper
        let result = graph.read_u64(test_offset as usize);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_value);
    }

    #[test]
    fn test_read_u64_bounds_checking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mmap = graph.mmap.read();
        let file_size = mmap.len();
        drop(mmap);

        // Try to read beyond the end of the file
        let result = graph.read_u64(file_size);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::CorruptedData)));

        // Try to read where we'd overflow
        let result = graph.read_u64(file_size - 4); // Only 4 bytes left, need 8
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_record_multiple_nodes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write multiple node records
        for i in 0..10 {
            let node_id = VertexId(i);
            let record = records::NodeRecord::new(node_id.0, i as u32 * 10);

            let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
            let bytes = record.to_bytes();

            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap after each write
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Read all records back and verify
        for i in 0..10 {
            let result = graph.get_node_record(VertexId(i));
            assert!(result.is_some(), "Node {} should exist", i);

            let record = result.unwrap();
            // Copy fields to avoid unaligned reference errors
            let id = record.id;
            let label_id = record.label_id;

            assert_eq!(id, i);
            assert_eq!(label_id, (i as u32) * 10);
        }

        // Verify non-written records return None
        let _result = graph.get_node_record(VertexId(10));
        // This will return None because the record will be all zeros (not written)
        // and id field won't match, or it could be considered valid with zeros
        // For a more accurate test, we'd check if label_id is 0 or handle zero-initialized memory
    }

    #[test]
    fn test_get_edge_record_multiple_edges() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);

        // Write multiple edge records
        for i in 0..10 {
            let edge_id = EdgeId(i);
            let record = records::EdgeRecord::new(edge_id.0, i as u32 * 5, i * 2, i * 2 + 1);

            let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
            let bytes = record.to_bytes();

            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap after each write
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Read all records back and verify
        for i in 0..10 {
            let result = graph.get_edge_record(EdgeId(i));
            assert!(result.is_some(), "Edge {} should exist", i);

            let record = result.unwrap();
            // Copy fields to avoid unaligned reference errors
            let id = record.id;
            let label_id = record.label_id;
            let src = record.src;
            let dst = record.dst;

            assert_eq!(id, i);
            assert_eq!(label_id, (i as u32) * 5);
            assert_eq!(src, i * 2);
            assert_eq!(dst, i * 2 + 1);
        }
    }

    // =========================================================================
    // Phase 2.3: Property Loading Tests
    // =========================================================================

    #[test]
    fn test_load_properties_returns_empty_for_no_properties() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // prop_head == u64::MAX indicates no properties
        let result = graph.load_properties(u64::MAX);
        assert!(result.is_ok());
        let properties = result.unwrap();
        assert!(properties.is_empty(), "Should return empty HashMap");
    }

    #[test]
    fn test_load_properties_single_property() {
        use crate::value::Value;
        use records::PropertyEntry;
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Setup: Intern a string for the property key
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("name"); // This will get ID 0
        }

        // Get property arena offset from header
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);
        let prop_arena_offset = header.property_arena_offset;
        drop(mmap);

        // Create a property entry for "name" = "Alice"
        let key_id = 0u32; // "name" string ID
        let value = Value::String("Alice".to_string());
        let mut value_bytes = Vec::new();
        value.serialize(&mut value_bytes);
        let value_len = value_bytes.len() as u32;

        // Write property entry header
        let entry = PropertyEntry::new(key_id, value.discriminant(), value_len, u64::MAX);
        let entry_bytes = entry.to_bytes();

        {
            let mut file = graph.file.write();
            // Write header
            file.seek(SeekFrom::Start(prop_arena_offset)).unwrap();
            file.write_all(&entry_bytes).unwrap();
            // Write value data
            file.write_all(&value_bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Load the property
        let result = graph.load_properties(prop_arena_offset);
        assert!(result.is_ok(), "Should load property successfully");
        let properties = result.unwrap();

        assert_eq!(properties.len(), 1, "Should have one property");
        assert_eq!(
            properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_load_properties_multiple_properties() {
        use crate::value::Value;
        use records::{PropertyEntry, PROPERTY_ENTRY_HEADER_SIZE};
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Setup: Intern strings for property keys
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("name"); // ID 0
            string_table.intern("age"); // ID 1
            string_table.intern("active"); // ID 2
        }

        // Get property arena offset
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);
        let prop_arena_offset = header.property_arena_offset;
        drop(mmap);

        // Create three properties: name, age, active
        let properties = vec![
            (0u32, Value::String("Bob".to_string())),
            (1u32, Value::Int(30)),
            (2u32, Value::Bool(true)),
        ];

        let mut current_offset = prop_arena_offset;
        let mut file = graph.file.write();

        for (i, (key_id, value)) in properties.iter().enumerate() {
            // Serialize value
            let mut value_bytes = Vec::new();
            value.serialize(&mut value_bytes);
            let value_len = value_bytes.len() as u32;

            // Determine next offset or u64::MAX if last
            let next = if i < properties.len() - 1 {
                current_offset + PROPERTY_ENTRY_HEADER_SIZE as u64 + value_len as u64
            } else {
                u64::MAX
            };

            // Write property entry
            let entry = PropertyEntry::new(*key_id, value.discriminant(), value_len, next);
            let entry_bytes = entry.to_bytes();

            file.seek(SeekFrom::Start(current_offset)).unwrap();
            file.write_all(&entry_bytes).unwrap();
            file.write_all(&value_bytes).unwrap();

            current_offset = next;
        }

        file.sync_data().unwrap();
        drop(file);

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Load all properties
        let result = graph.load_properties(prop_arena_offset);
        assert!(result.is_ok(), "Should load all properties successfully");
        let loaded = result.unwrap();

        assert_eq!(loaded.len(), 3, "Should have three properties");
        assert_eq!(loaded.get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(loaded.get("age"), Some(&Value::Int(30)));
        assert_eq!(loaded.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_load_properties_all_value_types() {
        use crate::value::{EdgeId as ValueEdgeId, Value, VertexId as ValueVertexId};
        use records::PropertyEntry;
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Setup: Intern property keys
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("null_prop"); // ID 0
            string_table.intern("bool_prop"); // ID 1
            string_table.intern("int_prop"); // ID 2
            string_table.intern("float_prop"); // ID 3
            string_table.intern("string_prop"); // ID 4
            string_table.intern("list_prop"); // ID 5
            string_table.intern("map_prop"); // ID 6
            string_table.intern("vertex_prop"); // ID 7
            string_table.intern("edge_prop"); // ID 8
        }

        // Get property arena offset
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);
        let prop_arena_offset = header.property_arena_offset;
        drop(mmap);

        // Create properties with all value types
        let mut map = std::collections::HashMap::new();
        map.insert("key".to_string(), Value::Int(42));

        let properties = vec![
            (0u32, Value::Null),
            (1u32, Value::Bool(false)),
            (2u32, Value::Int(-123)),
            (3u32, Value::Float(3.14159)),
            (4u32, Value::String("test".to_string())),
            (
                5u32,
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
            (6u32, Value::Map(map)),
            (7u32, Value::Vertex(ValueVertexId(999))),
            (8u32, Value::Edge(ValueEdgeId(888))),
        ];

        let mut current_offset = prop_arena_offset;
        let mut file = graph.file.write();

        for (i, (key_id, value)) in properties.iter().enumerate() {
            // Serialize value
            let mut value_bytes = Vec::new();
            value.serialize(&mut value_bytes);
            let value_len = value_bytes.len() as u32;

            // Determine next offset
            let next = if i < properties.len() - 1 {
                current_offset + records::PROPERTY_ENTRY_HEADER_SIZE as u64 + value_len as u64
            } else {
                u64::MAX
            };

            // Write property entry
            let entry = PropertyEntry::new(*key_id, value.discriminant(), value_len, next);
            let entry_bytes = entry.to_bytes();

            file.seek(SeekFrom::Start(current_offset)).unwrap();
            file.write_all(&entry_bytes).unwrap();
            file.write_all(&value_bytes).unwrap();

            current_offset = next;
        }

        file.sync_data().unwrap();
        drop(file);

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Load all properties
        let result = graph.load_properties(prop_arena_offset);
        assert!(result.is_ok(), "Should load all property types");
        let loaded = result.unwrap();

        assert_eq!(loaded.len(), 9, "Should have all nine properties");
        assert_eq!(loaded.get("null_prop"), Some(&Value::Null));
        assert_eq!(loaded.get("bool_prop"), Some(&Value::Bool(false)));
        assert_eq!(loaded.get("int_prop"), Some(&Value::Int(-123)));
        assert_eq!(loaded.get("float_prop"), Some(&Value::Float(3.14159)));
        assert_eq!(
            loaded.get("string_prop"),
            Some(&Value::String("test".to_string()))
        );
        assert!(matches!(loaded.get("list_prop"), Some(Value::List(_))));
        assert!(matches!(loaded.get("map_prop"), Some(Value::Map(_))));
        assert_eq!(
            loaded.get("vertex_prop"),
            Some(&Value::Vertex(ValueVertexId(999)))
        );
        assert_eq!(
            loaded.get("edge_prop"),
            Some(&Value::Edge(ValueEdgeId(888)))
        );
    }

    #[test]
    fn test_load_properties_corrupted_offset() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get file size
        let mmap = graph.mmap.read();
        let file_size = mmap.len();
        drop(mmap);

        // Try to load properties at an out-of-bounds offset
        let result = graph.load_properties(file_size as u64 + 1000);
        assert!(result.is_err(), "Should fail on out-of-bounds offset");
        assert!(
            matches!(result, Err(StorageError::CorruptedData)),
            "Should return CorruptedData error"
        );
    }

    #[test]
    fn test_load_properties_invalid_string_id() {
        use crate::value::Value;
        use records::PropertyEntry;
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get property arena offset
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);
        let prop_arena_offset = header.property_arena_offset;
        drop(mmap);

        // Create a property with an invalid key_id (not in string table)
        let invalid_key_id = 9999u32;
        let value = Value::String("test".to_string());
        let mut value_bytes = Vec::new();
        value.serialize(&mut value_bytes);
        let value_len = value_bytes.len() as u32;

        let entry = PropertyEntry::new(invalid_key_id, value.discriminant(), value_len, u64::MAX);
        let entry_bytes = entry.to_bytes();

        {
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(prop_arena_offset)).unwrap();
            file.write_all(&entry_bytes).unwrap();
            file.write_all(&value_bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Try to load the property with invalid key
        let result = graph.load_properties(prop_arena_offset);
        assert!(result.is_err(), "Should fail on invalid string ID");
        assert!(
            matches!(result, Err(StorageError::CorruptedData)),
            "Should return CorruptedData error"
        );
    }

    #[test]
    fn test_load_properties_truncated_value_data() {
        use records::PropertyEntry;
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Setup: Intern a property key
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("test"); // ID 0
        }

        // Get file size
        let mmap = graph.mmap.read();
        let file_size = mmap.len();
        drop(mmap);

        // Write property entry near the end of the file so that the claimed
        // value_len extends beyond the file boundary. This tests the bounds
        // check in load_properties().
        //
        // Property entry header is 17 bytes. We position it so that:
        // - The header fits in the file
        // - The claimed value data (1000 bytes) would extend beyond file end
        let entry_offset = file_size - 20; // Just enough room for header (17 bytes)
        let key_id = 0u32;
        let value_len = 1000u32; // Claims 1000 bytes which won't fit
        let entry = PropertyEntry::new(key_id, 0x05 /* String */, value_len, u64::MAX);
        let entry_bytes = entry.to_bytes();

        {
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(entry_offset as u64)).unwrap();
            file.write_all(&entry_bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap (file size unchanged, we just overwrote existing space)
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Try to load the property - value data extends beyond file bounds
        let result = graph.load_properties(entry_offset as u64);
        assert!(result.is_err(), "Should fail when value data is truncated");
        assert!(
            matches!(result, Err(StorageError::CorruptedData)),
            "Should return CorruptedData error"
        );
    }

    // =========================================================================
    // Phase 2.5: Index Rebuilding Tests
    // =========================================================================

    #[test]
    fn test_rebuild_indexes_empty_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Rebuild indexes on empty database
        let result = graph.rebuild_indexes();
        assert!(result.is_ok(), "Should rebuild indexes successfully");

        // Verify indexes are empty (node_count and edge_count are 0)
        let vertex_labels = graph.vertex_labels.read();
        assert!(vertex_labels.is_empty(), "Vertex labels should be empty");

        let edge_labels = graph.edge_labels.read();
        assert!(edge_labels.is_empty(), "Edge labels should be empty");
    }

    #[test]
    fn test_rebuild_indexes_with_nodes() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write several node records with different labels
        let nodes = vec![
            (VertexId(0), 10u32), // label_id 10
            (VertexId(1), 10u32), // label_id 10
            (VertexId(2), 20u32), // label_id 20
            (VertexId(3), 10u32), // label_id 10
            (VertexId(4), 30u32), // label_id 30
        ];

        for (node_id, label_id) in &nodes {
            let record = records::NodeRecord::new(node_id.0, *label_id);
            let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Update header to reflect node_count
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = nodes.len() as u64;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
            drop(file);

            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        // Rebuild indexes
        let result = graph.rebuild_indexes();
        assert!(result.is_ok(), "Should rebuild indexes successfully");

        // Verify vertex labels index
        let vertex_labels = graph.vertex_labels.read();
        assert_eq!(vertex_labels.len(), 3, "Should have 3 different labels");

        // Check label 10 has nodes 0, 1, 3
        let label_10 = vertex_labels.get(&10).unwrap();
        assert_eq!(label_10.len(), 3);
        assert!(label_10.contains(0));
        assert!(label_10.contains(1));
        assert!(label_10.contains(3));

        // Check label 20 has node 2
        let label_20 = vertex_labels.get(&20).unwrap();
        assert_eq!(label_20.len(), 1);
        assert!(label_20.contains(2));

        // Check label 30 has node 4
        let label_30 = vertex_labels.get(&30).unwrap();
        assert_eq!(label_30.len(), 1);
        assert!(label_30.contains(4));
    }

    #[test]
    fn test_rebuild_indexes_excludes_deleted_nodes() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write node records, some deleted
        let nodes = vec![
            (VertexId(0), 10u32, false), // not deleted
            (VertexId(1), 10u32, true),  // deleted
            (VertexId(2), 10u32, false), // not deleted
            (VertexId(3), 20u32, true),  // deleted
            (VertexId(4), 20u32, false), // not deleted
        ];

        for (node_id, label_id, deleted) in &nodes {
            let mut record = records::NodeRecord::new(node_id.0, *label_id);
            if *deleted {
                record.mark_deleted();
            }

            let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Update header to reflect node_count
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = nodes.len() as u64;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
            drop(file);

            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        // Rebuild indexes
        let result = graph.rebuild_indexes();
        assert!(result.is_ok(), "Should rebuild indexes successfully");

        // Verify only non-deleted nodes are in the index
        let vertex_labels = graph.vertex_labels.read();

        // Label 10 should have nodes 0 and 2 (not 1, which is deleted)
        let label_10 = vertex_labels.get(&10).unwrap();
        assert_eq!(label_10.len(), 2);
        assert!(label_10.contains(0));
        assert!(label_10.contains(2));
        assert!(!label_10.contains(1), "Deleted node should not be in index");

        // Label 20 should have node 4 (not 3, which is deleted)
        let label_20 = vertex_labels.get(&20).unwrap();
        assert_eq!(label_20.len(), 1);
        assert!(label_20.contains(4));
        assert!(!label_20.contains(3), "Deleted node should not be in index");
    }

    #[test]
    fn test_rebuild_indexes_with_edges() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);

        // Write several edge records with different labels
        let edges = vec![
            (EdgeId(0), 100u32), // label_id 100
            (EdgeId(1), 100u32), // label_id 100
            (EdgeId(2), 200u32), // label_id 200
            (EdgeId(3), 100u32), // label_id 100
            (EdgeId(4), 300u32), // label_id 300
        ];

        for (edge_id, label_id) in &edges {
            let record = records::EdgeRecord::new(edge_id.0, *label_id, 0, 0);
            let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Update header to reflect edge_count
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.edge_count = edges.len() as u64;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
            drop(file);

            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        // Rebuild indexes
        let result = graph.rebuild_indexes();
        assert!(result.is_ok(), "Should rebuild indexes successfully");

        // Verify edge labels index
        let edge_labels = graph.edge_labels.read();
        assert_eq!(edge_labels.len(), 3, "Should have 3 different labels");

        // Check label 100 has edges 0, 1, 3
        let label_100 = edge_labels.get(&100).unwrap();
        assert_eq!(label_100.len(), 3);
        assert!(label_100.contains(0));
        assert!(label_100.contains(1));
        assert!(label_100.contains(3));

        // Check label 200 has edge 2
        let label_200 = edge_labels.get(&200).unwrap();
        assert_eq!(label_200.len(), 1);
        assert!(label_200.contains(2));

        // Check label 300 has edge 4
        let label_300 = edge_labels.get(&300).unwrap();
        assert_eq!(label_300.len(), 1);
        assert!(label_300.contains(4));
    }

    #[test]
    fn test_rebuild_indexes_excludes_deleted_edges() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);

        // Write edge records, some deleted
        let edges = vec![
            (EdgeId(0), 100u32, false), // not deleted
            (EdgeId(1), 100u32, true),  // deleted
            (EdgeId(2), 100u32, false), // not deleted
            (EdgeId(3), 200u32, true),  // deleted
            (EdgeId(4), 200u32, false), // not deleted
        ];

        for (edge_id, label_id, deleted) in &edges {
            let mut record = records::EdgeRecord::new(edge_id.0, *label_id, 0, 0);
            if *deleted {
                record.mark_deleted();
            }

            let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Update header to reflect edge_count
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.edge_count = edges.len() as u64;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
            drop(file);

            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        // Rebuild indexes
        let result = graph.rebuild_indexes();
        assert!(result.is_ok(), "Should rebuild indexes successfully");

        // Verify only non-deleted edges are in the index
        let edge_labels = graph.edge_labels.read();

        // Label 100 should have edges 0 and 2 (not 1, which is deleted)
        let label_100 = edge_labels.get(&100).unwrap();
        assert_eq!(label_100.len(), 2);
        assert!(label_100.contains(0));
        assert!(label_100.contains(2));
        assert!(
            !label_100.contains(1),
            "Deleted edge should not be in index"
        );

        // Label 200 should have edge 4 (not 3, which is deleted)
        let label_200 = edge_labels.get(&200).unwrap();
        assert_eq!(label_200.len(), 1);
        assert!(label_200.contains(4));
        assert!(
            !label_200.contains(3),
            "Deleted edge should not be in index"
        );
    }

    #[test]
    fn test_rebuild_indexes_on_reopen() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create database and write some nodes
        {
            let graph = MmapGraph::open(&path).unwrap();

            let nodes = vec![
                (VertexId(0), 10u32),
                (VertexId(1), 10u32),
                (VertexId(2), 20u32),
            ];

            for (node_id, label_id) in &nodes {
                let record = records::NodeRecord::new(node_id.0, *label_id);
                let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
                let bytes = record.to_bytes();

                let mut file = graph.file.write();
                file.seek(SeekFrom::Start(offset as u64)).unwrap();
                file.write_all(&bytes).unwrap();
                file.sync_data().unwrap();
                drop(file);

                // Remap
                let file = graph.file.read();
                let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
                *graph.mmap.write() = new_mmap;
                drop(file);
            }

            // Update header to reflect node_count
            {
                let mmap = graph.mmap.read();
                let mut header = MmapGraph::read_header(&mmap);
                header.node_count = nodes.len() as u64;
                drop(mmap);

                let mut file = graph.file.write();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&header.to_bytes()).unwrap();
                file.sync_data().unwrap();
                drop(file);

                let file = graph.file.read();
                let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
                *graph.mmap.write() = new_mmap;
            }

            // First rebuild
            graph.rebuild_indexes().unwrap();

            // Verify indexes
            let vertex_labels = graph.vertex_labels.read();
            assert_eq!(vertex_labels.len(), 2);
        } // Drop graph to close

        // Reopen database - indexes should be rebuilt automatically
        let graph = MmapGraph::open(&path).unwrap();

        // Verify indexes are rebuilt correctly on open
        {
            let vertex_labels = graph.vertex_labels.read();
            assert_eq!(vertex_labels.len(), 2, "Should have 2 different labels");

            let label_10 = vertex_labels.get(&10).unwrap();
            assert_eq!(label_10.len(), 2);
            assert!(label_10.contains(0));
            assert!(label_10.contains(1));

            let label_20 = vertex_labels.get(&20).unwrap();
            assert_eq!(label_20.len(), 1);
            assert!(label_20.contains(2));
        }
    }

    // =========================================================================
    // Phase 2.6: GraphStorage Trait Implementation Tests
    // =========================================================================

    /// Helper to create a test graph with nodes and edges written to disk.
    /// Returns the graph and vectors of (vertex_id, label_id) and edge data.
    fn setup_test_graph_with_data() -> (TempDir, MmapGraph) {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Intern labels in string table
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("person"); // ID 0
            string_table.intern("software"); // ID 1
            string_table.intern("knows"); // ID 2
            string_table.intern("created"); // ID 3
        }

        // Write node records
        // Node 0: person
        // Node 1: person
        // Node 2: software
        let nodes = vec![
            (VertexId(0), 0u32), // label_id 0 = "person"
            (VertexId(1), 0u32), // label_id 0 = "person"
            (VertexId(2), 1u32), // label_id 1 = "software"
        ];

        for (node_id, label_id) in &nodes {
            let record = records::NodeRecord::new(node_id.0, *label_id);
            let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Update header to reflect node_count
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = nodes.len() as u64;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        {
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        // Rebuild indexes
        graph.rebuild_indexes().unwrap();

        (dir, graph)
    }

    #[test]
    fn test_graph_storage_get_vertex() {
        let (_dir, graph) = setup_test_graph_with_data();

        // Test getting existing vertices
        let v0 = graph.get_vertex(VertexId(0));
        assert!(v0.is_some(), "Vertex 0 should exist");
        let v0 = v0.unwrap();
        assert_eq!(v0.id, VertexId(0));
        assert_eq!(v0.label, "person");

        let v2 = graph.get_vertex(VertexId(2));
        assert!(v2.is_some(), "Vertex 2 should exist");
        let v2 = v2.unwrap();
        assert_eq!(v2.id, VertexId(2));
        assert_eq!(v2.label, "software");

        // Test getting non-existent vertex
        let v999 = graph.get_vertex(VertexId(999));
        assert!(v999.is_none(), "Vertex 999 should not exist");
    }

    #[test]
    fn test_graph_storage_vertex_count() {
        let (_dir, graph) = setup_test_graph_with_data();
        assert_eq!(graph.vertex_count(), 3, "Should have 3 vertices");
    }

    #[test]
    fn test_graph_storage_edge_count_empty() {
        let (_dir, graph) = setup_test_graph_with_data();
        assert_eq!(graph.edge_count(), 0, "Should have 0 edges initially");
    }

    #[test]
    fn test_graph_storage_vertices_with_label() {
        let (_dir, graph) = setup_test_graph_with_data();

        // Get vertices with label "person"
        let people: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(people.len(), 2, "Should have 2 people");
        assert!(people.iter().all(|v| v.label == "person"));

        // Get vertices with label "software"
        let software: Vec<_> = graph.vertices_with_label("software").collect();
        assert_eq!(software.len(), 1, "Should have 1 software");
        assert_eq!(software[0].label, "software");

        // Get vertices with non-existent label
        let unknown: Vec<_> = graph.vertices_with_label("unknown").collect();
        assert_eq!(unknown.len(), 0, "Should have 0 unknown vertices");
    }

    #[test]
    fn test_graph_storage_all_vertices() {
        let (_dir, graph) = setup_test_graph_with_data();

        let all: Vec<_> = graph.all_vertices().collect();
        assert_eq!(all.len(), 3, "Should iterate over all 3 vertices");

        // Check all expected IDs are present
        let ids: Vec<_> = all.iter().map(|v| v.id).collect();
        assert!(ids.contains(&VertexId(0)));
        assert!(ids.contains(&VertexId(1)));
        assert!(ids.contains(&VertexId(2)));
    }

    #[test]
    fn test_graph_storage_interner() {
        let (_dir, graph) = setup_test_graph_with_data();

        let interner = graph.interner();

        // Check that we can resolve interned strings
        assert_eq!(interner.resolve(0), Some("person"));
        assert_eq!(interner.resolve(1), Some("software"));
        assert_eq!(interner.resolve(2), Some("knows"));
        assert_eq!(interner.resolve(3), Some("created"));

        // Check lookup works
        assert_eq!(interner.lookup("person"), Some(0));
        assert_eq!(interner.lookup("software"), Some(1));
        assert_eq!(interner.lookup("unknown"), None);
    }

    #[test]
    fn test_graph_storage_get_vertex_with_properties() {
        use crate::value::Value;
        use records::PropertyEntry;
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Intern strings
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("person"); // ID 0
            string_table.intern("name"); // ID 1
            string_table.intern("age"); // ID 2
        }

        // Get property arena offset
        let prop_arena_offset = {
            let mmap = graph.mmap.read();
            let header = MmapGraph::read_header(&mmap);
            header.property_arena_offset
        };

        // Write properties: name="Alice", age=30
        let name_value = Value::String("Alice".to_string());
        let mut name_bytes = Vec::new();
        name_value.serialize(&mut name_bytes);

        let age_value = Value::Int(30);
        let mut age_bytes = Vec::new();
        age_value.serialize(&mut age_bytes);

        // Calculate offsets
        let name_entry_offset = prop_arena_offset;
        let age_entry_offset = name_entry_offset
            + records::PROPERTY_ENTRY_HEADER_SIZE as u64
            + name_bytes.len() as u64;

        // Write name property (points to age)
        let name_entry = PropertyEntry::new(
            1, // key_id for "name"
            name_value.discriminant(),
            name_bytes.len() as u32,
            age_entry_offset, // next points to age
        );

        // Write age property (end of list)
        let age_entry = PropertyEntry::new(
            2, // key_id for "age"
            age_value.discriminant(),
            age_bytes.len() as u32,
            u64::MAX, // end of list
        );

        {
            let mut file = graph.file.write();

            // Write name entry
            file.seek(SeekFrom::Start(name_entry_offset)).unwrap();
            file.write_all(&name_entry.to_bytes()).unwrap();
            file.write_all(&name_bytes).unwrap();

            // Write age entry
            file.seek(SeekFrom::Start(age_entry_offset)).unwrap();
            file.write_all(&age_entry.to_bytes()).unwrap();
            file.write_all(&age_bytes).unwrap();

            file.sync_data().unwrap();
        }

        // Write node record with properties
        let mut node_record = records::NodeRecord::new(0, 0); // label_id 0 = "person"
        node_record.prop_head = prop_arena_offset;

        {
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
            file.write_all(&node_record.to_bytes()).unwrap();
            file.sync_data().unwrap();
        }

        // Update header
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = 1;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        {
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        graph.rebuild_indexes().unwrap();

        // Now test get_vertex
        let vertex = graph.get_vertex(VertexId(0)).expect("Vertex should exist");
        assert_eq!(vertex.id, VertexId(0));
        assert_eq!(vertex.label, "person");
        assert_eq!(vertex.properties.len(), 2);
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
    }

    /// Helper to set up a graph with edges for testing adjacency traversal
    fn setup_graph_with_edges() -> (TempDir, MmapGraph) {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Intern labels
        {
            let mut string_table = graph.string_table.write();
            string_table.intern("person"); // ID 0
            string_table.intern("knows"); // ID 1
        }

        // Create 3 nodes: 0, 1, 2
        // Create edges:
        //   Edge 0: 0 -> 1 (knows), next_out=1, next_in=MAX
        //   Edge 1: 0 -> 2 (knows), next_out=MAX, next_in=MAX
        //   Edge 2: 1 -> 0 (knows), next_out=MAX, next_in=MAX

        // Node 0: first_out_edge=0, first_in_edge=2
        // Node 1: first_out_edge=2, first_in_edge=0
        // Node 2: first_out_edge=MAX, first_in_edge=1

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);

        // Write node records
        {
            let mut file = graph.file.write();

            // Node 0: person, first_out=0, first_in=2
            let mut node0 = records::NodeRecord::new(0, 0);
            node0.first_out_edge = 0;
            node0.first_in_edge = 2;
            file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
            file.write_all(&node0.to_bytes()).unwrap();

            // Node 1: person, first_out=2, first_in=0
            let mut node1 = records::NodeRecord::new(1, 0);
            node1.first_out_edge = 2;
            node1.first_in_edge = 0;
            file.seek(SeekFrom::Start((HEADER_SIZE + NODE_RECORD_SIZE) as u64))
                .unwrap();
            file.write_all(&node1.to_bytes()).unwrap();

            // Node 2: person, first_out=MAX, first_in=1
            let mut node2 = records::NodeRecord::new(2, 0);
            node2.first_out_edge = u64::MAX;
            node2.first_in_edge = 1;
            file.seek(SeekFrom::Start((HEADER_SIZE + 2 * NODE_RECORD_SIZE) as u64))
                .unwrap();
            file.write_all(&node2.to_bytes()).unwrap();

            file.sync_data().unwrap();
        }

        // Write edge records
        {
            let mut file = graph.file.write();

            // Edge 0: 0->1 (knows), next_out=1, next_in=MAX
            let mut edge0 = records::EdgeRecord::new(0, 1, 0, 1); // label_id=1="knows", src=0, dst=1
            edge0.next_out = 1;
            edge0.next_in = u64::MAX;
            file.seek(SeekFrom::Start(edge_table_offset as u64))
                .unwrap();
            file.write_all(&edge0.to_bytes()).unwrap();

            // Edge 1: 0->2 (knows), next_out=MAX, next_in=MAX
            let mut edge1 = records::EdgeRecord::new(1, 1, 0, 2);
            edge1.next_out = u64::MAX;
            edge1.next_in = u64::MAX;
            file.seek(SeekFrom::Start(
                (edge_table_offset + EDGE_RECORD_SIZE) as u64,
            ))
            .unwrap();
            file.write_all(&edge1.to_bytes()).unwrap();

            // Edge 2: 1->0 (knows), next_out=MAX, next_in=MAX
            let mut edge2 = records::EdgeRecord::new(2, 1, 1, 0);
            edge2.next_out = u64::MAX;
            edge2.next_in = u64::MAX;
            file.seek(SeekFrom::Start(
                (edge_table_offset + 2 * EDGE_RECORD_SIZE) as u64,
            ))
            .unwrap();
            file.write_all(&edge2.to_bytes()).unwrap();

            file.sync_data().unwrap();
        }

        // Update header
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = 3;
            header.edge_count = 3;
            drop(mmap);

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header.to_bytes()).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        {
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
        }

        graph.rebuild_indexes().unwrap();

        (dir, graph)
    }

    #[test]
    fn test_graph_storage_get_edge() {
        let (_dir, graph) = setup_graph_with_edges();

        let edge = graph.get_edge(EdgeId(0)).expect("Edge 0 should exist");
        assert_eq!(edge.id, EdgeId(0));
        assert_eq!(edge.label, "knows");
        assert_eq!(edge.src, VertexId(0));
        assert_eq!(edge.dst, VertexId(1));

        let edge2 = graph.get_edge(EdgeId(2)).expect("Edge 2 should exist");
        assert_eq!(edge2.src, VertexId(1));
        assert_eq!(edge2.dst, VertexId(0));

        // Non-existent edge
        assert!(graph.get_edge(EdgeId(999)).is_none());
    }

    #[test]
    fn test_graph_storage_edge_count() {
        let (_dir, graph) = setup_graph_with_edges();
        assert_eq!(graph.edge_count(), 3);
    }

    #[test]
    fn test_graph_storage_out_edges() {
        let (_dir, graph) = setup_graph_with_edges();

        // Node 0 has 2 outgoing edges (to nodes 1 and 2)
        let out_edges: Vec<_> = graph.out_edges(VertexId(0)).collect();
        assert_eq!(out_edges.len(), 2, "Node 0 should have 2 outgoing edges");
        assert!(out_edges.iter().all(|e| e.src == VertexId(0)));

        // Check destinations
        let dsts: Vec<_> = out_edges.iter().map(|e| e.dst).collect();
        assert!(dsts.contains(&VertexId(1)));
        assert!(dsts.contains(&VertexId(2)));

        // Node 1 has 1 outgoing edge (to node 0)
        let out_edges1: Vec<_> = graph.out_edges(VertexId(1)).collect();
        assert_eq!(out_edges1.len(), 1);
        assert_eq!(out_edges1[0].dst, VertexId(0));

        // Node 2 has no outgoing edges
        let out_edges2: Vec<_> = graph.out_edges(VertexId(2)).collect();
        assert_eq!(out_edges2.len(), 0);

        // Non-existent node returns empty iterator
        let out_edges999: Vec<_> = graph.out_edges(VertexId(999)).collect();
        assert_eq!(out_edges999.len(), 0);
    }

    #[test]
    fn test_graph_storage_in_edges() {
        let (_dir, graph) = setup_graph_with_edges();

        // Node 0 has 1 incoming edge (from node 1)
        let in_edges: Vec<_> = graph.in_edges(VertexId(0)).collect();
        assert_eq!(in_edges.len(), 1, "Node 0 should have 1 incoming edge");
        assert_eq!(in_edges[0].src, VertexId(1));
        assert_eq!(in_edges[0].dst, VertexId(0));

        // Node 1 has 1 incoming edge (from node 0)
        let in_edges1: Vec<_> = graph.in_edges(VertexId(1)).collect();
        assert_eq!(in_edges1.len(), 1);
        assert_eq!(in_edges1[0].src, VertexId(0));

        // Node 2 has 1 incoming edge (from node 0)
        let in_edges2: Vec<_> = graph.in_edges(VertexId(2)).collect();
        assert_eq!(in_edges2.len(), 1);
        assert_eq!(in_edges2[0].src, VertexId(0));

        // Non-existent node returns empty iterator
        let in_edges999: Vec<_> = graph.in_edges(VertexId(999)).collect();
        assert_eq!(in_edges999.len(), 0);
    }

    #[test]
    fn test_graph_storage_edges_with_label() {
        let (_dir, graph) = setup_graph_with_edges();

        // All 3 edges have label "knows"
        let knows_edges: Vec<_> = graph.edges_with_label("knows").collect();
        assert_eq!(knows_edges.len(), 3);
        assert!(knows_edges.iter().all(|e| e.label == "knows"));

        // No edges with label "created"
        let created_edges: Vec<_> = graph.edges_with_label("created").collect();
        assert_eq!(created_edges.len(), 0);
    }

    #[test]
    fn test_graph_storage_all_edges() {
        let (_dir, graph) = setup_graph_with_edges();

        let all: Vec<_> = graph.all_edges().collect();
        assert_eq!(all.len(), 3);

        let ids: Vec<_> = all.iter().map(|e| e.id).collect();
        assert!(ids.contains(&EdgeId(0)));
        assert!(ids.contains(&EdgeId(1)));
        assert!(ids.contains(&EdgeId(2)));
    }

    #[test]
    fn test_graph_storage_empty_graph() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Empty graph
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);

        // All iterators should be empty
        assert_eq!(graph.all_vertices().count(), 0);
        assert_eq!(graph.all_edges().count(), 0);
        assert_eq!(graph.vertices_with_label("person").count(), 0);
        assert_eq!(graph.edges_with_label("knows").count(), 0);
        assert_eq!(graph.out_edges(VertexId(0)).count(), 0);
        assert_eq!(graph.in_edges(VertexId(0)).count(), 0);

        // get_vertex and get_edge should return None
        assert!(graph.get_vertex(VertexId(0)).is_none());
        assert!(graph.get_edge(EdgeId(0)).is_none());
    }
}
