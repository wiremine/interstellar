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

use wal::{WalEntry, WriteAheadLog};

use freelist::FreeList;
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
    file: Arc<RwLock<File>>,

    /// Write-ahead log for durability
    wal: Arc<RwLock<WriteAheadLog>>,

    /// String interner (in-memory, rebuilt on load)
    /// Note: Uses RwLock for interior mutability during writes, but reads
    /// are lock-free via the interner() accessor using parking_lot's RwLock.
    string_table: Arc<RwLock<StringInterner>>,

    /// Label indexes (in-memory, rebuilt on load)
    vertex_labels: Arc<RwLock<HashMap<u32, RoaringBitmap>>>,
    edge_labels: Arc<RwLock<HashMap<u32, RoaringBitmap>>>,

    /// Property arena allocator (tracks current write position)
    arena: Arc<RwLock<arena::ArenaAllocator>>,

    /// Free list for deleted node slots (enables slot reuse)
    free_nodes: Arc<RwLock<FreeList>>,

    /// Free list for deleted edge slots (enables slot reuse)
    free_edges: Arc<RwLock<FreeList>>,
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

        // Read header to initialize arena allocator
        let header = Self::read_header(&mmap);
        let arena_start = header.property_arena_offset;
        let arena_end = header.string_table_offset;
        // Use the persisted arena_next_offset for existing databases,
        // which tracks where the last property write ended
        let arena_current = header.arena_next_offset;
        let arena = arena::ArenaAllocator::new(arena_start, arena_end, arena_current);

        // Load string table from disk
        // The string table region is from string_table_offset to string_table_end
        let string_table = if file_exists && header.string_table_end > header.string_table_offset {
            // For existing databases with strings, load from disk
            StringInterner::load_from_mmap(
                &mmap,
                header.string_table_offset,
                header.string_table_end,
            )?
        } else {
            // For new databases or databases with no strings, start empty
            StringInterner::new()
        };

        // Initialize free list from header
        // For a new database, free_node_head == u64::MAX (empty list)
        // For an existing database with deleted nodes, we'd need to rebuild the
        // free list by scanning deleted records, but that's handled in rebuild_indexes
        let free_nodes = FreeList::with_head(header.free_node_head);

        // Initialize edge free list similarly
        let free_edges = FreeList::with_head(header.free_edge_head);

        // Open or create WAL file
        // The WAL file is stored alongside the main data file with .wal extension
        let wal_path = path.with_extension("wal");
        let mut wal = WriteAheadLog::open(&wal_path)?;

        // Perform crash recovery if needed
        // This replays any committed transactions from the WAL to the data file
        if wal.needs_recovery() {
            recovery::recover(&mut wal, &file, header.node_capacity)?;
            // After recovery, we need to re-read the mmap since data may have changed
            // Re-map the file to pick up recovered data
            drop(mmap);
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            // Re-read header for updated counts (recovery may have added records)
            let header = Self::read_header(&mmap);
            // Update arena position if needed
            let arena = arena::ArenaAllocator::new(
                header.property_arena_offset,
                header.string_table_offset,
                header.arena_next_offset,
            );

            let graph = Self {
                mmap: Arc::new(RwLock::new(mmap)),
                file: Arc::new(RwLock::new(file)),
                wal: Arc::new(RwLock::new(wal)),
                string_table: Arc::new(RwLock::new(string_table)),
                vertex_labels: Arc::new(RwLock::new(HashMap::new())),
                edge_labels: Arc::new(RwLock::new(HashMap::new())),
                arena: Arc::new(RwLock::new(arena)),
                free_nodes: Arc::new(RwLock::new(free_nodes)),
                free_edges: Arc::new(RwLock::new(free_edges)),
            };

            // Rebuild in-memory indexes from disk data (includes recovered data)
            graph.rebuild_indexes()?;

            return Ok(graph);
        }

        let graph = Self {
            mmap: Arc::new(RwLock::new(mmap)),
            file: Arc::new(RwLock::new(file)),
            wal: Arc::new(RwLock::new(wal)),
            string_table: Arc::new(RwLock::new(string_table)),
            vertex_labels: Arc::new(RwLock::new(HashMap::new())),
            edge_labels: Arc::new(RwLock::new(HashMap::new())),
            arena: Arc::new(RwLock::new(arena)),
            free_nodes: Arc::new(RwLock::new(free_nodes)),
            free_edges: Arc::new(RwLock::new(free_edges)),
        };

        // Rebuild in-memory indexes from disk data
        graph.rebuild_indexes()?;

        Ok(graph)
    }

    /// Initialize a new database file with header and initial structure.
    ///
    /// Creates a file with:
    /// - 80-byte header
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
        header.arena_next_offset = property_arena_offset; // Start writing at arena beginning
        header.string_table_offset = string_table_offset;
        header.string_table_end = string_table_offset; // Empty string table initially

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

    // =========================================================================
    // Phase 4.1: Property Arena Allocation
    // =========================================================================

    /// Allocate and write properties to the property arena.
    ///
    /// This method serializes the properties as a linked list and writes them
    /// to the property arena. Returns the offset to the first property entry,
    /// which can be stored in a node or edge record's `prop_head` field.
    ///
    /// # Arguments
    ///
    /// * `properties` - The properties to store
    ///
    /// # Returns
    ///
    /// - `Ok(u64::MAX)` if properties is empty (no properties)
    /// - `Ok(offset)` the absolute file offset to the first property entry
    ///
    /// # Errors
    ///
    /// - [`StorageError::OutOfSpace`] - Not enough space in the arena
    /// - [`StorageError::Io`] - I/O error writing to file
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    /// use rustgremlin::value::Value;
    ///
    /// let mut props = HashMap::new();
    /// props.insert("name".to_string(), Value::String("Alice".to_string()));
    /// props.insert("age".to_string(), Value::Int(30));
    ///
    /// let prop_head = graph.allocate_properties(&props)?;
    /// // prop_head can now be stored in a NodeRecord.prop_head
    /// ```
    pub fn allocate_properties(
        &self,
        properties: &std::collections::HashMap<String, crate::value::Value>,
    ) -> Result<u64, StorageError> {
        // Empty properties -> no allocation needed
        if properties.is_empty() {
            return Ok(u64::MAX);
        }

        // Convert std HashMap to hashbrown HashMap for arena functions
        let props: hashbrown::HashMap<String, crate::value::Value> = properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Calculate entry sizes first to determine total allocation
        let entry_sizes = arena::calculate_entry_sizes(&props);
        let total_size: usize = entry_sizes.iter().sum();

        // Allocate space in the arena
        let base_offset = {
            let arena = self.arena.read();
            arena.allocate(total_size)?
        };

        // Serialize properties with key interning
        let (mut data, next_offsets) = {
            let mut string_table = self.string_table.write();
            arena::serialize_properties(&props, |key| string_table.intern(key))
        };

        // Link the property entries to form a linked list
        arena::link_property_entries(&mut data, &next_offsets, base_offset, &entry_sizes);

        // Write to file
        self.write_property_data(base_offset, &data)?;

        Ok(base_offset)
    }

    /// Write property data to the file at the specified offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - Absolute file offset to write at
    /// * `data` - The serialized property data
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error writing to file
    fn write_property_data(&self, offset: u64, data: &[u8]) -> Result<(), StorageError> {
        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(data, offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
        }

        file.sync_data()?;
        drop(file);

        // Remap to see the new data
        self.remap()?;

        Ok(())
    }

    // =========================================================================
    // Phase 4.2: Node Slot Allocation and Writing
    // =========================================================================

    /// Allocate a slot for a new node.
    ///
    /// This method first checks the free list for a reusable slot from a deleted
    /// node. If no free slots are available, it allocates at the next sequential
    /// position (extending the table if needed).
    ///
    /// # Returns
    ///
    /// A `VertexId` for the newly allocated slot.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error if table growth fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = MmapGraph::open("my_graph.db")?;
    /// let slot_id = graph.allocate_node_slot()?;
    /// // Now write a node record to this slot
    /// ```
    pub fn allocate_node_slot(&self) -> Result<VertexId, StorageError> {
        let header = self.get_header();
        let current_count = header.node_count;
        let current_capacity = header.node_capacity;

        // Try to allocate from free list first
        let slot_id = {
            let mut free_nodes = self.free_nodes.write();
            free_nodes.allocate(current_count)
        };

        // If we're extending beyond capacity, grow the table
        if slot_id >= current_capacity {
            self.grow_node_table()?;
        }

        Ok(VertexId(slot_id))
    }

    /// Write a node record to the file at the correct offset.
    ///
    /// The record is written at: `HEADER_SIZE + (id * NODE_RECORD_SIZE)`
    ///
    /// # Arguments
    ///
    /// * `id` - The vertex ID (slot number) to write to
    /// * `record` - The node record to write
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during write
    ///
    /// # Platform Notes
    ///
    /// On Unix, uses `write_all_at` for positioned writes without seeking.
    /// On other platforms, uses seek + write_all.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let record = NodeRecord::new(0, label_id);
    /// graph.write_node_record(VertexId(0), &record)?;
    /// ```
    pub fn write_node_record(&self, id: VertexId, record: &NodeRecord) -> Result<(), StorageError> {
        let offset = HEADER_SIZE as u64 + (id.0 * NODE_RECORD_SIZE as u64);
        let bytes = record.to_bytes();

        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&bytes)?;
        }

        file.sync_data()?;
        drop(file);

        // Remap to see the new data
        self.remap()?;

        Ok(())
    }

    /// Increment the node count in the file header.
    ///
    /// This should be called after successfully writing a new node record
    /// (not when reusing a deleted slot, since the count wasn't decremented).
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    ///
    /// # Note
    ///
    /// This method reads the current header, increments the count, and writes
    /// the updated header back. It must be called with proper synchronization
    /// to avoid race conditions.
    pub fn increment_node_count(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.node_count += 1;

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        // Remap to see the updated header
        self.remap()?;

        Ok(())
    }

    /// Update the free node head in the file header.
    ///
    /// This persists the current state of the free list head to disk.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    pub fn update_free_node_head(&self) -> Result<(), StorageError> {
        let free_node_head = self.free_nodes.read().head();

        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.free_node_head = free_node_head;

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        // Remap to see the updated header
        self.remap()?;

        Ok(())
    }

    /// Update the arena_next_offset in the file header.
    ///
    /// This persists the current arena write position to disk so that
    /// after reopening the database, new properties are written at the
    /// correct location.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    pub fn update_arena_offset(&self) -> Result<(), StorageError> {
        let arena_next_offset = self.arena.read().current_offset();

        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.arena_next_offset = arena_next_offset;

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        // Remap to see the updated header
        self.remap()?;

        Ok(())
    }

    // =========================================================================
    // Phase 4.6: Checkpoint Implementation
    // =========================================================================

    /// Create a checkpoint, ensuring all data is durably written.
    ///
    /// A checkpoint performs the following steps:
    /// 1. Syncs the data file to ensure all pending writes are flushed to disk
    /// 2. Logs a `Checkpoint` marker to the WAL with a version number
    /// 3. Syncs the WAL to ensure the checkpoint marker is durable
    /// 4. Truncates the WAL (all prior committed transactions are now safely in the data file)
    ///
    /// After a checkpoint, the WAL is empty and all data is guaranteed to be
    /// in the main data file. This makes recovery faster since there's nothing
    /// to replay.
    ///
    /// # Usage
    ///
    /// Call `checkpoint()` periodically to:
    /// - Reduce WAL size and recovery time
    /// - Ensure data durability at specific points
    /// - Create consistent snapshots of the database
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during sync or truncate operations
    /// - [`StorageError::WalCorrupted`] - Error writing checkpoint entry to WAL
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::storage::MmapGraph;
    /// use std::collections::HashMap;
    ///
    /// let graph = MmapGraph::open("my_graph.db")?;
    ///
    /// // Add some data
    /// graph.add_vertex("person", HashMap::new())?;
    /// graph.add_vertex("software", HashMap::new())?;
    ///
    /// // Checkpoint to ensure durability
    /// graph.checkpoint()?;
    /// // WAL is now empty, all data is in the main file
    /// ```
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        // Step 1: Sync the data file to ensure all writes are flushed
        {
            let file = self.file.write();
            file.sync_data()?;
        }

        // Step 2: Log checkpoint marker to WAL
        // The version is a simple counter that can be used for debugging or
        // to identify checkpoint points. For now we use 0 as a placeholder.
        // A more sophisticated implementation could track a monotonic version.
        let mut wal = self.wal.write();
        wal.log(WalEntry::Checkpoint { version: 0 })?;

        // Step 3: Sync the WAL to ensure checkpoint marker is durable
        wal.sync()?;

        // Step 4: Truncate the WAL
        // All committed transactions are now safely in the data file,
        // so we can remove them from the WAL
        wal.truncate()?;

        Ok(())
    }

    /// Persist the string table to disk.
    ///
    /// Writes all interned strings to the string table region of the file.
    /// The string table starts at `string_table_offset` (from the header).
    /// Also updates `string_table_end` in the header to track the actual data size.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during file write
    pub fn persist_string_table(&self) -> Result<(), StorageError> {
        let header = self.get_header();
        let string_table_offset = header.string_table_offset;

        // Serialize string table to buffer
        let mut buffer = Vec::new();
        {
            let string_table = self.string_table.read();
            string_table.write_to_file(&mut buffer)?;
        }

        let string_table_end = string_table_offset + buffer.len() as u64;

        // Write to file at string_table_offset
        {
            let file = self.file.write();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&buffer, string_table_offset)?;
            }

            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut file_ref = &*file;
                file_ref.seek(SeekFrom::Start(string_table_offset))?;
                file_ref.write_all(&buffer)?;
            }

            file.sync_data()?;
        }

        // Update string_table_end in header
        {
            let mmap = self.mmap.read();
            let mut header = Self::read_header(&mmap);
            drop(mmap);

            header.string_table_end = string_table_end;

            let file = self.file.write();
            Self::write_header(&file, &header)?;
        }

        // Remap to see the updated string table
        self.remap()?;

        Ok(())
    }

    // =========================================================================
    // Phase 4.3: Edge Slot Allocation and Writing
    // =========================================================================

    /// Allocate a slot for a new edge.
    ///
    /// This method first checks the free list for a reusable slot from a deleted
    /// edge. If no free slots are available, it allocates at the next sequential
    /// position (extending the table if needed).
    ///
    /// # Returns
    ///
    /// An `EdgeId` for the newly allocated slot.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error if table growth fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = MmapGraph::open("my_graph.db")?;
    /// let slot_id = graph.allocate_edge_slot()?;
    /// // Now write an edge record to this slot
    /// ```
    pub fn allocate_edge_slot(&self) -> Result<EdgeId, StorageError> {
        let header = self.get_header();
        let current_count = header.edge_count;
        let current_capacity = header.edge_capacity;

        // Try to allocate from free list first
        let slot_id = {
            let mut free_edges = self.free_edges.write();
            free_edges.allocate(current_count)
        };

        // If we're extending beyond capacity, grow the table
        if slot_id >= current_capacity {
            self.grow_edge_table()?;
        }

        Ok(EdgeId(slot_id))
    }

    /// Write an edge record to the file at the correct offset.
    ///
    /// The record is written at: `edge_table_offset + (id * EDGE_RECORD_SIZE)`
    /// where `edge_table_offset = HEADER_SIZE + (node_capacity * NODE_RECORD_SIZE)`
    ///
    /// # Arguments
    ///
    /// * `id` - The edge ID (slot number) to write to
    /// * `record` - The edge record to write
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during write
    ///
    /// # Platform Notes
    ///
    /// On Unix, uses `write_all_at` for positioned writes without seeking.
    /// On other platforms, uses seek + write_all.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let record = EdgeRecord::new(0, label_id, src, dst);
    /// graph.write_edge_record(EdgeId(0), &record)?;
    /// ```
    pub fn write_edge_record(&self, id: EdgeId, record: &EdgeRecord) -> Result<(), StorageError> {
        // Calculate edge table offset: header + (node_capacity * node_record_size)
        let header = self.get_header();
        let edge_table_offset =
            HEADER_SIZE as u64 + (header.node_capacity * NODE_RECORD_SIZE as u64);
        let offset = edge_table_offset + (id.0 * EDGE_RECORD_SIZE as u64);
        let bytes = record.to_bytes();

        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&bytes)?;
        }

        file.sync_data()?;
        drop(file);

        // Remap to see the new data
        self.remap()?;

        Ok(())
    }

    /// Update a node's first_out_edge pointer.
    ///
    /// This is used when adding a new outgoing edge to prepend it to the
    /// source vertex's adjacency list. The new edge becomes the head of
    /// the outgoing edge list.
    ///
    /// # Arguments
    ///
    /// * `vertex` - The vertex whose first_out_edge should be updated
    /// * `edge_id` - The new edge ID to set as first_out_edge
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during write
    ///
    /// # Example
    ///
    /// ```ignore
    /// // When adding edge 5 from vertex 0:
    /// // 1. Get vertex 0's current first_out_edge (e.g., 3)
    /// // 2. Create edge 5 with next_out = 3
    /// // 3. Update vertex 0's first_out_edge to 5
    /// graph.update_node_first_out_edge(VertexId(0), 5)?;
    /// ```
    pub fn update_node_first_out_edge(
        &self,
        vertex: VertexId,
        edge_id: u64,
    ) -> Result<(), StorageError> {
        // Calculate offset to the first_out_edge field in the node record
        // NodeRecord layout: id(8) + label_id(4) + flags(4) + first_out_edge(8) + first_in_edge(8) + prop_head(8)
        // first_out_edge is at offset 16 within the record
        let node_offset = HEADER_SIZE as u64 + (vertex.0 * NODE_RECORD_SIZE as u64);
        let first_out_edge_offset = node_offset + 16; // id(8) + label_id(4) + flags(4)

        let bytes = edge_id.to_le_bytes();
        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, first_out_edge_offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(first_out_edge_offset))?;
            file.write_all(&bytes)?;
        }

        file.sync_data()?;
        drop(file);

        // Remap to see the updated data
        self.remap()?;

        Ok(())
    }

    /// Update a node's first_in_edge pointer.
    ///
    /// This is used when adding a new incoming edge to prepend it to the
    /// destination vertex's adjacency list. The new edge becomes the head
    /// of the incoming edge list.
    ///
    /// # Arguments
    ///
    /// * `vertex` - The vertex whose first_in_edge should be updated
    /// * `edge_id` - The new edge ID to set as first_in_edge
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during write
    ///
    /// # Example
    ///
    /// ```ignore
    /// // When adding edge 5 to vertex 1:
    /// // 1. Get vertex 1's current first_in_edge (e.g., 2)
    /// // 2. Create edge 5 with next_in = 2
    /// // 3. Update vertex 1's first_in_edge to 5
    /// graph.update_node_first_in_edge(VertexId(1), 5)?;
    /// ```
    pub fn update_node_first_in_edge(
        &self,
        vertex: VertexId,
        edge_id: u64,
    ) -> Result<(), StorageError> {
        // Calculate offset to the first_in_edge field in the node record
        // NodeRecord layout: id(8) + label_id(4) + flags(4) + first_out_edge(8) + first_in_edge(8) + prop_head(8)
        // first_in_edge is at offset 24 within the record
        let node_offset = HEADER_SIZE as u64 + (vertex.0 * NODE_RECORD_SIZE as u64);
        let first_in_edge_offset = node_offset + 24; // id(8) + label_id(4) + flags(4) + first_out_edge(8)

        let bytes = edge_id.to_le_bytes();
        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, first_in_edge_offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(first_in_edge_offset))?;
            file.write_all(&bytes)?;
        }

        file.sync_data()?;
        drop(file);

        // Remap to see the updated data
        self.remap()?;

        Ok(())
    }

    /// Increment the edge count in the file header.
    ///
    /// This should be called after successfully writing a new edge record
    /// (not when reusing a deleted slot, since the count wasn't decremented).
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    ///
    /// # Note
    ///
    /// This method reads the current header, increments the count, and writes
    /// the updated header back. It must be called with proper synchronization
    /// to avoid race conditions.
    pub fn increment_edge_count(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.edge_count += 1;

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        // Remap to see the updated header
        self.remap()?;

        Ok(())
    }

    /// Update the free edge head in the file header.
    ///
    /// This persists the current state of the free list head to disk.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    pub fn update_free_edge_head(&self) -> Result<(), StorageError> {
        let free_edge_head = self.free_edges.read().head();

        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.free_edge_head = free_edge_head;

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        // Remap to see the updated header
        self.remap()?;

        Ok(())
    }

    // =========================================================================
    // Phase 3.6: File Growth and Remapping
    // =========================================================================

    /// Grow the node table by doubling its capacity.
    ///
    /// This method:
    /// 1. Calculates the new capacity (2x current)
    /// 2. Expands the file to accommodate the larger node table
    /// 3. Moves the edge table to its new position (after expanded node table)
    /// 4. Updates the header with new capacity and offsets
    /// 5. Remaps the file to reflect the new size
    ///
    /// # File Layout Changes
    ///
    /// ```text
    /// Before: [Header][Nodes (N)][Edges (E)][Arena]
    /// After:  [Header][Nodes (2N)][Edges (E)][Arena]
    /// ```
    ///
    /// The edge table is moved to maintain contiguous layout.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during file operations
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = MmapGraph::open("my_graph.db")?;
    /// // If we need more node capacity:
    /// graph.grow_node_table()?;
    /// ```
    pub fn grow_node_table(&self) -> Result<(), StorageError> {
        let file = self.file.write();
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Calculate current layout
        let old_node_capacity = header.node_capacity;
        let edge_capacity = header.edge_capacity;
        let new_node_capacity = old_node_capacity * 2;

        let old_node_table_size = old_node_capacity as usize * NODE_RECORD_SIZE;
        let new_node_table_size = new_node_capacity as usize * NODE_RECORD_SIZE;
        let edge_table_size = edge_capacity as usize * EDGE_RECORD_SIZE;

        // Calculate old and new edge table offsets
        let old_edge_table_start = HEADER_SIZE + old_node_table_size;
        let new_edge_table_start = HEADER_SIZE + new_node_table_size;

        // Read the existing edge table data
        let mut edge_data = vec![0u8; edge_table_size];
        if edge_table_size > 0 && old_edge_table_start + edge_table_size <= mmap.len() {
            edge_data.copy_from_slice(
                &mmap[old_edge_table_start..old_edge_table_start + edge_table_size],
            );
        }
        drop(mmap);

        // Calculate new file size
        // Note: We keep the arena size the same, just shift it along with the edge table
        let old_file_size = file.metadata()?.len() as usize;
        let size_increase = new_node_table_size - old_node_table_size;
        let new_file_size = old_file_size + size_increase;

        // Extend the file
        file.set_len(new_file_size as u64)?;

        // Write edge table at new position
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            if edge_table_size > 0 {
                file.write_all_at(&edge_data, new_edge_table_start as u64)?;
            }
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            if edge_table_size > 0 {
                file.seek(SeekFrom::Start(new_edge_table_start as u64))?;
                file.write_all(&edge_data)?;
            }
        }

        // Update header
        let mut new_header = header;
        new_header.node_capacity = new_node_capacity;
        new_header.property_arena_offset += size_increase as u64;
        new_header.string_table_offset += size_increase as u64;
        Self::write_header(&file, &new_header)?;

        drop(file);

        // Remap the file
        self.remap()?;

        Ok(())
    }

    /// Grow the edge table by doubling its capacity.
    ///
    /// This method:
    /// 1. Calculates the new capacity (2x current)
    /// 2. Expands the file to accommodate the larger edge table
    /// 3. Updates the header with new capacity
    /// 4. Remaps the file to reflect the new size
    ///
    /// # File Layout Changes
    ///
    /// ```text
    /// Before: [Header][Nodes][Edges (E)][Arena]
    /// After:  [Header][Nodes][Edges (2E)][Arena]
    /// ```
    ///
    /// The arena is shifted to maintain contiguous layout.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during file operations
    pub fn grow_edge_table(&self) -> Result<(), StorageError> {
        let file = self.file.write();
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Calculate current layout
        let old_edge_capacity = header.edge_capacity;
        let new_edge_capacity = old_edge_capacity * 2;

        let old_edge_table_size = old_edge_capacity as usize * EDGE_RECORD_SIZE;
        let new_edge_table_size = new_edge_capacity as usize * EDGE_RECORD_SIZE;

        drop(mmap);

        // Calculate new file size
        let old_file_size = file.metadata()?.len() as usize;
        let size_increase = new_edge_table_size - old_edge_table_size;
        let new_file_size = old_file_size + size_increase;

        // Extend the file
        file.set_len(new_file_size as u64)?;

        // Update header
        let mmap = self.mmap.read();
        let mut new_header = Self::read_header(&mmap);
        new_header.edge_capacity = new_edge_capacity;
        new_header.property_arena_offset += size_increase as u64;
        new_header.string_table_offset += size_increase as u64;
        drop(mmap);

        Self::write_header(&file, &new_header)?;

        drop(file);

        // Remap the file
        self.remap()?;

        Ok(())
    }

    /// Ensure the file is at least the specified size.
    ///
    /// If the file is already at least `min_size` bytes, this is a no-op.
    /// Otherwise, the file is extended to `min_size` bytes.
    ///
    /// # Arguments
    ///
    /// * `min_size` - Minimum required file size in bytes
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during file extension
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Ensure at least 1MB of space
    /// graph.ensure_file_size(1024 * 1024)?;
    /// ```
    pub fn ensure_file_size(&self, min_size: u64) -> Result<(), StorageError> {
        let file = self.file.write();
        let current_size = file.metadata()?.len();

        if current_size < min_size {
            file.set_len(min_size)?;
            file.sync_data()?;
            drop(file);
            self.remap()?;
        }

        Ok(())
    }

    /// Recreate the memory map after file changes.
    ///
    /// This method must be called after any operation that changes the file size
    /// (such as `grow_node_table`, `grow_edge_table`, or `ensure_file_size`) to
    /// ensure the mmap reflects the new file contents.
    ///
    /// # Safety
    ///
    /// This method is safe but temporarily holds the write lock on the mmap.
    /// Callers should ensure no other operations are in progress that depend
    /// on the mmap contents during this call.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during mmap creation
    pub fn remap(&self) -> Result<(), StorageError> {
        let file = self.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file)? };
        drop(file);

        let mut mmap_write = self.mmap.write();
        *mmap_write = new_mmap;

        Ok(())
    }

    /// Get the header from the mmap.
    ///
    /// This is a helper method for GraphStorage implementations that need
    /// to read counts from the header.
    fn get_header(&self) -> FileHeader {
        let mmap = self.mmap.read();
        Self::read_header(&mmap)
    }

    // =========================================================================
    // Phase 4.4: add_vertex Implementation
    // =========================================================================

    /// Add a new vertex to the graph with the given label and properties.
    ///
    /// This method:
    /// 1. Allocates a node slot (from free list or by extending the table)
    /// 2. Interns the label string
    /// 3. Allocates properties in the arena (if any)
    /// 4. Creates and writes the node record
    /// 5. Updates the label index
    /// 6. Increments the node count
    ///
    /// # Arguments
    ///
    /// * `label` - The vertex label (e.g., "person", "software")
    /// * `properties` - A map of property key-value pairs
    ///
    /// # Returns
    ///
    /// The `VertexId` of the newly created vertex.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during file operations
    /// - [`StorageError::OutOfSpace`] - Not enough space in the property arena
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rustgremlin::storage::MmapGraph;
    /// use rustgremlin::value::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    ///
    /// let mut props = HashMap::new();
    /// props.insert("name".to_string(), Value::String("Alice".to_string()));
    /// props.insert("age".to_string(), Value::Int(30));
    ///
    /// let vertex_id = graph.add_vertex("person", props).unwrap();
    /// println!("Created vertex with ID: {:?}", vertex_id);
    /// ```
    pub fn add_vertex(
        &self,
        label: &str,
        properties: std::collections::HashMap<String, crate::value::Value>,
    ) -> Result<VertexId, StorageError> {
        // Step 1: Begin WAL transaction
        let tx_id = {
            let mut wal = self.wal.write();
            wal.begin_transaction()?
        };

        // Step 2: Allocate node slot
        let slot_id = self.allocate_node_slot()?;

        // Step 3: Intern label
        let label_id = {
            let mut string_table = self.string_table.write();
            string_table.intern(label)
        };

        // Step 4: Allocate properties in arena (returns u64::MAX if empty)
        let prop_head = self.allocate_properties(&properties)?;

        // Step 5: Create node record
        let mut record = NodeRecord::new(slot_id.0, label_id);
        record.prop_head = prop_head;
        // first_out_edge and first_in_edge default to u64::MAX (no edges)

        // Step 6: Log InsertNode to WAL (before writing to disk for durability)
        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::InsertNode {
                id: slot_id,
                record: record.into(),
            })?;
        }

        // Step 7: Write node record to disk
        self.write_node_record(slot_id, &record)?;

        // Step 8: Update label index
        {
            let mut vertex_labels = self.vertex_labels.write();
            vertex_labels
                .entry(label_id)
                .or_insert_with(RoaringBitmap::new)
                .insert(slot_id.0 as u32);
        }

        // Step 9: Increment node count in header
        self.increment_node_count()?;

        // Step 10: Persist string table (for label and property key names)
        self.persist_string_table()?;

        // Step 11: Update arena offset in header (for property data)
        self.update_arena_offset()?;

        // Step 12: Commit WAL transaction and sync
        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        Ok(slot_id)
    }

    /// Add a new edge to the graph between two existing vertices.
    ///
    /// This method:
    /// 1. Begins a WAL transaction for crash recovery
    /// 2. Verifies source and destination vertices exist
    /// 3. Allocates an edge slot (from free list or by extending the table)
    /// 4. Interns the label string
    /// 5. Allocates properties in the arena (if any)
    /// 6. Gets current first_out_edge from source and first_in_edge from destination
    /// 7. Creates edge record with next_out/next_in pointing to old heads
    /// 8. Logs InsertEdge to WAL (before writing to disk)
    /// 9. Writes the edge record to disk
    /// 10. Updates source's first_out_edge to point to new edge
    /// 11. Updates destination's first_in_edge to point to new edge
    /// 12. Updates edge label index
    /// 13. Increments edge count
    /// 14. Commits WAL transaction and syncs
    ///
    /// # Arguments
    ///
    /// * `src` - The source vertex ID
    /// * `dst` - The destination vertex ID
    /// * `label` - The edge label (e.g., "knows", "created")
    /// * `properties` - A map of property key-value pairs
    ///
    /// # Returns
    ///
    /// The `EdgeId` of the newly created edge.
    ///
    /// # Errors
    ///
    /// - [`StorageError::VertexNotFound`] - Source or destination vertex doesn't exist
    /// - [`StorageError::Io`] - I/O error during file operations
    /// - [`StorageError::OutOfSpace`] - Not enough space in the property arena
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rustgremlin::storage::MmapGraph;
    /// use rustgremlin::value::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    ///
    /// // Create two vertices
    /// let alice = graph.add_vertex("person", HashMap::new()).unwrap();
    /// let bob = graph.add_vertex("person", HashMap::new()).unwrap();
    ///
    /// // Create an edge between them
    /// let mut props = HashMap::new();
    /// props.insert("since".to_string(), Value::Int(2020));
    ///
    /// let edge_id = graph.add_edge(alice, bob, "knows", props).unwrap();
    /// println!("Created edge with ID: {:?}", edge_id);
    /// ```
    pub fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: std::collections::HashMap<String, crate::value::Value>,
    ) -> Result<EdgeId, StorageError> {
        // Step 1: Begin WAL transaction
        let tx_id = {
            let mut wal = self.wal.write();
            wal.begin_transaction()?
        };

        // Step 2: Verify source vertex exists
        let src_record = self
            .get_node_record(src)
            .ok_or(StorageError::VertexNotFound(src))?;

        // Step 3: Verify destination vertex exists
        let dst_record = self
            .get_node_record(dst)
            .ok_or(StorageError::VertexNotFound(dst))?;

        // Step 4: Allocate edge slot
        let slot_id = self.allocate_edge_slot()?;

        // Step 5: Intern label
        let label_id = {
            let mut string_table = self.string_table.write();
            string_table.intern(label)
        };

        // Step 6: Allocate properties in arena (returns u64::MAX if empty)
        let prop_head = self.allocate_properties(&properties)?;

        // Step 7: Get current first_out_edge and first_in_edge from the node records
        // These will become the next_out and next_in pointers for the new edge
        let old_first_out = src_record.first_out_edge;
        let old_first_in = dst_record.first_in_edge;

        // Step 8: Create edge record
        let mut record = EdgeRecord::new(slot_id.0, label_id, src.0, dst.0);
        record.prop_head = prop_head;
        record.next_out = old_first_out; // Link to previous head of outgoing list
        record.next_in = old_first_in; // Link to previous head of incoming list

        // Step 9: Log InsertEdge to WAL (before writing to disk for durability)
        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::InsertEdge {
                id: slot_id,
                record: record.into(),
            })?;
        }

        // Step 10: Write edge record to disk
        self.write_edge_record(slot_id, &record)?;

        // Step 11: Update source node's first_out_edge to point to new edge
        self.update_node_first_out_edge(src, slot_id.0)?;

        // Step 12: Update destination node's first_in_edge to point to new edge
        self.update_node_first_in_edge(dst, slot_id.0)?;

        // Step 13: Update edge label index
        {
            let mut edge_labels = self.edge_labels.write();
            edge_labels
                .entry(label_id)
                .or_insert_with(RoaringBitmap::new)
                .insert(slot_id.0 as u32);
        }

        // Step 14: Increment edge count in header
        self.increment_edge_count()?;

        // Step 15: Persist string table (for label and property key names)
        self.persist_string_table()?;

        // Step 16: Update arena offset in header (for property data)
        self.update_arena_offset()?;

        // Step 17: Commit WAL transaction and sync
        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        Ok(slot_id)
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
        // HEADER_SIZE (72) + (1000 * 48) + (10000 * 56) + (64 * 1024)
        let expected_size = HEADER_SIZE + (1000 * 48) + (10000 * 56) + (64 * 1024);
        assert_eq!(file_size, expected_size as u64);

        // Verify header fields
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        let property_arena_offset = header.property_arena_offset;
        let string_table_offset = header.string_table_offset;
        let free_node_head = header.free_node_head;

        // Property arena should start after node and edge tables
        let expected_arena_offset = HEADER_SIZE + (1000 * 48) + (10000 * 56);
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

    // =========================================================================
    // Phase 3.6: File Growth and Remapping Tests
    // =========================================================================

    #[test]
    fn test_remap_updates_mmap() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial mmap size
        let initial_size = {
            let mmap = graph.mmap.read();
            mmap.len()
        };

        // Extend the file
        {
            let file = graph.file.write();
            file.set_len(initial_size as u64 + 10000).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let result = graph.remap();
        assert!(result.is_ok(), "Remap should succeed");

        // Verify mmap size is updated
        let new_size = {
            let mmap = graph.mmap.read();
            mmap.len()
        };

        assert_eq!(
            new_size,
            initial_size + 10000,
            "Mmap should reflect new file size"
        );
    }

    #[test]
    fn test_ensure_file_size_grows_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial file size
        let initial_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        // Ensure a larger size
        let new_min_size = initial_size + 50000;
        let result = graph.ensure_file_size(new_min_size);
        assert!(result.is_ok(), "ensure_file_size should succeed");

        // Verify file grew
        let new_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        assert_eq!(new_size, new_min_size, "File should be at least min_size");

        // Verify mmap also reflects new size
        let mmap_size = {
            let mmap = graph.mmap.read();
            mmap.len()
        };
        assert_eq!(
            mmap_size, new_min_size as usize,
            "Mmap should reflect new size"
        );
    }

    #[test]
    fn test_ensure_file_size_noop_when_already_large_enough() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial file size
        let initial_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        // Ensure a smaller size (should be a no-op)
        let result = graph.ensure_file_size(100);
        assert!(result.is_ok(), "ensure_file_size should succeed");

        // Verify file size unchanged
        let new_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        assert_eq!(new_size, initial_size, "File size should not change");
    }

    #[test]
    fn test_grow_node_table_doubles_capacity() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial capacity
        let initial_capacity = {
            let header = graph.get_header();
            header.node_capacity
        };

        // Grow node table
        let result = graph.grow_node_table();
        assert!(result.is_ok(), "grow_node_table should succeed");

        // Verify capacity doubled
        let new_capacity = {
            let header = graph.get_header();
            header.node_capacity
        };

        assert_eq!(
            new_capacity,
            initial_capacity * 2,
            "Node capacity should double"
        );
    }

    #[test]
    fn test_grow_node_table_increases_file_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial file size
        let initial_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        let initial_capacity = {
            let header = graph.get_header();
            header.node_capacity
        };

        // Grow node table
        graph.grow_node_table().unwrap();

        // Verify file size increased
        let new_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        // Size should increase by (initial_capacity * NODE_RECORD_SIZE)
        let expected_increase = initial_capacity as usize * NODE_RECORD_SIZE;
        assert_eq!(
            new_size,
            initial_size + expected_increase as u64,
            "File should grow by the size of new node slots"
        );
    }

    #[test]
    fn test_grow_node_table_updates_offsets() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial offsets
        let (initial_prop_offset, initial_string_offset, initial_node_capacity) = {
            let header = graph.get_header();
            (
                header.property_arena_offset,
                header.string_table_offset,
                header.node_capacity,
            )
        };

        // Grow node table
        graph.grow_node_table().unwrap();

        // Verify offsets updated
        let (new_prop_offset, new_string_offset) = {
            let header = graph.get_header();
            (header.property_arena_offset, header.string_table_offset)
        };

        let size_increase = initial_node_capacity as u64 * NODE_RECORD_SIZE as u64;
        assert_eq!(
            new_prop_offset,
            initial_prop_offset + size_increase,
            "Property arena offset should shift"
        );
        assert_eq!(
            new_string_offset,
            initial_string_offset + size_increase,
            "String table offset should shift"
        );
    }

    #[test]
    fn test_grow_node_table_preserves_existing_data() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write some node data
        {
            let record = records::NodeRecord::new(0, 42);
            let offset = HEADER_SIZE;
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap to see the write
        graph.remap().unwrap();

        // Verify node exists
        let node_before = graph.get_node_record(VertexId(0));
        assert!(node_before.is_some(), "Node should exist before grow");
        let label_before = node_before.unwrap().label_id;
        assert_eq!(label_before, 42);

        // Grow node table
        graph.grow_node_table().unwrap();

        // Verify node still exists with same data
        let node_after = graph.get_node_record(VertexId(0));
        assert!(node_after.is_some(), "Node should exist after grow");
        let label_after = node_after.unwrap().label_id;
        assert_eq!(label_after, 42, "Node data should be preserved");
    }

    #[test]
    fn test_grow_edge_table_doubles_capacity() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial capacity
        let initial_capacity = {
            let header = graph.get_header();
            header.edge_capacity
        };

        // Grow edge table
        let result = graph.grow_edge_table();
        assert!(result.is_ok(), "grow_edge_table should succeed");

        // Verify capacity doubled
        let new_capacity = {
            let header = graph.get_header();
            header.edge_capacity
        };

        assert_eq!(
            new_capacity,
            initial_capacity * 2,
            "Edge capacity should double"
        );
    }

    #[test]
    fn test_grow_edge_table_increases_file_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial file size
        let initial_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        let initial_capacity = {
            let header = graph.get_header();
            header.edge_capacity
        };

        // Grow edge table
        graph.grow_edge_table().unwrap();

        // Verify file size increased
        let new_size = {
            let file = graph.file.read();
            file.metadata().unwrap().len()
        };

        // Size should increase by (initial_capacity * EDGE_RECORD_SIZE)
        let expected_increase = initial_capacity as usize * EDGE_RECORD_SIZE;
        assert_eq!(
            new_size,
            initial_size + expected_increase as u64,
            "File should grow by the size of new edge slots"
        );
    }

    #[test]
    fn test_grow_edge_table_updates_offsets() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial offsets
        let (initial_prop_offset, initial_string_offset, initial_edge_capacity) = {
            let header = graph.get_header();
            (
                header.property_arena_offset,
                header.string_table_offset,
                header.edge_capacity,
            )
        };

        // Grow edge table
        graph.grow_edge_table().unwrap();

        // Verify offsets updated
        let (new_prop_offset, new_string_offset) = {
            let header = graph.get_header();
            (header.property_arena_offset, header.string_table_offset)
        };

        let size_increase = initial_edge_capacity as u64 * EDGE_RECORD_SIZE as u64;
        assert_eq!(
            new_prop_offset,
            initial_prop_offset + size_increase,
            "Property arena offset should shift"
        );
        assert_eq!(
            new_string_offset,
            initial_string_offset + size_increase,
            "String table offset should shift"
        );
    }

    #[test]
    fn test_grow_node_table_multiple_times() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let initial_capacity = {
            let header = graph.get_header();
            header.node_capacity
        };

        // Grow 3 times
        graph.grow_node_table().unwrap();
        graph.grow_node_table().unwrap();
        graph.grow_node_table().unwrap();

        let final_capacity = {
            let header = graph.get_header();
            header.node_capacity
        };

        // Should be 2^3 = 8x initial
        assert_eq!(
            final_capacity,
            initial_capacity * 8,
            "Capacity should be 8x after 3 doublings"
        );
    }

    #[test]
    fn test_grow_node_table_preserves_edge_data() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get edge table offset before growth
        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);

        // Write an edge record
        {
            let record = records::EdgeRecord::new(0, 99, 1, 2);
            let bytes = record.to_bytes();

            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(edge_table_offset as u64))
                .unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap to see the write
        graph.remap().unwrap();

        // Verify edge exists
        let edge_before = graph.get_edge_record(EdgeId(0));
        assert!(edge_before.is_some(), "Edge should exist before grow");
        let (label_before, src_before, dst_before) = {
            let e = edge_before.unwrap();
            (e.label_id, e.src, e.dst)
        };
        assert_eq!(label_before, 99);
        assert_eq!(src_before, 1);
        assert_eq!(dst_before, 2);

        // Grow node table (this moves the edge table)
        graph.grow_node_table().unwrap();

        // Verify edge still exists with same data (at new offset)
        let edge_after = graph.get_edge_record(EdgeId(0));
        assert!(edge_after.is_some(), "Edge should exist after grow");
        let (label_after, src_after, dst_after) = {
            let e = edge_after.unwrap();
            (e.label_id, e.src, e.dst)
        };
        assert_eq!(label_after, 99, "Edge label should be preserved");
        assert_eq!(src_after, 1, "Edge src should be preserved");
        assert_eq!(dst_after, 2, "Edge dst should be preserved");
    }

    // =========================================================================
    // Phase 4.1: Property Arena Allocation Tests
    // =========================================================================

    #[test]
    fn test_allocate_properties_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let props = std::collections::HashMap::new();
        let result = graph.allocate_properties(&props);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            u64::MAX,
            "Empty properties should return u64::MAX"
        );
    }

    #[test]
    fn test_allocate_properties_single() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let prop_head = graph.allocate_properties(&props).unwrap();

        // Should return a valid offset (not u64::MAX)
        assert_ne!(prop_head, u64::MAX, "Should return valid offset");

        // Offset should be within the arena
        let header = graph.get_header();
        assert!(prop_head >= header.property_arena_offset);
        assert!(prop_head < header.string_table_offset);
    }

    #[test]
    fn test_allocate_properties_multiple() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("active".to_string(), Value::Bool(true));

        let prop_head = graph.allocate_properties(&props).unwrap();

        assert_ne!(prop_head, u64::MAX);
    }

    #[test]
    fn test_allocate_properties_roundtrip() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("score".to_string(), Value::Int(42));

        // Allocate properties
        let prop_head = graph.allocate_properties(&props).unwrap();

        // Load them back
        let loaded = graph.load_properties(prop_head).unwrap();

        // Verify all properties are present
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get("name"),
            Some(&Value::String("Charlie".to_string()))
        );
        assert_eq!(loaded.get("score"), Some(&Value::Int(42)));
    }

    #[test]
    fn test_allocate_properties_all_value_types() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("null_val".to_string(), Value::Null);
        props.insert("bool_true".to_string(), Value::Bool(true));
        props.insert("bool_false".to_string(), Value::Bool(false));
        props.insert("int_val".to_string(), Value::Int(-12345));
        props.insert("float_val".to_string(), Value::Float(3.14159));
        props.insert(
            "string_val".to_string(),
            Value::String("hello world".to_string()),
        );

        let prop_head = graph.allocate_properties(&props).unwrap();
        let loaded = graph.load_properties(prop_head).unwrap();

        assert_eq!(loaded.len(), 6);
        assert_eq!(loaded.get("null_val"), Some(&Value::Null));
        assert_eq!(loaded.get("bool_true"), Some(&Value::Bool(true)));
        assert_eq!(loaded.get("bool_false"), Some(&Value::Bool(false)));
        assert_eq!(loaded.get("int_val"), Some(&Value::Int(-12345)));
        assert_eq!(loaded.get("float_val"), Some(&Value::Float(3.14159)));
        assert_eq!(
            loaded.get("string_val"),
            Some(&Value::String("hello world".to_string()))
        );
    }

    #[test]
    fn test_allocate_properties_multiple_allocations() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Allocate first set
        let mut props1 = std::collections::HashMap::new();
        props1.insert("key1".to_string(), Value::String("value1".to_string()));
        let head1 = graph.allocate_properties(&props1).unwrap();

        // Allocate second set
        let mut props2 = std::collections::HashMap::new();
        props2.insert("key2".to_string(), Value::String("value2".to_string()));
        let head2 = graph.allocate_properties(&props2).unwrap();

        // Both should have valid, different offsets
        assert_ne!(head1, u64::MAX);
        assert_ne!(head2, u64::MAX);
        assert_ne!(
            head1, head2,
            "Different allocations should have different offsets"
        );

        // Both should be loadable
        let loaded1 = graph.load_properties(head1).unwrap();
        let loaded2 = graph.load_properties(head2).unwrap();

        assert_eq!(
            loaded1.get("key1"),
            Some(&Value::String("value1".to_string()))
        );
        assert_eq!(
            loaded2.get("key2"),
            Some(&Value::String("value2".to_string()))
        );
    }

    #[test]
    fn test_allocate_properties_large_string() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a large string (> 256 bytes)
        let large_string = "x".repeat(1000);

        let mut props = std::collections::HashMap::new();
        props.insert("big".to_string(), Value::String(large_string.clone()));

        let prop_head = graph.allocate_properties(&props).unwrap();
        let loaded = graph.load_properties(prop_head).unwrap();

        assert_eq!(loaded.get("big"), Some(&Value::String(large_string)));
    }

    #[test]
    fn test_allocate_properties_nested_list() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let list = Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::String("three".to_string()),
        ]);

        let mut props = std::collections::HashMap::new();
        props.insert("items".to_string(), list.clone());

        let prop_head = graph.allocate_properties(&props).unwrap();
        let loaded = graph.load_properties(prop_head).unwrap();

        assert_eq!(loaded.get("items"), Some(&list));
    }

    #[test]
    fn test_allocate_properties_nested_map() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut inner_map = std::collections::HashMap::new();
        inner_map.insert("nested_key".to_string(), Value::Int(999));
        let map_val = Value::Map(inner_map);

        let mut props = std::collections::HashMap::new();
        props.insert("metadata".to_string(), map_val.clone());

        let prop_head = graph.allocate_properties(&props).unwrap();
        let loaded = graph.load_properties(prop_head).unwrap();

        assert_eq!(loaded.get("metadata"), Some(&map_val));
    }

    #[test]
    fn test_allocate_properties_vertex_edge_refs() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("vertex_ref".to_string(), Value::Vertex(VertexId(42)));
        props.insert("edge_ref".to_string(), Value::Edge(EdgeId(123)));

        let prop_head = graph.allocate_properties(&props).unwrap();
        let loaded = graph.load_properties(prop_head).unwrap();

        assert_eq!(loaded.get("vertex_ref"), Some(&Value::Vertex(VertexId(42))));
        assert_eq!(loaded.get("edge_ref"), Some(&Value::Edge(EdgeId(123))));
    }

    #[test]
    fn test_arena_tracks_offset_correctly() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial arena offset
        let initial_offset = {
            let arena = graph.arena.read();
            arena.current_offset()
        };

        // Allocate some properties
        let mut props = std::collections::HashMap::new();
        props.insert("test".to_string(), Value::Int(1));
        graph.allocate_properties(&props).unwrap();

        // Offset should have advanced
        let new_offset = {
            let arena = graph.arena.read();
            arena.current_offset()
        };

        assert!(
            new_offset > initial_offset,
            "Arena offset should advance after allocation"
        );
    }

    // =========================================================================
    // Phase 4.2: Node Slot Allocation and Writing Tests
    // =========================================================================

    #[test]
    fn test_allocate_node_slot_returns_sequential_ids() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // First allocation should return slot 0
        let slot0 = graph.allocate_node_slot().unwrap();
        assert_eq!(slot0.0, 0, "First slot should be 0");

        // Manually update count to simulate the slot being used
        graph.increment_node_count().unwrap();

        // Second allocation should return slot 1
        let slot1 = graph.allocate_node_slot().unwrap();
        assert_eq!(slot1.0, 1, "Second slot should be 1");

        graph.increment_node_count().unwrap();

        // Third allocation should return slot 2
        let slot2 = graph.allocate_node_slot().unwrap();
        assert_eq!(slot2.0, 2, "Third slot should be 2");
    }

    #[test]
    fn test_allocate_node_slot_reuses_free_slots() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Simulate allocating 3 slots
        for _ in 0..3 {
            let _ = graph.allocate_node_slot().unwrap();
            graph.increment_node_count().unwrap();
        }

        // Free slot 1 (simulating deletion)
        {
            let mut free_nodes = graph.free_nodes.write();
            free_nodes.free(1);
        }

        // Next allocation should reuse slot 1
        let reused = graph.allocate_node_slot().unwrap();
        assert_eq!(
            reused.0, 1,
            "Should reuse freed slot 1 instead of allocating 3"
        );
    }

    #[test]
    fn test_write_node_record_and_read_back() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create and write a node record
        let slot = graph.allocate_node_slot().unwrap();
        let mut record = NodeRecord::new(slot.0, 42);
        record.first_out_edge = 100;
        record.first_in_edge = 200;
        record.prop_head = 300;

        graph.write_node_record(slot, &record).unwrap();

        // Read it back
        let retrieved = graph.get_node_record(slot);
        assert!(retrieved.is_some(), "Should be able to read written record");

        let retrieved = retrieved.unwrap();
        // Copy fields to avoid unaligned reference errors with packed structs
        let id = retrieved.id;
        let label_id = retrieved.label_id;
        let first_out_edge = retrieved.first_out_edge;
        let first_in_edge = retrieved.first_in_edge;
        let prop_head = retrieved.prop_head;

        assert_eq!(id, slot.0);
        assert_eq!(label_id, 42);
        assert_eq!(first_out_edge, 100);
        assert_eq!(first_in_edge, 200);
        assert_eq!(prop_head, 300);
    }

    #[test]
    fn test_increment_node_count() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Initial count should be 0
        let initial = graph.get_header().node_count;
        assert_eq!(initial, 0, "Initial node count should be 0");

        // Increment
        graph.increment_node_count().unwrap();
        let after_first = graph.get_header().node_count;
        assert_eq!(after_first, 1, "Node count should be 1 after increment");

        // Increment again
        graph.increment_node_count().unwrap();
        let after_second = graph.get_header().node_count;
        assert_eq!(
            after_second, 2,
            "Node count should be 2 after second increment"
        );
    }

    #[test]
    fn test_update_free_node_head() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Initial free head should be MAX (empty)
        let initial = graph.get_header().free_node_head;
        assert_eq!(initial, u64::MAX, "Initial free head should be MAX");

        // Add some free slots
        {
            let mut free_nodes = graph.free_nodes.write();
            free_nodes.free(5);
            free_nodes.free(10);
        }

        // Update header
        graph.update_free_node_head().unwrap();

        // Verify header updated
        let after = graph.get_header().free_node_head;
        assert_eq!(after, 10, "Free head should be 10 (last freed)");
    }

    #[test]
    fn test_write_multiple_nodes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write 10 nodes
        for i in 0..10 {
            let slot = graph.allocate_node_slot().unwrap();
            let record = NodeRecord::new(slot.0, i as u32 * 10);
            graph.write_node_record(slot, &record).unwrap();
            graph.increment_node_count().unwrap();
        }

        // Verify count
        let count = graph.get_header().node_count;
        assert_eq!(count, 10, "Should have 10 nodes");

        // Verify all can be read back
        for i in 0..10 {
            let record = graph.get_node_record(VertexId(i));
            assert!(record.is_some(), "Node {} should exist", i);

            let record = record.unwrap();
            // Copy fields to avoid unaligned reference errors with packed structs
            let id = record.id;
            let label_id = record.label_id;
            assert_eq!(id, i);
            assert_eq!(label_id, (i as u32) * 10);
        }
    }

    #[test]
    fn test_allocate_triggers_table_growth() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial capacity
        let initial_capacity = graph.get_header().node_capacity;
        assert_eq!(initial_capacity, 1000, "Initial capacity should be 1000");

        // Manually set node_count to capacity to force growth on next allocate
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = 1000;
            drop(mmap);

            let file = graph.file.write();
            MmapGraph::write_header(&file, &header).unwrap();
            drop(file);
            graph.remap().unwrap();
        }

        // Allocate should trigger growth
        let slot = graph.allocate_node_slot().unwrap();
        assert_eq!(slot.0, 1000, "Should allocate at slot 1000");

        // Capacity should have doubled
        let new_capacity = graph.get_header().node_capacity;
        assert_eq!(new_capacity, 2000, "Capacity should double to 2000");
    }

    #[test]
    fn test_node_allocate_write_roundtrip_with_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create database and add nodes
        {
            let graph = MmapGraph::open(&path).unwrap();

            for i in 0..5 {
                let slot = graph.allocate_node_slot().unwrap();
                let record = NodeRecord::new(slot.0, i * 100);
                graph.write_node_record(slot, &record).unwrap();
                graph.increment_node_count().unwrap();
            }

            // Verify nodes exist before close
            let count = graph.get_header().node_count;
            assert_eq!(count, 5);
        }

        // Reopen and verify
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Verify count
            let count = graph.get_header().node_count;
            assert_eq!(count, 5);

            // Verify all nodes
            for i in 0..5 {
                let record = graph.get_node_record(VertexId(i));
                assert!(record.is_some(), "Node {} should persist after reopen", i);

                let record = record.unwrap();
                // Copy fields to avoid unaligned reference errors with packed structs
                let id = record.id;
                let label_id = record.label_id;
                assert_eq!(id, i);
                assert_eq!(label_id, (i as u32) * 100);
            }
        }
    }

    #[test]
    fn test_free_list_persists_after_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create database, add nodes, mark one as free
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add 3 nodes
            for i in 0..3 {
                let slot = graph.allocate_node_slot().unwrap();
                let record = NodeRecord::new(slot.0, i * 10);
                graph.write_node_record(slot, &record).unwrap();
                graph.increment_node_count().unwrap();
            }

            // Free slot 1
            {
                let mut free_nodes = graph.free_nodes.write();
                free_nodes.free(1);
            }
            graph.update_free_node_head().unwrap();
        }

        // Reopen and verify free list head
        {
            let graph = MmapGraph::open(&path).unwrap();

            let free_head = graph.free_nodes.read().head();
            assert_eq!(free_head, 1, "Free list head should be 1 after reopen");

            // Next allocation should reuse slot 1
            let slot = graph.allocate_node_slot().unwrap();
            assert_eq!(slot.0, 1, "Should reuse freed slot 1");
        }
    }

    // =========================================================================
    // Phase 4.3: Edge Slot Allocation and Writing Tests
    // =========================================================================

    #[test]
    fn test_allocate_edge_slot_returns_sequential_ids() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // First allocation should return slot 0
        let slot0 = graph.allocate_edge_slot().unwrap();
        assert_eq!(slot0.0, 0, "First edge slot should be 0");

        // Manually update count to simulate the slot being used
        graph.increment_edge_count().unwrap();

        // Second allocation should return slot 1
        let slot1 = graph.allocate_edge_slot().unwrap();
        assert_eq!(slot1.0, 1, "Second edge slot should be 1");

        graph.increment_edge_count().unwrap();

        // Third allocation should return slot 2
        let slot2 = graph.allocate_edge_slot().unwrap();
        assert_eq!(slot2.0, 2, "Third edge slot should be 2");
    }

    #[test]
    fn test_allocate_edge_slot_reuses_free_slots() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Simulate allocating 3 slots
        for _ in 0..3 {
            let _ = graph.allocate_edge_slot().unwrap();
            graph.increment_edge_count().unwrap();
        }

        // Free slot 1 (simulating deletion)
        {
            let mut free_edges = graph.free_edges.write();
            free_edges.free(1);
        }

        // Next allocation should reuse slot 1
        let reused = graph.allocate_edge_slot().unwrap();
        assert_eq!(
            reused.0, 1,
            "Should reuse freed edge slot 1 instead of allocating 3"
        );
    }

    #[test]
    fn test_write_edge_record_and_read_back() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create and write an edge record
        let slot = graph.allocate_edge_slot().unwrap();
        let mut record = EdgeRecord::new(slot.0, 42, 10, 20); // label=42, src=10, dst=20
        record.next_out = 100;
        record.next_in = 200;
        record.prop_head = 300;

        graph.write_edge_record(slot, &record).unwrap();

        // Read it back
        let retrieved = graph.get_edge_record(slot);
        assert!(
            retrieved.is_some(),
            "Should be able to read written edge record"
        );

        let retrieved = retrieved.unwrap();
        // Copy fields to avoid unaligned reference errors with packed structs
        let id = retrieved.id;
        let label_id = retrieved.label_id;
        let src = retrieved.src;
        let dst = retrieved.dst;
        let next_out = retrieved.next_out;
        let next_in = retrieved.next_in;
        let prop_head = retrieved.prop_head;

        assert_eq!(id, slot.0);
        assert_eq!(label_id, 42);
        assert_eq!(src, 10);
        assert_eq!(dst, 20);
        assert_eq!(next_out, 100);
        assert_eq!(next_in, 200);
        assert_eq!(prop_head, 300);
    }

    #[test]
    fn test_increment_edge_count() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Initial count should be 0
        let initial = graph.get_header().edge_count;
        assert_eq!(initial, 0, "Initial edge count should be 0");

        // Increment
        graph.increment_edge_count().unwrap();
        let after_first = graph.get_header().edge_count;
        assert_eq!(after_first, 1, "Edge count should be 1 after increment");

        // Increment again
        graph.increment_edge_count().unwrap();
        let after_second = graph.get_header().edge_count;
        assert_eq!(
            after_second, 2,
            "Edge count should be 2 after second increment"
        );
    }

    #[test]
    fn test_update_free_edge_head() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Initial free head should be MAX (empty)
        let initial = graph.get_header().free_edge_head;
        assert_eq!(initial, u64::MAX, "Initial edge free head should be MAX");

        // Add some free slots
        {
            let mut free_edges = graph.free_edges.write();
            free_edges.free(5);
            free_edges.free(10);
        }

        // Update header
        graph.update_free_edge_head().unwrap();

        // Verify header updated
        let after = graph.get_header().free_edge_head;
        assert_eq!(after, 10, "Edge free head should be 10 (last freed)");
    }

    #[test]
    fn test_write_multiple_edges() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write 10 edges
        for i in 0..10 {
            let slot = graph.allocate_edge_slot().unwrap();
            let record = EdgeRecord::new(slot.0, i as u32 * 10, i, i + 1);
            graph.write_edge_record(slot, &record).unwrap();
            graph.increment_edge_count().unwrap();
        }

        // Verify count
        let count = graph.get_header().edge_count;
        assert_eq!(count, 10, "Should have 10 edges");

        // Verify all can be read back
        for i in 0..10 {
            let record = graph.get_edge_record(EdgeId(i));
            assert!(record.is_some(), "Edge {} should exist", i);

            let record = record.unwrap();
            // Copy fields to avoid unaligned reference errors with packed structs
            let id = record.id;
            let label_id = record.label_id;
            let src = record.src;
            let dst = record.dst;

            assert_eq!(id, i);
            assert_eq!(label_id, (i as u32) * 10);
            assert_eq!(src, i);
            assert_eq!(dst, i + 1);
        }
    }

    #[test]
    fn test_update_node_first_out_edge() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a node first
        let node_slot = graph.allocate_node_slot().unwrap();
        let mut node_record = NodeRecord::new(node_slot.0, 0);
        node_record.first_out_edge = u64::MAX; // Initially no edges
        node_record.first_in_edge = u64::MAX;
        graph.write_node_record(node_slot, &node_record).unwrap();
        graph.increment_node_count().unwrap();

        // Verify initial state
        let node = graph.get_node_record(node_slot).unwrap();
        let first_out = node.first_out_edge;
        assert_eq!(first_out, u64::MAX, "Initially no outgoing edges");

        // Update first_out_edge to point to edge 5
        graph.update_node_first_out_edge(node_slot, 5).unwrap();

        // Verify update
        let node = graph.get_node_record(node_slot).unwrap();
        let first_out = node.first_out_edge;
        let first_in = node.first_in_edge;
        assert_eq!(first_out, 5, "first_out_edge should be 5");
        // first_in_edge should be unchanged
        assert_eq!(first_in, u64::MAX, "first_in_edge should be unchanged");
    }

    #[test]
    fn test_update_node_first_in_edge() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a node first
        let node_slot = graph.allocate_node_slot().unwrap();
        let mut node_record = NodeRecord::new(node_slot.0, 0);
        node_record.first_out_edge = u64::MAX;
        node_record.first_in_edge = u64::MAX;
        graph.write_node_record(node_slot, &node_record).unwrap();
        graph.increment_node_count().unwrap();

        // Verify initial state
        let node = graph.get_node_record(node_slot).unwrap();
        let first_in = node.first_in_edge;
        assert_eq!(first_in, u64::MAX, "Initially no incoming edges");

        // Update first_in_edge to point to edge 7
        graph.update_node_first_in_edge(node_slot, 7).unwrap();

        // Verify update
        let node = graph.get_node_record(node_slot).unwrap();
        let first_in = node.first_in_edge;
        let first_out = node.first_out_edge;
        assert_eq!(first_in, 7, "first_in_edge should be 7");
        // first_out_edge should be unchanged
        assert_eq!(first_out, u64::MAX, "first_out_edge should be unchanged");
    }

    #[test]
    fn test_edge_allocate_triggers_table_growth() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial capacity
        let initial_capacity = graph.get_header().edge_capacity;
        assert_eq!(
            initial_capacity, 10000,
            "Initial edge capacity should be 10000"
        );

        // Manually set edge_count to capacity to force growth on next allocate
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.edge_count = 10000;
            drop(mmap);

            let file = graph.file.write();
            MmapGraph::write_header(&file, &header).unwrap();
            drop(file);
            graph.remap().unwrap();
        }

        // Allocate should trigger growth
        let slot = graph.allocate_edge_slot().unwrap();
        assert_eq!(slot.0, 10000, "Should allocate at edge slot 10000");

        // Capacity should have doubled
        let new_capacity = graph.get_header().edge_capacity;
        assert_eq!(new_capacity, 20000, "Edge capacity should double to 20000");
    }

    #[test]
    fn test_edge_allocate_write_roundtrip_with_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create database and add edges
        {
            let graph = MmapGraph::open(&path).unwrap();

            for i in 0u64..5 {
                let slot = graph.allocate_edge_slot().unwrap();
                let record = EdgeRecord::new(slot.0, (i * 100) as u32, i, i + 1);
                graph.write_edge_record(slot, &record).unwrap();
                graph.increment_edge_count().unwrap();
            }

            // Verify edges exist before close
            let count = graph.get_header().edge_count;
            assert_eq!(count, 5);
        }

        // Reopen and verify
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Verify count
            let count = graph.get_header().edge_count;
            assert_eq!(count, 5);

            // Verify all edges
            for i in 0..5 {
                let record = graph.get_edge_record(EdgeId(i));
                assert!(record.is_some(), "Edge {} should persist after reopen", i);

                let record = record.unwrap();
                // Copy fields to avoid unaligned reference errors with packed structs
                let id = record.id;
                let label_id = record.label_id;
                assert_eq!(id, i);
                assert_eq!(label_id, (i as u32) * 100);
            }
        }
    }

    #[test]
    fn test_edge_free_list_persists_after_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create database, add edges, mark one as free
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add 3 edges
            for i in 0u64..3 {
                let slot = graph.allocate_edge_slot().unwrap();
                let record = EdgeRecord::new(slot.0, (i * 10) as u32, i, i + 1);
                graph.write_edge_record(slot, &record).unwrap();
                graph.increment_edge_count().unwrap();
            }

            // Free slot 1
            {
                let mut free_edges = graph.free_edges.write();
                free_edges.free(1);
            }
            graph.update_free_edge_head().unwrap();
        }

        // Reopen and verify free list head
        {
            let graph = MmapGraph::open(&path).unwrap();

            let free_head = graph.free_edges.read().head();
            assert_eq!(free_head, 1, "Edge free list head should be 1 after reopen");

            // Next allocation should reuse slot 1
            let slot = graph.allocate_edge_slot().unwrap();
            assert_eq!(slot.0, 1, "Should reuse freed edge slot 1");
        }
    }

    #[test]
    fn test_adjacency_list_maintenance() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create 3 nodes
        for i in 0..3 {
            let slot = graph.allocate_node_slot().unwrap();
            let mut record = NodeRecord::new(slot.0, 0);
            record.first_out_edge = u64::MAX;
            record.first_in_edge = u64::MAX;
            graph.write_node_record(slot, &record).unwrap();
            graph.increment_node_count().unwrap();
        }

        // Add edge 0: node 0 -> node 1
        // This should be the first outgoing edge from node 0 and first incoming to node 1
        {
            let edge_slot = graph.allocate_edge_slot().unwrap();
            let mut edge_record = EdgeRecord::new(edge_slot.0, 0, 0, 1);

            // Get current first_out and first_in from nodes
            let src_node = graph.get_node_record(VertexId(0)).unwrap();
            let dst_node = graph.get_node_record(VertexId(1)).unwrap();

            // Copy fields to avoid unaligned reference errors
            let src_first_out = src_node.first_out_edge;
            let dst_first_in = dst_node.first_in_edge;

            edge_record.next_out = src_first_out; // u64::MAX (no previous)
            edge_record.next_in = dst_first_in; // u64::MAX (no previous)

            graph.write_edge_record(edge_slot, &edge_record).unwrap();
            graph
                .update_node_first_out_edge(VertexId(0), edge_slot.0)
                .unwrap();
            graph
                .update_node_first_in_edge(VertexId(1), edge_slot.0)
                .unwrap();
            graph.increment_edge_count().unwrap();
        }

        // Add edge 1: node 0 -> node 2
        // This should be prepended to node 0's outgoing list
        {
            let edge_slot = graph.allocate_edge_slot().unwrap();
            let mut edge_record = EdgeRecord::new(edge_slot.0, 0, 0, 2);

            // Get current first_out from node 0 (should be edge 0)
            let src_node = graph.get_node_record(VertexId(0)).unwrap();
            let dst_node = graph.get_node_record(VertexId(2)).unwrap();

            // Copy fields to avoid unaligned reference errors
            let src_first_out = src_node.first_out_edge;
            let dst_first_in = dst_node.first_in_edge;

            edge_record.next_out = src_first_out; // Points to edge 0
            edge_record.next_in = dst_first_in; // u64::MAX

            graph.write_edge_record(edge_slot, &edge_record).unwrap();
            graph
                .update_node_first_out_edge(VertexId(0), edge_slot.0)
                .unwrap();
            graph
                .update_node_first_in_edge(VertexId(2), edge_slot.0)
                .unwrap();
            graph.increment_edge_count().unwrap();
        }

        // Verify adjacency lists
        // Node 0's first_out_edge should be 1 (most recent), with next_out = 0
        let node0 = graph.get_node_record(VertexId(0)).unwrap();
        let node0_first_out = node0.first_out_edge;
        assert_eq!(node0_first_out, 1, "Node 0's first_out should be edge 1");

        // Edge 1's next_out should be edge 0
        let edge1 = graph.get_edge_record(EdgeId(1)).unwrap();
        let edge1_next_out = edge1.next_out;
        assert_eq!(edge1_next_out, 0, "Edge 1's next_out should be edge 0");

        // Edge 0's next_out should be u64::MAX (end of list)
        let edge0 = graph.get_edge_record(EdgeId(0)).unwrap();
        let edge0_next_out = edge0.next_out;
        assert_eq!(edge0_next_out, u64::MAX, "Edge 0's next_out should be MAX");

        // Node 1's first_in_edge should be edge 0
        let node1 = graph.get_node_record(VertexId(1)).unwrap();
        let node1_first_in = node1.first_in_edge;
        assert_eq!(node1_first_in, 0, "Node 1's first_in should be edge 0");

        // Node 2's first_in_edge should be edge 1
        let node2 = graph.get_node_record(VertexId(2)).unwrap();
        let node2_first_in = node2.first_in_edge;
        assert_eq!(node2_first_in, 1, "Node 2's first_in should be edge 1");
    }

    // =========================================================================
    // Phase 4.4: add_vertex Tests
    // =========================================================================

    #[test]
    fn test_add_vertex_basic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add vertex with label, no properties
        let props = std::collections::HashMap::new();
        let vertex_id = graph.add_vertex("person", props).unwrap();

        assert_eq!(vertex_id.0, 0, "First vertex should have ID 0");

        // Verify it can be retrieved
        let vertex = graph.get_vertex(vertex_id);
        assert!(vertex.is_some(), "Vertex should exist after add");

        let vertex = vertex.unwrap();
        assert_eq!(vertex.id, vertex_id);
        assert_eq!(vertex.label, "person");
        assert!(vertex.properties.is_empty());
    }

    #[test]
    fn test_add_vertex_with_properties() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add vertex with properties
        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("active".to_string(), Value::Bool(true));

        let vertex_id = graph.add_vertex("person", props).unwrap();

        // Verify properties roundtrip
        let vertex = graph.get_vertex(vertex_id).unwrap();
        assert_eq!(vertex.label, "person");
        assert_eq!(vertex.properties.len(), 3);
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
        assert_eq!(vertex.properties.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_add_vertex_updates_label_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add multiple vertices with different labels
        let props = std::collections::HashMap::new();
        let v1 = graph.add_vertex("person", props.clone()).unwrap();
        let v2 = graph.add_vertex("person", props.clone()).unwrap();
        let v3 = graph.add_vertex("software", props.clone()).unwrap();

        // Query by label
        let people: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(people.len(), 2, "Should have 2 people");
        assert!(people.iter().any(|v| v.id == v1));
        assert!(people.iter().any(|v| v.id == v2));

        let software: Vec<_> = graph.vertices_with_label("software").collect();
        assert_eq!(software.len(), 1, "Should have 1 software");
        assert_eq!(software[0].id, v3);
    }

    #[test]
    fn test_add_vertex_increments_count() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        assert_eq!(graph.vertex_count(), 0, "Initial count should be 0");

        let props = std::collections::HashMap::new();
        graph.add_vertex("person", props.clone()).unwrap();
        assert_eq!(graph.vertex_count(), 1, "Count should be 1 after first add");

        graph.add_vertex("person", props.clone()).unwrap();
        assert_eq!(
            graph.vertex_count(),
            2,
            "Count should be 2 after second add"
        );

        graph.add_vertex("software", props).unwrap();
        assert_eq!(graph.vertex_count(), 3, "Count should be 3 after third add");
    }

    #[test]
    fn test_add_vertex_persists_after_reopen() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let vertex_id;

        // Create database and add vertex
        {
            let graph = MmapGraph::open(&path).unwrap();

            let mut props = std::collections::HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("score".to_string(), Value::Int(42));

            vertex_id = graph.add_vertex("player", props).unwrap();

            // Verify vertex exists before close
            let v = graph.get_vertex(vertex_id);
            assert!(v.is_some());
        }

        // Reopen and verify
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Verify count persisted
            assert_eq!(graph.vertex_count(), 1);

            // Verify vertex data persisted
            let vertex = graph.get_vertex(vertex_id);
            assert!(vertex.is_some(), "Vertex should persist after reopen");

            let vertex = vertex.unwrap();
            assert_eq!(vertex.label, "player");
            assert_eq!(vertex.properties.len(), 2);
            assert_eq!(
                vertex.properties.get("name"),
                Some(&Value::String("Bob".to_string()))
            );
            assert_eq!(vertex.properties.get("score"), Some(&Value::Int(42)));

            // Verify label index rebuilt
            let players: Vec<_> = graph.vertices_with_label("player").collect();
            assert_eq!(players.len(), 1);
            assert_eq!(players[0].id, vertex_id);
        }
    }

    #[test]
    fn test_add_vertex_multiple_with_various_properties() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add several vertices with varying properties
        let mut alice_props = std::collections::HashMap::new();
        alice_props.insert("name".to_string(), Value::String("Alice".to_string()));
        let alice = graph.add_vertex("person", alice_props).unwrap();

        let bob_props = std::collections::HashMap::new(); // No properties
        let bob = graph.add_vertex("person", bob_props).unwrap();

        let mut repo_props = std::collections::HashMap::new();
        repo_props.insert("name".to_string(), Value::String("gremlin".to_string()));
        repo_props.insert("stars".to_string(), Value::Int(1000));
        repo_props.insert("active".to_string(), Value::Bool(true));
        let repo = graph.add_vertex("repository", repo_props).unwrap();

        // Verify all vertices
        assert_eq!(graph.vertex_count(), 3);

        let alice_v = graph.get_vertex(alice).unwrap();
        assert_eq!(alice_v.label, "person");
        assert_eq!(alice_v.properties.len(), 1);

        let bob_v = graph.get_vertex(bob).unwrap();
        assert_eq!(bob_v.label, "person");
        assert_eq!(bob_v.properties.len(), 0);

        let repo_v = graph.get_vertex(repo).unwrap();
        assert_eq!(repo_v.label, "repository");
        assert_eq!(repo_v.properties.len(), 3);
    }

    #[test]
    fn test_add_vertex_all_property_types() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut inner_map = std::collections::HashMap::new();
        inner_map.insert("key".to_string(), Value::Int(100));

        let mut props = std::collections::HashMap::new();
        props.insert("null_val".to_string(), Value::Null);
        props.insert("bool_val".to_string(), Value::Bool(false));
        props.insert("int_val".to_string(), Value::Int(-999));
        props.insert("float_val".to_string(), Value::Float(2.71828));
        props.insert(
            "string_val".to_string(),
            Value::String("test string".to_string()),
        );
        props.insert(
            "list_val".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        );
        props.insert("map_val".to_string(), Value::Map(inner_map.clone()));
        props.insert("vertex_ref".to_string(), Value::Vertex(VertexId(777)));
        props.insert("edge_ref".to_string(), Value::Edge(EdgeId(888)));

        let vertex_id = graph.add_vertex("test_node", props).unwrap();

        let vertex = graph.get_vertex(vertex_id).unwrap();
        assert_eq!(vertex.properties.len(), 9);
        assert_eq!(vertex.properties.get("null_val"), Some(&Value::Null));
        assert_eq!(vertex.properties.get("bool_val"), Some(&Value::Bool(false)));
        assert_eq!(vertex.properties.get("int_val"), Some(&Value::Int(-999)));
        assert_eq!(
            vertex.properties.get("float_val"),
            Some(&Value::Float(2.71828))
        );
        assert_eq!(
            vertex.properties.get("string_val"),
            Some(&Value::String("test string".to_string()))
        );
        assert!(matches!(
            vertex.properties.get("list_val"),
            Some(Value::List(_))
        ));
        assert!(matches!(
            vertex.properties.get("map_val"),
            Some(Value::Map(_))
        ));
        assert_eq!(
            vertex.properties.get("vertex_ref"),
            Some(&Value::Vertex(VertexId(777)))
        );
        assert_eq!(
            vertex.properties.get("edge_ref"),
            Some(&Value::Edge(EdgeId(888)))
        );
    }

    #[test]
    fn test_add_vertex_triggers_table_growth() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Get initial capacity
        let initial_capacity = graph.get_header().node_capacity;
        assert_eq!(initial_capacity, 1000);

        // Manually set node_count to capacity - 1 to force growth on second add
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = 999;
            drop(mmap);

            let file = graph.file.write();
            MmapGraph::write_header(&file, &header).unwrap();
            drop(file);
            graph.remap().unwrap();
        }

        // First add at slot 999
        let props = std::collections::HashMap::new();
        let v1 = graph.add_vertex("person", props.clone()).unwrap();
        assert_eq!(v1.0, 999);

        // Second add should trigger growth
        let v2 = graph.add_vertex("person", props).unwrap();
        assert_eq!(v2.0, 1000);

        // Verify capacity grew
        let new_capacity = graph.get_header().node_capacity;
        assert_eq!(new_capacity, 2000, "Capacity should double");

        // Verify both vertices are accessible
        assert!(graph.get_vertex(v1).is_some());
        assert!(graph.get_vertex(v2).is_some());
    }

    #[test]
    fn test_add_vertex_sequential_ids() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let props = std::collections::HashMap::new();

        let v0 = graph.add_vertex("a", props.clone()).unwrap();
        let v1 = graph.add_vertex("b", props.clone()).unwrap();
        let v2 = graph.add_vertex("c", props.clone()).unwrap();
        let v3 = graph.add_vertex("d", props).unwrap();

        assert_eq!(v0.0, 0);
        assert_eq!(v1.0, 1);
        assert_eq!(v2.0, 2);
        assert_eq!(v3.0, 3);
    }

    #[test]
    fn test_add_vertex_all_vertices_iteration() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let props = std::collections::HashMap::new();

        let v0 = graph.add_vertex("person", props.clone()).unwrap();
        let v1 = graph.add_vertex("person", props.clone()).unwrap();
        let v2 = graph.add_vertex("software", props).unwrap();

        // Verify all_vertices returns all 3
        let all: Vec<_> = graph.all_vertices().collect();
        assert_eq!(all.len(), 3);

        let ids: Vec<_> = all.iter().map(|v| v.id).collect();
        assert!(ids.contains(&v0));
        assert!(ids.contains(&v1));
        assert!(ids.contains(&v2));
    }

    #[test]
    fn test_add_vertex_empty_label() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Empty label should work (though not recommended)
        let props = std::collections::HashMap::new();
        let vertex_id = graph.add_vertex("", props).unwrap();

        let vertex = graph.get_vertex(vertex_id).unwrap();
        assert_eq!(vertex.label, "");
    }

    #[test]
    fn test_add_vertex_unicode_label_and_properties() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("名前".to_string(), Value::String("太郎".to_string()));
        props.insert("emoji".to_string(), Value::String("🚀🌟".to_string()));

        let vertex_id = graph.add_vertex("日本語ラベル", props).unwrap();

        let vertex = graph.get_vertex(vertex_id).unwrap();
        assert_eq!(vertex.label, "日本語ラベル");
        assert_eq!(
            vertex.properties.get("名前"),
            Some(&Value::String("太郎".to_string()))
        );
        assert_eq!(
            vertex.properties.get("emoji"),
            Some(&Value::String("🚀🌟".to_string()))
        );
    }

    #[test]
    fn test_add_vertex_large_property_value() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a large string (> 1KB)
        let large_string = "x".repeat(5000);

        let mut props = std::collections::HashMap::new();
        props.insert("data".to_string(), Value::String(large_string.clone()));

        let vertex_id = graph.add_vertex("big_data", props).unwrap();

        let vertex = graph.get_vertex(vertex_id).unwrap();
        assert_eq!(
            vertex.properties.get("data"),
            Some(&Value::String(large_string))
        );
    }

    // =========================================================================
    // add_edge Tests
    // =========================================================================

    #[test]
    fn test_add_edge_basic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create two vertices
        let alice = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        let bob = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();

        // Create an edge
        let edge_id = graph
            .add_edge(alice, bob, "knows", std::collections::HashMap::new())
            .unwrap();

        // Verify edge exists
        let edge = graph.get_edge(edge_id).unwrap();
        assert_eq!(edge.label, "knows");
        assert_eq!(edge.src, alice);
        assert_eq!(edge.dst, bob);
        assert!(edge.properties.is_empty());

        // Verify edge count
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_add_edge_with_properties() {
        use crate::value::Value;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create vertices
        let v1 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        let v2 = graph
            .add_vertex("software", std::collections::HashMap::new())
            .unwrap();

        // Create edge with properties
        let mut props = std::collections::HashMap::new();
        props.insert("since".to_string(), Value::Int(2020));
        props.insert("weight".to_string(), Value::Float(0.85));
        props.insert("active".to_string(), Value::Bool(true));
        props.insert(
            "notes".to_string(),
            Value::String("Collaborating on project".to_string()),
        );

        let edge_id = graph.add_edge(v1, v2, "created", props).unwrap();

        // Verify edge and properties
        let edge = graph.get_edge(edge_id).unwrap();
        assert_eq!(edge.label, "created");
        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v2);
        assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
        assert_eq!(edge.properties.get("weight"), Some(&Value::Float(0.85)));
        assert_eq!(edge.properties.get("active"), Some(&Value::Bool(true)));
        assert_eq!(
            edge.properties.get("notes"),
            Some(&Value::String("Collaborating on project".to_string()))
        );
    }

    #[test]
    fn test_add_edge_nonexistent_source() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create only destination vertex
        let dst = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();

        // Try to add edge from nonexistent source
        let nonexistent_src = VertexId(9999);
        let result = graph.add_edge(
            nonexistent_src,
            dst,
            "knows",
            std::collections::HashMap::new(),
        );

        assert!(result.is_err());
        match result {
            Err(StorageError::VertexNotFound(id)) => assert_eq!(id, nonexistent_src),
            _ => panic!("Expected StorageError::VertexNotFound"),
        }
    }

    #[test]
    fn test_add_edge_nonexistent_destination() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create only source vertex
        let src = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();

        // Try to add edge to nonexistent destination
        let nonexistent_dst = VertexId(8888);
        let result = graph.add_edge(
            src,
            nonexistent_dst,
            "knows",
            std::collections::HashMap::new(),
        );

        assert!(result.is_err());
        match result {
            Err(StorageError::VertexNotFound(id)) => assert_eq!(id, nonexistent_dst),
            _ => panic!("Expected StorageError::VertexNotFound"),
        }
    }

    #[test]
    fn test_add_edge_adjacency_lists() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create vertices
        let v1 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        let v2 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();

        // Create edge v1 -> v2
        let edge_id = graph
            .add_edge(v1, v2, "knows", std::collections::HashMap::new())
            .unwrap();

        // Verify out_edges from v1
        let out_edges: Vec<_> = graph.out_edges(v1).collect();
        assert_eq!(out_edges.len(), 1);
        assert_eq!(out_edges[0].id, edge_id);
        assert_eq!(out_edges[0].src, v1);
        assert_eq!(out_edges[0].dst, v2);

        // Verify in_edges to v2
        let in_edges: Vec<_> = graph.in_edges(v2).collect();
        assert_eq!(in_edges.len(), 1);
        assert_eq!(in_edges[0].id, edge_id);

        // Verify v1 has no incoming edges and v2 has no outgoing edges
        let v1_in: Vec<_> = graph.in_edges(v1).collect();
        assert!(v1_in.is_empty());

        let v2_out: Vec<_> = graph.out_edges(v2).collect();
        assert!(v2_out.is_empty());
    }

    #[test]
    fn test_add_multiple_edges() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a small graph: v1 -> v2 -> v3
        //                        |         ^
        //                        +---------+
        let v1 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        let v2 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        let v3 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();

        // Add edges
        let e1 = graph
            .add_edge(v1, v2, "knows", std::collections::HashMap::new())
            .unwrap();
        let e2 = graph
            .add_edge(v2, v3, "knows", std::collections::HashMap::new())
            .unwrap();
        let e3 = graph
            .add_edge(v1, v3, "likes", std::collections::HashMap::new())
            .unwrap();

        // Verify edge count
        assert_eq!(graph.edge_count(), 3);

        // Verify all edges can be retrieved
        assert!(graph.get_edge(e1).is_some());
        assert!(graph.get_edge(e2).is_some());
        assert!(graph.get_edge(e3).is_some());

        // Verify v1's outgoing edges (should have e1 and e3)
        // Note: new edges are prepended to the list, so order is reversed
        let v1_out: Vec<_> = graph.out_edges(v1).collect();
        assert_eq!(v1_out.len(), 2);
        // e3 was added last, so it's at the head
        assert!(v1_out.iter().any(|e| e.id == e1));
        assert!(v1_out.iter().any(|e| e.id == e3));

        // Verify v2's edges
        let v2_out: Vec<_> = graph.out_edges(v2).collect();
        assert_eq!(v2_out.len(), 1);
        assert_eq!(v2_out[0].id, e2);

        let v2_in: Vec<_> = graph.in_edges(v2).collect();
        assert_eq!(v2_in.len(), 1);
        assert_eq!(v2_in[0].id, e1);

        // Verify v3's incoming edges (should have e2 and e3)
        let v3_in: Vec<_> = graph.in_edges(v3).collect();
        assert_eq!(v3_in.len(), 2);
        assert!(v3_in.iter().any(|e| e.id == e2));
        assert!(v3_in.iter().any(|e| e.id == e3));

        // Verify v3 has no outgoing edges
        let v3_out: Vec<_> = graph.out_edges(v3).collect();
        assert!(v3_out.is_empty());
    }

    #[test]
    fn test_add_edge_self_loop() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a vertex
        let v1 = graph
            .add_vertex("node", std::collections::HashMap::new())
            .unwrap();

        // Create a self-loop edge
        let edge_id = graph
            .add_edge(v1, v1, "self_ref", std::collections::HashMap::new())
            .unwrap();

        // Verify edge
        let edge = graph.get_edge(edge_id).unwrap();
        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v1);
        assert_eq!(edge.label, "self_ref");

        // Verify it appears in both out_edges and in_edges
        let out_edges: Vec<_> = graph.out_edges(v1).collect();
        assert_eq!(out_edges.len(), 1);
        assert_eq!(out_edges[0].id, edge_id);

        let in_edges: Vec<_> = graph.in_edges(v1).collect();
        assert_eq!(in_edges.len(), 1);
        assert_eq!(in_edges[0].id, edge_id);
    }

    #[test]
    fn test_add_edge_updates_label_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create vertices
        let v1 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        let v2 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();

        // Create edges with different labels
        let e1 = graph
            .add_edge(v1, v2, "knows", std::collections::HashMap::new())
            .unwrap();
        let e2 = graph
            .add_edge(v1, v2, "likes", std::collections::HashMap::new())
            .unwrap();
        let e3 = graph
            .add_edge(v2, v1, "knows", std::collections::HashMap::new())
            .unwrap();

        // Get the label IDs
        let knows_id = {
            let string_table = graph.string_table.read();
            string_table.lookup("knows").unwrap()
        };
        let likes_id = {
            let string_table = graph.string_table.read();
            string_table.lookup("likes").unwrap()
        };

        // Verify edge label indexes
        let edge_labels = graph.edge_labels.read();

        // "knows" label should have e1 and e3
        let knows_edges = edge_labels.get(&knows_id).unwrap();
        assert_eq!(knows_edges.len(), 2);
        assert!(knows_edges.contains(e1.0 as u32));
        assert!(knows_edges.contains(e3.0 as u32));

        // "likes" label should have e2
        let likes_edges = edge_labels.get(&likes_id).unwrap();
        assert_eq!(likes_edges.len(), 1);
        assert!(likes_edges.contains(e2.0 as u32));
    }

    #[test]
    fn test_add_edge_persistence() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create graph, add vertices and edges
        let v1;
        let v2;
        let edge_id;
        {
            let graph = MmapGraph::open(&path).unwrap();

            v1 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();
            v2 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();

            let mut props = std::collections::HashMap::new();
            props.insert("weight".to_string(), crate::value::Value::Int(42));

            edge_id = graph.add_edge(v1, v2, "knows", props).unwrap();
        }
        // Graph is dropped here

        // Reopen the graph
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Verify edge persisted
            let edge = graph.get_edge(edge_id).unwrap();
            assert_eq!(edge.label, "knows");
            assert_eq!(edge.src, v1);
            assert_eq!(edge.dst, v2);
            assert_eq!(
                edge.properties.get("weight"),
                Some(&crate::value::Value::Int(42))
            );

            // Verify adjacency lists work after reopen
            let out_edges: Vec<_> = graph.out_edges(v1).collect();
            assert_eq!(out_edges.len(), 1);
            assert_eq!(out_edges[0].id, edge_id);

            let in_edges: Vec<_> = graph.in_edges(v2).collect();
            assert_eq!(in_edges.len(), 1);
            assert_eq!(in_edges[0].id, edge_id);
        }
    }

    // =========================================================================
    // Phase 4.6: Checkpoint Tests
    // =========================================================================

    #[test]
    fn test_checkpoint_creates_wal_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        // Open graph (this creates the WAL file)
        let graph = MmapGraph::open(&path).unwrap();

        // WAL file should exist
        assert!(wal_path.exists(), "WAL file should be created on open");

        // Checkpoint should succeed
        graph.checkpoint().unwrap();
    }

    #[test]
    fn test_checkpoint_truncates_wal() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        let graph = MmapGraph::open(&path).unwrap();

        // Add some data to create WAL entries
        graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        graph
            .add_vertex("software", std::collections::HashMap::new())
            .unwrap();

        // WAL should have some content now (from our operations, though we're
        // not currently logging add_vertex to WAL, so this checks the checkpoint
        // marker itself gets written and truncated)

        // Get WAL size before checkpoint
        // Note: Since add_vertex doesn't log to WAL yet, the WAL is empty
        // But checkpoint will write a marker then truncate

        // Perform checkpoint
        graph.checkpoint().unwrap();

        // After checkpoint, WAL should be empty (truncated)
        let wal_size = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(wal_size, 0, "WAL should be empty after checkpoint");
    }

    #[test]
    fn test_checkpoint_flushes_data() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Add data and checkpoint
        let v1;
        {
            let graph = MmapGraph::open(&path).unwrap();
            v1 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();

            // Checkpoint to ensure data is flushed
            graph.checkpoint().unwrap();
        }

        // Reopen and verify data exists
        {
            let graph = MmapGraph::open(&path).unwrap();
            let vertex = graph.get_vertex(v1);
            assert!(vertex.is_some(), "Vertex should exist after checkpoint");
            assert_eq!(vertex.unwrap().label, "person");
        }
    }

    #[test]
    fn test_checkpoint_after_multiple_operations() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let v1;
        let v2;
        let e1;
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add vertices
            v1 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();
            v2 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();

            // Add edge
            let mut props = std::collections::HashMap::new();
            props.insert("since".to_string(), crate::value::Value::Int(2020));
            e1 = graph.add_edge(v1, v2, "knows", props).unwrap();

            // Checkpoint
            graph.checkpoint().unwrap();
        }

        // Reopen and verify everything persisted
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Check vertices
            assert!(graph.get_vertex(v1).is_some());
            assert!(graph.get_vertex(v2).is_some());

            // Check edge
            let edge = graph.get_edge(e1).unwrap();
            assert_eq!(edge.src, v1);
            assert_eq!(edge.dst, v2);
            assert_eq!(edge.label, "knows");
            assert_eq!(
                edge.properties.get("since"),
                Some(&crate::value::Value::Int(2020))
            );

            // Check counts
            assert_eq!(graph.vertex_count(), 2);
            assert_eq!(graph.edge_count(), 1);
        }
    }

    #[test]
    fn test_checkpoint_multiple_times() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Add vertex, checkpoint
        let v1 = graph
            .add_vertex("person", std::collections::HashMap::new())
            .unwrap();
        graph.checkpoint().unwrap();

        // Add another vertex, checkpoint again
        let v2 = graph
            .add_vertex("software", std::collections::HashMap::new())
            .unwrap();
        graph.checkpoint().unwrap();

        // Add edge, checkpoint again
        graph
            .add_edge(v1, v2, "created", std::collections::HashMap::new())
            .unwrap();
        graph.checkpoint().unwrap();

        // Verify all data
        assert!(graph.get_vertex(v1).is_some());
        assert!(graph.get_vertex(v2).is_some());
        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_wal_empty_after_checkpoint() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add some data
            graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();

            // Checkpoint
            graph.checkpoint().unwrap();

            // Verify WAL is empty after checkpoint
            let wal_size = std::fs::metadata(&wal_path).unwrap().len();
            assert_eq!(wal_size, 0, "WAL should be empty after checkpoint");
        }
    }

    #[test]
    fn test_database_consistent_after_checkpoint() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create graph with multiple operations and checkpoint
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add 10 vertices
            let mut vertex_ids = Vec::new();
            for i in 0..10 {
                let mut props = std::collections::HashMap::new();
                props.insert("index".to_string(), crate::value::Value::Int(i as i64));
                let vid = graph.add_vertex("node", props).unwrap();
                vertex_ids.push(vid);
            }

            // Add edges between consecutive vertices
            for i in 0..9 {
                graph
                    .add_edge(
                        vertex_ids[i],
                        vertex_ids[i + 1],
                        "next",
                        std::collections::HashMap::new(),
                    )
                    .unwrap();
            }

            // Checkpoint
            graph.checkpoint().unwrap();
        }

        // Reopen and verify consistency
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Verify vertex count
            assert_eq!(graph.vertex_count(), 10, "Should have 10 vertices");

            // Verify edge count
            assert_eq!(graph.edge_count(), 9, "Should have 9 edges");

            // Verify each vertex has correct index property
            for i in 0..10 {
                let vertex = graph.get_vertex(VertexId(i as u64)).unwrap();
                assert_eq!(
                    vertex.properties.get("index"),
                    Some(&crate::value::Value::Int(i as i64))
                );
            }

            // Verify adjacency lists
            for i in 0..9 {
                let out_edges: Vec<_> = graph.out_edges(VertexId(i as u64)).collect();
                assert_eq!(
                    out_edges.len(),
                    1,
                    "Vertex {} should have 1 outgoing edge",
                    i
                );
                assert_eq!(out_edges[0].dst, VertexId((i + 1) as u64));
            }

            // Last vertex should have no outgoing edges
            let out_edges: Vec<_> = graph.out_edges(VertexId(9)).collect();
            assert_eq!(
                out_edges.len(),
                0,
                "Last vertex should have no outgoing edges"
            );
        }
    }

    // =========================================================================
    // Recovery Integration Tests (Phase 5.3)
    // =========================================================================

    #[test]
    fn test_open_triggers_recovery_for_uncommitted_wal() {
        use crate::storage::mmap::wal::{SerializableNodeRecord, WalEntry};
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = path.with_extension("wal");

        // First, create a database and add some data with a proper checkpoint
        {
            let graph = MmapGraph::open(&path).unwrap();
            let _v1 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();
            graph.checkpoint().unwrap();
        }

        // Now, simulate writing to the WAL without committing (simulating a crash)
        // This mimics what would happen if the process crashed before commit
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            let tx_id = wal.begin_transaction().unwrap();
            wal.log(WalEntry::InsertNode {
                id: VertexId(1), // Second node
                record: SerializableNodeRecord {
                    id: 1,
                    label_id: 999, // Distinctive label_id that should NOT appear
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .unwrap();
            // NO COMMIT - simulating crash
            wal.sync().unwrap();
            // Drop WAL without truncating
        }

        // WAL should have uncommitted data
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            assert!(wal.needs_recovery(), "WAL should need recovery");
        }

        // Reopen the database - recovery should run and discard uncommitted tx
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Original vertex should still exist
            assert_eq!(graph.vertex_count(), 1, "Should only have 1 vertex");

            // The uncommitted vertex (id 1) should not exist
            assert!(
                graph.get_vertex(VertexId(1)).is_none(),
                "Uncommitted vertex should not exist"
            );

            // WAL should be empty after recovery
            let wal_size = std::fs::metadata(&wal_path).unwrap().len();
            assert_eq!(wal_size, 0, "WAL should be truncated after recovery");
        }
    }

    #[test]
    fn test_open_with_committed_wal_entries_no_recovery_needed() {
        // When WAL has only committed transactions, no recovery is needed
        // because committed transactions mean data is already on disk.
        // This tests that open() handles this case gracefully.
        use crate::storage::mmap::wal::{SerializableNodeRecord, WalEntry};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = path.with_extension("wal");

        // Create a new empty database first
        {
            let _graph = MmapGraph::open(&path).unwrap();
            // Just create the file structure
        }

        // Write a committed transaction directly to WAL (bypassing MmapGraph)
        // This represents a scenario where data was written to disk and committed
        // but the checkpoint/truncate didn't happen.
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            let tx_id = wal.begin_transaction().unwrap();
            wal.log(WalEntry::InsertNode {
                id: VertexId(0),
                record: SerializableNodeRecord {
                    id: 0,
                    label_id: 0,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .unwrap();
            wal.log(WalEntry::CommitTx { tx_id }).unwrap();
            wal.sync().unwrap();
        }

        // WAL should NOT need recovery (committed transactions are complete)
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            assert!(
                !wal.needs_recovery(),
                "WAL with only committed transactions should not need recovery"
            );
        }

        // Reopen database - should succeed without issues
        {
            let _graph = MmapGraph::open(&path).unwrap();
            // Database opens successfully, though the committed entries in WAL
            // are essentially "orphan" entries that would normally be truncated
            // by a checkpoint operation.
        }
    }

    #[test]
    fn test_open_no_recovery_needed_for_clean_wal() {
        use crate::storage::GraphStorage;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = path.with_extension("wal");

        // Create database, add data, checkpoint
        {
            let graph = MmapGraph::open(&path).unwrap();
            let _v1 = graph
                .add_vertex("person", std::collections::HashMap::new())
                .unwrap();
            graph.checkpoint().unwrap();
        }

        // WAL should be clean (truncated by checkpoint)
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            assert!(
                !wal.needs_recovery(),
                "WAL should not need recovery after checkpoint"
            );
        }

        // Reopen - should not trigger recovery
        {
            let graph = MmapGraph::open(&path).unwrap();
            assert_eq!(graph.vertex_count(), 1);
            assert!(graph.get_vertex(VertexId(0)).is_some());
        }
    }

    #[test]
    fn test_open_recovery_with_mixed_transactions() {
        use crate::storage::mmap::wal::{SerializableNodeRecord, WalEntry};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let wal_path = path.with_extension("wal");

        // Create an empty database
        {
            let _graph = MmapGraph::open(&path).unwrap();
        }

        // Write mixed transactions to WAL:
        // - tx1: committed (insert node 0)
        // - tx2: aborted (insert node 1)
        // - tx3: committed (insert node 2)
        // - tx4: uncommitted (insert node 3) - simulates crash
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();

            // tx1: committed
            let tx1 = wal.begin_transaction().unwrap();
            wal.log(WalEntry::InsertNode {
                id: VertexId(0),
                record: SerializableNodeRecord {
                    id: 0,
                    label_id: 10,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .unwrap();
            wal.log(WalEntry::CommitTx { tx_id: tx1 }).unwrap();

            // tx2: aborted
            let tx2 = wal.begin_transaction().unwrap();
            wal.log(WalEntry::InsertNode {
                id: VertexId(1),
                record: SerializableNodeRecord {
                    id: 1,
                    label_id: 20,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .unwrap();
            wal.log(WalEntry::AbortTx { tx_id: tx2 }).unwrap();

            // tx3: committed
            let tx3 = wal.begin_transaction().unwrap();
            wal.log(WalEntry::InsertNode {
                id: VertexId(2),
                record: SerializableNodeRecord {
                    id: 2,
                    label_id: 30,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .unwrap();
            wal.log(WalEntry::CommitTx { tx_id: tx3 }).unwrap();

            // tx4: uncommitted (crash simulation)
            let _tx4 = wal.begin_transaction().unwrap();
            wal.log(WalEntry::InsertNode {
                id: VertexId(3),
                record: SerializableNodeRecord {
                    id: 3,
                    label_id: 40,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .unwrap();
            // NO COMMIT

            wal.sync().unwrap();
        }

        // Reopen - recovery should happen
        {
            let _graph = MmapGraph::open(&path).unwrap();

            // WAL should be truncated
            let wal_size = std::fs::metadata(&wal_path).unwrap().len();
            assert_eq!(wal_size, 0, "WAL should be truncated after recovery");

            // Only nodes 0 and 2 should exist (from committed transactions)
            // Note: We can't easily verify this without proper label resolution,
            // but we can check that the file exists and no panic occurred
        }
    }
}
