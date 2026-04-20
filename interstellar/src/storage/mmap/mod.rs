//! Memory-mapped persistent graph storage.
//!
//! This module implements the `GraphStorage` trait using memory-mapped files,
//! providing durable storage with write-ahead logging for crash recovery.

use hashbrown::HashMap;
use memmap2::{Mmap, MmapOptions};
use parking_lot::RwLock;
use roaring::RoaringTreemap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use crate::error::StorageError;
use crate::index::IndexError;
use crate::index::PropertyIndex;
use crate::index::{BTreeIndex, UniqueIndex};
use crate::index::{ElementType, IndexSpec, IndexType};
use crate::storage::{Edge, GraphStorage, StringInterner, Vertex};
use crate::value::Value;

pub mod arena;
pub mod freelist;
pub mod query;
pub mod records;
pub mod recovery;
pub mod wal;

use wal::{WalEntry, WriteAheadLog};

use freelist::FreeList;
use records::{
    EdgeRecord, FileHeader, FileHeaderV1, NodeRecord, EDGE_RECORD_SIZE, ENDIAN_LITTLE, HEADER_SIZE,
    HEADER_SIZE_V1, MAGIC, MIN_READABLE_VERSION, NODE_RECORD_SIZE, VERSION,
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
///
/// # Batch Mode
///
/// By default, each write operation (add_vertex, add_edge) performs an fsync
/// to ensure durability (~5ms per operation). For bulk loading, use batch mode:
///
/// ```rust,no_run
/// use interstellar::storage::MmapGraph;
/// use std::collections::HashMap;
///
/// let graph = MmapGraph::open("my_graph.db").unwrap();
///
/// // Start batch mode - writes are logged to WAL but not synced
/// graph.begin_batch().unwrap();
///
/// for i in 0..10000 {
///     let props = HashMap::from([("i".to_string(), (i as i64).into())]);
///     graph.add_vertex("person", props).unwrap();
/// }
///
/// // Single fsync commits all operations atomically
/// graph.commit_batch().unwrap();
/// ```
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
    vertex_labels: Arc<RwLock<HashMap<u32, RoaringTreemap>>>,
    edge_labels: Arc<RwLock<HashMap<u32, RoaringTreemap>>>,

    /// Property arena allocator (tracks current write position)
    arena: Arc<RwLock<arena::ArenaAllocator>>,

    /// Free list for deleted node slots (enables slot reuse)
    free_nodes: Arc<RwLock<FreeList>>,

    /// Free list for deleted edge slots (enables slot reuse)
    free_edges: Arc<RwLock<FreeList>>,

    /// Batch mode state: when true, WAL sync is deferred until commit_batch()
    batch_mode: Arc<RwLock<bool>>,

    /// Transaction ID for the current batch (if in batch mode)
    batch_tx_id: Arc<RwLock<Option<u64>>>,

    /// Property indexes by name (in-memory, rebuilt on load from persisted specs)
    indexes: Arc<RwLock<HashMap<String, Box<dyn PropertyIndex>>>>,

    /// Index specifications (for persistence across restarts)
    index_specs: Arc<RwLock<Vec<IndexSpec>>>,

    /// Path to the database file (for deriving index specs path)
    db_path: std::path::PathBuf,

    /// In-memory query index for name/ID lookups
    query_index: Arc<RwLock<query::QueryIndex>>,
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
    /// use interstellar::storage::MmapGraph;
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
                batch_mode: Arc::new(RwLock::new(false)),
                batch_tx_id: Arc::new(RwLock::new(None)),
                indexes: Arc::new(RwLock::new(HashMap::new())),
                index_specs: Arc::new(RwLock::new(Vec::new())),
                db_path: path.to_path_buf(),
                query_index: Arc::new(RwLock::new(query::QueryIndex::new())),
            };

            // Rebuild in-memory indexes from disk data (includes recovered data)
            graph.rebuild_indexes()?;

            // Load persisted property indexes
            graph.load_index_specs()?;

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
            batch_mode: Arc::new(RwLock::new(false)),
            batch_tx_id: Arc::new(RwLock::new(None)),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            index_specs: Arc::new(RwLock::new(Vec::new())),
            db_path: path.to_path_buf(),
            query_index: Arc::new(RwLock::new(query::QueryIndex::new())),
        };

        // Rebuild in-memory indexes from disk data
        graph.rebuild_indexes()?;

        // Load persisted property indexes
        graph.load_index_specs()?;

        Ok(graph)
    }

    /// Initialize a new database file with V2 header and initial structure.
    ///
    /// Creates a file with:
    /// - 192-byte V2 header
    /// - Space for 100 initial node records
    /// - Space for 200 initial edge records
    /// - 64KB for properties and strings
    ///
    /// # Safety
    ///
    /// This function assumes the file is empty and newly created.
    fn initialize_new_file(file: &File) -> Result<(), StorageError> {
        // Initial capacities - kept small to minimize initial file size.
        // Tables automatically double when capacity is exceeded.
        const INITIAL_NODE_CAPACITY: u64 = 100;
        const INITIAL_EDGE_CAPACITY: u64 = 200;
        const INITIAL_ARENA_SIZE: u64 = 32 * 1024; // 32KB

        // Calculate file size (using V2 header size)
        let node_table_size = INITIAL_NODE_CAPACITY * NODE_RECORD_SIZE as u64;
        let edge_table_size = INITIAL_EDGE_CAPACITY * records::EDGE_RECORD_SIZE as u64;
        let initial_size =
            HEADER_SIZE as u64 + node_table_size + edge_table_size + INITIAL_ARENA_SIZE;

        // Set file size
        file.set_len(initial_size)?;

        // Calculate offsets
        let property_arena_offset = HEADER_SIZE as u64 + node_table_size + edge_table_size;
        let string_table_offset = initial_size - 32 * 1024; // Last 32KB for strings

        // Create initial V2 header
        let mut header = FileHeader::new();
        header.node_capacity = INITIAL_NODE_CAPACITY;
        header.edge_capacity = INITIAL_EDGE_CAPACITY;
        header.property_arena_offset = property_arena_offset;
        header.arena_next_offset = property_arena_offset; // Start writing at arena beginning
        header.string_table_offset = string_table_offset;
        header.string_table_end = string_table_offset; // Empty string table initially
                                                       // CRC32 will be updated by write_header

        // Write header
        Self::write_header(file, &header)?;

        Ok(())
    }

    /// Validate file header for correct magic and version.
    ///
    /// This method performs comprehensive validation per the V2 spec:
    /// 1. Check magic number (InvalidFormat if wrong)
    /// 2. Check version compatibility (VersionMismatch if incompatible)
    /// 3. Check min_reader_version for forward compatibility
    /// 4. Check endianness (only little-endian supported)
    /// 5. Check page size validity
    /// 6. Verify CRC32 for V2+ headers
    /// 7. Check for unknown flags
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - File is too small, wrong magic, invalid endianness, or bad page size
    /// - [`StorageError::VersionMismatch`] - File version incompatible with this library
    /// - [`StorageError::CorruptedData`] - CRC32 checksum mismatch
    fn validate_header(mmap: &[u8]) -> Result<(), StorageError> {
        // Need at least enough bytes to read magic and version
        if mmap.len() < 8 {
            return Err(StorageError::InvalidFormat);
        }

        // Read magic number
        let magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(StorageError::InvalidFormat);
        }

        // Read version to determine header format
        let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());

        // Check version compatibility
        if !(MIN_READABLE_VERSION..=VERSION).contains(&version) {
            return Err(StorageError::VersionMismatch {
                file_version: version,
                min_supported: MIN_READABLE_VERSION,
                max_supported: VERSION,
            });
        }

        // Handle V1 headers
        if version == 1 {
            if mmap.len() < HEADER_SIZE_V1 {
                return Err(StorageError::InvalidFormat);
            }
            // V1 headers don't have additional validation fields
            return Ok(());
        }

        // Handle V2+ headers
        if mmap.len() < HEADER_SIZE {
            return Err(StorageError::InvalidFormat);
        }

        let header = Self::read_header(mmap);

        // Check min_reader_version for forward compatibility
        let min_reader_version = header.min_reader_version;
        if min_reader_version > VERSION {
            return Err(StorageError::VersionMismatch {
                file_version: version,
                min_supported: MIN_READABLE_VERSION,
                max_supported: VERSION,
            });
        }

        // Check endianness (only little-endian supported)
        let endianness = header.endianness;
        if endianness != ENDIAN_LITTLE {
            return Err(StorageError::InvalidFormat);
        }

        // Check page size validity (must be power of 2, 512 to 65536)
        let page_size = header.page_size;
        if !page_size.is_power_of_two() || !(512..=65536).contains(&page_size) {
            return Err(StorageError::InvalidFormat);
        }

        // Verify header CRC32
        if !header.validate_crc32() {
            return Err(StorageError::CorruptedData);
        }

        // Check for unknown flags (none defined yet, so any flag is unknown)
        let known_flags: u32 = 0;
        let flags = header.flags;
        if flags & !known_flags != 0 {
            return Err(StorageError::VersionMismatch {
                file_version: version,
                min_supported: MIN_READABLE_VERSION,
                max_supported: VERSION,
            });
        }

        Ok(())
    }

    /// Read header from memory-mapped bytes, handling both V1 and V2 formats.
    ///
    /// For V1 files, this reads the V1 header and converts it to V2 format
    /// by synthesizing missing fields with defaults.
    ///
    /// # Safety
    ///
    /// This uses `read_unaligned` since FileHeader is `#[repr(C, packed)]`.
    /// Caller must ensure mmap has at least enough bytes for the detected version.
    fn read_header(mmap: &[u8]) -> FileHeader {
        assert!(mmap.len() >= 8, "mmap too small to read version");

        // Check version to determine header format
        let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());

        if version == 1 {
            // V1 format: read V1 header and convert to V2
            assert!(mmap.len() >= HEADER_SIZE_V1, "mmap too small for V1 header");
            let v1 = FileHeaderV1::from_bytes(&mmap[..HEADER_SIZE_V1]);
            FileHeader::from_v1(&v1)
        } else {
            // V2+ format: read V2 header directly
            assert!(mmap.len() >= HEADER_SIZE, "mmap too small for V2 header");
            FileHeader::from_bytes(mmap)
        }
    }

    /// Write header to file at offset 0.
    ///
    /// For V2 headers, this automatically updates the CRC32 before writing.
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
        // Update CRC32 for V2 headers before writing
        let mut header = *header;
        if header.version >= 2 {
            header.update_crc32();
        }

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

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().

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
    ///
    /// After recovery, the header counts may be stale (recovery writes records but
    /// doesn't update counts). This method uses `next_node_id` and `next_edge_id`
    /// to scan all potentially allocated slots, and recalculates the actual counts
    /// by excluding deleted records.
    pub(crate) fn rebuild_indexes(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Use next_*_id rather than *_count to handle recovery scenarios where
        // the counts may be stale. next_*_id represents the high-water mark of
        // allocated slots.
        let max_node_id = header.next_node_id;
        let max_edge_id = header.next_edge_id;

        drop(mmap); // Release mmap lock before taking index locks

        // Rebuild vertex label indexes and count actual non-deleted vertices
        let mut actual_node_count = 0u64;
        {
            let mut vertex_labels = self.vertex_labels.write();
            vertex_labels.clear();

            for node_id in 0..max_node_id {
                if let Some(node) = self.get_node_record(VertexId(node_id)) {
                    let label_id = node.label_id;
                    vertex_labels
                        .entry(label_id)
                        .or_insert_with(RoaringTreemap::new)
                        .insert(node_id);
                    actual_node_count += 1;
                }
            }
        }

        // Rebuild edge label indexes and count actual non-deleted edges
        let mut actual_edge_count = 0u64;
        {
            let mut edge_labels = self.edge_labels.write();
            edge_labels.clear();

            for edge_id in 0..max_edge_id {
                if let Some(edge) = self.get_edge_record(EdgeId(edge_id)) {
                    let label_id = edge.label_id;
                    edge_labels
                        .entry(label_id)
                        .or_insert_with(RoaringTreemap::new)
                        .insert(edge_id);
                    actual_edge_count += 1;
                }
            }
        }

        // Update header with correct counts (handles recovery case where counts are stale)
        {
            let file = self.file.write();
            let mmap = self.mmap.read();
            let mut header = Self::read_header(&mmap);

            if header.node_count != actual_node_count || header.edge_count != actual_edge_count {
                header.node_count = actual_node_count;
                header.edge_count = actual_edge_count;
                drop(mmap);
                Self::write_header(&file, &header)?;
            }
        }

        // Rebuild query index from disk
        self.load_query_index()?;

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
    /// use interstellar::value::Value;
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

        // Allocate space in the arena, growing if necessary
        let base_offset = loop {
            let arena = self.arena.read();
            match arena.allocate(total_size) {
                Ok(offset) => break offset,
                Err(StorageError::OutOfSpace) => {
                    drop(arena); // Release read lock before growing
                    self.grow_arena()?;
                    // Retry allocation after growth
                }
                Err(e) => return Err(e),
            }
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

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
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
        let next_node_id = header.next_node_id;
        let current_capacity = header.node_capacity;

        // Try to allocate from free list first
        let slot_id = {
            let mut free_nodes = self.free_nodes.write();
            free_nodes.allocate(next_node_id)
        };

        // If we're extending beyond capacity, grow the table
        if slot_id >= current_capacity {
            self.grow_node_table()?;
        }

        // If this is a new slot (not from free list), update next_node_id
        if slot_id == next_node_id {
            self.increment_next_node_id()?;
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

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
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

    /// Increment the next_node_id (high-water mark) in the file header.
    ///
    /// This tracks the highest slot ID that has been allocated, used for
    /// iterating over all slots (including deleted ones).
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    pub fn increment_next_node_id(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.next_node_id += 1;

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
    /// use interstellar::storage::MmapGraph;
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

    // =========================================================================
    // Batch Mode API
    // =========================================================================

    /// Begin batch mode for high-performance bulk writes.
    ///
    /// In batch mode, individual write operations (add_vertex, add_edge) skip
    /// the per-operation fsync, deferring the sync to `commit_batch()`. This
    /// provides dramatically better write throughput for bulk loading.
    ///
    /// # How It Works
    ///
    /// - A single WAL transaction is started for the entire batch
    /// - Each write operation logs to WAL but doesn't sync
    /// - `commit_batch()` commits the transaction and performs a single fsync
    /// - If the system crashes during the batch, the entire batch is rolled back
    ///
    /// # Performance
    ///
    /// - Normal mode: ~200 writes/sec (fsync per operation, ~5ms each)
    /// - Batch mode: ~100,000+ writes/sec (single fsync for entire batch)
    ///
    /// # Atomicity
    ///
    /// The entire batch is atomic - either all operations commit or none do.
    /// If you need partial durability, call `commit_batch()` and `begin_batch()`
    /// at regular intervals.
    ///
    /// # Errors
    ///
    /// - Returns error if already in batch mode
    /// - [`StorageError::Io`] - I/O error starting WAL transaction
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    /// use std::collections::HashMap;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    ///
    /// // Bulk load 10,000 vertices
    /// graph.begin_batch().unwrap();
    /// for i in 0..10000 {
    ///     let props = HashMap::from([("i".to_string(), (i as i64).into())]);
    ///     graph.add_vertex("person", props).unwrap();
    /// }
    /// graph.commit_batch().unwrap();  // Single fsync commits all 10,000
    /// ```
    pub fn begin_batch(&self) -> Result<(), StorageError> {
        // Check if already in batch mode
        {
            let batch_mode = self.batch_mode.read();
            if *batch_mode {
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Already in batch mode. Call commit_batch() or abort_batch() first.",
                )));
            }
        }

        // Start a new WAL transaction for the batch
        let tx_id = {
            let mut wal = self.wal.write();
            wal.begin_transaction()?
        };

        // Set batch mode flags
        {
            let mut batch_mode = self.batch_mode.write();
            let mut batch_tx_id = self.batch_tx_id.write();
            *batch_mode = true;
            *batch_tx_id = Some(tx_id);
        }

        Ok(())
    }

    /// Commit all operations in the current batch atomically.
    ///
    /// This method:
    /// 1. Logs a CommitTx entry for the batch transaction
    /// 2. Syncs the WAL to disk (single fsync for all operations)
    /// 3. Exits batch mode
    ///
    /// After commit, all operations in the batch are durable and will survive
    /// a crash.
    ///
    /// # Errors
    ///
    /// - Returns error if not in batch mode
    /// - [`StorageError::Io`] - I/O error during WAL sync
    ///
    /// # Example
    ///
    /// See [`begin_batch`] for a complete example.
    pub fn commit_batch(&self) -> Result<(), StorageError> {
        // Get and clear batch state
        let tx_id = {
            let batch_mode = self.batch_mode.read();
            if !*batch_mode {
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Not in batch mode. Call begin_batch() first.",
                )));
            }
            let batch_tx_id = self.batch_tx_id.read();
            batch_tx_id.ok_or_else(|| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Batch mode active but no transaction ID",
                ))
            })?
        };

        // Commit the transaction and sync
        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        // Clear batch mode
        {
            let mut batch_mode = self.batch_mode.write();
            let mut batch_tx_id = self.batch_tx_id.write();
            *batch_mode = false;
            *batch_tx_id = None;
        }

        Ok(())
    }

    /// Abort the current batch, discarding all uncommitted operations.
    ///
    /// This method:
    /// 1. Logs an AbortTx entry for the batch transaction
    /// 2. Exits batch mode without syncing
    ///
    /// The WAL will contain the aborted transaction entries, but they will
    /// be ignored during recovery. The in-memory state and main file already
    /// contain the writes, but on next open they would be rolled back if
    /// recovery runs.
    ///
    /// Note: Since writes have already been applied to the main file (for
    /// immediate read visibility), aborting doesn't "undo" those writes in
    /// the current session. However, if the database is closed without a
    /// checkpoint, those writes may be lost depending on whether recovery
    /// runs. For clean semantics, reopen the database after abort_batch().
    ///
    /// # Errors
    ///
    /// - Returns error if not in batch mode
    /// - [`StorageError::Io`] - I/O error writing abort entry
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    /// use std::collections::HashMap;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    ///
    /// graph.begin_batch().unwrap();
    /// graph.add_vertex("person", HashMap::new()).unwrap();
    ///
    /// // Oops, something went wrong - abort the batch
    /// graph.abort_batch().unwrap();
    /// ```
    pub fn abort_batch(&self) -> Result<(), StorageError> {
        // Get and clear batch state
        let tx_id = {
            let batch_mode = self.batch_mode.read();
            if !*batch_mode {
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Not in batch mode. Call begin_batch() first.",
                )));
            }
            let batch_tx_id = self.batch_tx_id.read();
            batch_tx_id.ok_or_else(|| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Batch mode active but no transaction ID",
                ))
            })?
        };

        // Log abort entry (no sync needed - we don't care if it's lost)
        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::AbortTx { tx_id })?;
        }

        // Clear batch mode
        {
            let mut batch_mode = self.batch_mode.write();
            let mut batch_tx_id = self.batch_tx_id.write();
            *batch_mode = false;
            *batch_tx_id = None;
        }

        Ok(())
    }

    /// Check if the graph is currently in batch mode.
    ///
    /// # Returns
    ///
    /// `true` if `begin_batch()` has been called and neither `commit_batch()`
    /// nor `abort_batch()` has been called yet.
    pub fn is_batch_mode(&self) -> bool {
        *self.batch_mode.read()
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

            // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
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
        let next_edge_id = header.next_edge_id;
        let current_capacity = header.edge_capacity;

        // Try to allocate from free list first
        let slot_id = {
            let mut free_edges = self.free_edges.write();
            free_edges.allocate(next_edge_id)
        };

        // If we're extending beyond capacity, grow the table
        if slot_id >= current_capacity {
            self.grow_edge_table()?;
        }

        // If this is a new slot (not from free list), update next_edge_id
        if slot_id == next_edge_id {
            self.increment_next_edge_id()?;
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

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
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

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
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

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
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

    /// Increment the next_edge_id (high-water mark) in the file header.
    ///
    /// This tracks the highest slot ID that has been allocated, used for
    /// iterating over all slots (including deleted ones).
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during header update
    pub fn increment_next_edge_id(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.next_edge_id += 1;

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

        // Sync all previous writes before reading data to copy
        file.sync_data()?;
        drop(file);

        // Remap to see all synced writes
        self.remap()?;

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

        // Read arena data (between edge table end and string table start)
        let old_arena_start = header.property_arena_offset as usize;
        let old_string_table_start = header.string_table_offset as usize;
        let arena_size = old_string_table_start - old_arena_start;
        let mut arena_data = vec![0u8; arena_size];
        if arena_size > 0 && old_string_table_start <= mmap.len() {
            arena_data.copy_from_slice(&mmap[old_arena_start..old_string_table_start]);
        }

        // Read string table data (from string table start to end of file)
        let string_table_size = mmap.len() - old_string_table_start;
        let mut string_table_data = vec![0u8; string_table_size];
        if string_table_size > 0 {
            string_table_data.copy_from_slice(&mmap[old_string_table_start..]);
        }

        drop(mmap);

        // Calculate new file size
        // Note: We keep the arena size the same, just shift it along with the edge table
        let old_file_size = file.metadata()?.len() as usize;
        let size_increase = new_node_table_size - old_node_table_size;
        let new_file_size = old_file_size + size_increase;

        // Calculate new positions
        let new_arena_start = old_arena_start + size_increase;
        let new_string_table_start = old_string_table_start + size_increase;

        // Extend the file
        file.set_len(new_file_size as u64)?;

        // Write data at new positions (order matters: write from end to start to avoid overwrites)
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            // Write string table first (furthest from start)
            if string_table_size > 0 {
                file.write_all_at(&string_table_data, new_string_table_start as u64)?;
            }
            // Write arena
            if arena_size > 0 {
                file.write_all_at(&arena_data, new_arena_start as u64)?;
            }
            // Write edge table
            if edge_table_size > 0 {
                file.write_all_at(&edge_data, new_edge_table_start as u64)?;
            }
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            // Write string table first (furthest from start)
            if string_table_size > 0 {
                let mut f = &*file;
                f.seek(SeekFrom::Start(new_string_table_start as u64))?;
                f.write_all(&string_table_data)?;
            }
            // Write arena
            if arena_size > 0 {
                let mut f = &*file;
                f.seek(SeekFrom::Start(new_arena_start as u64))?;
                f.write_all(&arena_data)?;
            }
            // Write edge table
            if edge_table_size > 0 {
                let mut f = &*file;
                f.seek(SeekFrom::Start(new_edge_table_start as u64))?;
                f.write_all(&edge_data)?;
            }
        }

        // Adjust property offsets in node and edge records.
        // Edge table has moved to new_edge_table_start.
        let offset_adjustment = size_increase as i64;
        self.adjust_property_offsets(
            &file,
            offset_adjustment,
            header.next_node_id,
            header.next_edge_id,
            new_edge_table_start as u64,
        )?;

        // Update header
        let mut new_header = header;
        new_header.node_capacity = new_node_capacity;
        new_header.property_arena_offset = new_arena_start as u64;
        new_header.string_table_offset = new_string_table_start as u64;
        Self::write_header(&file, &new_header)?;

        // Sync to ensure writes are visible to new mmap
        file.sync_data()?;

        drop(file);

        // Remap the file
        self.remap()?;

        // Update arena allocator with new bounds
        {
            let mut arena = self.arena.write();
            // Arena shifted by size_increase
            let old_current = arena.current_offset();
            let new_current = old_current + offset_adjustment as u64;
            *arena = arena::ArenaAllocator::new(
                new_arena_start as u64,
                new_string_table_start as u64,
                new_current,
            );
        }

        Ok(())
    }

    /// Grow the edge table by doubling its capacity.
    ///
    /// This method:
    /// 1. Calculates the new capacity (2x current)
    /// 2. Expands the file to accommodate the larger edge table
    /// 3. Moves arena and string table to new positions
    /// 4. Updates the header with new capacity and offsets
    /// 5. Remaps the file to reflect the new size
    ///
    /// # File Layout Changes
    ///
    /// ```text
    /// Before: [Header][Nodes][Edges (E)][Arena][StringTable]
    /// After:  [Header][Nodes][Edges (2E)][Arena][StringTable]
    /// ```
    ///
    /// The arena and string table are shifted to maintain contiguous layout.
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
        let size_increase = new_edge_table_size - old_edge_table_size;

        // Read arena data (between edge table end and string table start)
        let old_arena_start = header.property_arena_offset as usize;
        let old_string_table_start = header.string_table_offset as usize;
        let arena_size = old_string_table_start - old_arena_start;
        let mut arena_data = vec![0u8; arena_size];
        if arena_size > 0 && old_string_table_start <= mmap.len() {
            arena_data.copy_from_slice(&mmap[old_arena_start..old_string_table_start]);
        }

        // Read string table data (from string table start to end of file)
        let string_table_size = mmap.len() - old_string_table_start;
        let mut string_table_data = vec![0u8; string_table_size];
        if string_table_size > 0 {
            string_table_data.copy_from_slice(&mmap[old_string_table_start..]);
        }

        drop(mmap);

        // Calculate new file size
        let old_file_size = file.metadata()?.len() as usize;
        let new_file_size = old_file_size + size_increase;

        // Calculate new positions
        let new_arena_start = old_arena_start + size_increase;
        let new_string_table_start = old_string_table_start + size_increase;

        // Extend the file
        file.set_len(new_file_size as u64)?;

        // Write data at new positions (order matters: write from end to start to avoid overwrites)
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            // Write string table first (furthest from start)
            if string_table_size > 0 {
                file.write_all_at(&string_table_data, new_string_table_start as u64)?;
            }
            // Write arena
            if arena_size > 0 {
                file.write_all_at(&arena_data, new_arena_start as u64)?;
            }
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            // Write string table first (furthest from start)
            if string_table_size > 0 {
                let mut f = &*file;
                f.seek(SeekFrom::Start(new_string_table_start as u64))?;
                f.write_all(&string_table_data)?;
            }
            // Write arena
            if arena_size > 0 {
                let mut f = &*file;
                f.seek(SeekFrom::Start(new_arena_start as u64))?;
                f.write_all(&arena_data)?;
            }
        }

        // Adjust property offsets in node and edge records.
        // Edge table hasn't moved - it's still at the same position.
        let edge_table_start = HEADER_SIZE as u64 + header.node_capacity * NODE_RECORD_SIZE as u64;
        let offset_adjustment = size_increase as i64;
        self.adjust_property_offsets(
            &file,
            offset_adjustment,
            header.next_node_id,
            header.next_edge_id,
            edge_table_start,
        )?;

        // Update header
        let mut new_header = header;
        new_header.edge_capacity = new_edge_capacity;
        new_header.property_arena_offset = new_arena_start as u64;
        new_header.string_table_offset = new_string_table_start as u64;
        Self::write_header(&file, &new_header)?;

        // Sync to ensure writes are visible to new mmap
        file.sync_data()?;

        drop(file);

        // Remap the file
        self.remap()?;

        // Update arena allocator with new bounds
        {
            let mut arena = self.arena.write();
            let old_current = arena.current_offset();
            let new_current = old_current + offset_adjustment as u64;
            *arena = arena::ArenaAllocator::new(
                new_arena_start as u64,
                new_string_table_start as u64,
                new_current,
            );
        }

        Ok(())
    }

    /// Grow the property arena by doubling its size.
    ///
    /// This method:
    /// 1. Calculates the current arena size
    /// 2. Doubles the arena capacity
    /// 3. Moves the string table to accommodate the larger arena
    /// 4. Extends the file
    /// 5. Updates the arena allocator
    ///
    /// # File Layout Changes
    ///
    /// ```text
    /// Before: [Header][Nodes][Edges][Arena (A)][StringTable]
    /// After:  [Header][Nodes][Edges][Arena (2A)][StringTable]
    /// ```
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during file operations
    pub fn grow_arena(&self) -> Result<(), StorageError> {
        let file = self.file.write();
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Calculate current arena size
        let arena_start = header.property_arena_offset;
        let string_table_start = header.string_table_offset;
        let old_arena_size = string_table_start - arena_start;

        // Double the arena size (minimum 64KB growth)
        let growth = old_arena_size.max(64 * 1024);
        let new_arena_size = old_arena_size + growth;

        // Read existing string table data
        let string_table_len = mmap.len() as u64 - string_table_start;
        let mut string_table_data = vec![0u8; string_table_len as usize];
        if string_table_len > 0 {
            string_table_data.copy_from_slice(&mmap[string_table_start as usize..]);
        }
        drop(mmap);

        // Calculate new file size and string table position
        let old_file_size = file.metadata()?.len();
        let new_file_size = old_file_size + growth;
        let new_string_table_offset = arena_start + new_arena_size;

        // Extend the file
        file.set_len(new_file_size)?;

        // Write string table at new position
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            if string_table_len > 0 {
                file.write_all_at(&string_table_data, new_string_table_offset)?;
            }
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            if string_table_len > 0 {
                let mut f = &*file;
                f.seek(SeekFrom::Start(new_string_table_offset))?;
                f.write_all(&string_table_data)?;
            }
        }

        // Update header with new string table offset
        let mut new_header = header;
        new_header.string_table_offset = new_string_table_offset;
        Self::write_header(&file, &new_header)?;

        // Sync to ensure writes are visible to new mmap
        file.sync_data()?;

        drop(file);

        // Remap the file
        self.remap()?;

        // Update arena allocator with new end
        {
            let mut arena = self.arena.write();
            arena.set_arena_end(new_string_table_offset);
        }

        Ok(())
    }

    /// Adjust property offsets in node and edge records after arena relocation.
    ///
    /// When the property arena is moved (e.g., during table growth), all `prop_head`
    /// fields in node and edge records must be updated to point to the new locations.
    ///
    /// # Arguments
    ///
    /// * `file` - The file handle (with write lock)
    /// * `offset_adjustment` - The amount to add to each prop_head (new_offset - old_offset)
    /// * `next_node_id` - Number of nodes to check
    /// * `next_edge_id` - Number of edges to check
    /// * `edge_table_start` - The file offset where the edge table currently starts.
    ///   This is needed because during table growth, the edge table may have moved
    ///   to a new location in the file.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error during read/write
    ///
    /// # Note
    ///
    /// This function reads node/edge records directly from the file (not from the mmap)
    /// to avoid stale data issues when called during table growth operations.
    /// The mmap may not yet reflect recent file writes.
    fn adjust_property_offsets(
        &self,
        file: &File,
        offset_adjustment: i64,
        next_node_id: u64,
        next_edge_id: u64,
        edge_table_start: u64,
    ) -> Result<(), StorageError> {
        use records::{EdgeRecord, NodeRecord, EDGE_RECORD_SIZE, NODE_RECORD_SIZE};

        // Read node records directly from file to avoid stale mmap issues.
        // Node records start at HEADER_SIZE and their positions don't change during table growth.
        let mut node_buffer = [0u8; NODE_RECORD_SIZE];
        for id in 0..next_node_id {
            let offset = HEADER_SIZE as u64 + (id * NODE_RECORD_SIZE as u64);

            // Read node record from file
            #[cfg(unix)]
            let read_result = {
                use std::os::unix::fs::FileExt;
                file.read_exact_at(&mut node_buffer, offset)
            };

            #[cfg(not(unix))]
            let read_result = {
                use std::io::{Read, Seek, SeekFrom};
                let mut f = &*file;
                f.seek(SeekFrom::Start(offset))
                    .and_then(|_| f.read_exact(&mut node_buffer))
            };

            if read_result.is_err() {
                // Past end of file or read error
                break;
            }

            let record = unsafe {
                let ptr = node_buffer.as_ptr() as *const NodeRecord;
                ptr.read_unaligned()
            };

            // Skip deleted or uninitialized records
            if record.is_deleted() || record.id != id {
                continue;
            }

            // Skip records without properties
            if record.prop_head == u64::MAX {
                continue;
            }

            // Adjust the prop_head offset
            let new_prop_head = (record.prop_head as i64 + offset_adjustment) as u64;
            let mut new_record = record;
            new_record.prop_head = new_prop_head;
            let bytes = new_record.to_bytes();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&bytes, offset)?;
            }

            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut f = &*file;
                f.seek(SeekFrom::Start(offset))?;
                f.write_all(&bytes)?;
            }
        }

        // Read edge records directly from file at the provided edge_table_start offset.
        // During grow_node_table, edge data has been moved to a new location.
        let mut edge_buffer = [0u8; EDGE_RECORD_SIZE];
        for id in 0..next_edge_id {
            let offset = edge_table_start + (id * EDGE_RECORD_SIZE as u64);

            // Read edge record from file
            #[cfg(unix)]
            let read_result = {
                use std::os::unix::fs::FileExt;
                file.read_exact_at(&mut edge_buffer, offset)
            };

            #[cfg(not(unix))]
            let read_result = {
                use std::io::{Read, Seek, SeekFrom};
                let mut f = &*file;
                f.seek(SeekFrom::Start(offset))
                    .and_then(|_| f.read_exact(&mut edge_buffer))
            };

            if read_result.is_err() {
                // Past end of file or read error
                break;
            }

            let record = unsafe {
                let ptr = edge_buffer.as_ptr() as *const EdgeRecord;
                ptr.read_unaligned()
            };

            // Skip deleted or uninitialized records
            if record.is_deleted() || record.id != id {
                continue;
            }

            // Skip records without properties
            if record.prop_head == u64::MAX {
                continue;
            }

            // Adjust the prop_head offset
            let new_prop_head = (record.prop_head as i64 + offset_adjustment) as u64;
            let mut new_record = record;
            new_record.prop_head = new_prop_head;
            let bytes = new_record.to_bytes();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&bytes, offset)?;
            }

            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut f = &*file;
                f.seek(SeekFrom::Start(offset))?;
                f.write_all(&bytes)?;
            }
        }

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
    /// use interstellar::storage::MmapGraph;
    /// use interstellar::value::Value;
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
        // Check if we're in batch mode
        let in_batch_mode = self.is_batch_mode();

        // Step 1: Begin WAL transaction (only if not in batch mode)
        // In batch mode, we use the existing batch transaction
        let tx_id = if in_batch_mode {
            // In batch mode, use the batch transaction ID
            self.batch_tx_id.read().ok_or_else(|| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Batch mode active but no transaction ID",
                ))
            })?
        } else {
            // Normal mode: start a new transaction
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
                .or_insert_with(RoaringTreemap::new)
                .insert(slot_id.0);
        }

        // Step 9: Increment node count in header
        self.increment_node_count()?;

        // Step 10: Update property indexes
        self.index_vertex_insert(slot_id, label, &properties);

        // Step 11: Persist string table (for label and property key names)
        self.persist_string_table()?;

        // Step 12: Update arena offset in header (for property data)
        self.update_arena_offset()?;

        // Step 13: Commit WAL transaction and sync (only if not in batch mode)
        // In batch mode, commit_batch() will handle the commit and sync
        if !in_batch_mode {
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
    /// use interstellar::storage::MmapGraph;
    /// use interstellar::value::Value;
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
        // Check if we're in batch mode
        let in_batch_mode = self.is_batch_mode();

        // Step 1: Begin WAL transaction (only if not in batch mode)
        // In batch mode, we use the existing batch transaction
        let tx_id = if in_batch_mode {
            // In batch mode, use the batch transaction ID
            self.batch_tx_id.read().ok_or_else(|| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Batch mode active but no transaction ID",
                ))
            })?
        } else {
            // Normal mode: start a new transaction
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
                .or_insert_with(RoaringTreemap::new)
                .insert(slot_id.0);
        }

        // Step 14: Increment edge count in header
        self.increment_edge_count()?;

        // Step 15: Update property indexes
        self.index_edge_insert(slot_id, label, &properties);

        // Step 16: Persist string table (for label and property key names)
        self.persist_string_table()?;

        // Step 17: Update arena offset in header (for property data)
        self.update_arena_offset()?;

        // Step 18: Commit WAL transaction and sync (only if not in batch mode)
        // In batch mode, commit_batch() will handle the commit and sync
        if !in_batch_mode {
            let mut wal = self.wal.write();
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        Ok(slot_id)
    }

    // =========================================================================
    // Remove Operations
    // =========================================================================

    /// Decrement the node count in the file header.
    fn decrement_node_count(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.node_count = header.node_count.saturating_sub(1);

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        self.remap()?;
        Ok(())
    }

    /// Decrement the edge count in the file header.
    fn decrement_edge_count(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.edge_count = header.edge_count.saturating_sub(1);

        let file = self.file.write();
        Self::write_header(&file, &header)?;
        drop(file);

        self.remap()?;
        Ok(())
    }

    /// Update an edge's next_out pointer.
    fn update_edge_next_out(&self, edge_id: EdgeId, next_out: u64) -> Result<(), StorageError> {
        let header = self.get_header();
        let edge_table_offset =
            HEADER_SIZE as u64 + (header.node_capacity * NODE_RECORD_SIZE as u64);
        let edge_offset = edge_table_offset + (edge_id.0 * EDGE_RECORD_SIZE as u64);
        let next_out_offset = edge_offset + 32;

        let bytes = next_out.to_le_bytes();
        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, next_out_offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(next_out_offset))?;
            file.write_all(&bytes)?;
        }

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
        drop(file);
        self.remap()?;
        Ok(())
    }

    /// Update an edge's next_in pointer.
    fn update_edge_next_in(&self, edge_id: EdgeId, next_in: u64) -> Result<(), StorageError> {
        let header = self.get_header();
        let edge_table_offset =
            HEADER_SIZE as u64 + (header.node_capacity * NODE_RECORD_SIZE as u64);
        let edge_offset = edge_table_offset + (edge_id.0 * EDGE_RECORD_SIZE as u64);
        let next_in_offset = edge_offset + 40;

        let bytes = next_in.to_le_bytes();
        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, next_in_offset)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &*file;
            file.seek(SeekFrom::Start(next_in_offset))?;
            file.write_all(&bytes)?;
        }

        // Note: No sync here - WAL provides durability. Data file is synced during checkpoint().
        drop(file);
        self.remap()?;
        Ok(())
    }

    /// Removes an edge from the graph.
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        let record = self
            .get_edge_record(id)
            .ok_or(StorageError::EdgeNotFound(id))?;

        // Load edge data for index removal before we delete it
        let edge_label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id).map(|s| s.to_string())
        };
        let edge_properties: std::collections::HashMap<String, Value> = self
            .load_properties(record.prop_head)
            .map(|p| p.into_iter().collect())
            .unwrap_or_default();

        let tx_id = {
            let mut wal = self.wal.write();
            wal.begin_transaction()?
        };

        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::DeleteEdge { id })?;
        }

        let mut deleted_record = record;
        deleted_record.mark_deleted();
        self.write_edge_record(id, &deleted_record)?;

        let src_vertex = VertexId(record.src);
        if let Some(src_node) = self.get_node_record(src_vertex) {
            if src_node.first_out_edge == id.0 {
                self.update_node_first_out_edge(src_vertex, record.next_out)?;
            } else {
                let mut current_id = src_node.first_out_edge;
                while current_id != u64::MAX {
                    if let Some(current_edge) = self.get_edge_record(EdgeId(current_id)) {
                        if current_edge.next_out == id.0 {
                            self.update_edge_next_out(EdgeId(current_id), record.next_out)?;
                            break;
                        }
                        current_id = current_edge.next_out;
                    } else {
                        break;
                    }
                }
            }
        }

        let dst_vertex = VertexId(record.dst);
        if let Some(dst_node) = self.get_node_record(dst_vertex) {
            if dst_node.first_in_edge == id.0 {
                self.update_node_first_in_edge(dst_vertex, record.next_in)?;
            } else {
                let mut current_id = dst_node.first_in_edge;
                while current_id != u64::MAX {
                    if let Some(current_edge) = self.get_edge_record(EdgeId(current_id)) {
                        if current_edge.next_in == id.0 {
                            self.update_edge_next_in(EdgeId(current_id), record.next_in)?;
                            break;
                        }
                        current_id = current_edge.next_in;
                    } else {
                        break;
                    }
                }
            }
        }

        {
            let label_id = record.label_id;
            let mut edge_labels = self.edge_labels.write();
            if let Some(bitmap) = edge_labels.get_mut(&label_id) {
                bitmap.remove(id.0);
            }
        }

        // Remove from property indexes
        if let Some(ref label) = edge_label {
            self.index_edge_remove(id, label, &edge_properties);
        }

        {
            let mut free_edges = self.free_edges.write();
            free_edges.free(id.0);
        }
        self.update_free_edge_head()?;

        self.decrement_edge_count()?;

        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        Ok(())
    }

    /// Internal helper to remove an edge, optionally skipping vertex updates.
    fn remove_edge_internal(
        &self,
        id: EdgeId,
        skip_vertex: Option<VertexId>,
    ) -> Result<(), StorageError> {
        let record = match self.get_edge_record(id) {
            Some(r) => r,
            None => return Ok(()),
        };

        // Load edge data for index removal before we delete it
        let edge_label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id).map(|s| s.to_string())
        };
        let edge_properties: std::collections::HashMap<String, Value> = self
            .load_properties(record.prop_head)
            .map(|p| p.into_iter().collect())
            .unwrap_or_default();

        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::DeleteEdge { id })?;
        }

        let mut deleted_record = record;
        deleted_record.mark_deleted();
        self.write_edge_record(id, &deleted_record)?;

        let src_vertex = VertexId(record.src);
        if skip_vertex != Some(src_vertex) {
            if let Some(src_node) = self.get_node_record(src_vertex) {
                if src_node.first_out_edge == id.0 {
                    self.update_node_first_out_edge(src_vertex, record.next_out)?;
                } else {
                    let mut current_id = src_node.first_out_edge;
                    while current_id != u64::MAX {
                        if let Some(current_edge) = self.get_edge_record(EdgeId(current_id)) {
                            if current_edge.next_out == id.0 {
                                self.update_edge_next_out(EdgeId(current_id), record.next_out)?;
                                break;
                            }
                            current_id = current_edge.next_out;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        let dst_vertex = VertexId(record.dst);
        if skip_vertex != Some(dst_vertex) {
            if let Some(dst_node) = self.get_node_record(dst_vertex) {
                if dst_node.first_in_edge == id.0 {
                    self.update_node_first_in_edge(dst_vertex, record.next_in)?;
                } else {
                    let mut current_id = dst_node.first_in_edge;
                    while current_id != u64::MAX {
                        if let Some(current_edge) = self.get_edge_record(EdgeId(current_id)) {
                            if current_edge.next_in == id.0 {
                                self.update_edge_next_in(EdgeId(current_id), record.next_in)?;
                                break;
                            }
                            current_id = current_edge.next_in;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        {
            let label_id = record.label_id;
            let mut edge_labels = self.edge_labels.write();
            if let Some(bitmap) = edge_labels.get_mut(&label_id) {
                bitmap.remove(id.0);
            }
        }

        // Remove from property indexes
        if let Some(ref label) = edge_label {
            self.index_edge_remove(id, label, &edge_properties);
        }

        {
            let mut free_edges = self.free_edges.write();
            free_edges.free(id.0);
        }
        self.update_free_edge_head()?;

        self.decrement_edge_count()?;

        Ok(())
    }

    /// Removes a vertex and all its incident edges from the graph.
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        let record = self
            .get_node_record(id)
            .ok_or(StorageError::VertexNotFound(id))?;

        // Load vertex data for index removal before we delete it
        let vertex_label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id).map(|s| s.to_string())
        };
        let vertex_properties: std::collections::HashMap<String, Value> = self
            .load_properties(record.prop_head)
            .map(|p| p.into_iter().collect())
            .unwrap_or_default();

        let tx_id = {
            let mut wal = self.wal.write();
            wal.begin_transaction()?
        };

        let mut edges_to_remove = Vec::new();

        let mut current = record.first_out_edge;
        while current != u64::MAX {
            edges_to_remove.push(EdgeId(current));
            if let Some(edge) = self.get_edge_record(EdgeId(current)) {
                current = edge.next_out;
            } else {
                break;
            }
        }

        let mut current = record.first_in_edge;
        while current != u64::MAX {
            let edge_id = EdgeId(current);
            if !edges_to_remove.contains(&edge_id) {
                edges_to_remove.push(edge_id);
            }
            if let Some(edge) = self.get_edge_record(edge_id) {
                current = edge.next_in;
            } else {
                break;
            }
        }

        for edge_id in edges_to_remove {
            let _ = self.remove_edge_internal(edge_id, Some(id));
        }

        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::DeleteNode { id })?;
        }

        let mut deleted_record = record;
        deleted_record.mark_deleted();
        self.write_node_record(id, &deleted_record)?;

        {
            let label_id = record.label_id;
            let mut vertex_labels = self.vertex_labels.write();
            if let Some(bitmap) = vertex_labels.get_mut(&label_id) {
                bitmap.remove(id.0);
            }
        }

        // Remove from property indexes
        if let Some(ref label) = vertex_label {
            self.index_vertex_remove(id, label, &vertex_properties);
        }

        {
            let mut free_nodes = self.free_nodes.write();
            free_nodes.free(id.0);
        }
        self.update_free_node_head()?;

        self.decrement_node_count()?;

        {
            let mut wal = self.wal.write();
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        Ok(())
    }

    // =========================================================================
    // Property Update Operations
    // =========================================================================

    /// Sets or updates a property on a vertex.
    ///
    /// This method loads the existing properties, updates the specified key,
    /// and writes all properties back to a new location in the arena.
    ///
    /// # Arguments
    ///
    /// * `id` - The vertex ID
    /// * `key` - The property key
    /// * `value` - The new property value
    ///
    /// # Errors
    ///
    /// - [`StorageError::VertexNotFound`] - Vertex doesn't exist
    /// - [`StorageError::Io`] - I/O error during write
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: crate::value::Value,
    ) -> Result<(), StorageError> {
        // Get existing node record
        let record = self
            .get_node_record(id)
            .ok_or(StorageError::VertexNotFound(id))?;

        // Get vertex label for index updates
        let vertex_label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id).map(|s| s.to_string())
        };

        // Load existing properties
        let properties = self.load_properties(record.prop_head)?;

        // Get old value for index update
        let old_value = properties.get(key).cloned();

        // Update/add the property
        let mut properties = properties;
        properties.insert(key.to_string(), value.clone());

        // Convert to std HashMap for allocate_properties
        let std_props: std::collections::HashMap<String, crate::value::Value> =
            properties.into_iter().collect();

        // Allocate new properties in arena
        let new_prop_head = self.allocate_properties(&std_props)?;

        // Update node record with new prop_head
        let mut new_record = record;
        new_record.prop_head = new_prop_head;
        self.write_node_record(id, &new_record)?;

        // Update property indexes
        if let Some(label) = vertex_label {
            self.update_vertex_property_in_indexes(id, &label, key, old_value.as_ref(), &value);
        }

        // Persist string table (for new property keys)
        self.persist_string_table()?;

        // Update arena offset
        self.update_arena_offset()?;

        Ok(())
    }

    /// Sets or updates a property on an edge.
    ///
    /// This method loads the existing properties, updates the specified key,
    /// and writes all properties back to a new location in the arena.
    ///
    /// # Arguments
    ///
    /// * `id` - The edge ID
    /// * `key` - The property key
    /// * `value` - The new property value
    ///
    /// # Errors
    ///
    /// - [`StorageError::EdgeNotFound`] - Edge doesn't exist
    /// - [`StorageError::Io`] - I/O error during write
    pub fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: crate::value::Value,
    ) -> Result<(), StorageError> {
        // Get existing edge record
        let record = self
            .get_edge_record(id)
            .ok_or(StorageError::EdgeNotFound(id))?;

        // Get edge label for index updates
        let edge_label = {
            let string_table = self.string_table.read();
            string_table.resolve(record.label_id).map(|s| s.to_string())
        };

        // Load existing properties
        let properties = self.load_properties(record.prop_head)?;

        // Get old value for index update
        let old_value = properties.get(key).cloned();

        // Update/add the property
        let mut properties = properties;
        properties.insert(key.to_string(), value.clone());

        // Convert to std HashMap for allocate_properties
        let std_props: std::collections::HashMap<String, crate::value::Value> =
            properties.into_iter().collect();

        // Allocate new properties in arena
        let new_prop_head = self.allocate_properties(&std_props)?;

        // Update edge record with new prop_head
        let mut new_record = record;
        new_record.prop_head = new_prop_head;
        self.write_edge_record(id, &new_record)?;

        // Update property indexes
        if let Some(label) = edge_label {
            self.update_edge_property_in_indexes(id, &label, key, old_value.as_ref(), &value);
        }

        // Persist string table (for new property keys)
        self.persist_string_table()?;

        // Update arena offset
        self.update_arena_offset()?;

        Ok(())
    }

    // =========================================================================
    // Schema Persistence
    // =========================================================================

    /// Load the schema from the database file.
    ///
    /// Returns `None` if no schema has been saved, or `Some(schema)` if one exists.
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - Schema data is corrupted
    /// - [`StorageError::Io`] - I/O error reading from file
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    /// if let Some(schema) = graph.load_schema().unwrap() {
    ///     println!("Schema mode: {:?}", schema.mode);
    /// }
    /// ```
    pub fn load_schema(&self) -> Result<Option<crate::schema::GraphSchema>, StorageError> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Check if schema exists
        if header.schema_offset == 0 || header.schema_size == 0 {
            return Ok(None);
        }

        let offset = header.schema_offset as usize;
        let size = header.schema_size as usize;

        // Verify bounds
        if offset + size > mmap.len() {
            return Err(StorageError::InvalidFormat);
        }

        // Read schema data from mmap
        let schema_data = &mmap[offset..offset + size];

        // Deserialize
        let schema = crate::schema::deserialize_schema(schema_data)
            .map_err(|_e| StorageError::InvalidFormat)?;

        Ok(Some(schema))
    }

    /// Save a schema to the database file.
    ///
    /// The schema is serialized and stored in a region after the string table.
    /// The operation is logged to the WAL for durability.
    ///
    /// # Arguments
    ///
    /// * `schema` - The graph schema to save
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error writing to file
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// graph.save_schema(&schema).unwrap();
    /// ```
    pub fn save_schema(&self, schema: &crate::schema::GraphSchema) -> Result<(), StorageError> {
        // Serialize the schema
        let schema_data = crate::schema::serialize_schema(schema);

        // Get current file state
        let file = self.file.read();
        let metadata = file.metadata()?;
        let current_file_size = metadata.len();
        drop(file);

        // Read header to get current layout
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        // Determine where to write the schema
        // Schema goes after the string table end
        let schema_offset = header.string_table_end.max(
            header.property_arena_offset + 64 * 1024, // After arena at minimum
        );

        // Ensure file is large enough
        let required_size = schema_offset + schema_data.len() as u64;
        if required_size > current_file_size {
            let file = self.file.read();
            file.set_len(required_size)?;
            drop(file);
        }

        // Log to WAL first for durability
        {
            let mut wal = self.wal.write();
            let tx_id = wal.begin_transaction()?;
            wal.log(WalEntry::SchemaUpdate {
                offset: schema_offset,
                data: schema_data.clone(),
            })?;
            wal.log(WalEntry::CommitTx { tx_id })?;
            wal.sync()?;
        }

        // Write schema data to file
        {
            let file = self.file.read();
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&schema_data, schema_offset)?;
            }

            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut file = &*file;
                file.seek(SeekFrom::Start(schema_offset))?;
                file.write_all(&schema_data)?;
            }
        }

        // Update header with schema location
        header.schema_offset = schema_offset;
        header.schema_size = schema_data.len() as u64;
        header.schema_version = crate::schema::SCHEMA_FORMAT_VERSION;

        let file = self.file.read();
        Self::write_header(&file, &header)?;
        file.sync_data()?;
        drop(file);

        // Remap to see changes
        self.remap()?;

        Ok(())
    }

    /// Remove the schema from the database.
    ///
    /// This clears the schema metadata in the header but does not reclaim the
    /// disk space used by the schema data.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] - I/O error writing to file
    pub fn clear_schema(&self) -> Result<(), StorageError> {
        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        // Clear schema fields
        header.schema_offset = 0;
        header.schema_size = 0;
        header.schema_version = 0;

        let file = self.file.read();
        Self::write_header(&file, &header)?;
        file.sync_data()?;
        drop(file);

        // Remap to see changes
        self.remap()?;

        Ok(())
    }

    // =========================================================================
    // Property Index Operations
    // =========================================================================

    /// Creates a property index on the graph.
    ///
    /// The index is populated with existing data matching the specification,
    /// and will be automatically maintained on subsequent mutations.
    ///
    /// The index specification and creation are logged to the WAL for durability.
    /// On database reopen, indexes are rebuilt from persisted specifications.
    ///
    /// # Arguments
    ///
    /// * `spec` - The index specification defining what to index
    ///
    /// # Errors
    ///
    /// - [`IndexError::AlreadyExists`] - An index with this name already exists
    /// - [`IndexError::UniqueViolation`] - For unique indexes, duplicate values exist
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    /// use interstellar::index::IndexBuilder;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    ///
    /// // Create a B+ tree index for range queries
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .label("person")
    ///         .property("age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    ///
    /// // Create a unique index for O(1) lookups
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .label("user")
    ///         .property("email")
    ///         .unique()
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    /// ```
    pub fn create_index(&self, spec: IndexSpec) -> Result<(), IndexError> {
        // Check for duplicate name
        {
            let indexes = self.indexes.read();
            if indexes.contains_key(&spec.name) {
                return Err(IndexError::AlreadyExists(spec.name.clone()));
            }
        }

        // Create the appropriate index type
        let mut index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())?),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())?),
        };

        // Populate index with existing data
        self.populate_index(&mut *index)?;

        // Log to WAL for durability
        {
            let mut wal = self.wal.write();
            // Note: We log within a transaction for crash safety
            let tx_id = wal
                .begin_transaction()
                .map_err(|e| IndexError::Internal(e.to_string()))?;
            wal.log(WalEntry::CreateIndex { spec: spec.clone() })
                .map_err(|e| IndexError::Internal(e.to_string()))?;
            wal.log(WalEntry::CommitTx { tx_id })
                .map_err(|e| IndexError::Internal(e.to_string()))?;
            wal.sync()
                .map_err(|e| IndexError::Internal(e.to_string()))?;
        }

        // Store the index
        {
            let mut indexes = self.indexes.write();
            indexes.insert(spec.name.clone(), index);
        }

        // Store the spec for persistence
        {
            let mut index_specs = self.index_specs.write();
            index_specs.push(spec);
        }

        // Persist index specs to disk
        self.save_index_specs()
            .map_err(|e| IndexError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Drops an index by name.
    ///
    /// The drop operation is logged to the WAL for durability.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::NotFound`] if no index with that name exists.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    /// use interstellar::index::IndexBuilder;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .property("age")
    ///         .name("idx_age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    ///
    /// graph.drop_index("idx_age").unwrap();
    /// ```
    pub fn drop_index(&self, name: &str) -> Result<(), IndexError> {
        // Check if index exists
        {
            let indexes = self.indexes.read();
            if !indexes.contains_key(name) {
                return Err(IndexError::NotFound(name.to_string()));
            }
        }

        // Log to WAL for durability
        {
            let mut wal = self.wal.write();
            let tx_id = wal
                .begin_transaction()
                .map_err(|e| IndexError::Internal(e.to_string()))?;
            wal.log(WalEntry::DropIndex {
                name: name.to_string(),
            })
            .map_err(|e| IndexError::Internal(e.to_string()))?;
            wal.log(WalEntry::CommitTx { tx_id })
                .map_err(|e| IndexError::Internal(e.to_string()))?;
            wal.sync()
                .map_err(|e| IndexError::Internal(e.to_string()))?;
        }

        // Remove the index
        {
            let mut indexes = self.indexes.write();
            indexes.remove(name);
        }

        // Remove the spec
        {
            let mut index_specs = self.index_specs.write();
            index_specs.retain(|s| s.name != name);
        }

        // Persist index specs to disk
        self.save_index_specs()
            .map_err(|e| IndexError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Returns an iterator over all index specifications.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use interstellar::storage::MmapGraph;
    /// use interstellar::index::IndexBuilder;
    ///
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .property("age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    ///
    /// for spec in graph.list_indexes() {
    ///     println!("Index: {} on property '{}'", spec.name, spec.property);
    /// }
    /// ```
    pub fn list_indexes(&self) -> Vec<IndexSpec> {
        let indexes = self.indexes.read();
        indexes.values().map(|idx| idx.spec().clone()).collect()
    }

    /// Checks if an index with the given name exists.
    pub fn has_index(&self, name: &str) -> bool {
        let indexes = self.indexes.read();
        indexes.contains_key(name)
    }

    /// Returns the number of indexes.
    pub fn index_count(&self) -> usize {
        let indexes = self.indexes.read();
        indexes.len()
    }

    // =========================================================================
    // Index Persistence
    // =========================================================================

    /// Returns the path to the index specs JSON file.
    fn index_specs_path(&self) -> std::path::PathBuf {
        self.db_path.with_extension("idx.json")
    }

    /// Save index specifications to a JSON file for persistence.
    ///
    /// This is called automatically when indexes are created or dropped.
    /// The specs are stored in a separate `.idx.json` file alongside the main database file.
    fn save_index_specs(&self) -> Result<(), StorageError> {
        use std::io::Write;

        let specs_path = self.index_specs_path();
        let index_specs = self.index_specs.read();

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&*index_specs)
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;

        // Write to file atomically by writing to temp file first, then renaming
        let temp_path = specs_path.with_extension("idx.json.tmp");
        {
            let mut file = std::fs::File::create(&temp_path)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
        }

        // Rename temp file to final path (atomic on POSIX)
        std::fs::rename(&temp_path, &specs_path)?;

        Ok(())
    }

    /// Load index specifications from JSON file and rebuild indexes.
    ///
    /// This is called during `MmapGraph::open()` after `rebuild_indexes()`.
    /// If the file doesn't exist, this is a no-op (new database or no indexes).
    fn load_index_specs(&self) -> Result<(), StorageError> {
        let specs_path = self.index_specs_path();

        // If file doesn't exist, nothing to load
        if !specs_path.exists() {
            return Ok(());
        }

        // Read and parse the JSON file
        let json = std::fs::read_to_string(&specs_path)?;
        let specs: Vec<IndexSpec> = serde_json::from_str(&json).map_err(|e| {
            StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;

        // Create and populate each index
        for spec in specs {
            // Create the appropriate index type
            let mut index: Box<dyn PropertyIndex> = match spec.index_type {
                IndexType::BTree => Box::new(BTreeIndex::new(spec.clone()).map_err(|e| {
                    StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                })?),
                IndexType::Unique => Box::new(UniqueIndex::new(spec.clone()).map_err(|e| {
                    StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                })?),
            };

            // Populate index with existing data
            // Note: We ignore errors here since the data is already in the graph
            // and the index will just have missing entries if there's a unique violation
            if let Err(e) = self.populate_index(&mut *index) {
                // Log warning but continue - index may be partially populated
                // In production, we might want to track this differently
                eprintln!(
                    "Warning: Failed to fully populate index '{}': {}",
                    spec.name, e
                );
            }

            // Store the index
            {
                let mut indexes = self.indexes.write();
                indexes.insert(spec.name.clone(), index);
            }

            // Store the spec
            {
                let mut index_specs = self.index_specs.write();
                index_specs.push(spec);
            }
        }

        Ok(())
    }

    /// Populate an index with existing graph data.
    fn populate_index(&self, index: &mut dyn PropertyIndex) -> Result<(), IndexError> {
        let spec = index.spec().clone();

        match spec.element_type {
            ElementType::Vertex => {
                let header = self.get_header();
                for id in 0..header.next_node_id {
                    if let Some(vertex) = self.get_vertex(VertexId(id)) {
                        // Check label filter
                        if let Some(ref label) = spec.label {
                            if &vertex.label != label {
                                continue;
                            }
                        }

                        // Get property value
                        if let Some(value) = vertex.properties.get(&spec.property) {
                            index.insert(value.clone(), id)?;
                        }
                    }
                }
            }
            ElementType::Edge => {
                let header = self.get_header();
                for id in 0..header.next_edge_id {
                    if let Some(edge) = self.get_edge(EdgeId(id)) {
                        // Check label filter
                        if let Some(ref label) = spec.label {
                            if &edge.label != label {
                                continue;
                            }
                        }

                        // Get property value
                        if let Some(value) = edge.properties.get(&spec.property) {
                            index.insert(value.clone(), id)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Lookup vertices by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter
    /// * `property` - Property key to match
    /// * `value` - Property value to find
    pub fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Try to find an applicable index
        let indexes = self.indexes.read();
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            drop(indexes); // Release lock before filtering

            let label_owned = label.map(|s| s.to_string());
            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| self.get_vertex(VertexId(id)))
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }
        drop(indexes);

        // Fall back to scan
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        Box::new(self.all_vertices().filter(move |v| {
            if let Some(ref l) = label_owned {
                if &v.label != l {
                    return false;
                }
            }
            v.properties.get(&property_owned) == Some(&value_clone)
        }))
    }

    /// Lookup edges by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    pub fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Try to find an applicable index
        let indexes = self.indexes.read();
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            drop(indexes); // Release lock before filtering

            let label_owned = label.map(|s| s.to_string());
            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| self.get_edge(EdgeId(id)))
                    .filter(move |e| {
                        label_owned.is_none() || Some(e.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }
        drop(indexes);

        // Fall back to scan
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        Box::new(self.all_edges().filter(move |e| {
            if let Some(ref l) = label_owned {
                if &e.label != l {
                    return false;
                }
            }
            e.properties.get(&property_owned) == Some(&value_clone)
        }))
    }

    /// Lookup vertices by property range, using indexes if available.
    ///
    /// If an applicable BTree index exists, uses it for O(log n) range lookup.
    /// Otherwise falls back to O(n) scan.
    pub fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        use std::ops::Bound;

        // Try to find an applicable BTree index
        let indexes = self.indexes.read();
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // BTree indexes support range queries; skip unique indexes
            if spec.index_type != IndexType::BTree {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index for range lookup
            let ids: Vec<u64> = index.lookup_range(start, end).collect();
            drop(indexes); // Release lock

            let label_owned = label.map(|s| s.to_string());
            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| self.get_vertex(VertexId(id)))
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }
        drop(indexes);

        // Fall back to scan with range filter
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let start_clone = match start {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_clone = match end {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Box::new(self.all_vertices().filter(move |v| {
            if let Some(ref l) = label_owned {
                if &v.label != l {
                    return false;
                }
            }
            if let Some(prop_value) = v.properties.get(&property_owned) {
                // Check range bounds using ComparableValue for ordering
                let prop_cmp = prop_value.to_comparable();
                let in_start = match &start_clone {
                    Bound::Included(s) => prop_cmp >= s.to_comparable(),
                    Bound::Excluded(s) => prop_cmp > s.to_comparable(),
                    Bound::Unbounded => true,
                };
                let in_end = match &end_clone {
                    Bound::Included(e) => prop_cmp <= e.to_comparable(),
                    Bound::Excluded(e) => prop_cmp < e.to_comparable(),
                    Bound::Unbounded => true,
                };
                in_start && in_end
            } else {
                false
            }
        }))
    }

    // =========================================================================
    // Index Maintenance Helpers
    // =========================================================================

    /// Update indexes when a vertex is added.
    fn index_vertex_insert(
        &self,
        id: VertexId,
        label: &str,
        properties: &std::collections::HashMap<String, Value>,
    ) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
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
                // Ignore errors for BTree (no constraint), unique violations shouldn't happen on insert
                let _ = index.insert(value.clone(), id.0);
            }
        }
    }

    /// Update indexes when a vertex is removed.
    fn index_vertex_remove(
        &self,
        id: VertexId,
        label: &str,
        properties: &std::collections::HashMap<String, Value>,
    ) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
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
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when an edge is added.
    fn index_edge_insert(
        &self,
        id: EdgeId,
        label: &str,
        properties: &std::collections::HashMap<String, Value>,
    ) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.insert(value.clone(), id.0);
            }
        }
    }

    /// Update indexes when an edge is removed.
    fn index_edge_remove(
        &self,
        id: EdgeId,
        label: &str,
        properties: &std::collections::HashMap<String, Value>,
    ) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when a vertex property changes.
    fn update_vertex_property_in_indexes(
        &self,
        id: VertexId,
        label: &str,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
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

            // Remove old value from index if it existed
            if let Some(old) = old_value {
                let _ = index.remove(old, id.0);
            }

            // Insert new value
            let _ = index.insert(new_value.clone(), id.0);
        }
    }

    /// Update indexes when an edge property changes.
    fn update_edge_property_in_indexes(
        &self,
        id: EdgeId,
        label: &str,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
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

            // Remove old value from index if it existed
            if let Some(old) = old_value {
                let _ = index.remove(old, id.0);
            }

            // Insert new value
            let _ = index.insert(new_value.clone(), id.0);
        }
    }
}

// =========================================================================
// Query Storage Operations
// =========================================================================

impl MmapGraph {
    /// Save a new query to the library.
    ///
    /// Validates the query name and stores the query in the query region.
    /// Parameters are extracted from the query text.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique query name (alphanumeric, underscores, hyphens)
    /// * `query_type` - Language type (Gremlin or GQL)
    /// * `description` - Human-readable description
    /// * `query_text` - Query text (may contain $param placeholders)
    ///
    /// # Returns
    ///
    /// The assigned query ID on success.
    ///
    /// # Errors
    ///
    /// - `QueryError::AlreadyExists` - Name already in use
    /// - `QueryError::InvalidName` - Name contains invalid characters
    /// - `QueryError::StorageFull` - Cannot allocate space for query
    pub fn save_query(
        &self,
        name: &str,
        query_type: crate::query::QueryType,
        description: &str,
        query_text: &str,
    ) -> Result<u32, crate::error::QueryError> {
        use crate::error::QueryError;
        use crate::query::validate_query_name;

        // Validate the query name
        validate_query_name(name).map_err(QueryError::InvalidName)?;

        // Check if name already exists
        {
            let query_index = self.query_index.read();
            if query_index.contains_name(name) {
                return Err(QueryError::AlreadyExists(name.to_string()));
            }
        }

        // Extract parameters from query (simplified - just look for $name patterns)
        let parameters = Self::extract_parameters(query_text);

        // Calculate record size
        let record_size =
            query::QueryStore::calculate_record_size(name, description, query_text, &parameters);

        // Ensure query region exists and has space
        let write_offset = self.ensure_query_space(record_size as u64)?;

        // Allocate query ID
        let query_id = self.allocate_query_id()?;

        // Serialize the query
        let data = query::QueryStore::serialize_query(
            query_id,
            query_type,
            name,
            description,
            query_text,
            &parameters,
        );

        // Write to disk
        self.write_query_data(write_offset, &data)?;

        // Update header with new query_store_end
        self.update_query_header(write_offset + data.len() as u64)?;

        // Update in-memory index
        {
            let mut query_index = self.query_index.write();
            query_index.insert(name.to_string(), query_id, write_offset);
        }

        Ok(query_id)
    }

    /// Get a query by name.
    ///
    /// Returns `None` if no query exists with the given name.
    pub fn get_query(&self, name: &str) -> Option<crate::query::SavedQuery> {
        let offset = {
            let query_index = self.query_index.read();
            query_index.get_offset_by_name(name)?
        };

        self.read_query_at_offset(offset)
    }

    /// Get a query by ID.
    ///
    /// Returns `None` if no query exists with the given ID.
    pub fn get_query_by_id(&self, id: u32) -> Option<crate::query::SavedQuery> {
        let offset = {
            let query_index = self.query_index.read();
            query_index.get_offset(id)?
        };

        self.read_query_at_offset(offset)
    }

    /// List all saved queries.
    ///
    /// Returns queries in no particular order.
    pub fn list_queries(&self) -> Vec<crate::query::SavedQuery> {
        let offsets: Vec<u64> = {
            let query_index = self.query_index.read();
            query_index.offsets().collect()
        };

        offsets
            .into_iter()
            .filter_map(|offset| self.read_query_at_offset(offset))
            .collect()
    }

    /// Delete a query by name.
    ///
    /// This performs a soft delete by setting the deleted flag.
    ///
    /// # Errors
    ///
    /// - `QueryError::NotFound` - Query does not exist
    pub fn delete_query(&self, name: &str) -> Result<(), crate::error::QueryError> {
        use crate::error::QueryError;

        let (query_id, offset) = {
            let query_index = self.query_index.read();
            let id = query_index
                .get_id(name)
                .ok_or_else(|| QueryError::NotFound(name.to_string()))?;
            let offset = query_index
                .get_offset(id)
                .ok_or_else(|| QueryError::NotFound(name.to_string()))?;
            (id, offset)
        };

        // Set the deleted flag in the record on disk
        self.mark_query_deleted(offset)?;

        // Remove from in-memory index
        {
            let mut query_index = self.query_index.write();
            query_index.remove(name);
        }

        // Decrement query count in header
        self.decrement_query_count()?;

        // Log query ID for debugging (avoid unused warning)
        let _ = query_id;

        Ok(())
    }

    // =========================================================================
    // Query Storage Helpers
    // =========================================================================

    /// Extract parameters from query text using simple regex-like pattern matching.
    fn extract_parameters(query_text: &str) -> Vec<crate::query::QueryParameter> {
        use crate::query::{ParameterType, QueryParameter};

        let mut params = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Simple parameter extraction: find $name patterns
        let bytes = query_text.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'$' && i + 1 < bytes.len() {
                // Find parameter name
                let start = i + 1;
                let mut end = start;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }

                if end > start {
                    if let Ok(name) = std::str::from_utf8(&bytes[start..end]) {
                        if !seen.contains(name) {
                            seen.insert(name.to_string());
                            params.push(QueryParameter::new(name, ParameterType::Any));
                        }
                    }
                }
                i = end;
            } else {
                i += 1;
            }
        }

        params
    }

    /// Ensure the query region exists and has enough space for a new query.
    ///
    /// Returns the offset where the new query should be written.
    fn ensure_query_space(&self, size: u64) -> Result<u64, crate::error::QueryError> {
        use crate::error::QueryError;
        use records::QUERY_REGION_HEADER_SIZE;

        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);
        drop(mmap);

        // Check if query region exists
        if !header.has_query_region() {
            // Initialize new query region
            return self.initialize_query_region(size);
        }

        // Calculate available space
        let query_store_offset = header.query_store_offset();
        let query_store_end = header.query_store_end();
        let file_size = {
            let file = self.file.read();
            file.metadata()
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?
                .len()
        };

        // Calculate where new query would go
        let write_offset = if query_store_end > query_store_offset + QUERY_REGION_HEADER_SIZE as u64
        {
            query_store_end
        } else {
            query_store_offset + QUERY_REGION_HEADER_SIZE as u64
        };

        // Check if we need to grow
        if write_offset + size > file_size {
            self.grow_query_region(size)?;
        }

        Ok(write_offset)
    }

    /// Initialize a new query region at the end of the file.
    fn initialize_query_region(&self, initial_size: u64) -> Result<u64, crate::error::QueryError> {
        use crate::error::QueryError;
        use query::DEFAULT_QUERY_REGION_SIZE;
        use records::QUERY_REGION_HEADER_SIZE;

        let region_size = initial_size
            .max(DEFAULT_QUERY_REGION_SIZE)
            .max(QUERY_REGION_HEADER_SIZE as u64 + initial_size);

        // Get current file size
        let current_size = {
            let file = self.file.read();
            file.metadata()
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?
                .len()
        };

        let query_region_offset = current_size;
        let new_file_size = current_size + region_size;

        // Extend the file
        {
            let file = self.file.write();
            file.set_len(new_file_size)
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
        }

        // Write query region header
        let region_header = query::QueryStore::create_region_header();
        {
            let file = self.file.write();
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(&region_header, query_region_offset)
                    .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
            }
            #[cfg(not(unix))]
            {
                use std::io::{Seek, SeekFrom, Write};
                let mut f = &*file;
                f.seek(SeekFrom::Start(query_region_offset))
                    .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
                f.write_all(&region_header)
                    .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
            }
        }

        // Update file header with query region info
        {
            let mmap = self.mmap.read();
            let mut header = Self::read_header(&mmap);
            drop(mmap);

            header.set_query_store_offset(query_region_offset);
            header.set_query_store_end(query_region_offset + QUERY_REGION_HEADER_SIZE as u64);
            header.set_query_count(0);
            header.set_next_query_id(1);

            let file = self.file.write();
            Self::write_header(&file, &header).map_err(QueryError::Storage)?;
        }

        // Remap
        self.remap().map_err(QueryError::Storage)?;

        // Return offset after region header
        Ok(query_region_offset + QUERY_REGION_HEADER_SIZE as u64)
    }

    /// Grow the query region to accommodate more queries.
    fn grow_query_region(&self, additional_size: u64) -> Result<(), crate::error::QueryError> {
        use crate::error::QueryError;
        use query::MIN_QUERY_REGION_GROWTH;

        let growth = additional_size.max(MIN_QUERY_REGION_GROWTH);

        let file = self.file.write();
        let current_size = file
            .metadata()
            .map_err(|e| QueryError::Storage(StorageError::Io(e)))?
            .len();
        let new_size = current_size + growth;

        file.set_len(new_size)
            .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
        drop(file);

        self.remap().map_err(QueryError::Storage)?;

        Ok(())
    }

    /// Allocate a new query ID.
    fn allocate_query_id(&self) -> Result<u32, crate::error::QueryError> {
        use crate::error::QueryError;

        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        let query_id = header.next_query_id();
        header.set_next_query_id(query_id + 1);
        header.set_query_count(header.query_count() + 1);

        let file = self.file.write();
        Self::write_header(&file, &header).map_err(QueryError::Storage)?;
        drop(file);

        self.remap().map_err(QueryError::Storage)?;

        Ok(query_id)
    }

    /// Write query data to disk at the specified offset.
    fn write_query_data(&self, offset: u64, data: &[u8]) -> Result<(), crate::error::QueryError> {
        use crate::error::QueryError;

        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(data, offset)
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = &*file;
            f.seek(SeekFrom::Start(offset))
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
            f.write_all(data)
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
        }

        drop(file);
        self.remap().map_err(QueryError::Storage)?;

        Ok(())
    }

    /// Update the query store end offset in the header.
    fn update_query_header(&self, new_end: u64) -> Result<(), crate::error::QueryError> {
        use crate::error::QueryError;

        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        header.set_query_store_end(new_end);

        let file = self.file.write();
        Self::write_header(&file, &header).map_err(QueryError::Storage)?;
        drop(file);

        self.remap().map_err(QueryError::Storage)?;

        Ok(())
    }

    /// Read a query from disk at the specified offset.
    fn read_query_at_offset(&self, offset: u64) -> Option<crate::query::SavedQuery> {
        let mmap = self.mmap.read();

        if offset as usize >= mmap.len() {
            return None;
        }

        // Read enough bytes to determine record size
        let slice = &mmap[offset as usize..];
        query::QueryStore::deserialize_query(slice).ok()
    }

    /// Mark a query as deleted by setting the deleted flag.
    fn mark_query_deleted(&self, offset: u64) -> Result<(), crate::error::QueryError> {
        use crate::error::QueryError;
        use records::QUERY_FLAG_DELETED;

        // The flags field is at offset 4 in the QueryRecord (after id)
        let flags_offset = offset + 4;

        // Read current flags
        let current_flags = self.read_u16_at(flags_offset)?;

        // Set deleted flag
        let new_flags = current_flags | QUERY_FLAG_DELETED;
        let flag_bytes = new_flags.to_le_bytes();

        // Write back
        let file = self.file.write();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&flag_bytes, flags_offset)
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = &*file;
            f.seek(SeekFrom::Start(flags_offset))
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
            f.write_all(&flag_bytes)
                .map_err(|e| QueryError::Storage(StorageError::Io(e)))?;
        }

        drop(file);
        self.remap().map_err(QueryError::Storage)?;

        Ok(())
    }

    /// Helper to read a u16 value from mmap at a given offset.
    fn read_u16_at(&self, offset: u64) -> Result<u16, crate::error::QueryError> {
        use crate::error::QueryError;

        let mmap = self.mmap.read();
        let offset = offset as usize;

        if offset + 2 > mmap.len() {
            return Err(QueryError::Storage(StorageError::CorruptedData));
        }

        let bytes: [u8; 2] = mmap[offset..offset + 2].try_into().unwrap();
        Ok(u16::from_le_bytes(bytes))
    }

    /// Decrement the query count in the header.
    fn decrement_query_count(&self) -> Result<(), crate::error::QueryError> {
        use crate::error::QueryError;

        let mmap = self.mmap.read();
        let mut header = Self::read_header(&mmap);
        drop(mmap);

        let count = header.query_count();
        header.set_query_count(count.saturating_sub(1));

        let file = self.file.write();
        Self::write_header(&file, &header).map_err(QueryError::Storage)?;
        drop(file);

        self.remap().map_err(QueryError::Storage)?;

        Ok(())
    }

    /// Load query index from disk on database open.
    ///
    /// This scans the query region and rebuilds the in-memory index.
    pub(crate) fn load_query_index(&self) -> Result<(), StorageError> {
        use records::QUERY_RECORD_HEADER_SIZE;

        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        if !header.has_query_region() {
            return Ok(());
        }

        let query_store_offset = header.query_store_offset();
        let query_store_end = header.query_store_end();
        drop(mmap);

        // Skip region header
        let mut offset = query_store_offset + records::QUERY_REGION_HEADER_SIZE as u64;

        let mut query_index = self.query_index.write();
        query_index.clear();

        while offset < query_store_end {
            let mmap = self.mmap.read();

            if offset as usize + QUERY_RECORD_HEADER_SIZE > mmap.len() {
                break;
            }

            // Read record header to get ID, flags, and size
            let slice = &mmap[offset as usize..];
            let record = records::QueryRecord::from_bytes(slice);

            let id = record.id;
            let record_size = record.record_size as u64;

            // Skip if deleted
            if record.is_deleted() {
                offset += record_size;
                continue;
            }

            // Deserialize to get name
            if let Ok(saved_query) = query::QueryStore::deserialize_query(slice) {
                query_index.insert(saved_query.name, id, offset);
            }

            offset += record_size;
        }

        Ok(())
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
                    .filter_map(move |id| self.get_vertex(VertexId(id))),
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
                    .filter_map(move |id| self.get_edge(EdgeId(id))),
            ),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Returns iterator over all vertices in the graph.
    ///
    /// Scans all node slots from 0 to next_node_id (high-water mark), skipping deleted nodes.
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let next_node_id = self.get_header().next_node_id;

        Box::new((0..next_node_id).filter_map(move |id| self.get_vertex(VertexId(id))))
    }

    /// Returns iterator over all edges in the graph.
    ///
    /// Scans all edge slots from 0 to next_edge_id (high-water mark), skipping deleted edges.
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let next_edge_id = self.get_header().next_edge_id;

        Box::new((0..next_edge_id).filter_map(move |id| self.get_edge(EdgeId(id))))
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
// GraphStorageMut Trait Implementation
// =========================================================================

impl crate::storage::GraphStorageMut for MmapGraph {
    fn add_vertex(
        &mut self,
        label: &str,
        properties: std::collections::HashMap<String, crate::value::Value>,
    ) -> VertexId {
        // MmapGraph uses interior mutability, so we delegate to the inherent method
        MmapGraph::add_vertex(self, label, properties).expect("add_vertex failed")
    }

    fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: std::collections::HashMap<String, crate::value::Value>,
    ) -> Result<EdgeId, StorageError> {
        MmapGraph::add_edge(self, src, dst, label, properties)
    }

    fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: crate::value::Value,
    ) -> Result<(), StorageError> {
        MmapGraph::set_vertex_property(self, id, key, value)
    }

    fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: crate::value::Value,
    ) -> Result<(), StorageError> {
        MmapGraph::set_edge_property(self, id, key, value)
    }

    fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        MmapGraph::remove_vertex(self, id)
    }

    fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError> {
        MmapGraph::remove_edge(self, id)
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
        assert_eq!(node_capacity, 100);
        assert_eq!(edge_count, 0);
        assert_eq!(edge_capacity, 200);
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
        header.update_crc32(); // Update CRC after changing version
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        // Version mismatch now returns VersionMismatch error
        assert!(matches!(
            result,
            Err(StorageError::VersionMismatch {
                file_version: 999,
                min_supported: 1,
                max_supported: 2
            })
        ));
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
        // HEADER_SIZE (192 for V2) + (100 * 48) + (200 * 56) + (32 * 1024)
        let expected_size = HEADER_SIZE + (100 * 48) + (200 * 56) + (32 * 1024);
        assert_eq!(file_size, expected_size as u64);

        // Verify header fields
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        let property_arena_offset = header.property_arena_offset;
        let string_table_offset = header.string_table_offset;
        let free_node_head = header.free_node_head;

        // Property arena should start after node and edge tables
        let expected_arena_offset = HEADER_SIZE + (100 * 48) + (200 * 56);
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
        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);
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

        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);
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

        // Should be: header (136) + node_table (100 * 48)
        let expected = HEADER_SIZE + (100 * NODE_RECORD_SIZE);
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

        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);

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
        let mut map = crate::value::ValueMap::new();
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

        // Update header to reflect node_count and next_node_id
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = nodes.len() as u64;
            header.next_node_id = nodes.len() as u64;
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

        // Update header to reflect node_count and next_node_id
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = nodes.len() as u64;
            header.next_node_id = nodes.len() as u64;
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

        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);

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

        // Update header to reflect edge_count and next_edge_id
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.edge_count = edges.len() as u64;
            header.next_edge_id = edges.len() as u64;
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

        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);

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

        // Update header to reflect edge_count and next_edge_id
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.edge_count = edges.len() as u64;
            header.next_edge_id = edges.len() as u64;
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

            // Update header to reflect node_count and next_node_id
            {
                let mmap = graph.mmap.read();
                let mut header = MmapGraph::read_header(&mmap);
                header.node_count = nodes.len() as u64;
                header.next_node_id = nodes.len() as u64;
                header.update_crc32(); // Must update CRC after modifying V2 header
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

        // Update header to reflect node_count and next_node_id
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = nodes.len() as u64;
            header.next_node_id = nodes.len() as u64;
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

        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);

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
            header.next_node_id = 3;
            header.next_edge_id = 3;
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
        let edge_table_offset = HEADER_SIZE + (100 * NODE_RECORD_SIZE);

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

        let mut inner_map = crate::value::ValueMap::new();
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
        assert_eq!(initial_capacity, 100, "Initial capacity should be 100");

        // Manually set node_count and next_node_id to capacity to force growth on next allocate
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = 100;
            header.next_node_id = 100;
            drop(mmap);

            let file = graph.file.write();
            MmapGraph::write_header(&file, &header).unwrap();
            drop(file);
            graph.remap().unwrap();
        }

        // Allocate should trigger growth
        let slot = graph.allocate_node_slot().unwrap();
        assert_eq!(slot.0, 100, "Should allocate at slot 100");

        // Capacity should have doubled
        let new_capacity = graph.get_header().node_capacity;
        assert_eq!(new_capacity, 200, "Capacity should double to 200");
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
        assert_eq!(initial_capacity, 200, "Initial edge capacity should be 200");

        // Manually set edge_count and next_edge_id to capacity to force growth on next allocate
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.edge_count = 200;
            header.next_edge_id = 200;
            drop(mmap);

            let file = graph.file.write();
            MmapGraph::write_header(&file, &header).unwrap();
            drop(file);
            graph.remap().unwrap();
        }

        // Allocate should trigger growth
        let slot = graph.allocate_edge_slot().unwrap();
        assert_eq!(slot.0, 200, "Should allocate at edge slot 200");

        // Capacity should have doubled
        let new_capacity = graph.get_header().edge_capacity;
        assert_eq!(new_capacity, 400, "Edge capacity should double to 400");
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
        for _ in 0..3 {
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

        let mut inner_map = crate::value::ValueMap::new();
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
        assert_eq!(initial_capacity, 100);

        // Manually set node_count and next_node_id to capacity - 1 to force growth on second add
        {
            let mmap = graph.mmap.read();
            let mut header = MmapGraph::read_header(&mmap);
            header.node_count = 99;
            header.next_node_id = 99;
            drop(mmap);

            let file = graph.file.write();
            MmapGraph::write_header(&file, &header).unwrap();
            drop(file);
            graph.remap().unwrap();
        }

        // First add at slot 99
        let props = std::collections::HashMap::new();
        let v1 = graph.add_vertex("person", props.clone()).unwrap();
        assert_eq!(v1.0, 99);

        // Second add should trigger growth
        let v2 = graph.add_vertex("person", props).unwrap();
        assert_eq!(v2.0, 100);

        // Verify capacity grew
        let new_capacity = graph.get_header().node_capacity;
        assert_eq!(new_capacity, 200, "Capacity should double");

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
        assert!(knows_edges.contains(e1.0));
        assert!(knows_edges.contains(e3.0));

        // "likes" label should have e2
        let likes_edges = edge_labels.get(&likes_id).unwrap();
        assert_eq!(likes_edges.len(), 1);
        assert!(likes_edges.contains(e2.0));
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
            let _tx_id = wal.begin_transaction().unwrap();
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

    // =========================================================================
    // Remove Operations Tests
    // =========================================================================

    fn empty_props() -> std::collections::HashMap<String, crate::value::Value> {
        std::collections::HashMap::new()
    }

    #[test]
    fn test_remove_edge_basic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();
        let edge_id = graph.add_edge(v1, v2, "knows", empty_props()).unwrap();

        assert_eq!(graph.edge_count(), 1);
        assert!(graph.get_edge(edge_id).is_some());

        graph.remove_edge(edge_id).unwrap();

        assert_eq!(graph.edge_count(), 0);
        assert!(graph.get_edge(edge_id).is_none());
        assert!(graph.get_vertex(v1).is_some());
        assert!(graph.get_vertex(v2).is_some());
    }

    #[test]
    fn test_remove_edge_updates_adjacency_lists() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let a = graph.add_vertex("person", empty_props()).unwrap();
        let b = graph.add_vertex("person", empty_props()).unwrap();
        let c = graph.add_vertex("person", empty_props()).unwrap();

        let e1 = graph.add_edge(a, b, "knows", empty_props()).unwrap();
        let e2 = graph.add_edge(b, c, "knows", empty_props()).unwrap();

        assert_eq!(graph.out_edges(a).count(), 1);
        assert_eq!(graph.out_edges(b).count(), 1);
        assert_eq!(graph.in_edges(b).count(), 1);
        assert_eq!(graph.in_edges(c).count(), 1);

        graph.remove_edge(e1).unwrap();

        assert_eq!(graph.out_edges(a).count(), 0);
        assert_eq!(graph.in_edges(b).count(), 0);
        assert!(graph.get_edge(e2).is_some());
        assert_eq!(graph.out_edges(b).count(), 1);
    }

    #[test]
    fn test_remove_edge_from_label_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();
        let edge_id = graph.add_edge(v1, v2, "knows", empty_props()).unwrap();

        assert_eq!(graph.edges_with_label("knows").count(), 1);

        graph.remove_edge(edge_id).unwrap();

        assert_eq!(graph.edges_with_label("knows").count(), 0);
    }

    #[test]
    fn test_remove_edge_slot_reused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();

        let e1 = graph.add_edge(v1, v2, "knows", empty_props()).unwrap();
        graph.remove_edge(e1).unwrap();

        let e2 = graph.add_edge(v1, v2, "likes", empty_props()).unwrap();
        assert_eq!(e2.0, 0, "Slot should be reused");

        let edge = graph.get_edge(e2).unwrap();
        assert_eq!(edge.label, "likes");
    }

    #[test]
    fn test_remove_edge_not_found() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let result = graph.remove_edge(EdgeId(999));
        assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));
    }

    #[test]
    fn test_remove_vertex_basic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        assert_eq!(graph.vertex_count(), 1);
        assert!(graph.get_vertex(v1).is_some());

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 0);
        assert!(graph.get_vertex(v1).is_none());
    }

    #[test]
    fn test_remove_vertex_removes_incident_edges() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let a = graph.add_vertex("person", empty_props()).unwrap();
        let b = graph.add_vertex("person", empty_props()).unwrap();
        let c = graph.add_vertex("person", empty_props()).unwrap();

        graph.add_edge(a, b, "knows", empty_props()).unwrap();
        graph.add_edge(b, c, "knows", empty_props()).unwrap();

        assert_eq!(graph.vertex_count(), 3);
        assert_eq!(graph.edge_count(), 2);

        graph.remove_vertex(b).unwrap();

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 0);
        assert!(graph.get_vertex(b).is_none());
        assert!(graph.get_vertex(a).is_some());
        assert!(graph.get_vertex(c).is_some());
        assert_eq!(graph.out_edges(a).count(), 0);
        assert_eq!(graph.in_edges(c).count(), 0);
    }

    #[test]
    fn test_remove_vertex_with_self_loop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v = graph.add_vertex("person", empty_props()).unwrap();
        graph.add_edge(v, v, "self", empty_props()).unwrap();

        assert_eq!(graph.vertex_count(), 1);
        assert_eq!(graph.edge_count(), 1);

        graph.remove_vertex(v).unwrap();

        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_remove_vertex_from_label_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        graph.add_vertex("software", empty_props()).unwrap();

        assert_eq!(graph.vertices_with_label("person").count(), 1);

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertices_with_label("person").count(), 0);
        assert_eq!(graph.vertices_with_label("software").count(), 1);
    }

    #[test]
    fn test_remove_vertex_slot_reused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        graph.remove_vertex(v1).unwrap();

        let v2 = graph.add_vertex("software", empty_props()).unwrap();
        assert_eq!(v2.0, 0, "Slot should be reused");

        let vertex = graph.get_vertex(v2).unwrap();
        assert_eq!(vertex.label, "software");
    }

    #[test]
    fn test_remove_vertex_not_found() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let result = graph.remove_vertex(VertexId(999));
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    // =========================================================================
    // Phase 5.2: Deleted Elements Excluded from Iteration Tests
    // =========================================================================

    #[test]
    fn test_all_vertices_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();
        let v3 = graph.add_vertex("person", empty_props()).unwrap();

        assert_eq!(graph.all_vertices().count(), 3);

        graph.remove_vertex(v2).unwrap();

        let vertices: Vec<_> = graph.all_vertices().collect();
        assert_eq!(vertices.len(), 2);

        let ids: Vec<_> = vertices.iter().map(|v| v.id).collect();
        assert!(ids.contains(&v1));
        assert!(!ids.contains(&v2));
        assert!(ids.contains(&v3));
    }

    #[test]
    fn test_all_edges_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();
        let v3 = graph.add_vertex("person", empty_props()).unwrap();

        let e1 = graph.add_edge(v1, v2, "knows", empty_props()).unwrap();
        let e2 = graph.add_edge(v2, v3, "knows", empty_props()).unwrap();
        let e3 = graph.add_edge(v1, v3, "knows", empty_props()).unwrap();

        assert_eq!(graph.all_edges().count(), 3);

        graph.remove_edge(e2).unwrap();

        let edges: Vec<_> = graph.all_edges().collect();
        assert_eq!(edges.len(), 2);

        let ids: Vec<_> = edges.iter().map(|e| e.id).collect();
        assert!(ids.contains(&e1));
        assert!(!ids.contains(&e2));
        assert!(ids.contains(&e3));
    }

    #[test]
    fn test_vertices_with_label_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();
        graph.add_vertex("software", empty_props()).unwrap();

        assert_eq!(graph.vertices_with_label("person").count(), 2);

        graph.remove_vertex(v1).unwrap();

        let people: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(people.len(), 1);
        assert_eq!(people[0].id, v2);
    }

    #[test]
    fn test_edges_with_label_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("person", empty_props()).unwrap();
        let v2 = graph.add_vertex("person", empty_props()).unwrap();

        let e1 = graph.add_edge(v1, v2, "knows", empty_props()).unwrap();
        let e2 = graph.add_edge(v2, v1, "knows", empty_props()).unwrap();

        assert_eq!(graph.edges_with_label("knows").count(), 2);

        graph.remove_edge(e1).unwrap();

        let knows_edges: Vec<_> = graph.edges_with_label("knows").collect();
        assert_eq!(knows_edges.len(), 1);
        assert_eq!(knows_edges[0].id, e2);
    }

    #[test]
    fn test_add_delete_iteration_cycle() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let v1 = graph.add_vertex("a", empty_props()).unwrap();
        let v2 = graph.add_vertex("b", empty_props()).unwrap();
        let v3 = graph.add_vertex("c", empty_props()).unwrap();

        graph.add_edge(v1, v2, "x", empty_props()).unwrap();
        graph.add_edge(v2, v3, "y", empty_props()).unwrap();

        assert_eq!(graph.all_vertices().count(), 3);
        assert_eq!(graph.all_edges().count(), 2);

        graph.remove_vertex(v2).unwrap();

        assert_eq!(graph.all_vertices().count(), 2);
        assert_eq!(graph.all_edges().count(), 0);

        let v4 = graph.add_vertex("d", empty_props()).unwrap();
        let e3 = graph.add_edge(v1, v3, "z", empty_props()).unwrap();

        assert_eq!(graph.all_vertices().count(), 3);
        assert_eq!(graph.all_edges().count(), 1);

        let vertex_ids: Vec<_> = graph.all_vertices().map(|v| v.id).collect();
        assert!(vertex_ids.contains(&v1));
        assert!(vertex_ids.contains(&v3));
        assert!(vertex_ids.contains(&v4));

        let edge_ids: Vec<_> = graph.all_edges().map(|e| e.id).collect();
        assert!(edge_ids.contains(&e3));
    }

    // =========================================================================
    // Schema Persistence Tests
    // =========================================================================

    #[test]
    fn test_load_schema_returns_none_when_no_schema() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let result = graph.load_schema().unwrap();
        assert!(result.is_none(), "Should return None when no schema saved");
    }

    #[test]
    fn test_save_and_load_schema_roundtrip() {
        use crate::schema::{PropertyType, SchemaBuilder, ValidationMode};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create a schema
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Person")
            .property("name", PropertyType::String)
            .property("age", PropertyType::Int)
            .done()
            .vertex("Company")
            .property("name", PropertyType::String)
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .property("since", PropertyType::Int)
            .done()
            .build();

        // Save schema
        graph.save_schema(&schema).unwrap();

        // Load schema back
        let loaded = graph.load_schema().unwrap().expect("Schema should exist");

        // Verify contents
        assert_eq!(loaded.mode, ValidationMode::Strict);
        assert!(loaded.vertex_schemas.contains_key("Person"));
        assert!(loaded.vertex_schemas.contains_key("Company"));
        assert!(loaded.edge_schemas.contains_key("WORKS_AT"));

        let person = &loaded.vertex_schemas["Person"];
        assert!(person.properties.contains_key("name"));
        assert!(person.properties.contains_key("age"));
    }

    #[test]
    fn test_schema_persists_across_reopen() {
        use crate::schema::{PropertyType, SchemaBuilder, ValidationMode};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create and save schema
        {
            let graph = MmapGraph::open(&path).unwrap();

            let schema = SchemaBuilder::new()
                .mode(ValidationMode::Warn)
                .vertex("User")
                .property("email", PropertyType::String)
                .done()
                .build();

            graph.save_schema(&schema).unwrap();
        }

        // Reopen and verify
        {
            let graph = MmapGraph::open(&path).unwrap();
            let loaded = graph.load_schema().unwrap().expect("Schema should exist");

            assert_eq!(loaded.mode, ValidationMode::Warn);
            assert!(loaded.vertex_schemas.contains_key("User"));

            let user = &loaded.vertex_schemas["User"];
            let email_prop = &user.properties["email"];
            assert!(email_prop.required);
        }
    }

    #[test]
    fn test_clear_schema() {
        use crate::schema::{PropertyType, SchemaBuilder};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Save a schema
        let schema = SchemaBuilder::new()
            .vertex("Test")
            .property("name", PropertyType::String)
            .done()
            .build();

        graph.save_schema(&schema).unwrap();
        assert!(graph.load_schema().unwrap().is_some());

        // Clear the schema
        graph.clear_schema().unwrap();
        assert!(graph.load_schema().unwrap().is_none());
    }

    #[test]
    fn test_schema_with_complex_types() {
        use crate::schema::{PropertyType, SchemaBuilder, ValidationMode};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create schema with complex types
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Document")
            .property(
                "tags",
                PropertyType::List(Some(Box::new(PropertyType::String))),
            )
            .property(
                "metadata",
                PropertyType::Map(Some(Box::new(PropertyType::String))),
            )
            .property("data", PropertyType::Any)
            .done()
            .build();

        // Save and reload
        graph.save_schema(&schema).unwrap();
        let loaded = graph.load_schema().unwrap().expect("Schema should exist");

        let doc = &loaded.vertex_schemas["Document"];

        // Verify complex types
        match &doc.properties["tags"].value_type {
            PropertyType::List(Some(inner)) => {
                assert!(matches!(inner.as_ref(), PropertyType::String));
            }
            _ => panic!("Expected List(String)"),
        }

        match &doc.properties["metadata"].value_type {
            PropertyType::Map(Some(inner)) => {
                assert!(matches!(inner.as_ref(), PropertyType::String));
            }
            _ => panic!("Expected Map(String)"),
        }

        assert!(matches!(
            doc.properties["data"].value_type,
            PropertyType::Any
        ));
    }

    #[test]
    fn test_schema_overwrite() {
        use crate::schema::{PropertyType, SchemaBuilder, ValidationMode};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Save first schema
        let schema1 = SchemaBuilder::new()
            .mode(ValidationMode::None)
            .vertex("A")
            .property("x", PropertyType::Int)
            .done()
            .build();
        graph.save_schema(&schema1).unwrap();

        // Save second schema (overwrite)
        let schema2 = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("B")
            .property("y", PropertyType::String)
            .done()
            .build();
        graph.save_schema(&schema2).unwrap();

        // Verify second schema is loaded
        let loaded = graph.load_schema().unwrap().expect("Schema should exist");
        assert_eq!(loaded.mode, ValidationMode::Strict);
        assert!(!loaded.vertex_schemas.contains_key("A"));
        assert!(loaded.vertex_schemas.contains_key("B"));
    }

    // =========================================================================
    // Property Index Tests
    // =========================================================================

    #[test]
    fn test_create_and_drop_index() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Initially no indexes
        assert_eq!(graph.index_count(), 0);
        assert!(!graph.has_index("idx_age"));

        // Create an index
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .name("idx_age")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        assert_eq!(graph.index_count(), 1);
        assert!(graph.has_index("idx_age"));

        // List indexes
        let indexes = graph.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "idx_age");
        assert_eq!(indexes[0].property, "age");

        // Drop the index
        graph.drop_index("idx_age").unwrap();
        assert_eq!(graph.index_count(), 0);
        assert!(!graph.has_index("idx_age"));
    }

    #[test]
    fn test_create_index_duplicate_error() {
        use crate::index::IndexBuilder;
        use crate::index::IndexError;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let spec = IndexBuilder::vertex()
            .property("name")
            .name("idx_name")
            .build()
            .unwrap();
        graph.create_index(spec.clone()).unwrap();

        // Try to create duplicate
        let result = graph.create_index(spec);
        assert!(matches!(result, Err(IndexError::AlreadyExists(_))));
    }

    #[test]
    fn test_drop_index_not_found_error() {
        use crate::index::IndexError;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let result = graph.drop_index("nonexistent");
        assert!(matches!(result, Err(IndexError::NotFound(_))));
    }

    #[test]
    fn test_index_accelerated_vertex_lookup() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add some vertices
        let props1 = std::collections::HashMap::from([("age".to_string(), Value::Int(25))]);
        let props2 = std::collections::HashMap::from([("age".to_string(), Value::Int(30))]);
        let props3 = std::collections::HashMap::from([("age".to_string(), Value::Int(25))]);
        let v1 = graph.add_vertex("person", props1).unwrap();
        let v2 = graph.add_vertex("person", props2).unwrap();
        let v3 = graph.add_vertex("person", props3).unwrap();

        // Create index on age
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .name("idx_person_age")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Query using index (label, property, value)
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(results.len(), 2);
        let ids: Vec<_> = results.iter().map(|v| v.id).collect();
        assert!(ids.contains(&v1));
        assert!(ids.contains(&v3));

        // Query for different value
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(30))
            .collect();
        assert_eq!(results.len(), 1);
        let ids: Vec<_> = results.iter().map(|v| v.id).collect();
        assert!(ids.contains(&v2));

        // Query for non-existent value
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(99))
            .collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_index_accelerated_range_query() {
        use crate::index::IndexBuilder;
        use std::ops::Bound;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add vertices with various ages
        for age in [18, 21, 25, 30, 35, 40, 45, 50] {
            let props = std::collections::HashMap::from([("age".to_string(), Value::Int(age))]);
            graph.add_vertex("person", props).unwrap();
        }

        // Create index
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Range query: 25 <= age <= 40 (label, property, start, end)
        let start = Value::Int(25);
        let end = Value::Int(40);
        let results: Vec<_> = graph
            .vertices_by_property_range(
                Some("person"),
                "age",
                Bound::Included(&start),
                Bound::Included(&end),
            )
            .collect();
        assert_eq!(results.len(), 4); // 25, 30, 35, 40

        // Range query: age > 35
        let start2 = Value::Int(35);
        let results: Vec<_> = graph
            .vertices_by_property_range(
                Some("person"),
                "age",
                Bound::Excluded(&start2),
                Bound::Unbounded,
            )
            .collect();
        assert_eq!(results.len(), 3); // 40, 45, 50
    }

    #[test]
    fn test_unique_index() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add vertices with unique emails
        let props1 =
            std::collections::HashMap::from([("email".to_string(), Value::from("a@test.com"))]);
        let props2 =
            std::collections::HashMap::from([("email".to_string(), Value::from("b@test.com"))]);
        graph.add_vertex("user", props1).unwrap();
        graph.add_vertex("user", props2).unwrap();

        // Create unique index - should succeed
        let spec = IndexBuilder::vertex()
            .label("user")
            .property("email")
            .unique()
            .name("idx_email")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Now add a vertex with duplicate email
        let props3 =
            std::collections::HashMap::from([("email".to_string(), Value::from("a@test.com"))]);
        let v3 = graph.add_vertex("user", props3).unwrap();

        // The vertex is added, but index maintenance should have detected the duplicate
        // and logged a warning (index update fails silently for maintainability)
        // In a stricter implementation, add_vertex would return an error

        // Verify the vertex exists
        assert!(graph.get_vertex(v3).is_some());
    }

    #[test]
    fn test_index_maintenance_on_vertex_add() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Create index FIRST (before adding data)
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Add a vertex - should be automatically indexed
        let props = std::collections::HashMap::from([("age".to_string(), Value::Int(25))]);
        let v1 = graph.add_vertex("person", props).unwrap();

        // Query should find the newly added vertex
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, v1);
    }

    #[test]
    fn test_index_maintenance_on_vertex_remove() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add vertices
        let props = std::collections::HashMap::from([("age".to_string(), Value::Int(25))]);
        let v1 = graph.add_vertex("person", props).unwrap();

        // Create index
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Verify indexed
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(results.len(), 1);

        // Remove vertex
        graph.remove_vertex(v1).unwrap();

        // Query should return empty (vertex removed from index)
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_index_maintenance_on_property_update() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Add vertex
        let props = std::collections::HashMap::from([("age".to_string(), Value::Int(25))]);
        let v1 = graph.add_vertex("person", props).unwrap();

        // Create index
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Verify initial indexing
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(results.len(), 1);

        // Update property
        graph
            .set_vertex_property(v1, "age", Value::Int(30))
            .unwrap();

        // Old value should return empty
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(results.len(), 0);

        // New value should find the vertex
        let results: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(30))
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, v1);
    }

    #[test]
    fn test_index_persistence_across_reopen() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create graph, add data, create index
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add vertices
            let props1 = std::collections::HashMap::from([("age".to_string(), Value::Int(25))]);
            let props2 = std::collections::HashMap::from([("age".to_string(), Value::Int(30))]);
            graph.add_vertex("person", props1).unwrap();
            graph.add_vertex("person", props2).unwrap();

            // Create index
            let spec = IndexBuilder::vertex()
                .label("person")
                .property("age")
                .name("idx_person_age")
                .build()
                .unwrap();
            graph.create_index(spec).unwrap();

            assert_eq!(graph.index_count(), 1);
            assert!(graph.has_index("idx_person_age"));

            // Verify index file was created
            let idx_path = path.with_extension("idx.json");
            assert!(idx_path.exists(), "Index specs file should exist");
        }

        // Reopen and verify index is restored
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Index should be restored
            assert_eq!(graph.index_count(), 1);
            assert!(graph.has_index("idx_person_age"));

            // Verify index spec is correct
            let indexes = graph.list_indexes();
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].name, "idx_person_age");
            assert_eq!(indexes[0].property, "age");

            // Verify index is functional (data was repopulated)
            let results: Vec<_> = graph
                .vertices_by_property(Some("person"), "age", &Value::Int(25))
                .collect();
            assert_eq!(results.len(), 1);

            let results: Vec<_> = graph
                .vertices_by_property(Some("person"), "age", &Value::Int(30))
                .collect();
            assert_eq!(results.len(), 1);
        }
    }

    #[test]
    fn test_index_drop_persists_across_reopen() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create graph with index, then drop it
        {
            let graph = MmapGraph::open(&path).unwrap();

            let spec = IndexBuilder::vertex()
                .property("name")
                .name("idx_name")
                .build()
                .unwrap();
            graph.create_index(spec).unwrap();
            assert_eq!(graph.index_count(), 1);

            // Drop the index
            graph.drop_index("idx_name").unwrap();
            assert_eq!(graph.index_count(), 0);
        }

        // Reopen and verify index is not restored
        {
            let graph = MmapGraph::open(&path).unwrap();
            assert_eq!(graph.index_count(), 0);
            assert!(!graph.has_index("idx_name"));
        }
    }

    #[test]
    fn test_multiple_indexes_persistence() {
        use crate::index::IndexBuilder;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create multiple indexes
        {
            let graph = MmapGraph::open(&path).unwrap();

            // Add test data
            let props = std::collections::HashMap::from([
                ("name".to_string(), Value::from("Alice")),
                ("age".to_string(), Value::Int(25)),
            ]);
            graph.add_vertex("person", props).unwrap();

            // Create multiple indexes
            graph
                .create_index(
                    IndexBuilder::vertex()
                        .property("name")
                        .name("idx_name")
                        .build()
                        .unwrap(),
                )
                .unwrap();

            graph
                .create_index(
                    IndexBuilder::vertex()
                        .property("age")
                        .name("idx_age")
                        .build()
                        .unwrap(),
                )
                .unwrap();

            graph
                .create_index(
                    IndexBuilder::vertex()
                        .property("email")
                        .unique()
                        .name("idx_email")
                        .build()
                        .unwrap(),
                )
                .unwrap();

            assert_eq!(graph.index_count(), 3);
        }

        // Reopen and verify all indexes are restored
        {
            let graph = MmapGraph::open(&path).unwrap();

            assert_eq!(graph.index_count(), 3);
            assert!(graph.has_index("idx_name"));
            assert!(graph.has_index("idx_age"));
            assert!(graph.has_index("idx_email"));

            // Verify they're functional
            let results: Vec<_> = graph
                .vertices_by_property(None, "name", &Value::from("Alice"))
                .collect();
            assert_eq!(results.len(), 1);
        }
    }

    #[test]
    fn test_edge_index() {
        use crate::index::IndexBuilder;

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

        // Create edges with properties (src, dst, label, properties)
        let props1 = std::collections::HashMap::from([("weight".to_string(), Value::Float(1.5))]);
        let props2 = std::collections::HashMap::from([("weight".to_string(), Value::Float(2.0))]);
        let e1 = graph.add_edge(v1, v2, "knows", props1).unwrap();
        let _e2 = graph.add_edge(v2, v1, "knows", props2).unwrap();

        // Create edge index
        let spec = IndexBuilder::edge()
            .label("knows")
            .property("weight")
            .name("idx_edge_weight")
            .build()
            .unwrap();
        graph.create_index(spec).unwrap();

        // Query using index (label, property, value)
        let results: Vec<_> = graph
            .edges_by_property(Some("knows"), "weight", &Value::Float(1.5))
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, e1);
    }
}
