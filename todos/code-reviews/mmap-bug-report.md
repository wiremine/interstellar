# Memory-Mapped Storage Backend Bug Report

This document details bugs identified in the `src/storage/mmap/` module during a comprehensive code review.

---

## Critical Severity

### 1. WAL Transaction ID Counter Resets on Reopen - **FIXED**

**File:** `src/storage/mmap/wal.rs:488-492`

**Bug:** The `WriteAheadLog::open()` initializes `next_tx_id` to 0 on every open, regardless of existing transactions in the WAL. If the database is closed and reopened without full recovery, new transactions will reuse transaction IDs from the prior session.

**Impact:** Transaction ID collisions can cause recovery to apply the wrong transaction or skip operations entirely, leading to data corruption or data loss.

**Fix Applied:** Added `scan_max_tx_id()` method that scans the WAL during `open()` to find the maximum transaction ID. The `next_tx_id` is now initialized to `max_tx_id + 1` if any transactions exist, or 0 for empty WAL files.

---

### 2. Recovery Doesn't Adjust Property Offsets After Table Growth - **DEFERRED (Design Limitation)**

**File:** `src/storage/mmap/recovery.rs:93-158`

**Bug:** When `grow_node_table()` or `grow_edge_table()` is called, property offsets are shifted. However, recovery replays records from the WAL with their original `prop_head` values, which may now point to incorrect locations if the tables grew and shifted the arena.

The WAL stores `SerializableNodeRecord` and `SerializableEdgeRecord` with `prop_head` offsets that were valid at write time. If table growth occurred and property data was relocated, these offsets become stale.

**Impact:** After crash recovery, properties may be read from garbage memory locations, leading to corrupted data or crashes.

**Status:** This is a design limitation that requires more extensive refactoring to fix properly. Options include:
1. Log table growth operations to WAL and replay them during recovery
2. Store relative offsets (offset from arena start) in WAL entries instead of absolute offsets
3. Store property key/value data in WAL instead of offsets, re-allocate properties during recovery

**Workaround:** The current implementation calls `checkpoint()` after table growth operations, which truncates the WAL. As long as checkpoints are performed regularly (which they are after `grow_*` operations), this bug is mitigated.

**Note:** Table growth operations DO include proper checkpointing behavior (the file is synced and remapped), reducing the window for this bug to manifest. However, if a crash occurs between growth and the next checkpoint, data corruption is possible.

---

### 3. Stale Mmap During `adjust_property_offsets` - **FIXED**

**File:** `src/storage/mmap/mod.rs:2191-2294`

**Bug:** `adjust_property_offsets()` reads from `self.mmap.read()` while holding the file write lock. However, the mmap was created before the file was extended in `grow_node_table()` or `grow_edge_table()`. The mmap view is stale and doesn't reflect the new file size or data positions.

**Impact:** Reads stale/corrupt data when adjusting property offsets, may write incorrect offsets or miss records entirely. Can cause property data corruption.

**Fix Applied:** Refactored `adjust_property_offsets()` to:
1. Accept `edge_table_start` as a parameter (the caller knows where edges are)
2. Read node/edge records directly from the file using positioned I/O (`read_exact_at` on Unix, `seek`+`read_exact` on other platforms)
3. No longer depends on mmap for reading records during table growth

---

### 4. Non-Atomic Seek+Write on Non-Unix Platforms

**File:** `src/storage/mmap/mod.rs` (multiple locations), `src/storage/mmap/recovery.rs` (multiple locations)

**Bug:** On non-Unix platforms, the code uses `seek()` + `write_all()` which is not atomic:

```rust
#[cfg(not(unix))]
{
    use std::io::{Seek, SeekFrom, Write};
    let mut file = &*file;
    file.seek(SeekFrom::Start(offset))?;  // Can be interrupted
    file.write_all(&bytes)?;              // Writes to wrong position
}
```

If two threads or a signal interrupt between seek and write, data will be written to the wrong location.

**Impact:** Data corruption on Windows and other non-Unix platforms under concurrent access.

**Suggested Fix:** Use platform-specific positioned I/O on Windows (`WriteFile` with offset) or use a mutex to serialize file operations. Consider using `pwrite` emulation.

---

## High Severity

### 5. FreeList.len() Returns Incorrect Count

**File:** `src/storage/mmap/freelist.rs:248-258`

**Bug:** The `len()` method returns `next_pointers.len()` when the list is non-empty, but the head entry itself is not counted in `next_pointers` when the list has exactly one element.

```rust
pub fn len(&self) -> usize {
    if self.head == u64::MAX {
        0
    } else {
        // BUG: When only one item is freed, next_pointers has 1 entry
        // pointing to u64::MAX, so this returns 1, which is correct.
        // But the comment says "head is always in the list, plus all entries"
        // which is misleading. The implementation is actually correct but
        // the mental model in the comment is wrong.
        self.next_pointers.len()
    }
}
```

**Verification:** Looking at `free()`:
```rust
pub fn free(&mut self, slot_id: u64) {
    self.next_pointers.insert(slot_id, self.head);  // Inserts slot_id -> old_head
    self.head = slot_id;
}
```

When first item (5) is freed: `next_pointers = {5 -> MAX}`, `head = 5`. `len()` = 1. Correct.
When second item (3) is freed: `next_pointers = {5 -> MAX, 3 -> 5}`, `head = 3`. `len()` = 2. Correct.

**Status:** After further analysis, the implementation is actually correct. The comment in the code is misleading but the logic is sound. This is NOT a bug.

---

### 6. TOCTOU (Time-of-Check-Time-of-Use) in Allocation - **FIXED**

**File:** `src/storage/mmap/arena.rs:119-134`

**Bug:** The arena allocator used atomic fetch_add followed by a bounds check and potential fetch_sub rollback. This pattern has a race condition where two concurrent allocations could both exceed bounds, or the rollback could interfere with another thread's allocation.

**Impact:** Can allocate past arena bounds or have inconsistent arena state under heavy concurrent writes.

**Fix Applied:** Replaced fetch_add/fetch_sub pattern with a compare-and-swap loop:
```rust
loop {
    let current = self.current_offset.load(Ordering::SeqCst);
    let new_offset = current + size;
    if new_offset > self.arena_end {
        return Err(StorageError::OutOfSpace);
    }
    if self.current_offset.compare_exchange(
        current, new_offset, Ordering::SeqCst, Ordering::SeqCst
    ).is_ok() {
        return Ok(current);
    }
    // Retry on contention
}
```

---

### 7. grow_node_table/grow_edge_table Move Data While Mmap Is Live

**File:** `src/storage/mmap/mod.rs:1806-1951`, `src/storage/mmap/mod.rs:1974-2084`

**Bug:** During table growth, data is read from the mmap into memory, the file is extended, and data is written to new positions. However, other threads may be reading from the mmap concurrently, seeing either old data positions or partially written new data.

```rust
// Read arena data while holding mmap read lock
let mut arena_data = vec![0u8; arena_size];
arena_data.copy_from_slice(&mmap[old_arena_start..old_string_table_start]);
drop(mmap);  // Released!

// ... file is extended ...
// ... data written to new positions ...

// Other threads still using old mmap see stale data!
```

**Impact:** Concurrent readers see corrupted data during table growth. Race conditions can cause crashes or data corruption.

**Suggested Fix:** Use a readers-writer approach where growth operations acquire exclusive access to both file and mmap. Consider copy-on-write or double-buffering for the mmap.

---

### 8. Missing Sync Before Remap in Multiple Locations

**File:** `src/storage/mmap/mod.rs` (various write methods)

**Bug:** Several methods write to the file then call `remap()` without syncing first:

```rust
pub fn write_property_data(&self, offset: u64, data: &[u8]) -> Result<(), StorageError> {
    // ... write to file ...
    drop(file);
    self.remap()?;  // Remap without sync - may not see writes!
    Ok(())
}
```

Without `sync_data()` before remap, the kernel may not have flushed dirty pages to the file, so the new mmap may not reflect the written data.

**Impact:** Readers may not see recently written data until a later sync occurs. Can cause inconsistent views and "missing" writes.

**Suggested Fix:** Call `file.sync_data()` before dropping the file handle and remapping.

---

## Medium Severity

### 9. Panic in from_bytes() Methods

**File:** `src/storage/mmap/records.rs`, `src/storage/mmap/wal.rs`

**Bug:** All `from_bytes()` methods use `assert!` which panics in release builds:

```rust
pub fn from_bytes(bytes: &[u8]) -> Self {
    assert!(bytes.len() >= HEADER_SIZE, "Buffer too small for FileHeader");
    // ...
}
```

**Impact:** Corrupted data can crash the database instead of returning an error.

**Suggested Fix:** Return `Result<Self, StorageError>` instead of panicking:
```rust
pub fn from_bytes(bytes: &[u8]) -> Result<Self, StorageError> {
    if bytes.len() < HEADER_SIZE {
        return Err(StorageError::CorruptedData);
    }
    // ...
}
```

---

### 10. read_u32/read_u64/read_u8 Re-Acquire Mmap Lock

**File:** `src/storage/mmap/mod.rs:538-582`

**Bug:** These helper methods acquire the mmap read lock on each call:

```rust
fn read_u32(&self, offset: usize) -> Result<u32, StorageError> {
    let mmap = self.mmap.read();  // Lock acquired for each small read!
    // ...
}
```

When `load_properties()` calls these repeatedly in a loop, it acquires and releases the mmap lock multiple times. This is inefficient and can cause inconsistent reads if the mmap is remapped between calls.

**Impact:** Performance degradation and potential inconsistency in property loading.

**Suggested Fix:** Hold the lock once and pass the mmap slice to helper functions, or read all property data in a single operation.

---

### 11. UpdateProperty WAL Entry Not Replayed During Recovery

**File:** `src/storage/mmap/recovery.rs:130-134`

**Bug:** The `UpdateProperty` WAL entry is logged but skipped during recovery:

```rust
WalEntry::UpdateProperty { .. } => {
    // Property updates require property arena support
    // For now, skip these during recovery
    // TODO: Implement property update recovery in Phase 4
    stats.properties_updated += 1;
}
```

**Impact:** Property updates made after the last checkpoint are silently lost on crash recovery.

**Suggested Fix:** Implement property update replay by allocating new property entries during recovery and updating the record's `prop_head`.

---

### 12. Edge Table Offset Calculation Doesn't Account for Table Growth

**File:** `src/storage/mmap/recovery.rs:225-229`

**Bug:** `edge_offset()` uses the passed `node_capacity` parameter, but during recovery this may not match the current file layout if table growth occurred:

```rust
fn edge_offset(id: EdgeId, node_capacity: u64) -> u64 {
    HEADER_SIZE as u64
        + (node_capacity * NODE_RECORD_SIZE as u64)
        + (id.0 * EDGE_RECORD_SIZE as u64)
}
```

If `grow_node_table()` was called but not logged to WAL, recovery uses the old capacity.

**Impact:** Edge records may be written to or read from wrong offsets during recovery.

**Suggested Fix:** Read current node_capacity from file header during recovery instead of using the passed parameter.

---

### 13. batch_mode Flags Can Get Out of Sync

**File:** `src/storage/mmap/mod.rs:1282-1317`

**Bug:** If `commit_batch()` fails after logging but before clearing the flags, the batch_mode state becomes inconsistent:

```rust
pub fn commit_batch(&self) -> Result<(), StorageError> {
    // ... validate batch state ...
    
    {
        let mut wal = self.wal.write();
        wal.log(WalEntry::CommitTx { tx_id })?;  // If this fails...
        wal.sync()?;  // ...or this fails...
    }
    
    // ...flags are never cleared, leaving batch_mode = true
    {
        let mut batch_mode = self.batch_mode.write();
        let mut batch_tx_id = self.batch_tx_id.write();
        *batch_mode = false;
        *batch_tx_id = None;
    }
    Ok(())
}
```

**Impact:** After a WAL write failure, subsequent operations think they're in batch mode but the transaction was never properly committed.

**Suggested Fix:** Clear batch flags on error or use RAII pattern to ensure cleanup.

---

## Low Severity

### 14. Checkpoint Version Always 0

**File:** `src/storage/mmap/mod.rs:1173`

**Bug:** The checkpoint entry always uses version 0:

```rust
wal.log(WalEntry::Checkpoint { version: 0 })?;
```

**Impact:** No ability to track checkpoint history or debug checkpoint issues.

**Suggested Fix:** Use a monotonically increasing counter, perhaps stored in the header.

---

### 15. index_specs File Written Without Directory Sync

**File:** `src/storage/mmap/mod.rs:3584-3606`

**Bug:** The `save_index_specs()` uses rename for atomicity but doesn't sync the directory:

```rust
std::fs::rename(&temp_path, &specs_path)?;
// Missing: sync parent directory
```

**Impact:** On crash, the rename may not be durable on some filesystems.

**Suggested Fix:** Sync the parent directory after rename:
```rust
std::fs::rename(&temp_path, &specs_path)?;
if let Some(parent) = specs_path.parent() {
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()?;
}
```

---

### 16. Debug eprintln During Index Loading

**File:** `src/storage/mmap/mod.rs:3641-3644`

**Bug:** Uses `eprintln!` for error logging in production code:

```rust
eprintln!(
    "Warning: Failed to fully populate index '{}': {}",
    spec.name, e
);
```

**Impact:** Pollutes stderr, not appropriate for library code.

**Suggested Fix:** Use `log` or `tracing` crate, or return the error.

---

### 17. Potential Integer Overflow in Offset Calculations

**File:** `src/storage/mmap/mod.rs` (multiple locations)

**Bug:** Offset calculations don't check for overflow:

```rust
let offset = HEADER_SIZE as u64 + (id.0 * NODE_RECORD_SIZE as u64);
```

With very large `id.0`, this could overflow.

**Impact:** Wrap-around to small offset values, writing to wrong file locations.

**Suggested Fix:** Use `checked_mul` and `checked_add`:
```rust
let offset = (id.0)
    .checked_mul(NODE_RECORD_SIZE as u64)
    .and_then(|x| x.checked_add(HEADER_SIZE as u64))
    .ok_or(StorageError::Overflow)?;
```

---

## Summary

| Severity | Total | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 4 | 2 | 2 |
| High | 4 | 1 | 3 |
| Medium | 5 | 0 | 5 |
| Low | 4 | 0 | 4 |

### Fixed Bugs:
1. **Critical: WAL transaction ID reuse after reopen** - Now scans WAL to find max tx_id
2. **Critical: Stale mmap reads during property offset adjustment** - Now reads directly from file
3. **High: TOCTOU race in arena allocation** - Now uses compare-and-swap loop

### Deferred:
1. **Critical: Property offset corruption during recovery after table growth** - Design limitation, mitigated by checkpointing

### Remaining Critical Issues:
1. Non-atomic writes on Windows (25+ locations, requires significant refactoring)

These remaining issues should be addressed before the mmap backend is used in production.
