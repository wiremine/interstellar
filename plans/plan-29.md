# Plan 29: Fix Critical Mmap Storage Bugs

## Overview

Fix the critical and high-severity bugs identified in the mmap storage backend bug review (`code-reviews/mmap-bug-report.md`).

## Bugs Fixed

### Bug 1: WAL Transaction ID Counter Resets on Reopen ✅ FIXED
**File:** `src/storage/mmap/wal.rs`
**Problem:** `next_tx_id` starts at 0 on every open, causing ID collisions after restart.
**Fix:** Added `scan_max_tx_id()` method that scans WAL during open to find max transaction ID.

### Bug 3: Stale Mmap During `adjust_property_offsets()` ✅ FIXED
**File:** `src/storage/mmap/mod.rs`
**Problem:** Reads from old mmap after file extension before remap.
**Fix:** Refactored to read directly from file using positioned I/O. Also added `edge_table_start` parameter since caller knows the correct location.

### Bug 6: TOCTOU Race in Arena Allocation ✅ FIXED
**File:** `src/storage/mmap/arena.rs`
**Problem:** fetch_add + bounds check + fetch_sub rollback has race conditions.
**Fix:** Replaced with compare-and-swap loop for atomic allocation.

## Bugs Deferred

### Bug 2: Recovery Doesn't Adjust Property Offsets After Table Growth ⏸️ DEFERRED
**File:** `src/storage/mmap/recovery.rs`
**Problem:** WAL stores absolute property offsets that become stale after table growth.
**Status:** Design limitation. Mitigated by checkpointing after table growth operations. Would require significant refactoring to fix properly.

### Bug 4: Non-Atomic Seek+Write on Non-Unix Platforms ⏸️ DEFERRED
**File:** Multiple locations (25+ instances)
**Problem:** `seek()` + `write_all()` is not atomic, can be interrupted.
**Status:** Requires significant refactoring across many files. Lower priority as primary target is Unix systems.

## Testing

All 2192 tests pass after the fixes:
- `cargo test --lib --features mmap` - OK
- `cargo clippy --features mmap -- -D warnings` - OK

## Files Modified

- `src/storage/mmap/wal.rs` - Added `scan_max_tx_id()` method
- `src/storage/mmap/mod.rs` - Refactored `adjust_property_offsets()` to read from file, added `edge_table_start` parameter
- `src/storage/mmap/arena.rs` - Fixed allocation race condition
- `code-reviews/mmap-bug-report.md` - Updated with fix status

## Summary

- 3 bugs fixed (2 critical, 1 high)
- 2 bugs deferred (1 critical design limitation, 1 critical platform-specific)
- All tests pass
- No clippy warnings
