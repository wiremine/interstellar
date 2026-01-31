# Dead Code Audit Report

**Date:** 2026-01-18  
**Codebase:** Interstellar Graph Database  
**Status:** Feature Complete Phase

---

## Executive Summary

This audit identifies potentially dead code in the Interstellar codebase. Items are categorized by confidence level and type. The codebase is generally clean, with most dead code consisting of legacy migration artifacts and placeholder modules.

**Total Findings:** 14 items across 6 categories

---

## 1. Empty/Placeholder Modules

### HIGH CONFIDENCE

| Item | Location | Evidence | Recommendation |
|------|----------|----------|----------------|
| `algorithms` module | `src/algorithms/mod.rs:1-2` | Contains only a placeholder comment. Zero usages found. Declared in `lib.rs` but never imported. | Remove or implement planned algorithms (BFS, DFS, shortest path) |

```rust
// Current content (entire file):
// Algorithms module placeholder per setup spec
```

---

## 2. Dead Functions

### HIGH CONFIDENCE

| Function | Location | Evidence | Recommendation |
|----------|----------|----------|----------------|
| `compile_union()` | `src/gql/compiler_legacy.rs:376` | Marked `#[allow(dead_code)]`. Only wraps `compile_union_with_params()` with default params. Never called - only the `_with_params` variant is used. | Remove or consolidate |
| `Compiler::resolve_parameter()` | `src/gql/compiler_legacy.rs:459` | Marked `#[allow(dead_code)]`. Method defined but never invoked anywhere in codebase. | Remove |
| `GraphTraversalSource::create_context()` | `src/traversal/source.rs:495` | Marked `#[allow(dead_code)]` with comment "Will be used in future phases for lazy execution". Never called. | Keep if planned; remove if not |

---

## 3. Dead Struct Fields

### HIGH CONFIDENCE

| Field | Location | Evidence | Recommendation |
|-------|----------|----------|----------------|
| `BindingInfo.pattern_index` | `src/gql/compiler_legacy.rs:428` | Marked `#[allow(dead_code)]`. Field is set but never read. | Remove or use |
| `BindingInfo.is_node` | `src/gql/compiler_legacy.rs:430` | Marked `#[allow(dead_code)]`. Field is set but never read. | Remove or use |

```rust
#[derive(Debug, Clone)]
struct BindingInfo {
    #[allow(dead_code)]
    pattern_index: usize,  // Set but never read
    #[allow(dead_code)]
    is_node: bool,         // Set but never read
}
```

---

## 4. Never-Constructed Error Variants

### MEDIUM CONFIDENCE

These error variants exist but are never constructed. They appear to be legacy artifacts from a migration.

| Variant | Location | Evidence | Recommendation |
|---------|----------|----------|----------------|
| `ParseError::MissingClauseLegacy` | `src/gql/error.rs:119` | Only in pattern matching (line 210). Never constructed. | Remove |
| `ParseError::InvalidLiteralLegacy` | `src/gql/error.rs:131` | Only in pattern matching (line 211). Never constructed. | Remove |
| `CompileError::UnsupportedExpressionLegacy` | `src/gql/error.rs:241` | Only defined, never constructed. | Remove |
| `CompileError::AggregateInWhereLegacy` | `src/gql/error.rs:249` | Only defined, never constructed. | Remove |

**Note:** The non-legacy versions of these errors (with `Span` information) are actively used.

---

## 5. Stale `#[allow(dead_code)]` Annotations

### FALSE POSITIVES - These items ARE used

The following have `#[allow(dead_code)]` annotations but are actively called:

| Item | Location | Actual Usage |
|------|----------|--------------|
| `TraversalPipeline::into_steps()` | `src/traversal/pipeline.rs:162` | Called 12+ times in `source.rs`, `builder.rs` |
| `RepeatModifiers::satisfies_until()` | `src/traversal/repeat.rs:320` | Called in `RepeatIterator` (line 506) |
| `RepeatModifiers::should_emit()` | `src/traversal/repeat.rs:347` | Called in `RepeatIterator` (line 529) |

**Recommendation:** Remove these stale annotations as they are misleading.

---

## 6. Code Duplication

### MEDIUM CONFIDENCE

| Item | Locations | Issue | Recommendation |
|------|-----------|-------|----------------|
| `ListPredicateKind` enum | `src/gql/compiler_legacy.rs:436` and `src/gql/parser.rs:2471` | Same enum defined in two files | Extract to shared module |

---

## 7. Test-Only Code (Not Dead)

The following items are marked with `#[allow(dead_code)]` but are legitimately test-only:

| Item | Location | Status |
|------|----------|--------|
| `MmapGraph::edge_table_offset()` | `src/storage/mmap/mod.rs:525` | Used in tests; may be needed in Phase 3+ |

---

## Recommended Actions

### Immediate Cleanup (High Priority)

1. **Remove empty algorithms module** or implement planned algorithms:
   ```bash
   rm src/algorithms/mod.rs
   # Also remove `mod algorithms;` from lib.rs
   ```

2. **Remove dead functions in compiler_legacy.rs:**
   - `compile_union()` (line 376)
   - `Compiler::resolve_parameter()` (line 459)

3. **Remove unused BindingInfo fields** or implement their usage

### Migration Cleanup (Medium Priority)

4. **Remove legacy error variants** that are never constructed:
   - `ParseError::MissingClauseLegacy`
   - `ParseError::InvalidLiteralLegacy`
   - `CompileError::UnsupportedExpressionLegacy`
   - `CompileError::AggregateInWhereLegacy`

### Code Quality (Low Priority)

5. **Remove stale `#[allow(dead_code)]` annotations** from:
   - `TraversalPipeline::into_steps()` 
   - `RepeatModifiers::satisfies_until()`
   - `RepeatModifiers::should_emit()`

6. **Consider extracting** `ListPredicateKind` to a shared location

---

## Verification Commands

```bash
# Check for dead code warnings from compiler
cargo build --all-features 2>&1 | grep -E "warning.*dead_code"

# Search for usages of a specific function
rg "function_name" src/

# Find all #[allow(dead_code)] annotations
rg "#\[allow\(dead_code\)\]" src/
```

---

## Items Verified as Active

The following modules/items were investigated but confirmed as actively used:

- `index` module - 68 usages across codebase
- `schema` module - 51 usages across codebase  
- `gql` module - heavily used
- `rhai` module - feature-gated, active when `rhai` feature enabled
- `mmap` storage - feature-gated, active when `mmap` feature enabled
- `kani_proofs` module - properly gated with `#[cfg(kani)]`
- All `RhaiStep` enum variants - actively used
- `CloneSack`, `GroupKey`, `GroupValue` - exported in prelude, used
