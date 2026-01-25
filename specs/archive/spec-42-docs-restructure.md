# Spec 42: Documentation Restructuring

## Overview

Restructure project documentation into a user-facing `docs/` directory targeting external developers using Interstellar as a library.

## Goals

1. **Move API references** (`Gremlin_api.md`, `gql_api.md`) into organized `docs/` structure
2. **Create comprehensive user documentation** with getting-started guides, concepts, and references
3. **Keep internal docs separate** (`guiding-documents/`, `specs/`, `plans/`) for contributors

## Non-Goals

- Rewriting the README.md (only add docs/ link)
- Merging guiding-documents into docs/
- Auto-generating API docs from code (future consideration)

## Target Audience

External users/developers using Interstellar as a Rust library in their projects.

---

## Directory Structure

```
docs/
├── README.md                      # Docs index with navigation
│
├── getting-started/
│   ├── installation.md            # Dependencies, Cargo.toml setup, features
│   ├── quick-start.md             # Basic usage (extracted from README)
│   └── examples.md                # Walkthrough of key examples
│
├── api/
│   ├── gremlin.md                 # ← Gremlin_api.md (moved + polished)
│   ├── gql.md                     # ← gql_api.md (moved + polished)
│   ├── rhai.md                    # Rhai scripting API reference
│   └── predicates.md              # p:: module reference
│
├── concepts/
│   ├── architecture.md            # High-level architecture overview
│   ├── storage-backends.md        # InMemory vs MmapGraph comparison
│   ├── traversal-model.md         # How traversals work, lazy evaluation
│   └── concurrency.md             # Thread safety, snapshots, mutations
│
├── guides/
│   ├── graph-modeling.md          # Schema design, labels, properties
│   ├── querying.md                # Query patterns, Gremlin vs GQL
│   ├── mutations.md               # Adding/updating/deleting data
│   ├── performance.md             # Indexing, batch mode, optimization
│   └── testing.md                 # How to test graph code
│
└── reference/
    ├── value-types.md             # Value enum, type conversions
    ├── error-handling.md          # Error types, Result patterns
    ├── feature-flags.md           # mmap, rhai, full-text features
    └── glossary.md                # Terms: traverser, step, predicate, etc.
```

---

## Implementation Plan

### Phase 1: Structure & Moves (High Priority)

| Task | Description |
|------|-------------|
| 1.1 | Create `docs/` directory structure |
| 1.2 | Move `Gremlin_api.md` → `docs/api/gremlin.md` |
| 1.3 | Move `gql_api.md` → `docs/api/gql.md` |
| 1.4 | Create `docs/README.md` index page |
| 1.5 | Update root `README.md` with docs/ link |

### Phase 2: Getting Started (High Priority)

| Task | Description |
|------|-------------|
| 2.1 | Create `docs/getting-started/installation.md` |
| 2.2 | Create `docs/getting-started/quick-start.md` |
| 2.3 | Create `docs/getting-started/examples.md` |

### Phase 3: API Reference (Medium Priority)

| Task | Description |
|------|-------------|
| 3.1 | Create `docs/api/rhai.md` |
| 3.2 | Create `docs/api/predicates.md` |

### Phase 4: Concepts (Medium Priority)

| Task | Description |
|------|-------------|
| 4.1 | Create `docs/concepts/architecture.md` |
| 4.2 | Create `docs/concepts/storage-backends.md` |
| 4.3 | Create `docs/concepts/traversal-model.md` |
| 4.4 | Create `docs/concepts/concurrency.md` |

### Phase 5: Guides (Low Priority)

| Task | Description |
|------|-------------|
| 5.1 | Create `docs/guides/graph-modeling.md` |
| 5.2 | Create `docs/guides/querying.md` |
| 5.3 | Create `docs/guides/mutations.md` |
| 5.4 | Create `docs/guides/performance.md` |
| 5.5 | Create `docs/guides/testing.md` |

### Phase 6: Reference (Low Priority)

| Task | Description |
|------|-------------|
| 6.1 | Create `docs/reference/value-types.md` |
| 6.2 | Create `docs/reference/error-handling.md` |
| 6.3 | Create `docs/reference/feature-flags.md` |
| 6.4 | Create `docs/reference/glossary.md` |

---

## Content Sources

| New Doc | Primary Source(s) |
|---------|-------------------|
| `getting-started/*` | `README.md` sections |
| `api/gremlin.md` | `Gremlin_api.md` (direct move) |
| `api/gql.md` | `gql_api.md` (direct move) |
| `api/rhai.md` | `Gremlin_api.md` Rhai sections |
| `api/predicates.md` | `Gremlin_api.md` predicates table |
| `concepts/*` | `guiding-documents/overview.md`, `storage.md` (simplified) |
| `guides/*` | New content from examples/ |
| `reference/*` | New content from codebase |

---

## Kept Separate (Not Moved)

| Directory | Reason |
|-----------|--------|
| `guiding-documents/` | Internal design docs for future features |
| `specs/` | Implementation specifications |
| `plans/` | Development planning history |
| `code-reviews/` | Internal review notes |

---

## Style Guidelines

### Document Structure

Each document should follow this structure:

```markdown
# Title

Brief description (1-2 sentences).

## Overview / Introduction

Context and what this doc covers.

## Main Content

Organized with clear headings.

## Examples

Code examples with explanations.

## See Also

Links to related docs.
```

### Code Examples

- Use `rust` fence for Rust code
- Use `sql` fence for GQL queries
- Use `javascript` or `js` fence for Rhai scripts
- Include imports in examples
- Show expected output where helpful

### Linking

- Use relative links between docs: `[Gremlin API](../api/gremlin.md)`
- Link to source code sparingly (APIs may change)
- Link to examples/ directory for runnable code

---

## Success Criteria

1. All files in `docs/` structure created
2. `Gremlin_api.md` and `gql_api.md` moved (not copied)
3. Root `README.md` links to `docs/`
4. All internal links within docs/ are valid
5. No broken references to moved files

---

## Future Considerations

- Auto-generate API docs from rustdoc
- Add search functionality (mdbook or similar)
- Version-specific documentation
- Translated documentation
