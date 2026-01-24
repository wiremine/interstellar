# Versioned On-Disk Layout

This document describes a **versioned on-disk layout strategy** for an embedded, SQLite-style graph database. The goal is to allow the physical storage format to evolve safely over time without breaking user data or trust.

---

## What “Versioned Layout” Means

A *versioned layout* means the **physical binary format** of the database file is explicitly versioned and evolvable.

* The engine knows exactly **how bytes on disk should be interpreted**
* Structural changes are intentional, detectable, and migratable
* Old data does not become unreadable by accident

This is distinct from schema versioning or logical model changes.

---

## Core Principles

1. **Explicit format version** — never infer structure heuristically
2. **Backward-compatible readers when possible**
3. **Forward incompatibility is explicit and safe**
4. **Structural changes bump the layout version**
5. **Migrations are deterministic and crash-safe**

---

## File Header (Minimum Contract)

Every database file begins with a fixed-size header.

Example:

```
MAGIC       8 bytes   "RUSTGRPH"
LAYOUT_VER  u32       (e.g. 1, 2, 3)
PAGE_SIZE   u32       (e.g. 4096)
FLAGS       u32       (capabilities / options)
RESERVED    ...       (future use)
```

The header answers three questions immediately:

* Is this a file we understand?
* What layout rules apply?
* Can this binary safely open it?

If the layout version is unsupported, the engine must refuse to open or fall back to read-only mode.

---

## Backward vs Forward Compatibility

### New Engine → Old File

* Preferred and usually supported
* Engine adapts to older structures
* Missing fields receive defaults

### Old Engine → New File

* Explicitly rejected
* Never guess or partially parse

Rule of thumb:

> It is acceptable to refuse to open a file. It is never acceptable to misinterpret bytes.

---

## What Requires a Layout Version Bump

A layout version must be incremented when:

* Vertex or edge record structure changes
* Adjacency storage format changes
* Index encoding changes
* WAL or snapshot format changes
* Page structure changes

If an older binary would read the file *incorrectly*, this is a layout change.

---

## Example: Evolving Edge Storage

### Layout v1

```
Edge {
  from: u64
  to: u64
  label_id: u32
}
```

### Layout v2

```
Edge {
  from: u64
  to: u64
  label_id: u32
  properties_offset: u64
}
```

A v2 engine can read v1 by synthesizing an empty properties block.
A v1 engine must refuse to read v2.

---

## Page-Level Versioning (Optional, Advanced)

Beyond a global layout version, individual pages may declare their own format:

```
PageHeader {
  page_type: EDGE_PAGE
  page_layout_ver: 2
}
```

Benefits:

* Incremental migrations
* Mixed-version pages during upgrades
* Lazy rewriting of cold data

This is powerful but not required for an initial implementation.

---

## Migration as a First-Class Operation

Layout upgrades should be explicit and tool-driven.

Example:

```
graphdb migrate mygraph.db
```

Migration guarantees:

* Deterministic execution
* Crash-safe (resume on restart)
* Progress reporting
* Clear error states

Never silently rewrite a file during open.

---

## Tooling Enabled by Versioned Layout

Once layout versions are explicit, you unlock:

* `graphdb verify` — structural integrity checks
* `graphdb dump --raw` — low-level inspection
* `graphdb compact` — rewrite into latest layout
* `graphdb diff a.db b.db` — structural comparison
* Offline repair and recovery tools

These dramatically increase user trust.

---

## Layout Version vs Schema Version

| Concept     | Layout Version  | Schema Version   |
| ----------- | --------------- | ---------------- |
| Scope       | Physical format | Logical model    |
| Owner       | Database engine | User/application |
| Change rate | Rare            | Frequent         |
| Migration   | Engine-driven   | User-driven      |

Keep these concerns strictly separate.

---

## SQLite-Inspired Rule

If changing something would cause an old binary to misinterpret bytes on disk:

**→ Bump the layout version.**

No exceptions.

---

## Why This Matters

A versioned layout:

* Makes upgrades boring
* Prevents silent corruption
* Enables long-lived files
* Encourages experimentation without fear

It is one of the strongest signals of a serious, trustworthy embedded database.

---

## Summary

A versioned on-disk layout provides:

* Explicit format contracts
* Safe evolution of storage structures
* Predictable upgrade paths
* A foundation for tooling and recovery

This design choice pays dividends for years and aligns perfectly with an embedded, SQLite-style philosophy.
