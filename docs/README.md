# Interstellar Documentation

Welcome to the Interstellar documentation. Interstellar is a high-performance Rust graph database with dual query APIs: Gremlin-style fluent traversals and GQL (Graph Query Language).

## Getting Started

New to Interstellar? Start here:

- [Installation](getting-started/installation.md) - Add Interstellar to your project
- [Quick Start](getting-started/quick-start.md) - Create your first graph and run queries
- [Examples](getting-started/examples.md) - Walkthrough of included examples

## API Reference

Detailed API documentation for all query interfaces:

- [Gremlin API](api/gremlin.md) - Fluent traversal API reference (Rust & Rhai)
- [GQL API](api/gql.md) - Graph Query Language reference
- [Rhai Scripting](api/rhai.md) - Embedded scripting for dynamic queries
- [Predicates](api/predicates.md) - Predicate functions (`eq`, `gt`, `within`, etc.)

## Concepts

Understand how Interstellar works:

- [Architecture](concepts/architecture.md) - High-level system design
- [Storage Backends](concepts/storage-backends.md) - InMemory vs MmapGraph
- [Traversal Model](concepts/traversal-model.md) - How traversals execute
- [Concurrency](concepts/concurrency.md) - Thread safety and snapshots

## Guides

Practical guides for common tasks:

- [Graph Modeling](guides/graph-modeling.md) - Schema design best practices
- [Querying](guides/querying.md) - Query patterns and techniques
- [Mutations](guides/mutations.md) - Adding, updating, and deleting data
- [Performance](guides/performance.md) - Optimization and tuning
- [Testing](guides/testing.md) - Testing graph-based code

## Reference

Detailed reference material:

- [Value Types](reference/value-types.md) - The `Value` enum and type system
- [Error Handling](reference/error-handling.md) - Error types and patterns
- [Feature Flags](reference/feature-flags.md) - Cargo features explained
- [Glossary](reference/glossary.md) - Terminology and definitions

---

## Quick Links

| Task | Go To |
|------|-------|
| Add Interstellar to my project | [Installation](getting-started/installation.md) |
| Write my first query | [Quick Start](getting-started/quick-start.md) |
| Query with Gremlin syntax | [Gremlin API](api/gremlin.md) |
| Query with SQL-like syntax | [GQL API](api/gql.md) |
| Use persistent storage | [Storage Backends](concepts/storage-backends.md) |
| Improve query performance | [Performance Guide](guides/performance.md) |

---

## Getting Help

- [GitHub Issues](https://github.com/your-org/interstellar/issues) - Report bugs or request features
- [Examples Directory](../examples/) - Runnable example programs

## Internal Documentation

For contributors and maintainers:

- `guiding-documents/` - Design documents for future features
- `specs/` - Implementation specifications
- `AGENTS.md` - Guidelines for AI coding agents
