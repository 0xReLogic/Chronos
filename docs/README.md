# ChronosDB Documentation

Complete technical documentation for ChronosDB distributed SQL database.

---

## Getting Started

**New to ChronosDB?** Start with the main [README.md](../README.md) for quick start examples and basic usage.

---

## Documentation Index

### Core Documentation

#### [Architecture](architecture.md)
Comprehensive technical architecture covering:
- System design and layered architecture
- Raft consensus implementation details
- Storage engine internals (Sled, compression, indexes)
- Edge-to-cloud synchronization protocol
- Data flow and sequence diagrams
- Performance characteristics and trade-offs

**Read this if:** You want to understand how ChronosDB works internally, contribute to the codebase, or make architectural decisions.

---

#### [Deployment Guide](deployment-guide.md)
Step-by-step deployment instructions for:
- Single-node, 3-node cluster, edge-to-cloud topologies
- Hardware and software requirements
- Build from source and cross-compilation
- Configuration (environment variables, CLI flags)
- Operational procedures (start/stop, backup/restore, health monitoring)
- Troubleshooting common issues
- Docker and systemd deployment

**Read this if:** You are deploying ChronosDB in development, staging, or production environments.

---

#### [SQL Reference](sql-reference.md)
Complete SQL syntax documentation:
- Supported data types (INT, FLOAT, STRING, BOOLEAN)
- DDL statements (CREATE TABLE, CREATE INDEX)
- DML statements (INSERT, SELECT, UPDATE, DELETE)
- Time-window aggregations (AVG_1H, AVG_24H, AVG_7D)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Query optimization tips
- Limitations and unsupported features

**Read this if:** You are writing SQL queries or designing database schemas for ChronosDB.

---

#### [API Reference](api-reference.md)
Complete API documentation for:
- gRPC services (SqlService, RaftService, SyncService, HealthService)
- HTTP admin endpoints (/health, /metrics)
- CLI commands (node, client, snapshot, admin)
- Embedded mode (Rust library API)
- Authentication and authorization
- TLS/mTLS configuration
- Error handling and retry strategies

**Read this if:** You are integrating ChronosDB into applications, building client libraries, or using the embedded mode.

---


## Additional Resources

### Source Code

- **Main Repository:** [github.com/0xReLogic/Chronos](https://github.com/0xReLogic/Chronos)
- **Protocol Buffers:** `proto/raft.proto`
- **SQL Grammar:** `src/parser/chronos.pest`

### Examples

- **Storage Demo:** `examples/storage_demo.rs` - IoT sensor data example
- **Raft Write Load:** `examples/raft_write_load.rs` - Distributed write benchmark

### Tests

- **Integration Tests:** `tests/raft_cluster.rs` - 3-node cluster scenarios
- **Property Tests:** `tests/raft_log_proptest.rs` - Raft log invariants
- **Sync Tests:** `tests/chimp_integration.rs` - Edge-to-cloud sync

---

## Documentation Roadmap

Future documentation planned:

- **Troubleshooting Guide:** Common issues and solutions
- **Performance Tuning:** Optimization strategies for specific workloads
- **Security Best Practices:** Hardening guide for production
- **Integration Examples:** Connecting ChronosDB to BI tools, streaming systems
- **Developer Guide:** Contributing to ChronosDB, code structure, testing

---

## Documentation Conventions

### Code Examples

All code examples are tested and runnable. Shell commands assume:
- Working directory: repository root
- Binary location: `./target/release/chronos` or `./chronos`
- Rust toolchain: 1.70+

### Placeholders

- `<value>`: Required parameter
- `[value]`: Optional parameter
- `...`: Continuation or omitted content

### Mermaid Diagrams

Architecture and flow diagrams use Mermaid syntax. View them in:
- GitHub (native rendering)
- VS Code (with Mermaid extension)
- Any Markdown viewer with Mermaid support

---

## Contributing to Documentation

Documentation improvements are welcome:

1. **Typos and clarifications:** Submit PR directly
2. **New sections:** Open issue first to discuss scope
3. **Code examples:** Ensure they are tested and runnable
4. **Diagrams:** Use Mermaid for consistency

---

## Version

This documentation is for **ChronosDB v1.0.0**.

For older versions, see git tags or release notes.
