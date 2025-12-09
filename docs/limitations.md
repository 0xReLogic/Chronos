# ChronosDB Limitations (pre-v1)

This page summarizes the **current limitations and non-goals** of ChronosDB as of the pre-v1 releases. It is meant to set expectations for real deployments.

## 1. SQL & Query Engine

- **Limited SQL surface area**
  - No JOINs, subqueries, or window functions.
  - Aggregations are limited (basic `AVG_1H/AVG_24H/AVG_7D` time-window helpers for `FLOAT` columns, no GROUP BY/HAVING).
  - No user-defined functions.

- **Per-row ACID only**
  - Writes are atomic and durable at the **single-row** level.
  - There is **no general multi-row / multi-table transaction support** yet (no `BEGIN/COMMIT/ROLLBACK`).

- **Query planning**
  - Simple planner aimed at small edge datasets.
  - Secondary indexes are supported for equality filters, but there is no cost-based planner or advanced pushdown.

## 2. Clustering & Consistency Model

- **Single-region focus**
  - Chronos today targets a single-region deployment (e.g., a factory, farm, or site) with a small cluster.
  - There is **no multi-region / cross-datacenter replication** or geo-distributed topology support yet.

- **Cluster size & topology**
  - Recommended cluster: **3-node Raft** (1 leader, 2 followers) for high availability.
  - Adding / removing nodes without downtime is not yet a polished, one-command workflow; it requires manual CLI orchestration.

- **Read behavior**
  - The public interface is effectively **leader-based** for reads.
  - Lease-based read optimizations exist internally but are conservative; there is no follower-read routing or tunable consistency API yet.

## 3. Durability, Backup & Restore

- **Snapshot-based backups only**
  - Chronos provides a **full snapshot** mechanism (create/restore) for node data directories.
  - There is **no incremental backup** or point-in-time recovery feature yet.

- **Operator responsibility**
  - It is the operator's responsibility to schedule snapshots, move them off-node, and periodically test restore procedures.

## 4. Edge Hardware & Platforms

- **Primary validation on Linux x86_64**
  - Benchmarks and soak/chaos tests are currently run on Linux x86_64 developer hardware.

- **ARM/Raspberry Pi validation still in progress**
  - Chronos is intended to run on ARM edge gateways (e.g., Raspberry Pi), but systematic performance and soak testing on those devices is still ongoing.
  - Until those runs are complete and documented, all published performance numbers should be interpreted as **x86_64-only**.

## 5. Observability & Tooling

- **Baseline observability only**
  - HTTP `/health` and `/metrics` endpoints expose JSON health and Prometheus-compatible metrics, but there is no bundled Grafana dashboard or alerting rules.
  - There is no dedicated external ops/dashboard application; operators are expected to plug metrics into their own monitoring stack.

- **CLI & admin tooling**
  - Admin commands (`chronos admin status|metrics`) focus on read-only status.
  - Cluster management commands (add/remove node, step-down leader, etc.) are planned but not yet part of the stable CLI surface.

## 6. Ecosystem & Integrations

- **Client ecosystem**
  - Chronos currently exposes a gRPC SQL API and a built-in REPL.
  - There is no official JDBC/ODBC driver, REST API, or Kubernetes operator yet.

- **Third-party tools**
  - Integration with external BI tools and streaming systems (Kafka, MQTT, etc.) is not provided out of the box and requires custom glue code.

## 7. Security & Multi-tenancy

- **Coarse-grained auth**
  - Security is based on **shared bearer tokens** with two primary roles: Admin and ReadOnly.
  - There is no per-row/column authorization model or rich, multi-tenant ACL system.

- **Secrets management**
  - Chronos does not manage secrets; operators must use their own secret storage (Kubernetes secrets, Vault, environment variables, etc.).

---

These limitations are expected for a small, edge-focused database at this stage. They are documented here so that operators can decide whether Chronos fits their specific production needs and where to place safety rails or compensating controls.
