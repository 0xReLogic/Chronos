# ChronosDB: Distributed SQL for Edge & IoT

[![Rust CI](https://github.com/0xReLogic/chronos/actions/workflows/rust.yml/badge.svg)](https://github.com/0xReLogic/chronos/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ChronosDB is a small distributed SQL database written in Rust, designed for edge gateways and IoT workloads. It uses the Raft consensus algorithm for replication and Sled as the embedded storage engine. The focus is on:

- Running on resource-constrained devices (small binary, low overhead).
- Handling intermittent networks with offline-first behavior.
- Providing a simple SQL interface for sensor and time-series style data.

---

## Architecture Overview

Chronos operates as a cluster of nodes, with one leader and multiple followers. All write operations are routed through the Raft consensus module, ensuring that every change is safely replicated across a majority of nodes before being committed.

```mermaid
graph TD
    Client([Client])

    subgraph "Chronos Cluster"
        direction TB
        Node1[Leader]
        Node2[Follower]
        Node3[Follower]
    end

    Client -- "Write (INSERT, etc.)" --> Node1
    Node1 -- "Replicate" --> Node2
    Node1 -- "Replicate" --> Node3
    Node2 -- "Ack" --> Node1
    Node3 -- "Ack" --> Node1
    Node1 -- "Commit OK" --> Client
    
    Client -- "Read (SELECT)" --> Node2
    Client -- "Read (SELECT)" --> Node3
    Node2 -- "Data" --> Client
    Node3 -- "Data" --> Client

    style Node1 fill:#f9f,stroke:#333,stroke-width:2px
```

---

## Key Features

- **Distributed Consensus with Raft:** Guarantees data consistency and availability through a robust, from-scratch implementation of the Raft algorithm.
- **SQL Interface:** Interact with your distributed data using familiar SQL commands (`CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`).
- **Fault Tolerance:** The cluster can survive the failure of minority nodes and continue operating, with a new leader elected automatically.
- **Persistent Storage:** Data is persisted to disk using Sled embedded database, ensuring durability and fast performance.
- **Async I/O:** Fully asynchronous storage operations with Tokio runtime for efficient concurrent access.
- **Built in Rust:** Leverages Rust's performance, safety, and concurrency features to build a reliable system.
- **Offline-First for Edge:** HLC timestamps, WAL + recovery, and offline queues (client + persistent storage) are designed for intermittent networks in IoT deployments.
- **Edge-Optimized Storage:** Secondary indexes on Sled, transparent row-level LZ4 compression, and an in-memory LRU row cache on indexed query paths.
- **Data Retention with TTL:** Per-table TTL (e.g. `CREATE TABLE sensors (id INT, temp FLOAT) WITH TTL=7d;`) with a background cleanup worker at a configurable interval (default 3600s) in node mode.
 - **Edge→Cloud Sync & Conflict Handling:** Persistent offline queue on edge nodes, batched sync via `SyncWorker`, per-`(table, key)` `HybridTimestamp` metadata stored in sled for Last-Write-Wins decisions that survive cloud restarts, and ID-based per-edge cursors so old batches are not re-applied after reconnect.

---

## Logging

Chronos uses `tracing` + `tracing-subscriber` under the hood, with a bridge from the standard `log` macros and optional rotating file logging via `env_logger`:

- Default log level is **INFO**, and can be overridden with `RUST_LOG`, for example:

  ```bash
  # Enable info logs for Chronos only
  RUST_LOG=chronos=info cargo run --release -- node ...

  # Enable debug logs globally (more verbose)
  RUST_LOG=debug cargo run --release -- node ...
  ```

- Optional rotating file logging can be enabled via:

  ```bash
  CHRONOS_LOG_FILE=chronos.log \
  CHRONOS_LOG_MAX_SIZE_MB=10 \
  CHRONOS_LOG_MAX_FILES=3 \
  cargo run --release -- node ...
  ```

This allows you to keep production nodes quiet while still enabling structured, timestamped logs for debugging and observability.

---

## Monitoring & Health

Each node exposes a small HTTP admin endpoint on `gRPC_port + 1000`.

- If the gRPC SQL/Raft server listens on `127.0.0.1:8000`, the admin HTTP endpoints are on `127.0.0.1:9000`.

Available paths:

- `/metrics` – Prometheus-style metrics, for example:
  - `chronos_sql_requests_total{kind="read"}`
  - `chronos_sql_requests_total{kind="write"}`
  - `chronos_raft_term`
  - `chronos_raft_role` (0=follower, 1=candidate, 2=leader)
  - `chronos_storage_size_bytes`

  These can be scraped directly by Prometheus and visualized in Grafana.

- `/health` – simple JSON health status, e.g.:

  ```json
  {"status":"ok","role":"Leader","term":2}
  ```

  This is suitable for load balancer or Kubernetes liveness/readiness checks.

---

## Security & Authentication

Chronos supports optional security features controlled entirely via environment variables.

> Note: Chronos does **not** automatically load `.env` files. Set all environment variables via your shell (for example `export VAR=...` or `set -a && source .env && set +a`) or via your process manager (systemd, Docker, etc.) before starting a node or client.

### Tokens & RBAC

- `CHRONOS_AUTH_TOKEN_ADMIN`  
  Admin bearer token for SQL gRPC. Requests with  
  `authorization: Bearer <CHRONOS_AUTH_TOKEN_ADMIN>`  
  can perform both reads and writes.

- `CHRONOS_AUTH_TOKEN_READONLY`  
  Read-only bearer token. Requests using this token may only execute `SELECT` queries; write attempts are rejected with `PERMISSION_DENIED`.

- `CHRONOS_AUTH_TOKEN`  
  Convenience variable read by the Chronos client REPL in distributed mode. When set, the REPL sends  
  `authorization: Bearer $CHRONOS_AUTH_TOKEN`  
  on every SQL request.

- `x-chronos-user` (gRPC/HTTP header)  
  Optional user identifier propagated by clients. The SQL server logs audit entries of the form:

  ```text
  audit_sql user=<user> role=<Admin|ReadOnly|None> sql=<...>
  ```

If neither `CHRONOS_AUTH_TOKEN_ADMIN` nor `CHRONOS_AUTH_TOKEN_READONLY` is set, authentication is disabled and the server behaves like earlier versions.

### TLS / mTLS for gRPC

TLS is also configured via environment variables:

- `CHRONOS_TLS_CERT` – PEM-encoded server certificate.
- `CHRONOS_TLS_KEY` – PEM-encoded private key.
- `CHRONOS_TLS_CA_CERT` – CA certificate used both to validate peers and, when set on the server, to require client certificates (mTLS).
- `CHRONOS_TLS_DOMAIN` – optional override for TLS SNI / domain name (default: `localhost`).

When `CHRONOS_TLS_CERT`, `CHRONOS_TLS_KEY` and `CHRONOS_TLS_CA_CERT` are all set:

- The node’s gRPC server starts with TLS enabled.
- All internal gRPC clients (`RaftClient`, `SqlClient`, `SyncClient`) also use TLS and present their own certificate for mTLS.

If these variables are unset, Chronos continues to run in plaintext mode, which is convenient for local development and testing.

---

## Quick Start

Get a Chronos cluster up and running in minutes.

### 1. Prerequisites

- Rust toolchain (latest stable)
- `protoc` (Protocol Buffers compiler)

### 2. Clone & Build

```bash
# Clone the repository
git clone https://github.com/0xReLogic/Chronos.git
cd Chronos

# Build the project
cargo build --release
```

### 3. Running Modes

This project currently supports a few main ways of running Chronos.

#### 3.1 Single-node local mode (no Raft, no sync)

Simplest way to try SQL and local storage:

```bash
cargo run --release -- single-node --data-dir data
```

This will open a local REPL:

```text
Welcome to Chronos SQL Database
Running in single-node mode
Enter SQL statements or 'exit' to quit
chronos> CREATE TABLE sensors (sensor_id INT, temperature FLOAT);
chronos> INSERT INTO sensors (sensor_id, temperature) VALUES (1, 25.5);
chronos> SELECT * FROM sensors;
chronos> exit
```

---

#### 3.2 3-node Raft cluster (consensus demo)

Open three terminals.

**Terminal 1 (node1):**

```bash
cargo run --release -- node \
  --id node1 \
  --data-dir data \
  --address 127.0.0.1:8000 \
  --peers node2=127.0.0.1:8001,node3=127.0.0.1:8002
```

**Terminal 2 (node2):**

```bash
cargo run --release -- node \
  --id node2 \
  --data-dir data \
  --address 127.0.0.1:8001 \
  --peers node1=127.0.0.1:8000,node3=127.0.0.1:8002
```

**Terminal 3 (node3):**

```bash
cargo run --release -- node \
  --id node3 \
  --data-dir data \
  --address 127.0.0.1:8002 \
  --peers node1=127.0.0.1:8000,node2=127.0.0.1:8001
```

**Client REPL (another terminal):**

```bash
cargo run --release -- client --leader 127.0.0.1:8000 --data-dir data
```

Contoh SQL:

```sql
chronos> CREATE TABLE users (id INT, name STRING, balance FLOAT);
chronos> INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100.50);
chronos> INSERT INTO users (id, name, balance) VALUES (2, 'Bob', 250.75);
chronos> SELECT id, name, balance FROM users;
```

---

#### 3.3 Edge + Cloud demo (offline-first + sync)

**Cloud node (central):**

```bash
cargo run --release -- node \
  --id cloud \
  --data-dir data \
  --address 127.0.0.1:8000 \
  --clean
```

**Edge node (gateway that syncs to the cloud):**

```bash
cargo run --release -- node \
  --id edge1 \
  --data-dir data \
  --address 127.0.0.1:8001 \
  --clean \
  --sync-target http://127.0.0.1:8000 \
  --sync-interval-secs 5 \
  --sync-batch-size 100
```

**Client to edge (write sensor data):**

```bash
cargo run --release -- client --leader 127.0.0.1:8001 --data-dir data
```

Di REPL edge:

```sql
chronos> CREATE TABLE sensors (sensor_id INT, temperature FLOAT);
chronos> INSERT INTO sensors (sensor_id, temperature) VALUES (1, 25.5);
chronos> INSERT INTO sensors (sensor_id, temperature) VALUES (2, 30.0);
chronos> INSERT INTO sensors (sensor_id, temperature) VALUES (1, 26.0);
```

**Client to cloud (check data that has been synced):**

```bash
cargo run --release -- client --leader 127.0.0.1:8000 --data-dir data
```

```sql
chronos> SELECT * FROM sensors;
```
### Edge Deployment Patterns

- **Small deployments (farms / livestock / small manufacturing):**
  - Sensors (ESP32, PLCs, etc.) → 1 edge gateway (Chronos node, e.g. STB / Raspberry Pi / old PC) → 1 cloud Chronos node.
  - 1–3 edge nodes per site are usually enough for hundreds of sensors with light–medium load.
  - Use per-table TTL to keep edge data size small.

- **Medium deployments (many edges per cloud):**
  - Several edge nodes (1–10) across multiple sites → 1–3 cloud Chronos nodes per region.
  - Each edge uses a unique `--id` (e.g. `farm-a-edge1`, `factory-1-gw`) so `edge_id` + per-edge cursor in the cloud stay unambiguous.
  - A safe starting point: hundreds to a few thousand write operations per second per cloud node, with `--sync-batch-size` around 100–1000 operations.

Behind the scenes:

- Edge writes are appended to a persistent offline queue (`__offline_queue__`) with monotonically increasing IDs.
- A background `SyncWorker` periodically drains this queue and sends `SyncRequest { edge_id, operations }` batches to the cloud.
- The cloud `SyncServer` uses per-`(table, key)` `HybridTimestamp` metadata (stored in sled) for LWW conflict resolution and tracks a per-edge `last_applied_id` cursor to avoid re-applying old batches after restarts.

---

#### 3.4 Check sync status with gRPC

If you have `grpcurl` installed, you can query lightweight sync status from the edge node (in this example `127.0.0.1:8001`):

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto raft.proto \
  127.0.0.1:8001 \
  raft.SyncStatusService/GetSyncStatus
```

Example output:

```json
{
  "lastSyncTsMs": "1765127479252",
  "lastSyncApplied": "3",
  "pendingOps": "3"
}
```

These fields come from `SyncStatusState`, which is updated by `SyncWorker` on each sync attempt.

---

## TTL & Data Retention

Chronos supports per-table TTL configured at table creation time:

```sql
CREATE TABLE sensors (id INT, temp FLOAT) WITH TTL=7d;
```

Rows in `sensors` become eligible for deletion once they are older than 7 days, based on the wall-clock time (`SystemTime::now`) recorded at insert.

In **node mode**, TTL is enforced by a background cleanup task running on each node every `ttl_cleanup_interval_secs` seconds (CLI flag, default `3600`). The task scans a compact TTL index and deletes expired rows, keeping secondary indexes and the in-memory LRU cache consistent.

---

## Time-Window Aggregations (1h/24h/7d AVG)

Chronos provides simple moving averages for `FLOAT` columns over common time windows via dedicated aggregate functions:

```sql
-- Last 1 hour
SELECT AVG_1H(temperature) FROM sensors;

-- Last 24 hours
SELECT AVG_24H(temperature) FROM sensors;

-- Last 7 days
SELECT AVG_7D(temperature) FROM sensors;
```

- Windows are trailing 1 hour / 24 hours / 7 days based on wall-clock time at insert/query.
- State is maintained in-memory using fixed-size minute/hour/day buckets and updated on every `INSERT`.
- Currently only `FLOAT` columns participate in these aggregations.
- Each function returns a single row with a single column `avg_1h(column_name)`, `avg_24h(column_name)`, or `avg_7d(column_name)`.
- Aggregation state is snapshotted via an Executor-local Sled tree and does not currently support additional filters, grouping, or multi-column projections.

This is an initial, edge-focused implementation; the roadmap includes extending to richer query shapes and tightening semantics for multi-node deployments.

## Embedded Mode (in-process)

Use Chronos directly as a library without starting a server:

```rust
use chronos::ChronosEmbedded;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = ChronosEmbedded::new("./data").await;

    db.execute("CREATE TABLE sensors (id INT, temperature FLOAT);").await?;
    db.execute("INSERT INTO sensors (id, temperature) VALUES (1, 25.5);").await?;

    let rows = db.execute("SELECT id, temperature FROM sensors;").await?;
    println!("{:?}", rows.rows);

    Ok(())
}

## Running Chronos with Docker

Chronos includes a `Dockerfile` and `docker-compose.yml` to spin up a 3-node Raft cluster for development or cloud environments.

```bash
# From the project root
docker-compose build
docker-compose up -d
```

This starts three Chronos nodes:

- node1 (leader): gRPC `127.0.0.1:8000`, admin `http://127.0.0.1:9000`
- node2: gRPC `127.0.0.1:8001`, admin `http://127.0.0.1:9001`
- node3: gRPC `127.0.0.1:8002`, admin `http://127.0.0.1:9002`

You can then connect from the host using the client REPL:

```bash
cargo run --release -- client --leader 127.0.0.1:8000 --data-dir data
```

> Note: Docker is primarily intended for development and cloud deployments.
> On constrained IoT devices, it is usually better to run the compiled `chronos` binary directly (size ~3.7MB) without Docker to minimize RAM and storage overhead.


## Testing

- Run the full test suite:

  ```bash
  cargo test
  ```

- Run the 3-node Raft cluster integration tests:

  ```bash
  cargo test --test raft_cluster
  ```

- Run the Raft log property-based tests:

  ```bash
  cargo test --test raft_log_proptest
  ```

The test suite includes multi-node Raft cluster scenarios (leader failover, follower restart, isolated minority node rejecting writes) and property-based checks for Raft log append/truncate invariants.

## Performance Benchmarks

Tested on Ubuntu 24.04.3 LTS with Sled storage engine (size-optimized build):

**Binary Size:**
- Optimized release build: **3.7MB** 
- Suitable for edge devices and embedded systems

**Insert Performance (single batch, no TTL):**
- 100 rows: ~10ms (median)
- 1,000 rows: ~34ms (median)
- Approx throughput for 1,000-row batch: **~28k rows/second** (including WAL + HLC overhead)

**Insert Performance (single batch, with per-table TTL):**
- 1,000 rows: ~44ms (median)
- Approx throughput: **~22k rows/second** (about 20–25% overhead vs. no-TTL)

**IoT-style Small-Batch Inserts (1,000 total rows):**
- 1 row per batch (1,000 × 1-row inserts)
  - No TTL: ~292ms  → **~3.4k rows/second**
  - With TTL: ~298ms → **~3.3k rows/second**
- 10 rows per batch (100 × 10-row inserts)
  - No TTL: ~67ms   → **~15k rows/second**
  - With TTL: ~79ms → **~12.6k rows/second**

These numbers are from a single Chronos node on a modest dev machine. They are sufficient for hundreds of industrial sensors producing 10–20 events per second per sensor, even with TTL-based data retention enabled.

**Query Performance (Full Scan, no TTL):**
- 100 rows: ~10–11ms
- 1,000 rows: ~36–38ms
- Near-linear scaling for small tables used in edge scenarios

**Distributed Raft Write Path (full stack):**
- Scenario: client → gRPC → SqlServer → Executor → Raft (leader + followers) → Sled.
- Example load (single client): `examples/raft_write_load.rs` sending 100k `INSERT` statements via the SQL service.
- Observed throughput on a modest dev machine:
  - Single-node mode (no followers): **~1.9k writes/second**.
  - 3-node Raft cluster (1 leader + 2 followers): **~1.8k writes/second** (only a few percent overhead vs single-node).
- Leader node resource profile during the 3-node test (approximate):
  - CPU: ~75% of one core while sustaining ~1.8k writes/second.
  - RAM: ~110MB maximum resident set size for the leader process.

This aligns well with typical edge gateway hardware (e.g., quad-core ARM Cortex-A53 with 2GB RAM): even under continuous write load the cluster stays within a small fraction of available CPU and memory.

Run benchmarks yourself:
```bash
cargo bench
```

Try the IoT sensor demo:
```bash
cargo run --example storage_demo
```

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
