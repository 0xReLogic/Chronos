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
  --id edge \
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


## Performance Benchmarks

Tested on Ubuntu 24.04.3 LTS with Sled storage engine (size-optimized build):

**Binary Size:**
- Optimized release build: **3.7MB** 
- Suitable for edge devices and embedded systems

**Insert Performance:**
- 100 rows: ~10.5ms (median)
- 1,000 rows: ~23.6ms (median)
- Approx throughput (with WAL + HLC): ~40,000 rows/second (batch inserts of 1,000 rows)

**Query Performance (Full Scan):**
- 100 rows: ~9.8ms
- 1,000 rows: ~25.0ms
- Linear scaling with low variance

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
