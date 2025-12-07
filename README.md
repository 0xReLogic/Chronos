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

### 3. Run the Cluster

Open three separate terminals.

**Terminal 1 (Node 1):**
```bash
cargo run --release -- node \
  --id node1 \
  --data-dir data \
  --address 127.0.0.1:8000 \
  --peers node2=127.0.0.1:8001,node3=127.0.0.1:8002
```

**Terminal 2 (Node 2):**
```bash
cargo run --release -- node \
  --id node2 \
  --data-dir data \
  --address 127.0.0.1:8001 \
  --peers node1=127.0.0.1:8000,node3=127.0.0.1:8002
```

**Terminal 3 (Node 3):**
```bash
cargo run --release -- node \
  --id node3 \
  --data-dir data \
  --address 127.0.0.1:8002 \
  --peers node1=127.0.0.1:8000,node2=127.0.0.1:8001
```

### 4. Connect with the Client

Open a fourth terminal.

```bash
cargo run --release -- client --leader 127.0.0.1:8000
```

### 5. Execute SQL

Now you can interact with your distributed database!

```sql
chronos> CREATE TABLE users (id INT, name STRING, balance FLOAT);
Query OK

chronos> INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100.50);
Query OK

chronos> INSERT INTO users (id, name, balance) VALUES (2, 'Bob', 250.75);
Query OK

chronos> SELECT id, name, balance FROM users;
+----+-------+---------+
| id | name  | balance |
+----+-------+---------+
| 1  | Alice | 100.5   |
| 2  | Bob   | 250.75  |
+----+-------+---------+

-- Delete a record
DELETE FROM users WHERE name = 'Bob';

-- Use a transaction
BEGIN;
INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 40);
COMMIT;
```

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

## Development Status

Chronos is a learning project and is not intended for production use. It currently implements:

- SQL parsing with Pest and execution (CREATE, INSERT, SELECT, UPDATE, DELETE, CREATE INDEX)
- Sled embedded database for persistent storage with async I/O
- Raft consensus algorithm for leader election and log replication
- gRPC-based networking for node communication with Protocol Buffers
- Async/await architecture with Tokio runtime

## Edge/IoT Roadmap

See `ROADMAP.md` for full details.

Current status:

- Phase 0: Foundation (completed).
- Phase 1: Offline-First Mode (completed):
  - Hybrid Logical Clock (HLC) timestamps for write operations.
  - Write-ahead log (WAL) on Sled with recovery on startup.
  - Connectivity detection and a gRPC health service per node.
  - Offline queue support:
    - In-memory queue in the distributed client REPL for network outages.
    - Persistent backing queue in `storage::offline_queue` (Sled tree `__offline_queue__`) for future sync.
- Phase 2: Sync Protocol (next): delta sync and conflict resolution built on top of these queues.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
