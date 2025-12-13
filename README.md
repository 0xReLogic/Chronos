# ChronosDB

[![Rust CI](https://github.com/0xReLogic/chronos/actions/workflows/rust.yml/badge.svg)](https://github.com/0xReLogic/chronos/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust Version](https://img.shields.io/badge/rust-1.91%2B-orange.svg)](https://www.rust-lang.org)
[![Binary Size](https://img.shields.io/badge/binary-<6MB-blue.svg)](https://github.com/0xReLogic/Chronos)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20arm-lightgrey.svg)](https://github.com/0xReLogic/Chronos)
[![Protocol](https://img.shields.io/badge/protocol-gRPC%20%7C%20HTTP-green.svg)](https://github.com/0xReLogic/Chronos)

**Production-grade distributed SQL database engineered from the ground up in Rust. Purpose-built for edge computing, IoT gateways, and resource-constrained environments with intermittent connectivity.**

## Architecture

```mermaid
graph TB
    subgraph "Farm Site A"
        subgraph "Sensors (ESP32)"
            S1[Temperature<br/>25.5°C]
            S2[Humidity<br/>65%]
            S3[Ammonia<br/>15 ppm]
            S4[CO2<br/>800 ppm]
        end
        
        subgraph "Edge Gateway"
            E1[ChronosDB Node 1<br/>Raspberry Pi 4<br/>Offline Queue: 0 ops]
        end
        
        S1 -->|HTTP /ingest| E1
        S2 -->|HTTP /ingest| E1
        S3 -->|HTTP /ingest| E1
        S4 -->|HTTP /ingest| E1
    end
    
    subgraph "Farm Site B"
        subgraph "Sensors (ESP32)"
            S5[Temperature<br/>26.0°C]
            S6[Humidity<br/>70%]
            S7[Soil Moisture<br/>45%]
            S8[Light<br/>850 lux]
        end
        
        subgraph "Edge Gateway"
            E2[ChronosDB Node 2<br/>Orange Pi Zero<br/>Offline Queue: 0 ops]
        end
        
        S5 -->|HTTP /ingest| E2
        S6 -->|HTTP /ingest| E2
        S7 -->|HTTP /ingest| E2
        S8 -->|HTTP /ingest| E2
    end
    
    subgraph "Cloud (AWS/Azure)"
        C1[ChronosDB Cloud<br/>3-Node Raft Cluster<br/>Leader + 2 Followers]
        DB[(Data<br/>TTL-configurable retention)]
    end
    
    E1 -.->|Sync every 5s<br/>LWW conflict resolution<br/>HybridTimestamp| C1
    E2 -.->|Sync every 5s<br/>LWW conflict resolution<br/>HybridTimestamp| C1
    C1 --> DB
    
    subgraph "Analytics"
        G[Grafana Dashboard]
        P[Prometheus]
    end
    
    C1 -->|/metrics| P
    C1 -->|SQL Query| G
    
    style E1 fill:#e1f5ff,stroke:#0066cc,stroke-width:2px
    style E2 fill:#e1f5ff,stroke:#0066cc,stroke-width:2px
    style C1 fill:#fff4e1,stroke:#ff9800,stroke-width:3px
    style DB fill:#f0f0f0,stroke:#666,stroke-width:2px
    style S1 fill:#ffebee,stroke:#c62828
    style S2 fill:#e3f2fd,stroke:#1565c0
    style S3 fill:#fff3e0,stroke:#e65100
    style S4 fill:#e8f5e9,stroke:#2e7d32
    style S5 fill:#ffebee,stroke:#c62828
    style S6 fill:#e3f2fd,stroke:#1565c0
    style S7 fill:#f3e5f5,stroke:#6a1b9a
    style S8 fill:#fff9c4,stroke:#f57f17
```

**Key Capabilities:**
- **Edge Autonomy:** Each gateway operates independently with local SQL storage
- **Offline Resilience:** Persistent queue buffers operations during network outages
- **Conflict-Free Sync:** HybridTimestamp-based LWW ensures deterministic conflict resolution
- **Real-Time Analytics:** `AVG_1H`/`AVG_24H`/`AVG_7D` windowed queries for FLOAT columns

## Key Features

- **Custom Raft Consensus:** From-scratch implementation with pre-vote RPC and leader-only reads/writes
- **Hybrid Logical Clocks (HLC):** UHLC timestamps used for LWW conflict resolution in edge-to-cloud sync
- **Bit-Level Compression:** Experimental Chimp compression for FLOAT columns (opt-in via `CHRONOS_CHIMP_ENABLE`)
- **Pluggable Storage Trait:** Sled backend with LZ4 row compression, secondary indexes, and an LRU cache
- **Offline-First Replication:** Persistent offline queue + sync worker with LWW + per-edge cursor tracking
- **Real-Time Aggregation Engine:** In-memory rolling buckets for `AVG_1H`/`AVG_24H`/`AVG_7D` persisted to `agg_state` on disk
- **Health / Connectivity:** gRPC HealthService exposes `Connected/Disconnected/Reconnecting` state
- **Efficient Serialization:** Protocol Buffers + bincode for RPC and internal encoding
- **Production Observability:** Prometheus `/metrics`, structured tracing, and rotating file logs
- **Optional Security:** Bearer tokens (Admin/ReadOnly) and optional TLS/mTLS

## Quick Start

### Installation

**Download pre-built binary:**
```bash
wget https://github.com/0xReLogic/Chronos/releases/latest/download/chronos
chmod +x chronos
sudo mv chronos /usr/local/bin/chronos
```

All commands below assume `chronos` is on your `PATH`. If you're running from the repo, replace `chronos` with `./target/release/chronos` (or `./chronos` if the binary is in your current directory).

**Or build from source:**
```bash
cargo build --release
# Binary: target/release/chronos (5.6MB)
# If you didn't install it, run: ./target/release/chronos
```

### Single-Node Mode

```bash
chronos single-node --data-dir data
```

### 3-Node Cluster

```bash
# Terminal 1
chronos node --id node1 --address 127.0.0.1:8000 \
  --peers node2=127.0.0.1:8001,node3=127.0.0.1:8002

# Terminal 2
chronos node --id node2 --address 127.0.0.1:8001 \
  --peers node1=127.0.0.1:8000,node3=127.0.0.1:8002

# Terminal 3
chronos node --id node3 --address 127.0.0.1:8002 \
  --peers node1=127.0.0.1:8000,node2=127.0.0.1:8001

# Client
chronos client --leader 127.0.0.1:8000
```

### Edge-to-Cloud Sync

```bash
# Cloud node
chronos node --id cloud --address 10.0.0.10:8000

# Edge node (syncs to cloud)
chronos node --id edge1 --address 192.168.1.100:8001 \
  --sync-target http://10.0.0.10:8000 \
  --sync-interval-secs 5
```

### HTTP Ingest (Gateway Mode)

Start a node with `--enable-ingest` to accept `POST /ingest` on the admin HTTP port (gRPC port + 1000).

```bash
chronos node --id edge1 --address 192.168.1.100:8001 \
  --sync-target http://10.0.0.10:8000 \
  --sync-interval-secs 5 \
  --enable-ingest
```

Create the `readings` table once:

```sql
CREATE TABLE readings (reading_id STRING, device_id STRING, ts INT, seq INT, metric STRING, value FLOAT);
```

Send an ingest request (admin port is `8001 + 1000 = 9001`):

```bash
curl -X POST http://192.168.1.100:9001/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "esp-001",
    "ts": 1733856000,
    "seq": 42,
    "metrics": { "temp": 30.5, "humidity": 40.2 }
  }'
```

## SQL Examples

```sql
-- Create table with TTL
CREATE TABLE sensors (id INT, temp FLOAT, device STRING) WITH TTL=7d;

-- Create index
CREATE INDEX idx_device ON sensors(device);

-- Insert data
INSERT INTO sensors VALUES (1, 25.5, 'sensor-01');

-- Query
SELECT * FROM sensors WHERE device = 'sensor-01';

-- Time-window aggregations
SELECT AVG_1H(temp) FROM sensors;
SELECT AVG_24H(temp) FROM sensors;
SELECT AVG_7D(temp) FROM sensors;
```

## Performance

**Binary:** 5.6MB (optimized)

**Throughput:**
- Single-node: ~28k rows/sec (1000-row batch)
- 3-node Raft: ~1.8k writes/sec
- IoT batches (10 rows): ~12.6k rows/sec

**Latency:**
- Full scan (1000 rows): ~9ms
- Indexed query: <5ms
- Leader-local read: ~2ms

**Resources:**
- RAM: ~50MB idle, ~110MB under load
- CPU: ~75% of one core at 1.8k writes/sec

## Documentation

Comprehensive documentation in `docs/`:

- [Architecture](docs/architecture.md) - System design, components, data flow
- [Deployment Guide](docs/deployment-guide.md) - Production deployment, operations
- [SQL Reference](docs/sql-reference.md) - Complete SQL syntax and examples
- [API Reference](docs/api-reference.md) - gRPC, HTTP, embedded mode APIs

## Configuration

### Environment Variables

```bash
# Logging
export RUST_LOG=chronos=info
export CHRONOS_LOG_FILE=chronos.log

# Authentication
export CHRONOS_AUTH_TOKEN_ADMIN=your-admin-token
export CHRONOS_AUTH_TOKEN_READONLY=your-readonly-token

# TLS/mTLS
export CHRONOS_TLS_CERT=/path/to/server.crt
export CHRONOS_TLS_KEY=/path/to/server.key
export CHRONOS_TLS_CA_CERT=/path/to/ca.crt

# Experimental: Chimp compression
export CHRONOS_CHIMP_ENABLE=1
```

## Operations

### Health Check

```bash
curl http://127.0.0.1:9000/health
# {"status":"ok","role":"Leader","term":5}
```

### Metrics

```bash
curl http://127.0.0.1:9000/metrics
# Prometheus-compatible metrics
```

### Backup

```bash
chronos snapshot create --data-dir ./data --output backup.snap
chronos snapshot restore --data-dir ./data --input backup.snap --force
```

## Testing

```bash
# Full test suite
cargo test

# Raft cluster tests
cargo test --test raft_cluster

# Property-based tests
cargo test --test raft_log_proptest

# Benchmarks
cargo bench
```

## License

MIT License - see LICENSE.md for details.
