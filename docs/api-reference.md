# ChronosDB API Reference

## Document Purpose

Complete API reference for ChronosDB gRPC services, HTTP endpoints, and embedded mode, including request/response formats, authentication, and error handling.

---

## System Overview

ChronosDB exposes three API surfaces:

1. **gRPC Services:** Primary interface for SQL execution, Raft consensus, and sync
2. **HTTP Admin API:** Health checks and Prometheus metrics
3. **Embedded Mode:** In-process Rust library

---

## gRPC Services

### Protocol Buffers Definition

**Location:** `proto/raft.proto`

**Services:**
- `RaftService`: Consensus protocol
- `SqlService`: SQL execution
- `HealthService`: Connectivity status
- `SyncService`: Edge-to-cloud synchronization
- `SyncStatusService`: Sync metrics

---

### SqlService

#### ExecuteSql

Execute SQL statement and return results.

**Request:**
```protobuf
message SqlRequest {
  string sql = 1;
}
```

**Response:**
```protobuf
message SqlResponse {
  bool success = 1;
  string error = 2;
  repeated string columns = 3;
  repeated Row rows = 4;
}

message Row {
  repeated Value values = 1;
}

message Value {
  oneof value {
    string string_value = 1;
    int64 int_value = 2;
    double float_value = 3;
    bool bool_value = 4;
    bool null_value = 5;
  }
}
```

**Example (grpcurl):**

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto raft.proto \
  -d '{"sql": "SELECT * FROM sensors"}' \
  127.0.0.1:8000 \
  raft.SqlService/ExecuteSql
```

**Response:**
```json
{
  "success": true,
  "columns": ["id", "temperature", "device"],
  "rows": [
    {
      "values": [
        {"intValue": "1"},
        {"floatValue": 25.5},
        {"stringValue": "sensor-01"}
      ]
    }
  ]
}
```

**Authentication:**

Add `authorization` metadata header:

```bash
grpcurl -plaintext \
  -H "authorization: Bearer your-admin-token" \
  -import-path ./proto \
  -proto raft.proto \
  -d '{"sql": "INSERT INTO sensors VALUES (1, 25.5, \"sensor-01\")"}' \
  127.0.0.1:8000 \
  raft.SqlService/ExecuteSql
```

**Authorization:**
- Admin token: Read + write operations
- Read-only token: SELECT only, writes rejected with `PERMISSION_DENIED`

**Error Codes:**
- `OK`: Success
- `INVALID_ARGUMENT`: SQL syntax error
- `NOT_FOUND`: Table not found
- `PERMISSION_DENIED`: Insufficient permissions
- `UNAVAILABLE`: Not the leader (distributed mode)
- `INTERNAL`: Storage or execution error

---

### RaftService

#### RequestVote

Request vote during leader election.

**Request:**
```protobuf
message RequestVoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}
```

**Response:**
```protobuf
message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}
```

**Usage:** Internal Raft protocol, not for client use.

---

#### PreVote

Pre-vote phase before incrementing term.

**Request:** Same as `RequestVoteRequest`

**Response:** Same as `RequestVoteResponse`

**Usage:** Internal Raft protocol, not for client use.

---

#### AppendEntries

Replicate log entries and heartbeats.

**Request:**
```protobuf
message AppendEntriesRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}

message LogEntry {
  uint64 term = 1;
  bytes command = 2;
}
```

**Response:**
```protobuf
message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
  uint64 match_index = 3;
}
```

**Usage:** Internal Raft protocol, not for client use.

---

### HealthService

#### GetConnectivity

Get node connectivity status.

**Request:**
```protobuf
message HealthRequest {}
```

**Response:**
```protobuf
message HealthResponse {
  string state = 1;  // "Connected", "Disconnected", or "Reconnecting"
}
```

**Example:**

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto raft.proto \
  127.0.0.1:8000 \
  raft.HealthService/GetConnectivity
```

**Response:**
```json
{
  "state": "Connected"
}
```

---

### SyncService

#### Sync

Synchronize operations from edge to cloud.

**Request:**
```protobuf
message SyncRequest {
  string edge_id = 1;
  repeated SyncOperation operations = 2;
}

message SyncOperation {
  uint64 id = 1;
  bytes data = 2;  // bincode(PersistentQueuedOperation)
}
```

**Response:**
```protobuf
message SyncResponse {
  uint64 applied = 1;
}
```

**Example (Rust client):**

```rust
use chronos::network::proto::sync_service_client::SyncServiceClient;
use chronos::network::proto::{SyncRequest, SyncOperation};

let mut client = SyncServiceClient::connect("http://10.0.0.10:8000").await?;

let request = SyncRequest {
    edge_id: "edge1".to_string(),
    operations: vec![
        SyncOperation {
            id: 1,
            data: bincode::encode_to_vec(&op, config)?,
        },
    ],
};

let response = client.sync(request).await?;
println!("Applied: {}", response.into_inner().applied);
```

**Behavior:**
- Cloud applies operations with LWW conflict resolution
- Updates per-edge cursor to prevent duplicate application
- Returns number of successfully applied operations

---

### SyncStatusService

#### GetSyncStatus

Get sync metrics from edge node.

**Request:**
```protobuf
message SyncStatusRequest {}
```

**Response:**
```protobuf
message SyncStatusResponse {
  uint64 last_sync_ts_ms = 1;
  uint64 last_sync_applied = 2;
  uint64 pending_ops = 3;
  string last_error = 4;
}
```

**Example:**

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto raft.proto \
  192.168.1.100:8001 \
  raft.SyncStatusService/GetSyncStatus
```

**Response:**
```json
{
  "lastSyncTsMs": "1765127479252",
  "lastSyncApplied": "5",
  "pendingOps": "0",
  "lastError": ""
}
```

**Fields:**
- `last_sync_ts_ms`: Last successful sync timestamp (Unix milliseconds)
- `last_sync_applied`: Number of operations applied in last sync
- `pending_ops`: Operations in offline queue waiting to sync
- `last_error`: Last sync error message (empty if no error)

---

## HTTP Admin API

### Endpoints

**Base URL:** `http://<node_address>:<grpc_port + 1000>`

Example: If gRPC is on `127.0.0.1:8000`, admin HTTP is on `127.0.0.1:9000`

---

### GET /health

Node health status.

**Request:**
```bash
curl http://127.0.0.1:9000/health
```

**Response:**
```json
{
  "status": "ok",
  "role": "Leader",
  "term": 5
}
```

**Fields:**
- `status`: Always "ok" if node is running
- `role`: Raft role (Leader, Follower, Candidate)
- `term`: Current Raft term

**Use Cases:**
- Load balancer health checks
- Kubernetes liveness/readiness probes
- Monitoring system integration

---

### GET /metrics

Prometheus-compatible metrics.

**Request:**
```bash
curl http://127.0.0.1:9000/metrics
```

**Response:**
```
# HELP chronos_sql_requests_total Total SQL requests
# TYPE chronos_sql_requests_total counter
chronos_sql_requests_total{kind="read"} 1523
chronos_sql_requests_total{kind="write"} 847

# HELP chronos_raft_term Current Raft term
# TYPE chronos_raft_term gauge
chronos_raft_term 5

# HELP chronos_raft_role Current Raft role (0=follower, 1=candidate, 2=leader)
# TYPE chronos_raft_role gauge
chronos_raft_role 2

# HELP chronos_storage_size_bytes Storage size in bytes
# TYPE chronos_storage_size_bytes gauge
chronos_storage_size_bytes 104857600
```

**Metrics:**
- `chronos_sql_requests_total{kind}`: SQL request counter (read/write)
- `chronos_raft_term`: Current Raft term
- `chronos_raft_role`: Raft role (0=follower, 1=candidate, 2=leader)
- `chronos_storage_size_bytes`: Total storage size

**Prometheus Configuration:**

```yaml
scrape_configs:
  - job_name: 'chronos'
    static_configs:
      - targets:
          - '127.0.0.1:9000'
          - '127.0.0.1:9001'
          - '127.0.0.1:9002'
    scrape_interval: 15s
```

---

## CLI Commands

### chronos single-node

Start single-node instance with local REPL.

**Syntax:**
```bash
chronos single-node [OPTIONS]
```

**Options:**
- `-d, --data-dir <DIR>`: Data directory (default: `data`)

**Example:**
```bash
chronos single-node --data-dir ./my-data
```

---

### chronos node

Start node in distributed cluster.

**Syntax:**
```bash
chronos node [OPTIONS]
```

**Options:**
- `-i, --id <ID>`: Unique node ID (required)
- `-d, --data-dir <DIR>`: Data directory (default: `data`)
- `-a, --address <ADDR>`: Listen address (default: `127.0.0.1:8000`)
- `-p, --peers <PEERS>`: Peer list (`id=address,id=address,...`)
- `--clean`: Clean data directory on start
- `--sync-target <URL>`: Cloud sync target URL
- `--sync-interval-secs <SECS>`: Sync interval (default: 5)
- `--sync-batch-size <SIZE>`: Sync batch size (default: 100)
- `--ttl-cleanup-interval-secs <SECS>`: TTL cleanup interval (default: 3600)

**Example:**
```bash
chronos node \
  --id node1 \
  --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.1:8000 \
  --peers node2=10.0.0.2:8001,node3=10.0.0.3:8002
```

---

### chronos client

Start SQL REPL client.

**Syntax:**
```bash
chronos client [OPTIONS]
```

**Options:**
- `-l, --leader <ADDR>`: Leader node address (distributed mode)
- `-d, --data-dir <DIR>`: Data directory (local mode, default: `data`)

**Example (distributed):**
```bash
chronos client --leader 10.0.0.1:8000
```

**Example (local):**
```bash
chronos client --data-dir ./data
```

---

### chronos snapshot

Backup and restore operations.

**Create Snapshot:**
```bash
chronos snapshot create \
  --data-dir <DIR> \
  --output <FILE>
```

**Restore Snapshot:**
```bash
chronos snapshot restore \
  --data-dir <DIR> \
  --input <FILE> \
  [--force]
```

**Example:**
```bash
# Create
chronos snapshot create \
  --data-dir /var/lib/chronos/cluster/node1 \
  --output /backups/node1-20250109.snap

# Restore
chronos snapshot restore \
  --data-dir /var/lib/chronos/cluster/node1 \
  --input /backups/node1-20250109.snap \
  --force
```

---

### chronos admin

Admin tooling.

**Status:**
```bash
chronos admin status --http <ADDR>
```

**Metrics:**
```bash
chronos admin metrics --http <ADDR>
```

**Example:**
```bash
chronos admin status --http 127.0.0.1:9000
chronos admin metrics --http 127.0.0.1:9000
```

---

## Embedded Mode

### Rust Library

Use ChronosDB as an in-process library without gRPC or Raft.

**Add Dependency:**

```toml
[dependencies]
chronos = { path = "../chronos" }
tokio = { version = "1", features = ["full"] }
```

**Example:**

```rust
use chronos::ChronosEmbedded;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = ChronosEmbedded::new("./data").await;

    // Create table
    db.execute("CREATE TABLE sensors (id INT, temperature FLOAT);").await?;

    // Insert data
    db.execute("INSERT INTO sensors (id, temperature) VALUES (1, 25.5);").await?;
    db.execute("INSERT INTO sensors (id, temperature) VALUES (2, 30.0);").await?;

    // Query data
    let result = db.execute("SELECT id, temperature FROM sensors;").await?;
    
    println!("Columns: {:?}", result.columns);
    for row in result.rows {
        println!("Row: {:?}", row);
    }

    Ok(())
}
```

**API:**

```rust
impl ChronosEmbedded {
    /// Create new embedded instance
    pub async fn new(data_dir: &str) -> Self;

    /// Execute SQL statement
    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult, ExecutorError>;
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}
```

**Use Cases:**
- Embedded applications
- Edge devices without network
- Testing and development
- Single-process deployments

---

## Authentication

### Bearer Token

ChronosDB uses bearer token authentication for gRPC services.

**Configuration:**

```bash
# Admin token (read + write)
export CHRONOS_AUTH_TOKEN_ADMIN=your-secure-admin-token

# Read-only token (SELECT only)
export CHRONOS_AUTH_TOKEN_READONLY=your-secure-readonly-token

# Client token (used by REPL)
export CHRONOS_AUTH_TOKEN=your-secure-admin-token
```

**gRPC Metadata:**

```bash
grpcurl -plaintext \
  -H "authorization: Bearer your-admin-token" \
  -d '{"sql": "SELECT * FROM sensors"}' \
  127.0.0.1:8000 \
  raft.SqlService/ExecuteSql
```

**Rust Client:**

```rust
use tonic::metadata::MetadataValue;
use tonic::Request;

let mut request = Request::new(SqlRequest {
    sql: "SELECT * FROM sensors".to_string(),
});

let token = MetadataValue::from_str("Bearer your-admin-token")?;
request.metadata_mut().insert("authorization", token);

let response = client.execute_sql(request).await?;
```

**Authorization Rules:**
- Admin token: All operations allowed
- Read-only token: Only SELECT allowed
- No token: Authentication disabled (development only)

**Audit Logging:**

SQL operations are logged with user attribution:

```
audit_sql user=alice role=Admin sql=INSERT INTO sensors VALUES (1, 25.5)
audit_sql user=bob role=ReadOnly sql=SELECT * FROM sensors
```

---

## TLS/mTLS

### Configuration

```bash
export CHRONOS_TLS_CERT=/etc/chronos/tls/server.crt
export CHRONOS_TLS_KEY=/etc/chronos/tls/server.key
export CHRONOS_TLS_CA_CERT=/etc/chronos/tls/ca.crt
export CHRONOS_TLS_DOMAIN=chronos.example.com
```

### gRPC Client (Rust)

```rust
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

let ca_cert = std::fs::read("/etc/chronos/tls/ca.crt")?;
let client_cert = std::fs::read("/etc/chronos/tls/client.crt")?;
let client_key = std::fs::read("/etc/chronos/tls/client.key")?;

let tls_config = ClientTlsConfig::new()
    .ca_certificate(Certificate::from_pem(ca_cert))
    .identity(Identity::from_pem(client_cert, client_key))
    .domain_name("chronos.example.com");

let channel = Channel::from_static("https://10.0.0.1:8000")
    .tls_config(tls_config)?
    .connect()
    .await?;

let mut client = SqlServiceClient::new(channel);
```

---

## Error Handling

### gRPC Status Codes

| Code | Description | Cause |
|------|-------------|-------|
| `OK` | Success | Operation completed successfully |
| `INVALID_ARGUMENT` | Invalid input | SQL syntax error, invalid parameters |
| `NOT_FOUND` | Resource not found | Table does not exist |
| `PERMISSION_DENIED` | Insufficient permissions | Read-only token attempting write |
| `UNAVAILABLE` | Service unavailable | Not the leader, node down |
| `INTERNAL` | Internal error | Storage error, execution error |
| `UNAUTHENTICATED` | Authentication failed | Invalid or missing token |

### Example Error Response

```json
{
  "success": false,
  "error": "Table sensors not found",
  "columns": [],
  "rows": []
}
```

### Retry Logic

**Recommended retry strategy:**

1. **UNAVAILABLE (not leader):**
   - Retry with exponential backoff
   - Discover new leader via health check
   - Max retries: 3

2. **INTERNAL (storage error):**
   - Retry once after 1s delay
   - If persistent, alert operator

3. **INVALID_ARGUMENT:**
   - Do not retry (client error)
   - Fix SQL syntax

**Example (Rust):**

```rust
use tokio::time::{sleep, Duration};

let mut retries = 0;
let max_retries = 3;

loop {
    match client.execute_sql(request.clone()).await {
        Ok(response) => break Ok(response),
        Err(e) if e.code() == Code::Unavailable && retries < max_retries => {
            retries += 1;
            sleep(Duration::from_millis(100 * 2_u64.pow(retries))).await;
        }
        Err(e) => break Err(e),
    }
}
```

---

## Rate Limiting

ChronosDB does not implement built-in rate limiting. Use external tools:

**nginx reverse proxy:**

```nginx
limit_req_zone $binary_remote_addr zone=chronos:10m rate=100r/s;

server {
    listen 8080;
    location / {
        limit_req zone=chronos burst=20;
        grpc_pass grpc://127.0.0.1:8000;
    }
}
```

**Envoy proxy:**

```yaml
rate_limits:
  - actions:
    - request_headers:
        header_name: ":authority"
        descriptor_key: "authority"
    per_connection_buffer_limit_bytes: 1048576
```

---

## Best Practices

### Client Implementation

1. **Connection pooling:**
   - Reuse gRPC channels
   - Pool size: 5-10 connections per client

2. **Timeout configuration:**
   - Read queries: 5s timeout
   - Write queries: 10s timeout
   - Sync operations: 30s timeout

3. **Error handling:**
   - Retry UNAVAILABLE with backoff
   - Log INTERNAL errors
   - Alert on repeated failures

4. **Authentication:**
   - Store tokens securely (environment variables, secret manager)
   - Rotate tokens periodically
   - Use read-only tokens for dashboards

5. **Monitoring:**
   - Track request latency (p50, p95, p99)
   - Monitor error rates
   - Alert on high latency or error spikes

---

This API reference covers all public interfaces in ChronosDB v1.0.0. For internal Raft protocol details, see `docs/architecture.md`.
