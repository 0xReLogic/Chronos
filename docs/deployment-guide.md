# ChronosDB Deployment Guide

## Document Purpose

Step-by-step deployment guide for ChronosDB in production and development environments, covering single-node, multi-node clusters, edge-to-cloud topologies, and operational procedures.

---

## System Overview

ChronosDB can be deployed in three primary configurations:

1. **Single-Node Mode:** Local development, testing, or standalone edge devices
2. **3-Node Raft Cluster:** High-availability production deployment
3. **Edge-to-Cloud Topology:** IoT gateways syncing to central cloud cluster

---

## Prerequisites

### Hardware Requirements

**Minimum (Edge Device):**
- CPU: 1 core (ARM Cortex-A53 or x86_64)
- RAM: 512MB
- Disk: 1GB available space
- Network: Intermittent connectivity acceptable

**Recommended (Production Node):**
- CPU: 2-4 cores
- RAM: 2GB
- Disk: 10GB SSD
- Network: Stable low-latency connection (<10ms between nodes)

### Software Requirements

- **Rust Toolchain:** 1.91+ (for building from source)
- **protoc:** Protocol Buffers compiler (build dependency)
- **Operating System:** Linux (Ubuntu 20.04+, Debian 11+, RHEL 8+)
- **Optional:** Docker 20.10+ for containerized deployment

---

## Installation

### Option 1: Download Pre-Built Binary (Recommended)

Download the latest release binary:

```bash
wget https://github.com/0xReLogic/Chronos/releases/latest/download/chronos
chmod +x chronos
sudo mv chronos /usr/local/bin/chronos
```

**Note:** GitHub Releases currently publish a Linux x86_64 binary named `chronos`.

**Verify Installation:**
```bash
chronos --version
```

### Option 2: Build from Source (Advanced)

**Warning:** Building from source requires ~6GB disk space for Rust toolchain and dependencies.

```bash
# Clone repository
git clone https://github.com/0xReLogic/Chronos.git
cd Chronos

# Build release binary
cargo build --release

# Binary location: target/release/chronos
```

**For ARM devices:**
```bash
# Install cross-compilation toolchain
rustup target add aarch64-unknown-linux-gnu
sudo apt-get install gcc-aarch64-linux-gnu

# Build for ARM
cargo build --release --target aarch64-unknown-linux-gnu
```

---

## Deployment Topologies

### 1. Single-Node Mode

**Use Case:** Local development, testing, embedded applications

#### Start Single-Node Instance

```bash
./chronos single-node --data-dir ./data
```

**What Happens:**
- Opens local REPL interface
- No network services started
- Data stored in `./data` directory
- Suitable for embedded mode or quick testing

#### Example Session

```
Welcome to Chronos SQL Database
Running in single-node mode
Enter SQL statements or 'exit' to quit

chronos> CREATE TABLE sensors (id INT, temp FLOAT, device STRING);
Table sensors created

chronos> INSERT INTO sensors (id, temp, device) VALUES (1, 25.5, 'sensor-01');
Inserted into sensors

chronos> SELECT * FROM sensors;
+----+------+-----------+
| id | temp | device    |
+----+------+-----------+
| 1  | 25.5 | sensor-01 |
+----+------+-----------+

chronos> exit
```

---

### 2. Three-Node Raft Cluster

**Use Case:** High-availability production deployment

#### Network Topology

```
Node 1 (Leader):    10.0.0.1:8000  (admin: 9000)
Node 2 (Follower):  10.0.0.2:8001  (admin: 9001)
Node 3 (Follower):  10.0.0.3:8002  (admin: 9002)
```

#### Start Node 1

```bash
./chronos node \
  --id node1 \
  --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.1:8000 \
  --peers node2=10.0.0.2:8001,node3=10.0.0.3:8002
```

#### Start Node 2

```bash
./chronos node \
  --id node2 \
  --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.2:8001 \
  --peers node1=10.0.0.1:8000,node3=10.0.0.3:8002
```

#### Start Node 3

```bash
./chronos node \
  --id node3 \
  --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.3:8002 \
  --peers node1=10.0.0.1:8000,node2=10.0.0.2:8001
```

#### Connect Client

```bash
./chronos client --leader 10.0.0.1:8000
```

#### Verify Cluster Health

```bash
# Check node 1 status
./chronos admin status --http 10.0.0.1:9000

# Expected output:
{"status":"ok","role":"Leader","term":1}

# Check node 2 status
./chronos admin status --http 10.0.0.2:9001

# Expected output:
{"status":"ok","role":"Follower","term":1}
```

---

### 3. Edge-to-Cloud Topology

**Use Case:** IoT sensors → edge gateway → cloud cluster

#### Architecture

```
[Sensors] → [Edge Gateway (Chronos)] → [Cloud Cluster (3 nodes)]
              - Offline queue
              - Sync worker
              - LWW conflict resolution
```

#### Gateway ingest mode (ESP → HTTP)

Besides SQL/gRPC, the edge gateway can accept data directly from ESP/IoT devices via the HTTP admin ingest mode:

- Start the edge node with an extra flag:

  ```bash
  ./chronos node \
    --id edge1 \
    --data-dir /var/lib/chronos/edge \
    --address 192.168.1.100:8001 \
    --clean \
    --sync-target http://10.0.0.10:8000 \
    --sync-interval-secs 5 \
    --sync-batch-size 100 \
    --enable-ingest
  ```

- ESP sends payloads to the admin HTTP port (`address_port + 1000`), for example:

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

- Each metric is inserted into the `readings` table as one row `(reading_id, device_id, ts, seq, metric, value)`.
- You must create the `readings` table first:

  ```sql
  CREATE TABLE readings (reading_id STRING, device_id STRING, ts INT, seq INT, metric STRING, value FLOAT);
  ```

- Ingest is best-effort: requests are queued in-memory (HTTP `202`) and the worker retries up to 5 times per metric.

#### Start Cloud Node

```bash
./chronos node \
  --id cloud \
  --data-dir /var/lib/chronos/cloud \
  --address 10.0.0.10:8000 \
  --clean
```

#### Start Edge Node

```bash
./chronos node \
  --id edge1 \
  --data-dir /var/lib/chronos/edge \
  --address 192.168.1.100:8001 \
  --clean \
  --sync-target http://10.0.0.10:8000 \
  --sync-interval-secs 5 \
  --sync-batch-size 100
```

**Parameters:**
- `--sync-target`: Cloud node address
- `--sync-interval-secs`: Sync frequency (default: 5s)
- `--sync-batch-size`: Operations per batch (default: 100)

#### Write to Edge

```bash
./chronos client --leader 192.168.1.100:8001
```

```sql
CREATE TABLE sensors (id INT, temp FLOAT, device STRING);
INSERT INTO sensors VALUES (1, 25.5, 'sensor-01');
INSERT INTO sensors VALUES (2, 30.0, 'sensor-02');
```

#### Verify Sync to Cloud

```bash
./chronos client --leader 10.0.0.10:8000
```

```sql
SELECT * FROM sensors;
-- Should show both rows after sync interval
```

#### Check Sync Status

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto raft.proto \
  192.168.1.100:8001 \
  raft.SyncStatusService/GetSyncStatus
```

**Output:**
```json
{
  "lastSyncTsMs": "1765127479252",
  "lastSyncApplied": "2",
  "pendingOps": "0",
  "lastError": ""
}
```

---

## Configuration

### Environment Variables

#### Logging

```bash
# Log level (trace, debug, info, warn, error)
export RUST_LOG=chronos=info

# File logging (optional)
export CHRONOS_LOG_FILE=/var/log/chronos/chronos.log
export CHRONOS_LOG_MAX_SIZE_MB=10
export CHRONOS_LOG_MAX_FILES=3
```

#### Security (Authentication)

```bash
# Admin token (read + write)
export CHRONOS_AUTH_TOKEN_ADMIN=your-secure-admin-token

# Read-only token (SELECT only)
export CHRONOS_AUTH_TOKEN_READONLY=your-secure-readonly-token

# Client token (used by REPL)
export CHRONOS_AUTH_TOKEN=your-secure-admin-token
```

#### Security (TLS/mTLS)

```bash
# Enable TLS (server)
export CHRONOS_TLS_CERT=/etc/chronos/tls/server.crt
export CHRONOS_TLS_KEY=/etc/chronos/tls/server.key
export CHRONOS_TLS_CA_CERT=/etc/chronos/tls/ca.crt

# Optional (clients): TLS domain for certificate verification (default: localhost)
export CHRONOS_TLS_DOMAIN=chronos.example.com
```

#### Experimental Features

```bash
# Enable Chimp compression for FLOAT columns
export CHRONOS_CHIMP_ENABLE=1
```

### CLI Flags

```bash
chronos node --help

Options:
  -i, --id <ID>                           Unique node ID
  -d, --data-dir <DATA_DIR>               Data directory [default: data]
  -a, --address <ADDRESS>                 Listen address [default: 127.0.0.1:8000]
  -p, --peers <PEERS>                     Peer list (id=address,...)
      --clean                             Clean data directory on start
      --sync-target <SYNC_TARGET>         Cloud sync target URL
      --sync-interval-secs <SECS>         Sync interval [default: 5]
      --sync-batch-size <SIZE>            Sync batch size [default: 100]
      --ttl-cleanup-interval-secs <SECS>  TTL cleanup interval [default: 3600]
      --enable-ingest                     Enable HTTP ingest endpoint and background worker [default: false]
```

---

## Operational Procedures

### Start / Stop / Restart

#### systemd Service (Recommended)

Create `/etc/systemd/system/chronos.service`:

```ini
[Unit]
Description=ChronosDB Node
After=network.target

[Service]
Type=simple
User=chronos
Group=chronos
WorkingDirectory=/var/lib/chronos
EnvironmentFile=/etc/chronos/chronos.env
ExecStart=/usr/local/bin/chronos node \
  --id node1 \
  --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.1:8000 \
  --peers node2=10.0.0.2:8001,node3=10.0.0.3:8002
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

**Commands:**
```bash
sudo systemctl start chronos
sudo systemctl stop chronos
sudo systemctl restart chronos
sudo systemctl status chronos
sudo systemctl enable chronos  # Auto-start on boot
```

#### Manual Process Management

```bash
# Start in background
nohup ./chronos node --id node1 ... > /var/log/chronos.log 2>&1 &

# Get PID
ps aux | grep chronos

# Stop
kill <PID>

# Force stop
kill -9 <PID>
```

---

### Backup & Restore

#### Create Snapshot

```bash
# Stop node first (or ensure no active writes)
sudo systemctl stop chronos

# Create snapshot
./chronos snapshot create \
  --data-dir /var/lib/chronos/cluster/node1 \
  --output /backups/node1-$(date +%Y%m%d-%H%M%S).snap

# Restart node
sudo systemctl start chronos
```

#### Restore Snapshot

```bash
# Stop node
sudo systemctl stop chronos

# Restore snapshot
./chronos snapshot restore \
  --data-dir /var/lib/chronos/cluster/node1 \
  --input /backups/node1-20250109-120000.snap \
  --force

# Restart node
sudo systemctl start chronos
```

#### Automated Backups (cron)

```bash
# Edit crontab
crontab -e

# Add hourly backup job
0 * * * * /usr/local/bin/chronos snapshot create \
  --data-dir /var/lib/chronos/cluster/node1 \
  --output /backups/node1-$(date +\%Y\%m\%d-\%H\%M\%S).snap

# Add weekly cleanup (keep last 7 days)
0 0 * * 0 find /backups -name "node1-*.snap" -mtime +7 -delete
```

---

### Replace Failed Node

#### Scenario: Node 2 hardware failure

**Step 1: Stop failed node** (if still running)
```bash
ssh node2
sudo systemctl stop chronos
```

**Step 2: Provision new hardware**
- Same node ID: `node2`
- Same network address: `10.0.0.2:8001`

**Step 3: Restore from backup** (optional)
```bash
./chronos snapshot restore \
  --data-dir /var/lib/chronos/cluster/node2 \
  --input /backups/node2-latest.snap \
  --force
```

**Step 4: Start replacement node**
```bash
./chronos node \
  --id node2 \
  --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.2:8001 \
  --peers node1=10.0.0.1:8000,node3=10.0.0.3:8002
```

**Step 5: Verify sync**
```bash
./chronos admin status --http 10.0.0.2:9001
# Should show Follower role and current term
```

---

### Health Monitoring

#### HTTP Health Endpoint

```bash
curl http://10.0.0.1:9000/health
```

**Response:**
```json
{
  "status": "ok",
  "role": "Leader",
  "term": 5
}
```

**Use Cases:**
- Load balancer health checks
- Kubernetes liveness/readiness probes
- Monitoring system integration

#### Prometheus Metrics

```bash
curl http://10.0.0.1:9000/metrics
```

**Sample Output:**
```
# TYPE chronos_sql_requests_total counter
chronos_sql_requests_total{kind="read"} 1523
chronos_sql_requests_total{kind="write"} 847

# TYPE chronos_raft_term gauge
chronos_raft_term 5

# TYPE chronos_raft_role gauge
chronos_raft_role 2

# TYPE chronos_storage_size_bytes gauge
chronos_storage_size_bytes 104857600
```

#### Prometheus Configuration

`prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'chronos'
    static_configs:
      - targets:
          - '10.0.0.1:9000'
          - '10.0.0.2:9001'
          - '10.0.0.3:9002'
    scrape_interval: 15s
```

---

### Troubleshooting

#### Node Cannot Elect Leader

**Symptoms:**
- All nodes stuck in Candidate role
- Clients cannot connect

**Diagnosis:**
```bash
./chronos admin status --http 10.0.0.1:9000
# Check role and term across all nodes
```

**Causes:**
1. Network partition (nodes cannot reach each other)
2. Clock skew (>1s difference)
3. Insufficient nodes (need majority)

**Resolution:**
```bash
# Check network connectivity
ping 10.0.0.2
telnet 10.0.0.2 8001

# Check clock sync
timedatectl status

# Restart nodes sequentially
sudo systemctl restart chronos
```

#### Edge Node Not Syncing

**Symptoms:**
- `pending_ops` increasing
- Cloud not receiving data

**Diagnosis:**
```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto raft.proto \
  192.168.1.100:8001 \
  raft.SyncStatusService/GetSyncStatus
```

**Causes:**
1. Network unreachable
2. Cloud node down
3. TLS mismatch (if TLS is enabled)

**Resolution:**
```bash
# Test cloud connectivity
curl http://10.0.0.10:9000/health

# Check edge logs
journalctl -u chronos -f | grep sync

# If TLS is enabled, verify CHRONOS_TLS_* env vars are set consistently on both sides
```

#### High Disk Usage

**Symptoms:**
- Disk space >80%
- Write failures

**Diagnosis:**
```bash
du -sh /var/lib/chronos/*
```

**Resolution:**
```bash
# Enable TTL for tables
CREATE TABLE sensors (...) WITH TTL=7d;

# Manual cleanup (if TTL not set)
# Chronos SQL has no NOW()/INTERVAL helpers; compute the cutoff timestamp in your app.
DELETE FROM sensors WHERE timestamp < 1733856000;

# Increase TTL cleanup frequency
chronos node ... --ttl-cleanup-interval-secs 1800  # 30 minutes
```

---

## Docker Deployment

### Build Docker Image

```bash
docker build -t chronos:latest .
```

### Run Single Container

```bash
docker run -d \
  --name chronos-node1 \
  -p 8000:8000 \
  -p 9000:9000 \
  -v /var/lib/chronos:/data \
  chronos:latest \
  node --id node1 --data-dir /data --address 0.0.0.0:8000
```

### Docker Compose (3-Node Cluster)

`docker-compose.yml`:
```yaml
version: "3.9"

services:
  node1:
    build: .
    container_name: chronos-node1
    command: [
      "node",
      "--id", "node1",
      "--data-dir", "/data",
      "--address", "0.0.0.0:8000",
      "--peers", "node2=node2:8000,node3=node3:8000",
      "--enable-ingest"
    ]
    ports:
      - "8000:8000"   # gRPC SQL/Raft
      - "9000:9000"   # HTTP admin (gRPC port + 1000)
    environment:
      CHRONOS_AUTH_TOKEN_ADMIN: "admin-secret"
      CHRONOS_AUTH_TOKEN_READONLY: "readonly-secret"
    volumes:
      - node1-data:/data

  node2:
    build: .
    container_name: chronos-node2
    command: [
      "node",
      "--id", "node2",
      "--data-dir", "/data",
      "--address", "0.0.0.0:8000",
      "--peers", "node1=node1:8000,node3=node3:8000"
    ]
    ports:
      - "8001:8000"
      - "9001:9000"
    environment:
      CHRONOS_AUTH_TOKEN_ADMIN: "admin-secret"
      CHRONOS_AUTH_TOKEN_READONLY: "readonly-secret"
    volumes:
      - node2-data:/data

  node3:
    build: .
    container_name: chronos-node3
    command: [
      "node",
      "--id", "node3",
      "--data-dir", "/data",
      "--address", "0.0.0.0:8000",
      "--peers", "node1=node1:8000,node2=node2:8000"
    ]
    ports:
      - "8002:8000"
      - "9002:9000"
    environment:
      CHRONOS_AUTH_TOKEN_ADMIN: "admin-secret"
      CHRONOS_AUTH_TOKEN_READONLY: "readonly-secret"
    volumes:
      - node3-data:/data

volumes:
  node1-data:
  node2-data:
  node3-data:
```

Ingest: `node1` exposes `POST /ingest` at `http://localhost:9000/ingest` (unauthenticated).

**Start Cluster:**
```bash
docker-compose up -d
```

**Stop Cluster:**
```bash
docker-compose down
```

---

## Security Best Practices

### Authentication

1. **Always set auth tokens in production:**
   ```bash
   export CHRONOS_AUTH_TOKEN_ADMIN=$(openssl rand -hex 32)
   export CHRONOS_AUTH_TOKEN_READONLY=$(openssl rand -hex 32)
   ```

2. **Store tokens securely:**
   - Use environment files with restricted permissions
   - Integrate with secret management (Vault, AWS Secrets Manager)

3. **Rotate tokens periodically:**
   - Update environment variables
   - Restart nodes sequentially

### TLS/mTLS

1. **Generate certificates:**
   ```bash
   # CA certificate
   openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt -days 365 -nodes
   
   # Server certificate
   openssl req -newkey rsa:4096 -keyout server.key -out server.csr -nodes
   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365
   ```

2. **Enable TLS:**
   ```bash
   export CHRONOS_TLS_CERT=/etc/chronos/tls/server.crt
   export CHRONOS_TLS_KEY=/etc/chronos/tls/server.key
   export CHRONOS_TLS_CA_CERT=/etc/chronos/tls/ca.crt
   ```

3. **Verify TLS:**
   ```bash
   openssl s_client -connect 10.0.0.1:8000 -CAfile ca.crt
   ```

### Network Security

1. **Firewall rules:**
   ```bash
   # Allow gRPC ports (8000-8002)
   sudo ufw allow 8000:8002/tcp
   
   # Allow admin HTTP (9000-9002) only from monitoring subnet
   sudo ufw allow from 10.0.0.0/24 to any port 9000:9002
   ```

2. **Restrict admin endpoints:**
   - Bind admin HTTP to internal network only
   - Use reverse proxy (nginx) with authentication

---

## Performance Tuning

### Storage Optimization

```bash
# Increase cache size (default: 10_000 entries)
# Modify src/storage/sled_engine.rs (cache_capacity):
# let cache_capacity = NonZeroUsize::new(20_000).expect("non-zero cache size");
```

### Raft Tuning

```bash
# Reduce election timeout for faster failover
# Modify src/raft/config.rs:
election_timeout_min: 100,  // default: 800
election_timeout_max: 200,  // default: 1600

# Increase heartbeat frequency
heartbeat_interval: 30,  // default: 100
```

### Sync Optimization

```bash
# Increase batch size for high-throughput edge nodes
--sync-batch-size 1000

# Reduce sync interval for low-latency requirements
--sync-interval-secs 1
```

---

## Deployment Checklist

### Pre-Deployment

- [ ] Hardware meets minimum requirements
- [ ] Network connectivity verified between nodes
- [ ] Firewall rules configured
- [ ] TLS certificates generated (if using TLS)
- [ ] Auth tokens generated and stored securely
- [ ] Backup strategy defined
- [ ] Monitoring system configured (Prometheus/Grafana)

### Deployment

- [ ] Binary built and copied to all nodes
- [ ] systemd service files created
- [ ] Environment variables configured
- [ ] Data directories created with correct permissions
- [ ] Nodes started in sequence
- [ ] Cluster health verified
- [ ] Client connectivity tested

### Post-Deployment

- [ ] Backup job scheduled (cron)
- [ ] Monitoring alerts configured
- [ ] Log rotation enabled
- [ ] Documentation updated with node IPs
- [ ] Runbook created for common issues
- [ ] Team trained on operational procedures

---

This deployment guide covers production-ready configurations. For development and testing, simplified single-node or Docker Compose setups are sufficient.
