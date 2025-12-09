# ChronosDB Production Deployment Guide

This guide summarizes a practical path to deploying ChronosDB in a small production-like environment. It complements the internal production-readiness checklist and the quickstart examples in `README.md`.

## 1. Baseline Topology

- 3-node Raft cluster (single region):
  - 1 leader, 2 followers.
  - Each node runs the `chronos` binary with its own data directory.
- Optional: one or more edge nodes that sync to a cloud cluster using the built-in sync protocol.

## 2. Build and Distribute the Binary

1. On a build machine:

   ```bash
   cargo build --release
   ```

2. Copy `target/release/chronos` to each node (edge gateways, cloud VMs, etc.).

## 3. Configure Security (Recommended for Prod)

- Set `CHRONOS_AUTH_TOKEN_ADMIN` / `CHRONOS_AUTH_TOKEN_READONLY` for SQL auth.
- Configure TLS/mTLS via `CHRONOS_TLS_CERT`, `CHRONOS_TLS_KEY`, `CHRONOS_TLS_CA_CERT` as described in `README.md` ("Security & Authentication").

## 4. Start a 3-Node Cluster

On three machines (or three terminals for a small lab setup), start nodes similarly to the example in `README.md` (section "3.2 3-node Raft cluster"):

```bash
chronos node --id node1 --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.1:8000 \
  --peers node2=10.0.0.2:8001,node3=10.0.0.3:8002

chronos node --id node2 --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.2:8001 \
  --peers node1=10.0.0.1:8000,node3=10.0.0.3:8002

chronos node --id node3 --data-dir /var/lib/chronos/cluster \
  --address 10.0.0.3:8002 \
  --peers node1=10.0.0.1:8000,node2=10.0.0.2:8001
```

## 5. Verify Cluster Health

- Use the admin CLI from `README.md`:

  ```bash
  chronos admin status --http 10.0.0.1:9000
  chronos admin metrics --http 10.0.0.1:9000
  ```

- Confirm there is one `Leader` and two `Follower` nodes, and that basic `INSERT` / `SELECT` queries succeed via the SQL client.

## 6. Set Up Backups

- Use the snapshot CLI described in `README.md`:

  ```bash
  chronos snapshot create --data-dir /var/lib/chronos/node-1 --output /backups/node-1.snap
  ```

- Store snapshots off-node (object storage, NFS, etc.).

## 7. Edge → Cloud Deployment (Optional)

- Run a "cloud" node and one or more "edge" nodes following the example in `README.md` (section "3.3 Edge + Cloud demo").
- Ensure `--sync-target`, `--sync-interval-secs`, and `--sync-batch-size` are tuned for your link.

## 8. Operational Playbooks

For day-to-day operations, see the `README.md` "Operations Runbook" for start/stop, replacement, backup & restore, and health checks, and internal production documentation for higher-level SLO/SLA expectations.
