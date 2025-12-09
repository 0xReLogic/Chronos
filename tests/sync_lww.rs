use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chronos::common::timestamp::HybridTimestamp;
use chronos::executor::Executor;
use chronos::network::proto::sync_service_server::SyncService;
use chronos::network::proto::{SyncOperation, SyncRequest};
use chronos::network::{SqlClient, SyncServer};
use chronos::parser::{Parser, Value as SqlValue};
use chronos::storage::offline_queue::PersistentQueuedOperation;
use chronos::storage::wal::Operation as WalOperation;
use chronos::storage::Row as StorageRow;
use lz4_flex::block::compress_prepend_size;
use rand::Rng;
use tempfile::TempDir;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;
use tonic::Request;
use uhlc::HLC;

const BIN: &str = env!("CARGO_BIN_EXE_chronos");

async fn start_cloud_node(port: u16, data_dir: &str) -> Child {
    let mut cmd = Command::new(BIN);
    cmd.arg("node")
        .arg("--id")
        .arg("cloud")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--address")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--clean");

    cmd.env("RUST_LOG", "chronos=info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    cmd.spawn().expect("failed to spawn cloud node")
}

async fn start_edge_node(port: u16, data_dir: &str, sync_target: &str) -> Child {
    let mut cmd = Command::new(BIN);
    cmd.arg("node")
        .arg("--id")
        .arg("edge1")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--address")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--clean")
        .arg("--sync-target")
        .arg(sync_target)
        .arg("--sync-interval-secs")
        .arg("1")
        .arg("--sync-batch-size")
        .arg("100");

    cmd.env("RUST_LOG", "chronos=info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    cmd.spawn().expect("failed to spawn edge node")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_to_cloud_sync_basic_propagates_rows() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_cluster_basic");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut rng = rand::rng();
    let base: u16 = 23000 + (rng.random_range(0u16..1000) * 2);
    let cloud_port: u16 = base;
    let edge_port: u16 = base + 1;

    let mut cloud = start_cloud_node(cloud_port, data_dir_str).await;

    // Give cloud node time to start and elect itself as leader (single-node Raft).
    sleep(Duration::from_secs(3)).await;

    let cloud_addr = format!("127.0.0.1:{}", cloud_port);
    let sync_target = format!("http://{}", cloud_addr);

    let mut edge = start_edge_node(edge_port, data_dir_str, &sync_target).await;

    // Allow edge node to start and its SyncWorker to begin running.
    sleep(Duration::from_secs(3)).await;

    let edge_addr = format!("127.0.0.1:{}", edge_port);

    // Write data on the edge node.
    let mut edge_client = SqlClient::new(&edge_addr);
    edge_client.connect().await.expect("edge SqlClient connect");

    edge_client
        .execute_sql("CREATE TABLE sensors (sensor_id INT, temperature FLOAT);")
        .await
        .expect("create table on edge");

    let inserts = [(1, 10.0_f64), (2, 20.0_f64), (3, 30.0_f64)];

    for (id, temp) in inserts {
        let sql = format!(
            "INSERT INTO sensors (sensor_id, temperature) VALUES ({}, {});",
            id, temp
        );
        edge_client.execute_sql(&sql).await.expect("insert on edge");
    }

    // Wait for the SyncWorker on the edge to drain the offline queue and push
    // operations to the cloud node.
    sleep(Duration::from_secs(5)).await;

    // Verify that the cloud node now sees the same rows.
    let mut cloud_client = SqlClient::new(&cloud_addr);
    cloud_client
        .connect()
        .await
        .expect("cloud SqlClient connect");

    let mut rows_seen = 0usize;
    for _ in 0..20u32 {
        let resp = cloud_client
            .execute_sql("SELECT sensor_id, temperature FROM sensors;")
            .await
            .expect("select on cloud");

        if resp.success && resp.rows.len() >= 3 {
            rows_seen = resp.rows.len();
            break;
        }

        sleep(Duration::from_millis(500)).await;
    }

    assert!(
        rows_seen >= 3,
        "expected at least 3 rows on cloud after sync, got {}",
        rows_seen
    );

    let _ = edge.kill();
    let _ = cloud.kill();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_idempotent_replay_same_batch() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_idempotent");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let executor = Executor::new(data_dir_str).await;
    let executor = Arc::new(TokioMutex::new(executor));
    let sync_server = SyncServer::new(Arc::clone(&executor));

    let edge_id = "edge1".to_string();

    let ts = HybridTimestamp {
        ts: HLC::default().new_timestamp(),
        node_id: 1,
    };
    let pq = PersistentQueuedOperation {
        id: 1,
        timestamp: ts,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: b"row1".to_vec(),
            value: vec![],
        },
    };
    let data = bincode::serde::encode_to_vec(&pq, bincode::config::standard())
        .expect("encode PersistentQueuedOperation");
    let op = SyncOperation { id: pq.id, data };

    let req1 = SyncRequest {
        edge_id: edge_id.clone(),
        operations: vec![op.clone()],
    };
    let resp1 = sync_server
        .sync(Request::new(req1))
        .await
        .expect("first sync RPC")
        .into_inner();
    assert_eq!(resp1.applied, 1, "expected first sync to apply operation");

    let req2 = SyncRequest {
        edge_id,
        operations: vec![op],
    };
    let resp2 = sync_server
        .sync(Request::new(req2))
        .await
        .expect("second sync RPC")
        .into_inner();
    assert_eq!(
        resp2.applied, 0,
        "replaying same batch should be idempotent (applied=0)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_restart_persists_cursor_and_skips_old_ids() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_restart_cursor");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let executor1 = Executor::new(data_dir_str).await;
    let executor1 = Arc::new(TokioMutex::new(executor1));
    let sync_server1 = SyncServer::new(Arc::clone(&executor1));

    let edge_id = "edge1".to_string();

    let ts = HybridTimestamp {
        ts: HLC::default().new_timestamp(),
        node_id: 1,
    };

    let make_pq = |id: u64| PersistentQueuedOperation {
        id,
        timestamp: ts,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: format!("row{}", id).into_bytes(),
            value: vec![],
        },
    };

    let ops1: Vec<SyncOperation> = [1u64, 2, 3]
        .into_iter()
        .map(|id| {
            let pq = make_pq(id);
            let data = bincode::serde::encode_to_vec(&pq, bincode::config::standard())
                .expect("encode PersistentQueuedOperation");
            SyncOperation { id: pq.id, data }
        })
        .collect();

    let req1 = SyncRequest {
        edge_id: edge_id.clone(),
        operations: ops1,
    };
    let resp1 = sync_server1
        .sync(Request::new(req1))
        .await
        .expect("first sync RPC")
        .into_inner();
    assert_eq!(
        resp1.applied, 3,
        "expected first sync to apply all initial operations"
    );

    drop(sync_server1);
    drop(executor1);

    let executor2 = Executor::new(data_dir_str).await;
    let executor2 = Arc::new(TokioMutex::new(executor2));
    let sync_server2 = SyncServer::new(Arc::clone(&executor2));

    let ops2: Vec<SyncOperation> = [1u64, 2, 3, 4]
        .into_iter()
        .map(|id| {
            let pq = make_pq(id);
            let data = bincode::serde::encode_to_vec(&pq, bincode::config::standard())
                .expect("encode PersistentQueuedOperation");
            SyncOperation { id: pq.id, data }
        })
        .collect();

    let req2 = SyncRequest {
        edge_id,
        operations: ops2,
    };
    let resp2 = sync_server2
        .sync(Request::new(req2))
        .await
        .expect("second sync RPC")
        .into_inner();
    assert_eq!(
        resp2.applied, 1,
        "after restart, only IDs greater than cursor should be applied"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_delete_propagates_row_removal() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_delete");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let executor = Executor::new(data_dir_str).await;
    let executor = Arc::new(TokioMutex::new(executor));
    let sync_server = SyncServer::new(Arc::clone(&executor));
    let edge_id = "edge_delete".to_string();

    {
        let ast = Parser::parse("CREATE TABLE sensors (sensor_id INT, temperature FLOAT);")
            .expect("parse create table");
        let mut exec = executor.lock().await;
        let _ = exec.execute(ast).await.expect("execute create table");
    }

    let mut row = StorageRow::new();
    row.insert("sensor_id".to_string(), SqlValue::Integer(1));
    row.insert("temperature".to_string(), SqlValue::Float(10.0));
    let raw = bincode::encode_to_vec(&row, bincode::config::standard()).expect("encode row");

    let mut compressed = compress_prepend_size(&raw);
    let mut value = Vec::with_capacity(1 + compressed.len());
    value.push(1);
    value.append(&mut compressed);

    let hlc = HLC::default();
    let ts_insert = HybridTimestamp {
        ts: hlc.new_timestamp(),
        node_id: 1,
    };
    let ts_delete = HybridTimestamp {
        ts: hlc.new_timestamp(),
        node_id: 1,
    };

    let insert_pq = PersistentQueuedOperation {
        id: 1,
        timestamp: ts_insert,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: b"row1".to_vec(),
            value: value.clone(),
        },
    };
    let delete_pq = PersistentQueuedOperation {
        id: 2,
        timestamp: ts_delete,
        operation: WalOperation::Delete {
            table: "sensors".to_string(),
            key: b"row1".to_vec(),
        },
    };

    let insert_bytes = bincode::serde::encode_to_vec(&insert_pq, bincode::config::standard())
        .expect("encode insert pq");
    let delete_bytes = bincode::serde::encode_to_vec(&delete_pq, bincode::config::standard())
        .expect("encode delete pq");

    let insert_op = SyncOperation {
        id: insert_pq.id,
        data: insert_bytes,
    };
    let delete_op = SyncOperation {
        id: delete_pq.id,
        data: delete_bytes,
    };

    let req1 = SyncRequest {
        edge_id: edge_id.clone(),
        operations: vec![insert_op],
    };
    let resp1 = sync_server
        .sync(Request::new(req1))
        .await
        .expect("first sync RPC")
        .into_inner();
    assert_eq!(resp1.applied, 1, "insert should be applied");

    {
        let ast =
            Parser::parse("SELECT sensor_id, temperature FROM sensors;").expect("parse select");
        let mut exec = executor.lock().await;
        let res = exec.execute(ast).await.expect("execute select");
        assert_eq!(res.rows.len(), 1, "expected 1 row after insert via sync",);
    }

    let req2 = SyncRequest {
        edge_id,
        operations: vec![delete_op],
    };
    let resp2 = sync_server
        .sync(Request::new(req2))
        .await
        .expect("second sync RPC")
        .into_inner();
    assert_eq!(resp2.applied, 1, "delete should be applied");

    {
        let ast =
            Parser::parse("SELECT sensor_id, temperature FROM sensors;").expect("parse select");
        let mut exec = executor.lock().await;
        let res = exec.execute(ast).await.expect("execute select");
        assert_eq!(res.rows.len(), 0, "expected 0 rows after delete via sync",);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_mixed_batch_applies_only_newer_per_key() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_mixed_batch");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let executor = Executor::new(data_dir_str).await;
    let executor = Arc::new(TokioMutex::new(executor));
    let sync_server = SyncServer::new(Arc::clone(&executor));
    let edge_id = "edge_mixed".to_string();

    {
        let ast = Parser::parse("CREATE TABLE sensors (sensor_id INT, temperature FLOAT);")
            .expect("parse create table");
        let mut exec = executor.lock().await;
        let _ = exec.execute(ast).await.expect("execute create table");
    }

    let mut row_a_old = StorageRow::new();
    row_a_old.insert("sensor_id".to_string(), SqlValue::Integer(1));
    row_a_old.insert("temperature".to_string(), SqlValue::Float(10.0));

    let mut row_a_new = StorageRow::new();
    row_a_new.insert("sensor_id".to_string(), SqlValue::Integer(1));
    row_a_new.insert("temperature".to_string(), SqlValue::Float(20.0));

    let mut row_b = StorageRow::new();
    row_b.insert("sensor_id".to_string(), SqlValue::Integer(2));
    row_b.insert("temperature".to_string(), SqlValue::Float(15.0));

    let encode_row = |row: &StorageRow| {
        let raw = bincode::encode_to_vec(row, bincode::config::standard()).expect("encode row");
        let mut compressed = compress_prepend_size(&raw);
        let mut value = Vec::with_capacity(1 + compressed.len());
        value.push(1);
        value.append(&mut compressed);
        value
    };

    let value_a_old = encode_row(&row_a_old);
    let value_a_new = encode_row(&row_a_new);
    let value_b = encode_row(&row_b);

    let hlc = HLC::default();
    let ts_old = HybridTimestamp {
        ts: hlc.new_timestamp(),
        node_id: 1,
    };
    let ts_new = HybridTimestamp {
        ts: hlc.new_timestamp(),
        node_id: 1,
    };
    let ts_b = HybridTimestamp {
        ts: hlc.new_timestamp(),
        node_id: 1,
    };

    let pq_new = PersistentQueuedOperation {
        id: 1,
        timestamp: ts_new,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: b"rowA".to_vec(),
            value: value_a_new,
        },
    };
    let pq_stale = PersistentQueuedOperation {
        id: 2,
        timestamp: ts_old,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: b"rowA".to_vec(),
            value: value_a_old,
        },
    };
    let pq_b = PersistentQueuedOperation {
        id: 3,
        timestamp: ts_b,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: b"rowB".to_vec(),
            value: value_b,
        },
    };

    let op_new = SyncOperation {
        id: pq_new.id,
        data: bincode::serde::encode_to_vec(&pq_new, bincode::config::standard())
            .expect("encode pq_new"),
    };
    let op_stale = SyncOperation {
        id: pq_stale.id,
        data: bincode::serde::encode_to_vec(&pq_stale, bincode::config::standard())
            .expect("encode pq_stale"),
    };
    let op_b = SyncOperation {
        id: pq_b.id,
        data: bincode::serde::encode_to_vec(&pq_b, bincode::config::standard())
            .expect("encode pq_b"),
    };

    let req = SyncRequest {
        edge_id,
        operations: vec![op_new, op_stale, op_b],
    };
    let resp = sync_server
        .sync(Request::new(req))
        .await
        .expect("mixed batch sync RPC")
        .into_inner();
    assert_eq!(
        resp.applied, 2,
        "only newer op for rowA and rowB should be applied (total 2)",
    );

    {
        let ast =
            Parser::parse("SELECT sensor_id, temperature FROM sensors;").expect("parse select");
        let mut exec = executor.lock().await;
        let res = exec.execute(ast).await.expect("execute select");
        assert_eq!(
            res.rows.len(),
            2,
            "expected 2 rows (sensor_id=1 and sensor_id=2)",
        );

        let mut sensor1_count = 0;
        let mut sensor1_temp: Option<f64> = None;
        for row in res.rows {
            if let (Some(id_val), Some(temp_val)) = (row.first(), row.get(1)) {
                if let (SqlValue::Integer(id), SqlValue::Float(temp)) = (id_val, temp_val) {
                    if *id == 1 {
                        sensor1_count += 1;
                        sensor1_temp = Some(*temp);
                    }
                }
            }
        }

        assert_eq!(
            sensor1_count, 1,
            "expected exactly one row for sensor_id=1 after LWW",
        );
        let temp = sensor1_temp.expect("sensor_id=1 row missing");
        assert!(
            (temp - 20.0).abs() < f64::EPSILON,
            "expected latest temperature 20.0 for sensor_id=1, got {}",
            temp
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_multi_edge_has_independent_cursors() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_multi_edge");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let executor = Executor::new(data_dir_str).await;
    let executor = Arc::new(TokioMutex::new(executor));
    let sync_server = SyncServer::new(Arc::clone(&executor));

    let edge1 = "edge1".to_string();
    let edge2 = "edge2".to_string();

    let hlc = HLC::default();
    let next_ts = || HybridTimestamp {
        ts: hlc.new_timestamp(),
        node_id: 1,
    };

    let make_pq = |edge: &str, id: u64, ts: HybridTimestamp| PersistentQueuedOperation {
        id,
        timestamp: ts,
        operation: WalOperation::Insert {
            table: "sensors".to_string(),
            key: format!("{}_row{}", edge, id).into_bytes(),
            value: vec![],
        },
    };

    let to_ops = |pqs: Vec<PersistentQueuedOperation>| -> Vec<SyncOperation> {
        pqs.into_iter()
            .map(|pq| SyncOperation {
                id: pq.id,
                data: bincode::serde::encode_to_vec(&pq, bincode::config::standard())
                    .expect("encode pq"),
            })
            .collect()
    };

    let ops_edge1_first = to_ops(vec![
        make_pq(&edge1, 1, next_ts()),
        make_pq(&edge1, 2, next_ts()),
        make_pq(&edge1, 3, next_ts()),
    ]);

    let ops_edge2_first = to_ops(vec![make_pq(&edge2, 1, next_ts())]);

    let req1 = SyncRequest {
        edge_id: edge1.clone(),
        operations: ops_edge1_first,
    };
    let resp1 = sync_server
        .sync(Request::new(req1))
        .await
        .expect("edge1 first sync")
        .into_inner();
    assert_eq!(resp1.applied, 3, "edge1 should apply 3 ops");

    let req2 = SyncRequest {
        edge_id: edge2.clone(),
        operations: ops_edge2_first,
    };
    let resp2 = sync_server
        .sync(Request::new(req2))
        .await
        .expect("edge2 first sync")
        .into_inner();
    assert_eq!(resp2.applied, 1, "edge2 should apply 1 op");

    let ops_edge2_second = to_ops(vec![
        make_pq(&edge2, 1, next_ts()),
        make_pq(&edge2, 2, next_ts()),
        make_pq(&edge2, 3, next_ts()),
    ]);

    let req3 = SyncRequest {
        edge_id: edge2,
        operations: ops_edge2_second,
    };
    let resp3 = sync_server
        .sync(Request::new(req3))
        .await
        .expect("edge2 second sync")
        .into_inner();
    assert_eq!(
        resp3.applied, 2,
        "edge2 should apply only new IDs (2 and 3), independent of edge1 cursor",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_large_batch_is_idempotent_and_advances_cursor() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_large_batch");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let executor = Executor::new(data_dir_str).await;
    let executor = Arc::new(TokioMutex::new(executor));
    let sync_server = SyncServer::new(Arc::clone(&executor));

    let edge_id = "edge_large".to_string();
    let hlc = HLC::default();

    let mut ops: Vec<SyncOperation> = Vec::new();
    let batch_size: u64 = 256;

    for id in 1..=batch_size {
        let ts = HybridTimestamp {
            ts: hlc.new_timestamp(),
            node_id: 1,
        };
        let pq = PersistentQueuedOperation {
            id,
            timestamp: ts,
            operation: WalOperation::Insert {
                table: "sensors".to_string(),
                key: format!("row{}", id).into_bytes(),
                value: vec![],
            },
        };
        let data =
            bincode::serde::encode_to_vec(&pq, bincode::config::standard()).expect("encode pq");
        ops.push(SyncOperation { id, data });
    }

    let req1 = SyncRequest {
        edge_id: edge_id.clone(),
        operations: ops.clone(),
    };
    let resp1 = sync_server
        .sync(Request::new(req1))
        .await
        .expect("large batch first sync")
        .into_inner();
    assert_eq!(
        resp1.applied, batch_size,
        "first large batch should apply all operations",
    );

    let req2 = SyncRequest {
        edge_id,
        operations: ops,
    };
    let resp2 = sync_server
        .sync(Request::new(req2))
        .await
        .expect("large batch second sync")
        .into_inner();
    assert_eq!(
        resp2.applied, 0,
        "replaying same large batch should be idempotent (applied=0)",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_to_cloud_lww_conflict_uses_latest_value() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("sync_cluster_lww");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut rng = rand::rng();
    let base: u16 = 24000 + (rng.random_range(0u16..1000) * 2);
    let cloud_port: u16 = base;
    let edge_port: u16 = base + 1;

    let mut cloud = start_cloud_node(cloud_port, data_dir_str).await;

    sleep(Duration::from_secs(3)).await;

    let cloud_addr = format!("127.0.0.1:{}", cloud_port);
    let sync_target = format!("http://{}", cloud_addr);

    let mut edge = start_edge_node(edge_port, data_dir_str, &sync_target).await;

    sleep(Duration::from_secs(3)).await;

    let edge_addr = format!("127.0.0.1:{}", edge_port);

    let mut edge_client = SqlClient::new(&edge_addr);
    edge_client.connect().await.expect("edge SqlClient connect");

    edge_client
        .execute_sql("CREATE TABLE sensors (sensor_id INT, temperature FLOAT);")
        .await
        .expect("create table on edge");

    // Initial insert for sensor_id=1 with temperature=10.0
    edge_client
        .execute_sql("INSERT INTO sensors (sensor_id, temperature) VALUES (1, 10.0);")
        .await
        .expect("initial insert on edge");

    // Let this initial value sync to the cloud.
    sleep(Duration::from_secs(5)).await;

    // Now update sensor_id=1 with a newer temperature value on the edge.
    edge_client
        .execute_sql("UPDATE sensors SET temperature = 30.0 WHERE sensor_id = 1;")
        .await
        .expect("update on edge");

    let mut edge_found_latest = false;
    for _ in 0..10u32 {
        let resp = edge_client
            .execute_sql("SELECT sensor_id, temperature FROM sensors WHERE sensor_id = 1;")
            .await
            .expect("select on edge");

        if resp.success && !resp.rows.is_empty() {
            if let Some(temp_idx) = resp.columns.iter().position(|c| c == "temperature") {
                'outer_edge: for row in &resp.rows {
                    if let Some(cell) = row.values.get(temp_idx) {
                        if let Some(inner) = &cell.value {
                            let val = match inner {
                                chronos::network::proto::value::Value::FloatValue(f) => *f,
                                chronos::network::proto::value::Value::IntValue(i) => *i as f64,
                                _ => continue,
                            };

                            println!("[LWW DEBUG] edge observed temperature={} before sync", val);

                            if (val - 30.0).abs() < f64::EPSILON {
                                edge_found_latest = true;
                                break 'outer_edge;
                            }
                        }
                    }
                }

                if edge_found_latest {
                    break;
                }
            }
        }

        sleep(Duration::from_millis(200)).await;
    }

    assert!(
        edge_found_latest,
        "expected to observe updated temperature 30.0 for sensor_id=1 on edge before sync"
    );

    // Allow another sync interval for the updated value to propagate.
    sleep(Duration::from_secs(5)).await;

    let mut cloud_client = SqlClient::new(&cloud_addr);
    cloud_client
        .connect()
        .await
        .expect("cloud SqlClient connect");

    let mut found_latest = false;
    for _ in 0..20u32 {
        let resp = cloud_client
            .execute_sql("SELECT sensor_id, temperature FROM sensors WHERE sensor_id = 1;")
            .await
            .expect("select on cloud");

        if resp.success && !resp.rows.is_empty() {
            // Debug output to understand what the cloud currently sees.
            println!(
                "[LWW DEBUG] cloud SELECT returned {} rows, columns={:?}",
                resp.rows.len(),
                resp.columns
            );

            // Find the index of the temperature column, then scan all rows
            // for a numeric value equal to 30 (accepting either INT or FLOAT
            // representation depending on how the parser stored it).
            if let Some(temp_idx) = resp.columns.iter().position(|c| c == "temperature") {
                'outer: for row in &resp.rows {
                    if let Some(cell) = row.values.get(temp_idx) {
                        if let Some(inner) = &cell.value {
                            let val = match inner {
                                chronos::network::proto::value::Value::FloatValue(f) => *f,
                                chronos::network::proto::value::Value::IntValue(i) => *i as f64,
                                _ => {
                                    println!(
                                        "[LWW DEBUG] non-numeric temperature cell: {:?}",
                                        inner
                                    );
                                    continue;
                                }
                            };

                            println!("[LWW DEBUG] observed temperature={} on cloud", val);

                            if (val - 30.0).abs() < f64::EPSILON {
                                found_latest = true;
                                break 'outer;
                            }
                        }
                    }
                }

                if found_latest {
                    break;
                }
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    assert!(
        found_latest,
        "expected to observe temperature 30.0 for sensor_id=1 on cloud after LWW sync"
    );

    let _ = edge.kill();
    let _ = cloud.kill();
}
