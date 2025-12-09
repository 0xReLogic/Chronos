use chronos::parser::Value;
use chronos::storage::snapshot::{create_snapshot, restore_snapshot};
use chronos::storage::{Column, DataType, Row, SledEngine, StorageEngine, TableSchema};
use tempfile::TempDir;

#[tokio::test]
async fn snapshot_backup_restore_basic() {
    // Prepare source data dir
    let src = TempDir::new().expect("tempdir");
    let src_dir = src.path().to_str().unwrap().to_string();

    // Create table and insert a row
    let mut engine = SledEngine::new(&src_dir).expect("sled engine");
    engine.init().await.expect("init");

    let schema = TableSchema {
        name: "sensors".to_string(),
        columns: vec![
            Column {
                name: "device_id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "temp".to_string(),
                data_type: DataType::Float,
            },
        ],
        ttl_seconds: None,
    };

    engine
        .create_table("sensors", schema)
        .await
        .expect("create table");

    let mut row = Row::new();
    row.insert("device_id".to_string(), Value::Integer(42));
    row.insert("temp".to_string(), Value::Float(21.5));

    engine.insert("sensors", vec![row]).await.expect("insert");
    // Ensure data is flushed and release locks before snapshot
    engine.checkpoint().await.expect("checkpoint");
    engine.close().await.expect("close");
    drop(engine);

    // Create snapshot file
    let snap_dir = TempDir::new().expect("snapdir");
    let snap_path = snap_dir.path().join("backup.snap");
    create_snapshot(&src_dir, snap_path.to_str().unwrap()).expect("create snapshot");

    // Restore into destination dir
    let dst = TempDir::new().expect("dstdir");
    let dst_dir = dst.path().to_str().unwrap().to_string();
    restore_snapshot(&dst_dir, snap_path.to_str().unwrap(), false).expect("restore snapshot");

    // Open restored engine and verify data
    let engine2 = SledEngine::new(&dst_dir).expect("engine2");
    // Query without filter
    let rows = engine2.query("sensors", None).await.expect("query");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.get("device_id"), Some(&Value::Integer(42)));
    match r.get("temp") {
        Some(Value::Float(v)) => assert!((*v - 21.5).abs() < 1e-9),
        _ => panic!("bad temp"),
    }
}
