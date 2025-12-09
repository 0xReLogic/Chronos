use chronos::parser::Value;
use chronos::storage::{Column, DataType, Row, SledEngine, StorageEngine, TableSchema};
use tempfile::TempDir;

#[tokio::test]
async fn chimp_shadow_series_written_and_decodes() {
    // Enable chimp via env
    std::env::set_var("CHRONOS_CHIMP_ENABLE", "1");

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_str().unwrap().to_string();

    let mut engine = SledEngine::new(&data_dir).expect("engine");
    engine.init().await.expect("init");

    let schema = TableSchema {
        name: "sensors".into(),
        columns: vec![
            Column {
                name: "device_id".into(),
                data_type: DataType::Int,
            },
            Column {
                name: "temp".into(),
                data_type: DataType::Float,
            },
        ],
        ttl_seconds: None,
    };
    engine
        .create_table("sensors", schema)
        .await
        .expect("create");

    // Insert a few rows
    for (id, t) in [(1, 10.0_f64), (2, 10.0_f64), (3, 11.5_f64)] {
        let mut row = Row::new();
        row.insert("device_id".into(), Value::Integer(id));
        row.insert("temp".into(), Value::Float(t));
        engine.insert("sensors", vec![row]).await.expect("insert");
    }

    // Ensure all data is flushed and release sled lock before opening DB directly
    engine.checkpoint().await.expect("checkpoint");
    engine.close().await.expect("close");
    drop(engine);

    // Read the series shadow tree and decode
    let db = sled::open(&data_dir).unwrap();
    let meta = db.open_tree("__series_meta__").unwrap();
    let series = db.open_tree("series:chimp:sensors:temp").unwrap();

    let next_key = b"series:chimp:sensors:temp:next";
    let next_id = meta
        .get(next_key)
        .unwrap()
        .map(|b| {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&b);
            u64::from_be_bytes(arr)
        })
        .unwrap_or(0);

    assert!(next_id >= 1, "series next_id should be >= 1");

    // Scan all segments and decode, ensure values were captured
    use chronos::storage::compression::chimp::decode_series;
    let mut decoded = Vec::new();
    for item in series.iter() {
        let (_k, v) = item.expect("read series seg");
        let seg = decode_series(&v).expect("decode seg");
        decoded.extend(seg);
    }

    // We inserted 3 temps; since we batch per insert() call, there may be multiple segments,
    // but total decoded values must be >= 3 and include 10.0 and 11.5
    assert!(decoded.len() >= 3);
    assert!(decoded.iter().any(|v| (*v - 10.0).abs() < 1e-12));
    assert!(decoded.iter().any(|v| (*v - 11.5).abs() < 1e-12));
}
