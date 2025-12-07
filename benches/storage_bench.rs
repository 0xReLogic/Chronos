use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use chronos::storage::{
    create_storage_engine, Column, DataType, Row, StorageConfig, StorageEngine, TableSchema,
};
use chronos::parser::Value;
use tempfile::TempDir;

async fn setup_storage(config: StorageConfig) -> (Box<dyn StorageEngine>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = match config {
        StorageConfig::Sled { .. } => StorageConfig::Sled {
            data_dir: temp_dir.path().to_str().unwrap().to_string(),
        },
        StorageConfig::Csv { .. } => StorageConfig::Csv {
            data_dir: temp_dir.path().to_str().unwrap().to_string(),
        },
    };
    
    let mut engine = create_storage_engine(config).unwrap();
    engine.init().await.unwrap();
    
    (engine, temp_dir)
}

async fn create_sensor_table(engine: &mut Box<dyn StorageEngine>) {
    let schema = TableSchema {
        name: "sensors".to_string(),
        columns: vec![
            Column {
                name: "sensor_id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "temperature".to_string(),
                data_type: DataType::Float,
            },
            Column {
                name: "humidity".to_string(),
                data_type: DataType::Float,
            },
        ],
        ttl_seconds: None,
    };
    
    engine.create_table("sensors", schema).await.unwrap();
}

async fn create_sensor_table_with_ttl(engine: &mut Box<dyn StorageEngine>, ttl_seconds: u64) {
    let schema = TableSchema {
        name: "sensors_ttl".to_string(),
        columns: vec![
            Column {
                name: "sensor_id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "temperature".to_string(),
                data_type: DataType::Float,
            },
            Column {
                name: "humidity".to_string(),
                data_type: DataType::Float,
            },
        ],
        ttl_seconds: Some(ttl_seconds),
    };

    engine.create_table("sensors_ttl", schema).await.unwrap();
}

fn generate_sensor_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            let mut row = Row::new();
            row.insert("sensor_id".to_string(), Value::Integer(i as i64));
            row.insert("timestamp".to_string(), Value::Integer(1700000000 + i as i64));
            row.insert("temperature".to_string(), Value::Float(20.0 + (i % 30) as f64));
            row.insert("humidity".to_string(), Value::Float(40.0 + (i % 40) as f64));
            row
        })
        .collect()
}

fn benchmark_insert(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("insert");
    for size in [100, 1000] {
        group.bench_with_input(BenchmarkId::new("sled", size), &size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let (mut engine, _temp) = setup_storage(StorageConfig::Sled {
                    data_dir: "./bench_data".to_string(),
                })
                .await;
                
                create_sensor_table(&mut engine).await;
                
                let rows = generate_sensor_rows(size);
                engine.insert("sensors", rows).await.unwrap();
                
                black_box(engine);
            });
        });
    }
    
    group.finish();
}

fn benchmark_insert_small_batches(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("insert_small_batches");
    let total_rows: usize = 1000;

    for &batch_size in &[1_usize, 10_usize, 100_usize] {
        group.bench_with_input(
            BenchmarkId::new("sled_batch", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.to_async(&runtime).iter(|| async move {
                    let (mut engine, _temp) = setup_storage(StorageConfig::Sled {
                        data_dir: "./bench_data".to_string(),
                    })
                    .await;

                    create_sensor_table(&mut engine).await;

                    let rows = generate_sensor_rows(total_rows);
                    for chunk in rows.chunks(batch_size) {
                        engine.insert("sensors", chunk.to_vec()).await.unwrap();
                    }

                    black_box(engine);
                });
            },
        );
    }

    group.finish();
}

fn benchmark_insert_small_batches_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("insert_small_batches_ttl");
    let total_rows: usize = 1000;

    for &batch_size in &[1_usize, 10_usize, 100_usize] {
        group.bench_with_input(
            BenchmarkId::new("sled_ttl_batch", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.to_async(&runtime).iter(|| async move {
                    let (mut engine, _temp) = setup_storage(StorageConfig::Sled {
                        data_dir: "./bench_data".to_string(),
                    })
                    .await;

                    create_sensor_table_with_ttl(&mut engine, 24 * 60 * 60).await;

                    let rows = generate_sensor_rows(total_rows);
                    for chunk in rows.chunks(batch_size) {
                        engine
                            .insert("sensors_ttl", chunk.to_vec())
                            .await
                            .unwrap();
                    }

                    black_box(engine);
                });
            },
        );
    }

    group.finish();
}

fn benchmark_insert_ttl(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("insert_ttl");
    for size in [100, 1000] {
        group.bench_with_input(BenchmarkId::new("sled_ttl", size), &size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let (mut engine, _temp) = setup_storage(StorageConfig::Sled {
                    data_dir: "./bench_data".to_string(),
                })
                .await;

                // Table with a relatively long TTL so that TTL index is
                // maintained on insert, but cleanup work is not part of the
                // benchmark.
                create_sensor_table_with_ttl(&mut engine, 24 * 60 * 60).await;

                let rows = generate_sensor_rows(size);
                engine.insert("sensors_ttl", rows).await.unwrap();

                black_box(engine);
            });
        });
    }

    group.finish();
}

fn benchmark_query(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("query");
    
    for size in [100, 1000] {
        group.bench_with_input(BenchmarkId::new("sled_full_scan", size), &size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                let (mut engine, _temp) = setup_storage(StorageConfig::Sled {
                    data_dir: "./bench_data".to_string(),
                })
                .await;
                
                create_sensor_table(&mut engine).await;
                
                let rows = generate_sensor_rows(size);
                engine.insert("sensors", rows).await.unwrap();
                
                let results = engine.query("sensors", None).await.unwrap();
                
                black_box(results);
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_insert,
    benchmark_insert_ttl,
    benchmark_insert_small_batches,
    benchmark_insert_small_batches_ttl,
    benchmark_query,
);
criterion_main!(benches);
