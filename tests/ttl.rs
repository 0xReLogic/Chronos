use std::time::{SystemTime, UNIX_EPOCH};

use chronos::executor::Executor;
use chronos::parser::Parser;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_basic_expiry_removes_rows() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("ttl_basic");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut executor = Executor::new(data_dir_str).await;

    // Create a table with a short TTL
    let ast = Parser::parse(
        "CREATE TABLE sensors (sensor_id INT, temperature FLOAT) WITH TTL = 5s;",
    )
    .expect("parse create table with TTL");
    let _ = executor
        .execute(ast)
        .await
        .expect("execute create table with TTL");

    // Insert a single row
    let ast = Parser::parse(
        "INSERT INTO sensors (sensor_id, temperature) VALUES (1, 10.0);",
    )
    .expect("parse insert");
    let _ = executor.execute(ast).await.expect("execute insert");

    // Verify row is present before TTL cleanup
    let ast = Parser::parse("SELECT sensor_id, temperature FROM sensors;")
        .expect("parse select");
    let res = executor.execute(ast).await.expect("execute select");
    assert_eq!(res.rows.len(), 1, "row should exist before TTL cleanup");

    // Run TTL cleanup with now_secs far in the future so the row is expired
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs()
        .saturating_add(10);
    let deleted = executor
        .cleanup_expired(now_secs, 1000)
        .await
        .expect("cleanup_expired");
    assert_eq!(deleted, 1, "expected 1 row to be deleted by TTL cleanup");

    // The table should now be empty
    let ast = Parser::parse("SELECT sensor_id, temperature FROM sensors;")
        .expect("parse select after cleanup");
    let res = executor
        .execute(ast)
        .await
        .expect("execute select after cleanup");
    assert_eq!(res.rows.len(), 0, "table should be empty after TTL cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_does_not_affect_tables_without_ttl() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("ttl_mixed");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut executor = Executor::new(data_dir_str).await;

    // Table with TTL
    let ast = Parser::parse(
        "CREATE TABLE sensors_ttl (sensor_id INT, temperature FLOAT) WITH TTL = 5s;",
    )
    .expect("parse create table with TTL");
    let _ = executor
        .execute(ast)
        .await
        .expect("execute create table with TTL");

    // Table without TTL
    let ast = Parser::parse(
        "CREATE TABLE sensors_no_ttl (sensor_id INT, temperature FLOAT);",
    )
    .expect("parse create table without TTL");
    let _ = executor
        .execute(ast)
        .await
        .expect("execute create table without TTL");

    // Insert one row into each table
    let ast = Parser::parse(
        "INSERT INTO sensors_ttl (sensor_id, temperature) VALUES (1, 10.0);",
    )
    .expect("parse insert ttl");
    let _ = executor
        .execute(ast)
        .await
        .expect("execute insert ttl");

    let ast = Parser::parse(
        "INSERT INTO sensors_no_ttl (sensor_id, temperature) VALUES (1, 10.0);",
    )
    .expect("parse insert no ttl");
    let _ = executor
        .execute(ast)
        .await
        .expect("execute insert no ttl");

    // Sanity check before cleanup
    let ast = Parser::parse("SELECT sensor_id, temperature FROM sensors_ttl;")
        .expect("parse select ttl before");
    let res = executor
        .execute(ast)
        .await
        .expect("execute select ttl before");
    assert_eq!(
        res.rows.len(),
        1,
        "expected 1 row in sensors_ttl before cleanup",
    );

    let ast = Parser::parse("SELECT sensor_id, temperature FROM sensors_no_ttl;")
        .expect("parse select no ttl before");
    let res = executor
        .execute(ast)
        .await
        .expect("execute select no ttl before");
    assert_eq!(
        res.rows.len(),
        1,
        "expected 1 row in sensors_no_ttl before cleanup",
    );

    // Run TTL cleanup far in the future
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs()
        .saturating_add(10);
    let deleted = executor
        .cleanup_expired(now_secs, 1000)
        .await
        .expect("cleanup_expired");
    assert_eq!(
        deleted, 1,
        "expected only the TTL-enabled table row to be deleted",
    );

    // TTL table should be empty
    let ast = Parser::parse("SELECT sensor_id, temperature FROM sensors_ttl;")
        .expect("parse select ttl after");
    let res = executor
        .execute(ast)
        .await
        .expect("execute select ttl after");
    assert_eq!(
        res.rows.len(),
        0,
        "expected 0 rows in sensors_ttl after cleanup",
    );

    // Non-TTL table should still have its row
    let ast = Parser::parse("SELECT sensor_id, temperature FROM sensors_no_ttl;")
        .expect("parse select no ttl after");
    let res = executor
        .execute(ast)
        .await
        .expect("execute select no ttl after");
    assert_eq!(
        res.rows.len(),
        1,
        "expected 1 row in sensors_no_ttl after cleanup",
    );
}
