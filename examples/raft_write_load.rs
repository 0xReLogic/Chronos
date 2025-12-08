use std::time::Instant;

use chronos::network::SqlClient;

/// Simple load generator that exercises the full distributed write path:
/// client -> gRPC -> SqlServer -> Executor -> Raft -> Sled storage.
///
/// Usage (from repo root):
///
///   # run a Raft leader node first, e.g. on 127.0.0.1:8000
///   RUST_LOG=chronos=warn cargo run --release -- node \
///     --id node1 --data-dir data --address 127.0.0.1:8000
///
///   # then run this load generator and measure CPU/RAM with /usr/bin/time -v
///   /usr/bin/time -v cargo run --release --example raft_write_load -- \
///     --leader 127.0.0.1:8000 --ops 10000 --batch-size 1
///
/// The process will print total duration and approximate ops/sec; detailed
/// CPU and memory usage come from `/usr/bin/time -v`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut leader = "127.0.0.1:8000".to_string();
    let mut ops: u64 = 10_000;
    let mut batch_size: u64 = 1;

    // Very simple CLI parsing: --leader, --ops, --batch-size
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--leader" => {
                if let Some(v) = args.next() {
                    leader = v;
                }
            }
            "--ops" => {
                if let Some(v) = args.next() {
                    if let Ok(n) = v.parse::<u64>() {
                        ops = n;
                    }
                }
            }
            "--batch-size" => {
                if let Some(v) = args.next() {
                    if let Ok(n) = v.parse::<u64>() {
                        batch_size = n.max(1);
                    }
                }
            }
            _ => {}
        }
    }

    println!(
        "Starting raft_write_load: leader={}, ops={}, batch_size={}",
        leader, ops, batch_size
    );

    let mut client = SqlClient::new(&leader);
    client.connect().await?;

    // Ensure table exists; ignore "already exists" and just log other errors.
    let create_sql = "CREATE TABLE sensors (id INT, temperature FLOAT);";
    match client.execute_sql(create_sql).await {
        Ok(resp) => {
            if !resp.success {
                eprintln!("CREATE TABLE error: {}", resp.error);
            }
        }
        Err(e) => {
            eprintln!("CREATE TABLE RPC error: {}", e);
        }
    }

    let start = Instant::now();
    let mut i: u64 = 0;

    while i < ops {
        let sql = format!(
            "INSERT INTO sensors (id, temperature) VALUES ({}, {});",
            i,
            20.0 + (i % 10) as f64
        );

        let resp = client.execute_sql(&sql).await?;
        if !resp.success {
            eprintln!("INSERT error at op {}: {}", i, resp.error);
        }

        i += 1;

        if batch_size > 0 && i % batch_size == 0 {
            // simple progress marker
            println!("completed {} operations", i);
        }
    }

    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    let ops_f = ops as f64;
    let throughput = if secs > 0.0 { ops_f / secs } else { 0.0 };

    println!(
        "Done: {} ops in {:.3} s (≈ {:.1} ops/s)",
        ops, secs, throughput
    );

    Ok(())
}
