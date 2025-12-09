use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use chronos::network::SqlClient;
use rand::Rng;
use tempfile::TempDir;
use tokio::time::sleep;

const BIN: &str = env!("CARGO_BIN_EXE_chronos");

async fn start_node(id: &str, port: u16, peers: &[(&str, u16)], data_dir: &str) -> Child {
    let mut cmd = Command::new(BIN);
    cmd.arg("node")
        .arg("--id")
        .arg(id)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--address")
        .arg(format!("127.0.0.1:{}", port));

    if !peers.is_empty() {
        let peers_arg = peers
            .iter()
            .map(|(peer_id, peer_port)| format!("{}=127.0.0.1:{}", peer_id, peer_port))
            .collect::<Vec<_>>()
            .join(",");
        cmd.arg("--peers").arg(peers_arg);
    }

    cmd.env("RUST_LOG", "chronos=info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn chronos node")
}

async fn find_leader(addrs: &[&str]) -> String {
    // Retry for a while to allow elections and gRPC servers to settle.
    for _attempt in 0..20u32 {
        for addr in addrs {
            let mut client = SqlClient::new(addr);
            if client.connect().await.is_err() {
                continue;
            }

            // Use CREATE TABLE as a probe:
            // - followers respond with "Not the leader" without touching storage
            // - the leader either creates the table (success=true) or returns a
            //   Storage error about the table already existing.
            match client
                .execute_sql("CREATE TABLE sensors (id INT, temperature FLOAT);")
                .await
            {
                Ok(resp) => {
                    if resp.success
                        || resp
                            .error
                            .to_lowercase()
                            .contains("table 'sensors' already exists")
                    {
                        return addr.to_string();
                    }
                }
                Err(_) => continue,
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    panic!("no Raft leader found among provided addresses after retries");
}

fn is_addr_for_port(addr: &str, port: u16) -> bool {
    addr.ends_with(&format!(":{}", port))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_write_and_failover_persists_data() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("cluster");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    // Pick a random base port in a high range to reduce the chance of
    // collisions with other processes or previous test runs.
    let mut rng = rand::rng();
    let base: u16 = 20000 + (rng.random_range(0u16..1000) * 3);
    let port1: u16 = base;
    let port2: u16 = base + 1;
    let port3: u16 = base + 2;

    let mut node1 = start_node(
        "node1",
        port1,
        &[("node2", port2), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node2 = start_node(
        "node2",
        port2,
        &[("node1", port1), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node3 = start_node(
        "node3",
        port3,
        &[("node1", port1), ("node2", port2)],
        data_dir_str,
    )
    .await;

    // Give the cluster time to elect a leader and start serving gRPC.
    sleep(Duration::from_secs(3)).await;

    let addrs = [
        format!("127.0.0.1:{}", port1),
        format!("127.0.0.1:{}", port2),
        format!("127.0.0.1:{}", port3),
    ];
    let addr_slices: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();

    let leader_addr = find_leader(&addr_slices).await;

    // Write some data via the current leader.
    let mut leader_client = SqlClient::new(&leader_addr);
    leader_client
        .connect()
        .await
        .expect("leader SqlClient connect");

    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (1, 10.0);")
        .await
        .expect("insert 1");
    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (2, 20.0);")
        .await
        .expect("insert 2");

    let pre_failover_resp = leader_client
        .execute_sql("SELECT id, temperature FROM sensors;")
        .await
        .expect("select before failover");
    println!(
        "debug: leader {} rows before failover: {}",
        leader_addr,
        pre_failover_resp.rows.len()
    );

    // Kill the leader process to force a failover.
    if is_addr_for_port(&leader_addr, port1) {
        let _ = node1.kill();
    } else if is_addr_for_port(&leader_addr, port2) {
        let _ = node2.kill();
    } else if is_addr_for_port(&leader_addr, port3) {
        let _ = node3.kill();
    }

    // Allow time for a new leader election.
    sleep(Duration::from_secs(5)).await;

    // Find the new leader among the remaining nodes.
    let remaining: Vec<&str> = addr_slices
        .into_iter()
        .filter(|a| *a != leader_addr)
        .collect();

    let new_leader_addr = find_leader(&remaining).await;

    // Verify that the new leader can read back the data written before failover.
    let mut client_after = SqlClient::new(&new_leader_addr);
    client_after
        .connect()
        .await
        .expect("new leader SqlClient connect");

    let resp = client_after
        .execute_sql("SELECT id, temperature FROM sensors;")
        .await
        .expect("select after failover");

    for addr in &remaining {
        let mut client = SqlClient::new(addr);
        if let Err(e) = client.connect().await {
            println!("debug: connect to {} failed: {}", addr, e);
            continue;
        }
        match client
            .execute_sql("SELECT id, temperature FROM sensors;")
            .await
        {
            Ok(r) => {
                println!("debug: node {} rows after failover: {}", addr, r.rows.len());
            }
            Err(e) => {
                println!("debug: select on {} failed: {}", addr, e);
            }
        }
    }

    assert!(
        resp.success,
        "SELECT failed on new leader: success=false, error= {}",
        resp.error
    );
    assert!(
        resp.rows.len() >= 2,
        "expected at least 2 rows after failover, got {}",
        resp.rows.len()
    );

    // Best-effort cleanup: kill any remaining nodes.
    let _ = node1.kill();
    let _ = node2.kill();
    let _ = node3.kill();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_failover_mttr_under_60s() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("cluster");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut rng = rand::rng();
    let base: u16 = 24000 + (rng.random_range(0u16..1000) * 3);
    let port1: u16 = base;
    let port2: u16 = base + 1;
    let port3: u16 = base + 2;

    let mut node1 = start_node(
        "node1",
        port1,
        &[("node2", port2), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node2 = start_node(
        "node2",
        port2,
        &[("node1", port1), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node3 = start_node(
        "node3",
        port3,
        &[("node1", port1), ("node2", port2)],
        data_dir_str,
    )
    .await;

    sleep(Duration::from_secs(3)).await;

    let addrs = [
        format!("127.0.0.1:{}", port1),
        format!("127.0.0.1:{}", port2),
        format!("127.0.0.1:{}", port3),
    ];
    let addr_slices: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();

    let leader_addr = find_leader(&addr_slices).await;

    let mut leader_client = SqlClient::new(&leader_addr);
    leader_client
        .connect()
        .await
        .expect("leader SqlClient connect");
    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (42, 42.0);")
        .await
        .expect("initial insert before failover");

    let start = Instant::now();

    if is_addr_for_port(&leader_addr, port1) {
        let _ = node1.kill();
    } else if is_addr_for_port(&leader_addr, port2) {
        let _ = node2.kill();
    } else if is_addr_for_port(&leader_addr, port3) {
        let _ = node3.kill();
    }

    let remaining: Vec<&str> = addr_slices
        .iter()
        .copied()
        .filter(|a| *a != leader_addr)
        .collect();

    let new_leader_addr = find_leader(&remaining).await;

    let duration = start.elapsed();

    let mut client_after = SqlClient::new(&new_leader_addr);
    client_after
        .connect()
        .await
        .expect("new leader SqlClient connect");
    let resp = client_after
        .execute_sql("SELECT id, temperature FROM sensors;")
        .await
        .expect("select after failover for MTTR");

    assert!(
        resp.success,
        "SELECT failed on new leader after failover: success=false, error={}",
        resp.error
    );

    assert!(
        duration.as_secs_f64() < 60.0,
        "MTTR exceeded 60s: {:.3} seconds",
        duration.as_secs_f64()
    );

    let _ = node1.kill();
    let _ = node2.kill();
    let _ = node3.kill();
}

// Short soak + chaos harness: repeated writes with random follower restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_soak_and_chaos_smoke() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("cluster");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut rng = rand::rng();
    let base: u16 = 23000 + (rng.random_range(0u16..1000) * 3);
    let port1: u16 = base;
    let port2: u16 = base + 1;
    let port3: u16 = base + 2;

    let mut node1 = start_node(
        "node1",
        port1,
        &[("node2", port2), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node2 = start_node(
        "node2",
        port2,
        &[("node1", port1), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node3 = start_node(
        "node3",
        port3,
        &[("node1", port1), ("node2", port2)],
        data_dir_str,
    )
    .await;

    sleep(Duration::from_secs(3)).await;

    let addrs = [
        format!("127.0.0.1:{}", port1),
        format!("127.0.0.1:{}", port2),
        format!("127.0.0.1:{}", port3),
    ];
    let addr_slices: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();

    // Light-weight soak: 40 iterations of writes + occasional follower restart.
    for i in 0..40u32 {
        let leader_addr = find_leader(&addr_slices).await;

        let mut client = SqlClient::new(&leader_addr);
        if client.connect().await.is_err() {
            // If leader is temporarily unavailable, skip this iteration.
            sleep(Duration::from_millis(200)).await;
            continue;
        }

        let sql = format!(
            "INSERT INTO sensors (id, temperature) VALUES ({}, {});",
            i + 1,
            (i + 1) as f64 * 1.0
        );
        let _ = client.execute_sql(&sql).await;

        // Simple read sanity check on leader.
        let resp = client
            .execute_sql("SELECT id, temperature FROM sensors;")
            .await
            .expect("select on leader during soak");
        assert!(resp.success, "SELECT failed during soak: {}", resp.error);

        // Occasionally restart a follower while writes are ongoing.
        if i % 10 == 3 {
            let (follower_id, follower_port) = if !is_addr_for_port(&leader_addr, port1) {
                ("node1", port1)
            } else if !is_addr_for_port(&leader_addr, port2) {
                ("node2", port2)
            } else {
                ("node3", port3)
            };

            if follower_id == "node1" {
                let _ = node1.kill();
                node1 = start_node(
                    "node1",
                    follower_port,
                    &[("node2", port2), ("node3", port3)],
                    data_dir_str,
                )
                .await;
            } else if follower_id == "node2" {
                let _ = node2.kill();
                node2 = start_node(
                    "node2",
                    follower_port,
                    &[("node1", port1), ("node3", port3)],
                    data_dir_str,
                )
                .await;
            } else {
                let _ = node3.kill();
                node3 = start_node(
                    "node3",
                    follower_port,
                    &[("node1", port1), ("node2", port2)],
                    data_dir_str,
                )
                .await;
            }

            // Give the cluster a bit of time to settle after restart.
            sleep(Duration::from_secs(2)).await;
        }

        // Small delay between iterations to avoid hammering the cluster too hard.
        sleep(Duration::from_millis(200)).await;
    }

    // After the soak loop, ensure the cluster can still answer a SELECT via a leader.
    let final_leader = find_leader(&addr_slices).await;
    let mut client = SqlClient::new(&final_leader);
    client.connect().await.expect("final leader connect");
    let resp = client
        .execute_sql("SELECT id, temperature FROM sensors;")
        .await
        .expect("final select after soak");
    assert!(resp.success, "final SELECT failed: {}", resp.error);

    let _ = node1.kill();
    let _ = node2.kill();
    let _ = node3.kill();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_isolated_node_rejects_writes() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("cluster");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut rng = rand::rng();
    let base: u16 = 22000 + (rng.random_range(0u16..1000) * 3);
    let port1: u16 = base;
    let port2: u16 = base + 1;
    let port3: u16 = base + 2;

    let mut node1 = start_node(
        "node1",
        port1,
        &[("node2", port2), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node2 = start_node(
        "node2",
        port2,
        &[("node1", port1), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node3 = start_node(
        "node3",
        port3,
        &[("node1", port1), ("node2", port2)],
        data_dir_str,
    )
    .await;

    sleep(Duration::from_secs(3)).await;

    let addrs = [
        format!("127.0.0.1:{}", port1),
        format!("127.0.0.1:{}", port2),
        format!("127.0.0.1:{}", port3),
    ];
    let addr_slices: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();

    let leader_addr = find_leader(&addr_slices).await;

    let mut others: Vec<&str> = addr_slices
        .iter()
        .copied()
        .filter(|a| *a != leader_addr)
        .collect();
    others.sort();
    let isolated_addr = others[0];
    let killed_addr = others[1];

    if is_addr_for_port(&leader_addr, port1) {
        let _ = node1.kill();
    } else if is_addr_for_port(&leader_addr, port2) {
        let _ = node2.kill();
    } else {
        let _ = node3.kill();
    }

    if is_addr_for_port(killed_addr, port1) {
        let _ = node1.kill();
    } else if is_addr_for_port(killed_addr, port2) {
        let _ = node2.kill();
    } else {
        let _ = node3.kill();
    }

    sleep(Duration::from_secs(3)).await;

    let mut client = SqlClient::new(isolated_addr);
    client.connect().await.expect("isolated SqlClient connect");

    let resp = client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (99, 99.0);")
        .await
        .expect("insert on isolated node");

    assert!(
        !resp.success,
        "expected write on isolated node to fail, but success=true"
    );
    assert!(
        resp.error.to_lowercase().contains("not the leader"),
        "expected Not the leader error on isolated node, got: {}",
        resp.error
    );

    let _ = node1.kill();
    let _ = node2.kill();
    let _ = node3.kill();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_follower_restart_preserves_writes() {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("cluster");
    let data_dir_str = data_dir
        .to_str()
        .expect("data dir path should be valid UTF-8");

    let mut rng = rand::rng();
    let base: u16 = 21000 + (rng.random_range(0u16..1000) * 3);
    let port1: u16 = base;
    let port2: u16 = base + 1;
    let port3: u16 = base + 2;

    let mut node1 = start_node(
        "node1",
        port1,
        &[("node2", port2), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node2 = start_node(
        "node2",
        port2,
        &[("node1", port1), ("node3", port3)],
        data_dir_str,
    )
    .await;
    let mut node3 = start_node(
        "node3",
        port3,
        &[("node1", port1), ("node2", port2)],
        data_dir_str,
    )
    .await;

    sleep(Duration::from_secs(3)).await;

    let addrs = [
        format!("127.0.0.1:{}", port1),
        format!("127.0.0.1:{}", port2),
        format!("127.0.0.1:{}", port3),
    ];
    let addr_slices: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();

    let leader_addr = find_leader(&addr_slices).await;

    let mut leader_client = SqlClient::new(&leader_addr);
    leader_client
        .connect()
        .await
        .expect("leader SqlClient connect");

    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (1, 10.0);")
        .await
        .expect("insert 1");
    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (2, 20.0);")
        .await
        .expect("insert 2");

    let (follower_id, follower_port) = if !is_addr_for_port(&leader_addr, port1) {
        ("node1", port1)
    } else if !is_addr_for_port(&leader_addr, port2) {
        ("node2", port2)
    } else {
        ("node3", port3)
    };

    if follower_id == "node1" {
        let _ = node1.kill();
    } else if follower_id == "node2" {
        let _ = node2.kill();
    } else {
        let _ = node3.kill();
    }

    sleep(Duration::from_secs(1)).await;

    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (3, 30.0);")
        .await
        .expect("insert 3");
    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (4, 40.0);")
        .await
        .expect("insert 4");
    leader_client
        .execute_sql("INSERT INTO sensors (id, temperature) VALUES (5, 50.0);")
        .await
        .expect("insert 5");

    let restarted = if follower_id == "node1" {
        start_node(
            "node1",
            follower_port,
            &[("node2", port2), ("node3", port3)],
            data_dir_str,
        )
        .await
    } else if follower_id == "node2" {
        start_node(
            "node2",
            follower_port,
            &[("node1", port1), ("node3", port3)],
            data_dir_str,
        )
        .await
    } else {
        start_node(
            "node3",
            follower_port,
            &[("node1", port1), ("node2", port2)],
            data_dir_str,
        )
        .await
    };

    if follower_id == "node1" {
        node1 = restarted;
    } else if follower_id == "node2" {
        node2 = restarted;
    } else {
        node3 = restarted;
    }

    sleep(Duration::from_secs(5)).await;

    let new_leader_addr = find_leader(&addr_slices).await;

    let mut client_after = SqlClient::new(&new_leader_addr);
    client_after
        .connect()
        .await
        .expect("new leader SqlClient connect");

    let resp = client_after
        .execute_sql("SELECT id, temperature FROM sensors;")
        .await
        .expect("select after follower restart");

    assert!(
        resp.success,
        "SELECT failed on leader after follower restart: success=false, error={}",
        resp.error
    );
    assert!(
        resp.rows.len() >= 5,
        "expected at least 5 rows after follower restart, got {}",
        resp.rows.len()
    );

    let _ = node1.kill();
    let _ = node2.kill();
    let _ = node3.kill();
}
