use std::process::{Child, Command, Stdio};
use std::time::Duration;

use chronos::network::SqlClient;
use rand::Rng;
use tempfile::TempDir;
use tokio::time::sleep;

const BIN: &str = env!("CARGO_BIN_EXE_chronos");

async fn start_node(
    id: &str,
    port: u16,
    peers: &[(&str, u16)],
    data_dir: &str,
) -> Child {
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
    let mut rng = rand::thread_rng();
    let base: u16 = 20000 + (rng.gen_range(0u16..1000) * 3);
    let port1: u16 = base;
    let port2: u16 = base + 1;
    let port3: u16 = base + 2;

    let mut node1 = start_node("node1", port1, &[("node2", port2), ("node3", port3)], data_dir_str).await;
    let mut node2 = start_node("node2", port2, &[("node1", port1), ("node3", port3)], data_dir_str).await;
    let mut node3 = start_node("node3", port3, &[("node1", port1), ("node2", port2)], data_dir_str).await;

    // Give the cluster time to elect a leader and start serving gRPC.
    sleep(Duration::from_secs(3)).await;

    let addrs = vec![
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

    assert!(
        resp.success,
        "SELECT failed on new leader: success=false, error={}",
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
