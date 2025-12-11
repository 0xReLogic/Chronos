use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{body, Body, Method, Request, Response};
use tokio::sync::{mpsc::UnboundedSender, Mutex};

use crate::network::{metrics, IngestRequest};
use crate::raft::{NodeRole, RaftNode};

pub async fn run_http_admin(
    addr: SocketAddr,
    node: Arc<Mutex<RaftNode>>,
    data_dir: String,
    ingest_tx: Option<UnboundedSender<IngestRequest>>,
) -> Result<(), hyper::Error> {
    let node_arc = Arc::clone(&node);

    let make_svc = make_service_fn(move |_conn| {
        let node = Arc::clone(&node_arc);
        let data_dir = data_dir.clone();
        let ingest_tx = ingest_tx.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let node = Arc::clone(&node);
                let data_dir = data_dir.clone();
                let ingest_tx = ingest_tx.clone();
                async move { handle(req, node, data_dir, ingest_tx).await }
            }))
        }
    });

    hyper::Server::bind(&addr).serve(make_svc).await
}

async fn handle(
    req: Request<Body>,
    node: Arc<Mutex<RaftNode>>,
    data_dir: String,
    ingest_tx: Option<UnboundedSender<IngestRequest>>,
) -> Result<Response<Body>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let response = match (method, path.as_str()) {
        (Method::GET, "/metrics") => {
            let body = build_metrics(node, data_dir).await;
            Response::builder()
                .status(200)
                .header("Content-Type", "text/plain; version=0.0.4")
                .body(Body::from(body))
                .unwrap()
        }
        (Method::GET, "/health") => {
            let body = build_health(node).await;
            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap()
        }
        (Method::POST, "/ingest") => {
            if let Some(tx) = ingest_tx.clone() {
                match body::to_bytes(req.into_body()).await {
                    Ok(bytes) => match serde_json::from_slice::<IngestRequest>(&bytes) {
                        Ok(msg) => {
                            if let Err(e) = tx.send(msg) {
                                let body =
                                    format!("{{\"error\":\"ingest queue unavailable: {}\"}}", e);
                                Response::builder()
                                    .status(503)
                                    .header("Content-Type", "application/json")
                                    .body(Body::from(body))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(202)
                                    .header("Content-Type", "application/json")
                                    .body(Body::from("{\"status\":\"queued\"}"))
                                    .unwrap()
                            }
                        }
                        Err(e) => {
                            let body = format!("{{\"error\":\"invalid JSON: {}\"}}", e);
                            Response::builder()
                                .status(400)
                                .header("Content-Type", "application/json")
                                .body(Body::from(body))
                                .unwrap()
                        }
                    },
                    Err(e) => {
                        let body = format!("{{\"error\":\"failed to read request body: {}\"}}", e);
                        Response::builder()
                            .status(400)
                            .header("Content-Type", "application/json")
                            .body(Body::from(body))
                            .unwrap()
                    }
                }
            } else {
                Response::builder()
                    .status(503)
                    .header("Content-Type", "application/json")
                    .body(Body::from("{\"error\":\"ingest disabled\"}"))
                    .unwrap()
            }
        }
        _ => Response::builder().status(404).body(Body::empty()).unwrap(),
    };

    Ok(response)
}

async fn build_metrics(node: Arc<Mutex<RaftNode>>, data_dir: String) -> String {
    let (reads, writes) = metrics::snapshot_sql();

    let (role_value, term) = {
        let node_guard = node.lock().await;
        let role = match node_guard.state().role {
            NodeRole::Follower => 0,
            NodeRole::Candidate => 1,
            NodeRole::Leader => 2,
        };
        (role, node_guard.state().current_term)
    };

    let storage_size = tokio::task::spawn_blocking(move || dir_size(&data_dir))
        .await
        .unwrap_or(0);

    format!(
        concat!(
            "# TYPE chronos_sql_requests_total counter\n",
            "chronos_sql_requests_total{{kind=\"read\"}} {}\n",
            "chronos_sql_requests_total{{kind=\"write\"}} {}\n",
            "# TYPE chronos_raft_term gauge\n",
            "chronos_raft_term {}\n",
            "# TYPE chronos_raft_role gauge\n",
            "chronos_raft_role {}\n",
            "# TYPE chronos_storage_size_bytes gauge\n",
            "chronos_storage_size_bytes {}\n",
        ),
        reads, writes, term, role_value, storage_size,
    )
}

async fn build_health(node: Arc<Mutex<RaftNode>>) -> String {
    let (role, term) = {
        let node_guard = node.lock().await;
        let role_str = match node_guard.state().role {
            NodeRole::Follower => "Follower",
            NodeRole::Candidate => "Candidate",
            NodeRole::Leader => "Leader",
        };
        (role_str.to_string(), node_guard.state().current_term)
    };

    format!(
        "{{\"status\":\"ok\",\"role\":\"{}\",\"term\":{}}}",
        role, term
    )
}

fn dir_size(path: &str) -> u64 {
    use std::fs;
    use std::path::Path;

    fn walk(path: &Path, acc: &mut u64) {
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        *acc += meta.len();
                    } else if meta.is_dir() {
                        walk(&p, acc);
                    }
                }
            }
        }
    }

    let mut total = 0u64;
    walk(Path::new(path), &mut total);
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftConfig;
    use crate::raft::RaftNode;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn health_reports_ok_with_initial_follower_state() {
        let tmp_dir = tempdir().expect("tempdir");
        let data_dir = tmp_dir.path().to_string_lossy().to_string();

        let config = RaftConfig::new("node1", &data_dir);
        let node = Arc::new(Mutex::new(RaftNode::new(config)));

        let body = build_health(Arc::clone(&node)).await;

        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"role\":\"Follower\""));
        assert!(body.contains("\"term\":0"));
    }

    #[tokio::test]
    async fn metrics_exports_expected_keys() {
        let tmp_dir = tempdir().expect("tempdir");
        let data_dir = tmp_dir.path().to_string_lossy().to_string();

        let config = RaftConfig::new("node1", &data_dir);
        let node = Arc::new(Mutex::new(RaftNode::new(config)));

        // Record some SQL activity to touch the metrics counters.
        metrics::record_sql(true);
        metrics::record_sql(false);

        let body = build_metrics(Arc::clone(&node), data_dir).await;

        // Check that the main Prometheus metrics keys are present. We don't assert on
        // exact numeric values to keep the test robust across runs.
        assert!(body.contains("chronos_sql_requests_total{kind=\"read\"}"));
        assert!(body.contains("chronos_sql_requests_total{kind=\"write\"}"));
        assert!(body.contains("chronos_raft_term"));
        assert!(body.contains("chronos_raft_role"));
        assert!(body.contains("chronos_storage_size_bytes"));
    }
}
