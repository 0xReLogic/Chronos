use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::HttpBody as _;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response};
use tokio::sync::{mpsc::UnboundedSender, Mutex, Semaphore};

use crate::network::{metrics, IngestRequest};
use crate::raft::{NodeRole, RaftNode};

pub async fn run_http_admin(
    addr: SocketAddr,
    node: Arc<Mutex<RaftNode>>,
    data_dir: String,
    ingest_tx: Option<UnboundedSender<IngestRequest>>,
) -> Result<(), hyper::Error> {
    let node_arc = Arc::clone(&node);

    let ingest_max_body_bytes = std::env::var("CHRONOS_INGEST_MAX_BODY_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(128 * 1024);
    let ingest_max_inflight = std::env::var("CHRONOS_INGEST_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(64);
    let ingest_semaphore = Arc::new(Semaphore::new(ingest_max_inflight));

    let make_svc = make_service_fn(move |_conn| {
        let node = Arc::clone(&node_arc);
        let data_dir = data_dir.clone();
        let ingest_tx = ingest_tx.clone();
        let ingest_semaphore = Arc::clone(&ingest_semaphore);
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let node = Arc::clone(&node);
                let data_dir = data_dir.clone();
                let ingest_tx = ingest_tx.clone();
                let ingest_semaphore = Arc::clone(&ingest_semaphore);
                async move {
                    handle(
                        req,
                        node,
                        data_dir,
                        ingest_tx,
                        ingest_max_body_bytes,
                        ingest_semaphore,
                    )
                    .await
                }
            }))
        }
    });

    hyper::Server::bind(&addr).serve(make_svc).await
}

fn json_error(status: u16, msg: &str) -> Response<Body> {
    let body = format!("{{\"error\":\"{}\"}}", msg);
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn authenticate_http(req: &Request<Body>, allow_readonly: bool) -> Option<Response<Body>> {
    let admin_token = std::env::var("CHRONOS_AUTH_TOKEN_ADMIN").ok();
    let readonly_token = std::env::var("CHRONOS_AUTH_TOKEN_READONLY").ok();
    let require_auth = admin_token.is_some() || readonly_token.is_some();

    if !require_auth {
        return None;
    }

    let raw = match req.headers().get(header::AUTHORIZATION) {
        Some(v) => v,
        None => return Some(json_error(401, "missing authorization header")),
    };
    let raw_str = match raw.to_str() {
        Ok(v) => v,
        Err(_) => return Some(json_error(401, "invalid authorization header")),
    };

    const PREFIX: &str = "Bearer ";
    if !raw_str.starts_with(PREFIX) {
        return Some(json_error(401, "invalid authorization scheme"));
    }
    let token = &raw_str[PREFIX.len()..];

    if let Some(admin) = admin_token.as_deref() {
        if token == admin {
            return None;
        }
    }

    if let Some(ro) = readonly_token.as_deref() {
        if token == ro {
            if allow_readonly {
                return None;
            }
            return Some(json_error(
                403,
                "write operations are not allowed for read-only role",
            ));
        }
    }

    Some(json_error(401, "invalid token"))
}

enum ReadBodyLimitedError {
    TooLarge,
    Hyper(hyper::Error),
}

impl From<hyper::Error> for ReadBodyLimitedError {
    fn from(e: hyper::Error) -> Self {
        Self::Hyper(e)
    }
}

async fn read_body_limited(
    mut body: Body,
    max_bytes: usize,
) -> Result<Vec<u8>, ReadBodyLimitedError> {
    let mut out = Vec::new();
    while let Some(next) = body.data().await {
        let chunk = next?;
        if out.len() + chunk.len() > max_bytes {
            return Err(ReadBodyLimitedError::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

async fn handle(
    req: Request<Body>,
    node: Arc<Mutex<RaftNode>>,
    data_dir: String,
    ingest_tx: Option<UnboundedSender<IngestRequest>>,
    ingest_max_body_bytes: usize,
    ingest_semaphore: Arc<Semaphore>,
) -> Result<Response<Body>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let allow_readonly = method == Method::GET && (path == "/metrics" || path == "/health");
    let is_ingest = method == Method::POST && path == "/ingest";
    if allow_readonly || is_ingest {
        if let Some(resp) = authenticate_http(&req, allow_readonly) {
            return Ok(resp);
        }
    }

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
                match ingest_semaphore.clone().try_acquire_owned() {
                    Ok(_permit) => {
                        let too_large = req
                            .headers()
                            .get(header::CONTENT_LENGTH)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<usize>().ok())
                            .map(|len| len > ingest_max_body_bytes)
                            .unwrap_or(false);

                        if too_large {
                            json_error(413, "payload too large")
                        } else {
                            match read_body_limited(req.into_body(), ingest_max_body_bytes).await {
                                Ok(bytes) => {
                                    match serde_json::from_slice::<IngestRequest>(&bytes) {
                                        Ok(msg) => {
                                            if let Err(e) = tx.send(msg) {
                                                let body = format!(
                                                "{{\"error\":\"ingest queue unavailable: {}\"}}",
                                                e
                                            );
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
                                            let body =
                                                format!("{{\"error\":\"invalid JSON: {}\"}}", e);
                                            Response::builder()
                                                .status(400)
                                                .header("Content-Type", "application/json")
                                                .body(Body::from(body))
                                                .unwrap()
                                        }
                                    }
                                }
                                Err(ReadBodyLimitedError::TooLarge) => {
                                    json_error(413, "payload too large")
                                }
                                Err(ReadBodyLimitedError::Hyper(e)) => {
                                    let body = format!(
                                        "{{\"error\":\"failed to read request body: {}\"}}",
                                        e
                                    );
                                    Response::builder()
                                        .status(400)
                                        .header("Content-Type", "application/json")
                                        .body(Body::from(body))
                                        .unwrap()
                                }
                            }
                        }
                    }
                    Err(_) => json_error(429, "ingest rate limit exceeded"),
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
    use crate::network::TEST_ENV_LOCK;
    use crate::raft::RaftConfig;
    use crate::raft::RaftNode;
    use hyper::body::Bytes;
    use hyper::Request;
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

    #[test]
    fn ingest_rejects_payload_too_large() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("CHRONOS_AUTH_TOKEN_ADMIN");
            std::env::remove_var("CHRONOS_AUTH_TOKEN_READONLY");
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let tmp_dir = tempdir().expect("tempdir");
            let data_dir = tmp_dir.path().to_string_lossy().to_string();
            let config = RaftConfig::new("node1", &data_dir);
            let node = Arc::new(Mutex::new(RaftNode::new(config)));

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<IngestRequest>();

            let mut big_metrics = String::new();
            for i in 0..2048 {
                if i > 0 {
                    big_metrics.push(',');
                }
                big_metrics.push_str(&format!("\"m{}\":1.0", i));
            }
            let body = format!(
                "{{\"device_id\":\"esp-001\",\"ts\":1,\"seq\":1,\"metrics\":{{{}}}}}",
                big_metrics
            );

            let req = Request::builder()
                .method(Method::POST)
                .uri("/ingest")
                .header(header::CONTENT_LENGTH, body.len())
                .body(Body::from(body))
                .unwrap();

            let resp = handle(
                req,
                node,
                data_dir,
                Some(tx),
                128,
                Arc::new(Semaphore::new(64)),
            )
            .await
            .expect("handle ok");

            assert_eq!(resp.status(), 413);
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn ingest_returns_429_when_inflight_limit_exceeded() {
        let _lock = TEST_ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("CHRONOS_AUTH_TOKEN_ADMIN");
            std::env::remove_var("CHRONOS_AUTH_TOKEN_READONLY");
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let tmp_dir = tempdir().expect("tempdir");
            let data_dir = tmp_dir.path().to_string_lossy().to_string();
            let config = RaftConfig::new("node1", &data_dir);
            let node = Arc::new(Mutex::new(RaftNode::new(config)));

            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<IngestRequest>();
            let sem = Arc::new(Semaphore::new(1));
            let _permit = sem.clone().acquire_owned().await.expect("permit");

            let body = Bytes::from_static(
                b"{\"device_id\":\"esp-001\",\"ts\":1,\"seq\":1,\"metrics\":{\"temp\":30.5}}",
            );
            let req = Request::builder()
                .method(Method::POST)
                .uri("/ingest")
                .body(Body::from(body))
                .unwrap();

            let resp = handle(req, node, data_dir, Some(tx), 128 * 1024, sem)
                .await
                .expect("handle ok");
            assert_eq!(resp.status(), 429);
        });
    }
}
