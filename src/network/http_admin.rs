use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};
use tokio::sync::Mutex;

use crate::network::metrics;
use crate::raft::{NodeRole, RaftNode};

pub async fn run_http_admin(
    addr: SocketAddr,
    node: Arc<Mutex<RaftNode>>,
    data_dir: String,
) -> Result<(), hyper::Error> {
    let node_arc = Arc::clone(&node);

    let make_svc = make_service_fn(move |_conn| {
        let node = Arc::clone(&node_arc);
        let data_dir = data_dir.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let node = Arc::clone(&node);
                let data_dir = data_dir.clone();
                async move { handle(req, node, data_dir).await }
            }))
        }
    });

    Server::bind(&addr).serve(make_svc).await
}

async fn handle(
    req: Request<Body>,
    node: Arc<Mutex<RaftNode>>,
    data_dir: String,
) -> Result<Response<Body>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let body = build_metrics(node, data_dir).await;
            Response::builder()
                .status(200)
                .header("Content-Type", "text/plain; version=0.0.4")
                .body(Body::from(body))
                .unwrap()
        }
        (&Method::GET, "/health") => {
            let body = build_health(node).await;
            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap()
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
