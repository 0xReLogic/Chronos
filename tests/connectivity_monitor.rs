use std::net::SocketAddr;
use std::sync::Arc;

use chronos::network::proto::health_service_server::HealthServiceServer;
use chronos::network::{ConnectivityMonitor, ConnectivityState, HealthServer};
use tokio::sync::RwLock;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn connectivity_check_once_returns_true_for_healthy_server() {
    // Start a HealthServer that always reports the current ConnectivityState.
    let state = Arc::new(RwLock::new(ConnectivityState::Connected));
    let server = HealthServer::new(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = TcpListenerStream::new(listener);

    let serve = tokio::spawn(async move {
        Server::builder()
            .add_service(HealthServiceServer::new(server))
            .serve_with_incoming(incoming)
            .await
            .expect("serve health");
    });

    // Give the server a brief moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let ok = ConnectivityMonitor::check_once(&addr.to_string()).await;
    assert!(ok, "expected ConnectivityMonitor to report healthy server");

    // We don't strictly need to shut the server down; aborting is fine for tests.
    serve.abort();
}

#[tokio::test]
async fn connectivity_check_once_returns_false_for_unreachable_target() {
    // Port 0 is never a valid remote port; connection should fail deterministically.
    let ok = ConnectivityMonitor::check_once("127.0.0.1:0").await;
    assert!(
        !ok,
        "expected ConnectivityMonitor to report unreachable target as false"
    );
}
