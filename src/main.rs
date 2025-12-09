use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tonic::transport::{Certificate as TlsCertificate, Identity as TlsIdentity, ServerTlsConfig};

use chronos::network::{SharedSyncStatus, SyncStatusServer, SyncStatusState, SyncWorker};
use chronos::storage::snapshot::{create_snapshot, restore_snapshot};
use chronos::Executor;
use log::{error, info};
use tracing_log::LogTracer;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use chronos::repl::Repl;

use chronos::raft::{Raft, RaftConfig};

struct RotatingFile {
    path: String,
    max_size: u64,
    max_files: u32,
    file: File,
    current_size: u64,
}

#[derive(Subcommand)]
enum AdminCmd {
    /// Show node health from the HTTP admin endpoint (host:port of admin HTTP)
    Status {
        /// Admin HTTP address, e.g. 127.0.0.1:9000
        #[arg(short, long)]
        http: String,
    },
    /// Dump Prometheus metrics from the HTTP admin endpoint
    Metrics {
        /// Admin HTTP address, e.g. 127.0.0.1:9000
        #[arg(short, long)]
        http: String,
    },
}

fn http_get(host_port: &str, path: &str) -> Result<String, Box<dyn std::error::Error>> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let mut stream = TcpStream::connect(host_port)?;
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, host_port
    );
    stream.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let resp = String::from_utf8_lossy(&buf);
    if let Some(pos) = resp.find("\r\n\r\n") {
        Ok(resp[pos + 4..].to_string())
    } else {
        Ok(resp.to_string())
    }
}

fn init_logging() {
    let _ = LogTracer::init();

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_timer(fmt::time::UtcTime::rfc_3339());

    if std::env::var("CHRONOS_LOG_FILE").is_ok() {
        // When CHRONOS_LOG_FILE is set, we keep using env_logger + RotatingFile
        // to write plain logs to disk, while tracing collects structured logs
        // to stderr by default.
        let mut builder =
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));

        if let Ok(path) = std::env::var("CHRONOS_LOG_FILE") {
            let max_size_mb = std::env::var("CHRONOS_LOG_MAX_SIZE_MB")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(10);
            let max_files = std::env::var("CHRONOS_LOG_MAX_FILES")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(3);

            if let Ok(rot) = RotatingFile::new(path.clone(), max_size_mb * 1024 * 1024, max_files) {
                builder.target(env_logger::Target::Pipe(Box::new(rot)));
            }
        }

        builder.init();
    }

    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init();
}

impl RotatingFile {
    fn new(path: String, max_size: u64, max_files: u32) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path,
            max_size,
            max_files,
            file,
            current_size,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        // Close current file by dropping and reopening later
        // Rotate existing files: path.N-1 -> path.N, ..., path.1 -> path.2, path -> path.1
        for i in (1..self.max_files).rev() {
            let src = format!("{}.{}", self.path, i);
            let dst = format!("{}.{}", self.path, i + 1);
            let _ = std::fs::rename(&src, &dst);
        }

        let _ = std::fs::rename(&self.path, format!("{}.1", self.path));

        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.current_size = 0;
        Ok(())
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_size + buf.len() as u64 > self.max_size {
            self.rotate()?;
        }
        let n = self.file.write(buf)?;
        self.current_size += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[derive(Parser)]
#[command(name = "chronos")]
#[command(about = "A distributed SQL database built from scratch in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start a single-node database instance
    SingleNode {
        /// Directory to store data
        #[arg(short, long, default_value = "data")]
        data_dir: String,
    },

    /// Start a node in a distributed cluster
    Node {
        /// Unique ID for this node
        #[arg(short, long)]
        id: String,

        /// Directory to store data
        #[arg(short, long, default_value = "data")]
        data_dir: String,

        /// Address to listen on
        #[arg(short, long, default_value = "127.0.0.1:8000")]
        address: String,

        /// Comma-separated list of peer addresses (id=address)
        #[arg(short, long)]
        peers: Option<String>,

        /// Clean the data directory before starting
        #[arg(long)]
        clean: bool,

        /// Optional sync target URL (e.g. http://host:port) for edge nodes
        #[arg(long)]
        sync_target: Option<String>,

        /// Interval in seconds between sync attempts
        #[arg(long, default_value_t = 5)]
        sync_interval_secs: u64,

        /// Maximum number of operations per sync batch
        #[arg(long, default_value_t = 100)]
        sync_batch_size: usize,

        /// Interval in seconds between TTL cleanup passes
        #[arg(long, default_value_t = 3600)]
        ttl_cleanup_interval_secs: u64,
    },

    /// Start the Chronos client REPL
    Client {
        /// Address of the leader node for distributed mode. If not provided, runs in local mode.
        #[arg(short, long)]
        leader: Option<String>,

        /// Directory to store data in local mode
        #[arg(short, long, default_value = "data")]
        data_dir: String,
    },

    /// Snapshot management (backup/restore)
    Snapshot {
        #[command(subcommand)]
        cmd: SnapshotCmd,
    },

    /// Admin tooling
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Subcommand)]
enum SnapshotCmd {
    /// Create a snapshot of the node data directory
    Create {
        /// Directory to snapshot (e.g., data/node-1)
        #[arg(short, long, default_value = "data")]
        data_dir: String,
        /// Output snapshot file path
        #[arg(short, long)]
        output: String,
    },
    /// Restore a node data directory from a snapshot
    Restore {
        /// Directory to restore into (will be created). Refuses if not empty unless --force
        #[arg(short, long, default_value = "data")]
        data_dir: String,
        /// Input snapshot file path
        #[arg(short, long)]
        input: String,
        /// Overwrite non-empty directory
        #[arg(long, default_value_t = false)]
        force: bool,
    },
}

fn load_server_tls_from_env() -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    let cert_path = match std::env::var("CHRONOS_TLS_CERT") {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let key_path = std::env::var("CHRONOS_TLS_KEY")?;
    let ca_path = std::env::var("CHRONOS_TLS_CA_CERT").ok();

    let cert = std::fs::read(cert_path)?;
    let key = std::fs::read(key_path)?;
    let identity = TlsIdentity::from_pem(cert, key);

    let mut config = ServerTlsConfig::new().identity(identity);

    if let Some(ca_path) = ca_path {
        let ca_bytes = std::fs::read(ca_path)?;
        let ca = TlsCertificate::from_pem(ca_bytes);
        config = config.client_ca_root(ca);
    }

    Ok(Some(config))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    // Parse command line arguments
    let cli = Cli::parse();

    match cli.command {
        Command::SingleNode { data_dir } => {
            info!("Starting Chronos in single-node mode");

            // Create data directory if it doesn't exist
            let data_path = Path::new(&data_dir);
            if !data_path.exists() {
                std::fs::create_dir_all(data_path)?;
            }

            // Start REPL
            let mut repl = Repl::new(&data_dir).await;
            repl.run().await;
        }
        Command::Client { leader, data_dir } => {
            match leader {
                Some(leader_address) => {
                    info!("Starting Chronos client connecting to leader at {leader_address}");
                    let mut repl = Repl::with_distributed_mode(&leader_address);
                    repl.run().await;
                }
                None => {
                    info!("Starting Chronos client in local mode");

                    // Create data directory if it doesn't exist
                    let data_path = Path::new(&data_dir);
                    if !data_path.exists() {
                        std::fs::create_dir_all(data_path)?;
                    }

                    let mut repl = Repl::new(&data_dir).await;
                    repl.run().await;
                }
            }
        }
        Command::Snapshot { cmd } => match cmd {
            SnapshotCmd::Create { data_dir, output } => {
                info!("Creating snapshot from '{data_dir}' to '{output}'...");
                create_snapshot(&data_dir, &output)?;
                info!("Snapshot created: {output}");
            }
            SnapshotCmd::Restore {
                data_dir,
                input,
                force,
            } => {
                info!("Restoring snapshot '{input}' into '{data_dir}' (force={force})...");
                restore_snapshot(&data_dir, &input, force)?;
                info!("Snapshot restored into {data_dir}");
            }
        },
        Command::Admin { cmd } => match cmd {
            AdminCmd::Status { http } => {
                let body = http_get(&http, "/health")?;
                println!("{}", body);
            }
            AdminCmd::Metrics { http } => {
                let body = http_get(&http, "/metrics")?;
                println!("{}", body);
            }
        },
        Command::Node {
            id,
            data_dir,
            address,
            peers,
            clean,
            sync_target,
            sync_interval_secs,
            sync_batch_size,
            ttl_cleanup_interval_secs,
        } => {
            info!("Starting Chronos node {id} at {address}");

            let node_data_dir = format!("{data_dir}/{id}");

            if clean {
                info!(
                    "--clean flag detected, removing data directory: {}",
                    &node_data_dir
                );
                let node_data_path = Path::new(&node_data_dir);
                if node_data_path.exists() {
                    std::fs::remove_dir_all(node_data_path)?;
                }
            }

            // Create cluster data directory if it doesn't exist
            let data_path = Path::new(&data_dir);
            if !data_path.exists() {
                std::fs::create_dir_all(data_path)?;
            }

            // Create data directory for this node
            std::fs::create_dir_all(&node_data_dir)?;

            // Configure Raft for this node (including peers)
            let mut config = RaftConfig::new(&id, &node_data_dir);

            // Add peers
            if let Some(peers_str) = peers {
                for peer in peers_str.split(',') {
                    let parts: Vec<&str> = peer.split('=').collect();
                    if parts.len() == 2 {
                        let peer_id = parts[0];
                        let peer_addr = parts[1];

                        if peer_id != id {
                            config.add_peer(peer_id, peer_addr);
                            info!("Added peer: {peer_id} at {peer_addr}");
                        }
                    }
                }
            }

            // Create executor
            let executor = Arc::new(Mutex::new(Executor::new(&node_data_dir).await));

            // Shared sync status (used by SyncWorker and gRPC)
            let sync_status: SharedSyncStatus = Arc::new(Mutex::new(SyncStatusState::default()));

            // Start Raft with the configured peers and per-node data dir
            let raft = Raft::new(config);

            {
                let weak_node = Arc::downgrade(&raft.node);
                let mut exec = executor.lock().await;
                exec.set_raft_node(weak_node);
            }
            let (apply_tx, mut apply_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            {
                let mut node_lock = raft.node.lock().await;
                node_lock.set_apply_sender(apply_tx);
            }

            {
                let executor_for_apply = Arc::clone(&executor);
                tokio::spawn(async move {
                    while let Some(cmd) = apply_rx.recv().await {
                        let mut exec = executor_for_apply.lock().await;
                        if let Err(e) = exec.apply_command(&cmd).await {
                            error!("Raft apply worker error: {}", e);
                        }
                    }
                });
            }

            let _node = Arc::clone(&raft.node);
            raft.start().await?;

            // Connectivity monitor for this node's SQL endpoint
            let monitor = chronos::network::ConnectivityMonitor::new(
                &address,
                std::time::Duration::from_secs(3),
            );
            let connectivity_state = monitor.state();
            tokio::spawn(monitor.run());

            // Start gRPC server
            let addr = address.parse()?;
            let raft_server = chronos::network::RaftServer::new(Arc::clone(&raft.node));
            let sql_server =
                chronos::network::SqlServer::new(Arc::clone(&raft.node), Arc::clone(&executor));
            let health_server =
                chronos::network::HealthServer::new(Arc::clone(&connectivity_state));
            let sync_server = chronos::network::SyncServer::new(Arc::clone(&executor));
            let sync_status_server = SyncStatusServer::new(Arc::clone(&sync_status));

            // If this node is configured as an edge node with a sync target,
            // start a background SyncWorker that drains the persistent
            // offline queue via the Executor and pushes operations to the
            // target.
            if let Some(target) = sync_target {
                let worker = SyncWorker::new(
                    Arc::clone(&executor),
                    target,
                    std::time::Duration::from_secs(sync_interval_secs),
                    sync_batch_size,
                    Arc::clone(&sync_status),
                    id.clone(),
                );
                tokio::spawn(worker.run());
            }

            // Start a background TTL cleanup task that periodically removes
            // expired rows based on table TTL metadata. This runs on every
            // node independently and is best-effort: errors are logged but
            // do not terminate the node.
            {
                let executor_clone = Arc::clone(&executor);
                tokio::spawn(async move {
                    let limit: usize = 1000;

                    loop {
                        let now_secs = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        {
                            // Restrict the lifetime of the executor lock to just the
                            // cleanup_expired call so other tasks (like SQL handling)
                            // are not blocked for the entire sleep interval.
                            let mut exec = executor_clone.lock().await;
                            match exec.cleanup_expired(now_secs, limit).await {
                                Ok(cleaned) => {
                                    if cleaned > 0 {
                                        log::info!(
                                            "TTL cleanup removed {} expired rows (now_secs={})",
                                            cleaned,
                                            now_secs
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!("TTL cleanup error: {}", e);
                                }
                            }
                        }

                        sleep(Duration::from_secs(ttl_cleanup_interval_secs)).await;
                    }
                });
            }

            let http_addr = {
                let mut s: std::net::SocketAddr = addr;
                let port = s.port().saturating_add(1000);
                s.set_port(port);
                s
            };

            {
                let http_node = Arc::clone(&raft.node);
                let http_data_dir = node_data_dir.clone();
                tokio::spawn(async move {
                    if let Err(e) = chronos::network::http_admin::run_http_admin(
                        http_addr,
                        http_node,
                        http_data_dir,
                    )
                    .await
                    {
                        eprintln!("HTTP admin server error: {}", e);
                    }
                });
            }

            let mut server_builder = tonic::transport::Server::builder();

            if let Some(tls_config) = load_server_tls_from_env()? {
                server_builder = server_builder.tls_config(tls_config)?;
            }

            info!("gRPC server listening on {addr}");
            server_builder
                .add_service(chronos::network::proto::raft_service_server::RaftServiceServer::new(raft_server))
                .add_service(chronos::network::proto::sql_service_server::SqlServiceServer::new(sql_server))
                .add_service(chronos::network::proto::health_service_server::HealthServiceServer::new(health_server))
                .add_service(chronos::network::proto::sync_service_server::SyncServiceServer::new(sync_server))
                .add_service(chronos::network::proto::sync_status_service_server::SyncStatusServiceServer::new(sync_status_server))
                .serve(addr)
                .await?;
        }
    }

    Ok(())
}
