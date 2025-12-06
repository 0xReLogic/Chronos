use std::path::Path;
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tokio::sync::Mutex;
use chronos::Executor;
use env_logger::Env;
use log::info;

use chronos::repl::Repl;

use chronos::raft::{Raft, RaftConfig};

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    
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
        },
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
        },
        Command::Node { id, data_dir, address, peers, clean } => {
            info!("Starting Chronos node {id} at {address}");

            let node_data_dir = format!("{data_dir}/{id}");

            if clean {
                info!("--clean flag detected, removing data directory: {}", &node_data_dir);
                let node_data_path = Path::new(&node_data_dir);
                if node_data_path.exists() {
                    std::fs::remove_dir_all(node_data_path)?;
                }
            }
            
            // Create data directory if it doesn't exist
            let data_path = Path::new(&data_dir);
            if !data_path.exists() {
                std::fs::create_dir_all(data_path)?;
            }
            
            // Configure Raft
            let mut config = RaftConfig::new(&id, &data_dir);
            
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
            
            // Create data directory for this node
            let node_data_dir = format!("{data_dir}/{id}");
            std::fs::create_dir_all(&node_data_dir)?;
            
            // Create executor
            let executor = Arc::new(Mutex::new(Executor::new(&node_data_dir).await));
            
            // Start Raft
            let raft_config = RaftConfig::new(&id, &node_data_dir);
            let raft = Raft::new(raft_config);
            let _node = Arc::clone(&raft.node);
            raft.start().await?;
            
            // Start gRPC server
            let addr = address.parse()?;
            let raft_server = chronos::network::RaftServer::new(Arc::clone(&raft.node));
            let sql_server = chronos::network::SqlServer::new(Arc::clone(&raft.node), Arc::clone(&executor));

            info!("gRPC server listening on {addr}");
            tonic::transport::Server::builder()
                .add_service(chronos::network::proto::raft_service_server::RaftServiceServer::new(raft_server))
                .add_service(chronos::network::proto::sql_service_server::SqlServiceServer::new(sql_server))
                .serve(addr)
                .await?;
        },
    }
    
    Ok(())
}
