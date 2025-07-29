use thiserror::Error;
use tonic::Status;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("gRPC error: {0}")]
    GrpcError(#[from] Status),
    
    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Timeout error")]
    TimeoutError,
}