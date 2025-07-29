fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Phase 1: Single-node engine
    // No need for Protocol Buffers compilation yet
    
    tonic_build::compile_protos("proto/raft.proto")?;
    Ok(())
}