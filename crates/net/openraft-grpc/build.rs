fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only gRPC itself needs the generated code.
    if std::env::var_os("CARGO_FEATURE_TRANSPORT").is_none() {
        return Ok(());
    }
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_build::configure().build_server(true).build_client(true).compile_protos(&["proto/raft.proto"], &["proto"])?;
    Ok(())
}
