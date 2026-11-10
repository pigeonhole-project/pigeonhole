fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    // Paths from the package root, not from the include dir: tonic-build
    // tells cargo to rerun when these paths change, and cargo reads them from
    // the package root, so `build/...` was a missing file, which made the
    // crate and all that depends on it rebuild on every cargo call.
    tonic_build::configure().build_server(true).build_client(true).disable_comments(".").compile_protos(
        &[
            "proto/build/bazel/semver/semver.proto",
            "proto/google/rpc/status.proto",
            "proto/google/longrunning/operations.proto",
            "proto/build/bazel/remote/execution/v2/remote_execution.proto",
            "proto/google/bytestream/bytestream.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
