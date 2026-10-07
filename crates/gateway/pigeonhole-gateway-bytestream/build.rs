fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .disable_comments(".")
        .compile_protos(
            &[
                "build/bazel/semver/semver.proto",
                "google/rpc/status.proto",
                "google/longrunning/operations.proto",
                "build/bazel/remote/execution/v2/remote_execution.proto",
                "google/bytestream/bytestream.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
