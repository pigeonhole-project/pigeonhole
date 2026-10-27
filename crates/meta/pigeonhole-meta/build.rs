fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    prost_build::compile_protos(&["pigeonhole/meta/v1/meta.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
