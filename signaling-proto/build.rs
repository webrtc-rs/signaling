fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut v2_config = prost_build::Config::new();
    v2_config.protoc_executable(protoc);
    tonic_prost_build::configure().compile_with_config(
        v2_config,
        &["proto/signaling.v2.proto"],
        &["proto"],
    )?;

    println!("cargo:rerun-if-changed=proto/signaling.v2.proto");
    Ok(())
}
