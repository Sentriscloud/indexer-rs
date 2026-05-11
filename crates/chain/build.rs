//! Build-time codegen for the `sentrix.v1` gRPC schema.
//!
//! Inputs: `proto/sentrix.proto` (canonical copy of the chain's gRPC schema —
//! pulled from `sentrix-labs/sentrix/crates/sentrix-grpc/proto/sentrix.proto`
//! and kept in sync manually; CI guards against drift in a later phase).
//!
//! Output: client-only Rust bindings under `OUT_DIR/sentrix.v1.rs`,
//! re-exported from `crate::pb`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/sentrix.proto");

    // Older apt-installed protoc (Ubuntu 22.04 ships 3.12) treats proto3
    // `optional` as experimental and rejects without the explicit flag.
    // The proto declares `optional BlockHeight at_height = 2` so this is
    // load-bearing on bullseye/jammy CI runners.
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_with_config(config, &["proto/sentrix.proto"], &["proto"])?;
    Ok(())
}
