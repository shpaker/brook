use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace = manifest.join("../..").canonicalize()?;
    let proto_dir = workspace.join("proto");
    let proto_file = proto_dir.join("brook/v1/brook.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[&proto_file], &[&proto_dir])?;

    Ok(())
}
