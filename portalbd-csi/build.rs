fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let proto_path = format!("{}/proto/csi.proto", manifest_dir);
    let proto_dir = format!("{}/proto", manifest_dir);
    let descriptor_path = format!("{}/csi_descriptor.bin", out_dir);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&[proto_path], &[proto_dir])?;
    Ok(())
}
