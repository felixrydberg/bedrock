// Compiles the node-to-node protocols in `proto/`.
fn main() {
    println!("cargo:rerun-if-changed=proto/replication.proto");
    // SAFETY: build scripts are single-threaded.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap()) };
    tonic_build::configure()
        .bytes([".bedrock.replication.Delivery.events"])
        .compile_protos(&["proto/replication.proto"], &["proto"])
        .unwrap();
}
