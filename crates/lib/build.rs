// Compiles the node-to-node protocols in `proto/`.
fn main() {
    println!("cargo:rerun-if-changed=proto/replication.proto");
    println!("cargo:rerun-if-changed=proto/volume.proto");
    // SAFETY: build scripts are single-threaded.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap()) };
    tonic_build::configure()
        .bytes([".bedrock.replication.Delivery.events"])
        // Ordered maps encode the same bytes on every run, which a replayed seed needs.
        .btree_map(["."])
        .compile_protos(
            &["proto/replication.proto", "proto/volume.proto"],
            &["proto"],
        )
        .unwrap();
}
