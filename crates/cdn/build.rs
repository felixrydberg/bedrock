// Compiles the artifact transfer protocol in `proto/`.
fn main() {
    println!("cargo:rerun-if-changed=proto/artifact.proto");
    // SAFETY: build scripts are single-threaded.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap()) };
    tonic_build::configure()
        .bytes(["."])
        .compile_protos(&["proto/artifact.proto"], &["proto"])
        .unwrap();
}
