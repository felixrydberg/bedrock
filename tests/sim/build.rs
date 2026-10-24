// Compiles the shared test proto for `tests/determinism.rs` and `tests/transport.rs`.
fn main() {
    // SAFETY: build scripts are single-threaded.
    unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap()) };
    tonic_build::compile_protos("proto/sim.proto").unwrap();
}
