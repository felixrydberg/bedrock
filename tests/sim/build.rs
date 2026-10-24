// Compiles the B-23a spike proto for `tests/determinism.rs`; normal builds skip it.
fn main() {
    #[cfg(madsim)]
    {
        // SAFETY: build scripts are single-threaded.
        unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap()) };
        tonic_build::compile_protos("proto/sim.proto").unwrap();
    }
}
