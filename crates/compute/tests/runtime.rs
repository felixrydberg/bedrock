//! Execution seam contracts that require neither a volume server nor customer code.
use std::sync::Arc;

use bedrock_compute::actors::volume;
use bedrock_compute::runtime::{Error, ExecutionSpec, Executor, Handler, RustExecutor};
use bedrock_lib::actors::Kind;
use bedrock_lib::volume as pb;

#[test]
fn held_errors_redirect_to_the_exact_holder() {
    let holder = pb::Holder {
        cell: "cell-2".into(),
        host: "compute-7".into(),
    };
    let error = Error::from(volume::Error::Held(holder.clone()));
    match &error {
        Error::Redirect(actual) => assert_eq!(actual, &holder),
        other => panic!("expected redirect, got {other:?}"),
    }
    assert_eq!(error.to_string(), "held by compute-7 in cell-2");
}

#[test]
fn storage_failures_preserve_their_retry_and_diagnostic_information() {
    for original in [
        volume::Error::Fenced,
        volume::Error::StaleVersion,
        volume::Error::Unavailable("no quorum".into()),
        volume::Error::TooLarge(3_145_729),
        volume::Error::Rpc(tonic::Status::data_loss("corrupt page")),
    ] {
        let detail = original.to_string();
        let error = Error::from(original);
        assert_eq!(error.to_string(), format!("volume: {detail}"));
        match error {
            Error::Volume(actual) => assert_eq!(actual.to_string(), detail),
            other => panic!("storage failure became {other:?}"),
        }
    }
    let Error::Volume(volume::Error::Rpc(status)) =
        Error::from(volume::Error::Rpc(tonic::Status::data_loss("page 9")))
    else {
        panic!("RPC status was lost");
    };
    assert_eq!(status.code(), tonic::Code::DataLoss);
    assert_eq!(status.message(), "page 9");
}

#[test]
fn workers_never_start_a_registered_rust_handler() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let handler: Handler = Arc::new(|_, _| panic!("a Worker reached a Rust handler"));
        let executor = RustExecutor::new([
            (Kind::Do, handler.clone()),
            (Kind::D1, handler.clone()),
            (Kind::Queue, handler.clone()),
            (Kind::Cron, handler.clone()),
            (Kind::Workflow, handler.clone()),
            (Kind::Kv, handler),
        ]);
        for artifact in ["", "cdn/customer/worker-v1"] {
            let spec = ExecutionSpec::Worker {
                artifact: artifact.into(),
            };
            assert_eq!(spec.builtin(), None);
            match executor.start(spec).await {
                Err(Error::Unsupported(detail)) => assert!(detail.contains("sandbox")),
                _ => panic!("a Worker must be rejected by the Rust executor"),
            }
        }
    });
}
