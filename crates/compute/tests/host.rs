//! Acquisition failures must leave the host ready to retry without starting an executor.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bedrock_compute::actors::host::{ActorHost, Placement};
use bedrock_compute::actors::volume::{Host, Slot};
use bedrock_compute::runtime::{Error, ExecutionSpec, Executor, Instance};
use bedrock_lib::admin::Admin;
use bedrock_lib::transport::Peers;
use bedrock_lib::volume as pb;

struct RejectPlacement(AtomicUsize);

#[async_trait]
impl Placement for RejectPlacement {
    async fn slots(&self, _actor: &pb::Actor) -> Result<([Slot; 3], u64), Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Error::Unsupported("actor cannot be placed".into()))
    }
}

struct MustNotStart;

#[async_trait]
impl Executor for MustNotStart {
    async fn start(&self, _spec: ExecutionSpec) -> Result<Box<dyn Instance>, Error> {
        panic!("placement failed but an executor was started");
    }
}

#[test]
fn failed_acquisitions_propagate_errors_and_retry_placement() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let admin = Admin::new();
        let volume = Host::start(Arc::new(Peers::new(&admin)), pb::Holder::default());
        let placement = Arc::new(RejectPlacement(AtomicUsize::new(0)));
        let host = ActorHost::new(
            &admin,
            "cell-1",
            "dc1",
            volume,
            placement.clone(),
            Arc::new(MustNotStart),
            Arc::new(MustNotStart),
        );
        let actor = pb::Actor {
            namespace: "ns".into(),
            id: "unplaceable".into(),
            ..Default::default()
        };
        assert_eq!(host.awake().await, 0);
        for attempt in 1..=3 {
            match host.acquire(&actor).await {
                Err(Error::Unsupported(detail)) => assert_eq!(detail, "actor cannot be placed"),
                _ => panic!("placement error was not propagated"),
            }
            assert_eq!(placement.0.load(Ordering::SeqCst), attempt);
            assert_eq!(host.awake().await, 0);
        }
    });
}
