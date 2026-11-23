//! #117 / S-09: actual volume and Compute services, timers, storage commits and duplicate Wake transport.
//! Gateway routing is covered separately in actor_routing.rs. This contract runs in normal and madsim builds.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bedrock_compute::actors::host::{ActorHost, Placement};
use bedrock_compute::actors::serve::Actors;
use bedrock_compute::actors::storage::Storage;
use bedrock_compute::actors::volume::{Host, Slot};
use bedrock_compute::runtime::{Error, ExecutionSpec, Executor, Handler, Instance, RustExecutor};
use bedrock_lib::actors::Kind;
use bedrock_lib::admin::Admin;
use bedrock_lib::placement::{Directory, Owner, State};
#[cfg(not(madsim))]
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::transport::{Peers, tls};
use bedrock_lib::volume as pb;
use bedrock_volume::alarms::{Scanner, Wake};
use bedrock_volume::serve::Service;
use bedrock_volume::store::Store;

#[allow(clippy::disallowed_methods)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn actor(n: char) -> pb::Actor {
    pb::Actor {
        namespace: "ns".into(),
        id: format!("eu_{}a", n.to_string().repeat(25)),
        kind: Kind::Cron as i32,
    }
}

struct Fixed([Slot; 3]);
#[async_trait]
impl Placement for Fixed {
    async fn slots(&self, _: &pb::Actor) -> Result<([Slot; 3], u64), Error> {
        Ok((self.0.clone(), 0))
    }
}

struct First;
#[async_trait]
impl bedrock_volume::cold::Placement for First {
    async fn first_here(&self, _: &pb::Actor) -> Result<bool, tonic::Status> {
        Ok(true)
    }
    async fn first_cell(&self, _: &pb::Actor) -> Result<String, tonic::Status> {
        Ok("cell".into())
    }
    async fn truncate_others(&self, _: &pb::Actor, _: u64) -> Result<bool, tonic::Status> {
        unreachable!()
    }
}

struct Direct(String);
#[async_trait]
impl Wake for Direct {
    async fn wake(&self, actor: pb::Actor, at_ms: u64) -> Result<(), tonic::Status> {
        let reply = pb::actors_client::ActorsClient::connect(format!("http://{}", self.0))
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))?
            .invoke(pb::InvokeRequest {
                actor: Some(actor),
                alarm_ms: Some(at_ms),
                ..Default::default()
            })
            .await?
            .into_inner();
        if reply.code == 0 {
            Ok(())
        } else {
            Err(tonic::Status::new(reply.code.into(), reply.message))
        }
    }
}

type Trace = Arc<Mutex<Vec<&'static str>>>;

struct Delayed {
    inner: Arc<dyn Executor>,
    started: tokio::sync::Notify,
    finish: tokio::sync::Notify,
    starts: AtomicUsize,
}

#[async_trait]
impl Executor for Delayed {
    async fn start(&self, spec: ExecutionSpec) -> Result<Box<dyn Instance>, Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.finish.notified().await;
        self.inner.start(spec).await
    }
}

struct Alarm {
    storage: Storage,
    trace: Trace,
    panic: Arc<AtomicBool>,
    omit_clear_once: bool,
    retry_after: Option<tokio::time::Instant>,
}
#[async_trait]
impl Instance for Alarm {
    async fn invoke(&mut self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        if request == b"omit-clear" {
            self.omit_clear_once = true;
            self.storage
                .commit_with_alarm(Vec::new(), Some(now_ms() + 100))
                .await?;
        } else if request == b"sql" {
            let at = now_ms() + 60_000;
            self.storage
                .sql_with_alarm(|_| Ok(()), Some(at))
                .await?
                .unwrap();
            assert_eq!(
                self.storage.alarm_ms(),
                Some(at),
                "alarm-only SQL commit was dropped"
            );
            assert!(
                self.storage
                    .sql_with_alarm(|tx| tx.execute_batch("not valid SQL"), None)
                    .await?
                    .is_err()
            );
            assert_eq!(
                self.storage.alarm_ms(),
                Some(at),
                "failed SQL cleared the alarm"
            );
            self.storage
                .sql_with_alarm(|_| Ok(()), None)
                .await?
                .unwrap();
            assert_eq!(self.storage.alarm_ms(), None);
        } else {
            let at = u64::from_be_bytes(request.try_into().unwrap());
            self.storage.commit_with_alarm(Vec::new(), Some(at)).await?;
        }
        Ok(Vec::new())
    }
    async fn alarm(&mut self, _: u64) -> Result<(), Error> {
        self.trace.lock().unwrap().push("attempt");
        if self.panic.swap(false, Ordering::SeqCst) {
            // madsim treats task panics as fatal seed failures; actor_routing kills a real Compute node instead.
            #[cfg(not(madsim))]
            panic!("crash before alarm commit");
            #[cfg(madsim)]
            return Err(Error::Handler("failure before alarm commit".into()));
        }
        if self.omit_clear_once {
            self.omit_clear_once = false;
            self.retry_after = Some(bedrock_lib::monotonic_now());
            return Ok(());
        }
        if let Some(previous) = self.retry_after.take() {
            assert!(
                previous.elapsed() >= Duration::from_millis(100),
                "uncommitted alarm hot-looped"
            );
        }
        self.storage
            .commit_with_alarm(
                vec![pb::Change {
                    key: b"fired".to_vec(),
                    value: Some(vec![1]),
                }],
                None,
            )
            .await?;
        self.trace.lock().unwrap().push("commit");
        Ok(())
    }
}

async fn volume(
    i: usize,
) -> (
    Slot,
    Arc<Store>,
    Directory,
    bedrock_volume::alarms::LeaseLive,
) {
    let name = format!("volume-{i}");
    #[cfg(not(madsim))]
    let engine = Arc::new(MemoryEngine::new("volume"));
    #[cfg(madsim)]
    let engine = Arc::new(bedrock_lib::storage::sim::SimEngine::new(
        &name,
        Arc::new(|_| None),
        None,
        false,
    ));
    let store = Arc::new(Store::new(engine));
    store.hold(0, 0, 0).unwrap();
    let service = Service::new(&Admin::new(), store.clone(), &name, "cell", "dc").unwrap();
    let directory = BTreeMap::from([(
        0,
        vec![Owner {
            node: name.clone(),
            state: State::Ready,
        }],
    )]);
    *service.directory.write().unwrap() = directory.clone();
    let leases = service.lease_live();
    #[cfg(not(madsim))]
    let address = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(pb::volume_server::VolumeServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        address
    };
    #[cfg(madsim)]
    let address = {
        let address = format!("10.9.0.1:{}", 7000 + i);
        let listen = address.parse().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(pb::volume_server::VolumeServer::new(service))
                .serve(listen)
                .await
                .unwrap();
        });
        address
    };
    let slot = Slot {
        cell: "cell".into(),
        address,
        identity: tls::Identity {
            service: "volume".into(),
            cell: "cell".into(),
            node_id: name,
            host: "127.0.0.1".into(),
        },
    };
    (slot, store, directory, leases)
}

async fn scenario() -> Vec<&'static str> {
    let (a, store, directory, leases) = volume(0).await;
    let (b, _, _, _) = volume(1).await;
    let (c, _, _, _) = volume(2).await;
    let admin = Admin::new();
    let trace: Trace = Arc::default();
    let panic = Arc::new(AtomicBool::new(false));
    let handler: Handler = {
        let (trace, panic) = (trace.clone(), panic.clone());
        Arc::new(move |_, storage| {
            Box::new(Alarm {
                storage,
                trace: trace.clone(),
                panic: panic.clone(),
                omit_clear_once: false,
                retry_after: None,
            })
        })
    };
    let holder = pb::Holder {
        cell: "cell".into(),
        host: "compute".into(),
    };
    let volume = Host::start(Arc::new(Peers::new(&admin)), holder.clone());
    let placement = Arc::new(Fixed([a, b, c]));
    let executor = Arc::new(RustExecutor::new([(Kind::Cron, handler)]));
    let host = ActorHost::new(
        &admin,
        "cell",
        "dc",
        volume.clone(),
        placement.clone(),
        executor.clone(),
        Arc::new(RustExecutor::new([])),
    );
    #[cfg(not(madsim))]
    let address = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(pb::actors_server::ActorsServer::new(Actors::new(
                    host.clone(),
                    holder,
                )))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        address
    };
    #[cfg(madsim)]
    let address = {
        let service = Actors::new(host.clone(), holder);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(pb::actors_server::ActorsServer::new(service))
                .serve("10.9.0.1:7100".parse().unwrap())
                .await
                .unwrap();
        });
        "10.9.0.1:7100".into()
    };
    let mut scanner = Scanner::new(
        store,
        Arc::new(First),
        Arc::new(Direct(address)),
        "volume-0",
        leases,
    );
    let sql = host.acquire(&actor('d')).await.unwrap();
    sql.invoke(b"sql".to_vec()).await.unwrap();

    let target = actor('c');
    let awake = host.acquire(&target).await.unwrap();
    let at = now_ms() + 12_500;
    awake.invoke(at.to_be_bytes().to_vec()).await.unwrap();
    awake.alarm(at).await.unwrap(); // Future Wake does not run early.
    awake.alarm(now_ms() - 1).await.unwrap(); // A due Wake that is not the committed alarm does not run.
    assert!(trace.lock().unwrap().is_empty());
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(
        host.awake().await,
        0,
        "pending future alarm kept actor in memory"
    );
    let until = bedrock_lib::monotonic_now() + Duration::from_secs(3);
    while trace.lock().unwrap().len() < 2 && bedrock_lib::monotonic_now() < until {
        scanner.tick(&directory).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(*trace.lock().unwrap(), ["attempt", "commit"]);
    assert!(
        now_ms() <= at + 1_000,
        "sleeping alarm exceeded one second plus its local wake"
    );
    let awake = host.acquire(&target).await.unwrap();
    awake.alarm(at).await.unwrap();
    assert_eq!(
        trace.lock().unwrap().len(),
        2,
        "duplicate Wake reran cleared alarm"
    );
    let metrics = admin.render();
    let count = metrics
        .lines()
        .find(|line| line.starts_with("bedrock_compute_actor_alarm_lateness_seconds_count{"))
        .unwrap_or_else(|| panic!("missing alarm lateness metric: {metrics}"));
    assert!(
        count.contains("kind=\"cron\"") && count.ends_with(" 1"),
        "{count}"
    );
    assert!(
        !count.contains("namespace") && !count.contains(&target.id),
        "{count}"
    );

    // A second actor keeps host-level heartbeats alive across the first handler's failure.
    let keepalive = host.acquire(&actor('e')).await.unwrap();
    panic.store(true, Ordering::SeqCst);
    let at = now_ms() + 200;
    awake.invoke(at.to_be_bytes().to_vec()).await.unwrap();
    let until = bedrock_lib::monotonic_now() + Duration::from_secs(6);
    while trace.lock().unwrap().len() < 5 && bedrock_lib::monotonic_now() < until {
        scanner.tick(&directory).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        keepalive.serving(),
        "the host lease expired instead of releasing the failed actor"
    );
    assert_eq!(
        *trace.lock().unwrap(),
        ["attempt", "commit", "attempt", "attempt", "commit"]
    );
    // An Ok return without a commit is still pending, and the holder retries with backoff.
    host.acquire(&target)
        .await
        .unwrap()
        .invoke(b"omit-clear".to_vec())
        .await
        .unwrap();
    let until = bedrock_lib::monotonic_now() + Duration::from_secs(3);
    while trace.lock().unwrap().len() < 8 && bedrock_lib::monotonic_now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        &trace.lock().unwrap()[5..],
        ["attempt", "attempt", "commit"]
    );
    // Cancelling a routed Wake after its claim must still install the instance and its timer.
    let delayed = Arc::new(Delayed {
        inner: executor,
        started: tokio::sync::Notify::new(),
        finish: tokio::sync::Notify::new(),
        starts: AtomicUsize::new(0),
    });
    let waking_admin = Admin::new();
    let waking = ActorHost::new(
        &waking_admin,
        "cell",
        "dc",
        volume,
        placement,
        delayed.clone(),
        Arc::new(RustExecutor::new([])),
    );
    let cancelled = {
        let waking = waking.clone();
        tokio::spawn(async move { waking.acquire(&actor('f')).await })
    };
    delayed.started.notified().await;
    cancelled.abort();
    assert!(cancelled.await.is_err());
    delayed.finish.notify_one();
    let awake = waking.acquire(&actor('f')).await.unwrap();
    assert_eq!(
        delayed.starts.load(Ordering::SeqCst),
        1,
        "cancelled Wake abandoned the claimed instance"
    );
    awake
        .invoke((now_ms() + 100).to_be_bytes().to_vec())
        .await
        .unwrap();
    let until = bedrock_lib::monotonic_now() + Duration::from_secs(3);
    while trace.lock().unwrap().len() < 10 && bedrock_lib::monotonic_now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(&trace.lock().unwrap()[8..], ["attempt", "commit"]);
    trace.lock().unwrap().clone()
}

#[cfg(not(madsim))]
#[tokio::test]
async fn alarms_sleep_wake_clear_and_retry_after_handler_failure() {
    scenario().await;
}

#[cfg(madsim)]
#[test]
#[allow(clippy::disallowed_methods)]
fn alarms_sleep_wake_clear_and_retry_after_handler_failure_and_replay() {
    let run = |seed| {
        std::thread::spawn(move || {
            let mut runtime =
                madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
            runtime.set_time_limit(Duration::from_secs(60));
            runtime.block_on(async {
                madsim::runtime::Handle::current()
                    .create_node()
                    .ip("10.9.0.1".parse().unwrap())
                    .build()
                    .spawn(scenario())
                    .await
                    .unwrap()
            })
        })
        .join()
        .unwrap_or_else(|failure| {
            eprintln!("Replay: BEDROCK_ACTOR_ALARMS_SEED={seed} RUSTFLAGS=\"--cfg madsim\" cargo test -p bedrock-sim --test actor_alarms -- --nocapture");
            std::panic::resume_unwind(failure)
        })
    };
    let seeds = std::env::var("BEDROCK_ACTOR_ALARMS_SEED")
        .map(|value| vec![value.parse::<u64>().unwrap()])
        .unwrap_or_else(|_| vec![1, 2, 3]);
    for seed in seeds {
        let trace = run(seed);
        assert_eq!(trace, run(seed), "seed {seed} changed alarm trace");
        println!("actor alarms seed {seed}: {trace:?}");
    }
}
