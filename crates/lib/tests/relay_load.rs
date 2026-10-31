//! Opt-in measurement, never a substitute for the distributed hardening gate.
//! Run: cargo test -p bedrock-lib --release --test relay_load -- --ignored --nocapture
//! BEDROCK_RELAY_LOAD_SECONDS=60 extends the default ten-second producer window.
#![cfg(not(madsim))]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)] // Native measurement adapter: wall clock and /proc RSS.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bedrock_lib::buckets::{COUNT, bucket_of};
use bedrock_lib::relays::Hub;
use bedrock_lib::relays::replay::{Replay, ReplayConfig};
use bedrock_lib::replication::log::Log;
use bedrock_lib::replication::materialized::Retention;
use bedrock_lib::replication::pb;
use bedrock_lib::replication::writer::{Persistence, PersistenceMetrics, Write, Writer};
use bedrock_lib::replication::{Event, Operation};
use bedrock_lib::storage::StorageEngine;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::storage::namespace::memory::MemoryFactory;
use bedrock_lib::storage::rocksdb::{RocksDbAdapter, RocksDbOptions};
use bedrock_lib::transport::admin::Admin;
use bedrock_lib::transport::{Class, Peers};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

const SUBSCRIBERS: usize = 1000;
const TARGET_RATE: u64 = 10_000;
const PAYLOAD: usize = 256;
const IN_FLIGHT: usize = 256;
const REPLAY_BYTES: u64 = 16 * 1024 * 1024;
const DATASET: &str = "quicksilver";
const NAMESPACE: &str = "load";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn peers() -> Arc<Peers> {
    Arc::new(Peers::new(&Admin::new()))
}

struct Server {
    address: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(hub: Hub) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::relay_server::RelayServer::new(hub))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Server { address, task }
}

struct Stats {
    ready: AtomicU64,
    delivered: AtomicU64,
    wire_bytes: AtomicU64,
    errors: AtomicU64,
    covered: Vec<AtomicU64>,
    /// Millisecond histogram; the final bin means at least five seconds.
    lag: Vec<AtomicU64>,
}

async fn subscribe(
    address: &str,
    subscriber: usize,
    stats: Arc<Stats>,
) -> tokio::task::JoinHandle<()> {
    let (tx, rx) = mpsc::channel(16);
    tx.send(pb::RelayRequest {
        kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
            version: 1,
            tier: 0,
            member: None,
        })),
    })
    .await
    .unwrap();
    // Separate process-style connections avoid one HTTP/2 stream limit controlling the experiment.
    let connection = peers();
    let channel = connection.channel(address, Class::Control).unwrap();
    let mut stream = tokio::time::timeout(
        Duration::from_secs(10),
        pb::relay_client::RelayClient::new(channel).session(ReceiverStream::new(rx)),
    )
    .await
    .unwrap()
    .unwrap()
    .into_inner();
    assert!(matches!(
        stream.message().await.unwrap().unwrap().kind,
        Some(pb::relay_frame::Kind::Welcome(_))
    ));
    // Deterministic disjoint partition: every bucket belongs to exactly one selector.
    let buckets = (subscriber as u32..COUNT).step_by(SUBSCRIBERS).collect();
    tx.send(pb::RelayRequest {
        kind: Some(pb::relay_request::Kind::Follow(pb::Follow {
            dataset: DATASET.into(),
            namespace: NAMESPACE.into(),
            from: 1,
            buckets,
        })),
    })
    .await
    .unwrap();
    tokio::spawn(async move {
        let mut ready = false;
        let mut progress = tokio::time::interval(Duration::from_secs(10));
        loop {
            let frame = tokio::select! {biased;
                frame=stream.message()=>match frame{Ok(Some(frame))=>frame,_=>break},
                _=progress.tick()=>{
                    if tx.send(pb::RelayRequest{kind:Some(pb::relay_request::Kind::Progress(true))}).await.is_err(){break}
                    continue;
                }
            };
            stats
                .wire_bytes
                .fetch_add(frame.encoded_len() as u64, Ordering::Relaxed);
            match frame.kind {
                Some(pb::relay_frame::Kind::Range(_)) if !ready => {
                    ready = true;
                    stats.ready.fetch_add(1, Ordering::Relaxed);
                }
                Some(pb::relay_frame::Kind::Delivery(delivery)) => {
                    for bytes in delivery.events {
                        let Ok(event) = pb::Event::decode(bytes) else {
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            return;
                        };
                        if event.dataset != DATASET
                            || event.namespace != NAMESPACE
                            || bucket_of(DATASET, NAMESPACE, &event.key) as usize % SUBSCRIBERS
                                != subscriber
                        {
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        let lag = now_ms().saturating_sub(event.committed_at).min(5000) as usize;
                        stats.lag[lag].fetch_add(1, Ordering::Relaxed);
                        stats.delivered.fetch_add(1, Ordering::Relaxed);
                    }
                    stats.covered[subscriber].store(delivery.through, Ordering::Relaxed);
                    if tx
                        .send(pb::RelayRequest {
                            kind: Some(pb::relay_request::Kind::Ack(pb::Ack {
                                dataset: DATASET.into(),
                                namespace: NAMESPACE.into(),
                                applied: delivery.through,
                            })),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Some(pb::relay_frame::Kind::Error(error)) => {
                    if stats.errors.fetch_add(1, Ordering::Relaxed) < 3 {
                        eprintln!("subscriber {subscriber}: {} {}", error.code, error.detail);
                    }
                    return;
                }
                _ => {}
            }
        }
        stats.errors.fetch_add(1, Ordering::Relaxed);
    })
}

fn percentile(histogram: &[AtomicU64], percentile: u64) -> Option<usize> {
    let count: u64 = histogram
        .iter()
        .map(|bin| bin.load(Ordering::Relaxed))
        .sum();
    if count == 0 {
        return None;
    }
    let target = (count * percentile).div_ceil(100);
    let mut seen = 0;
    histogram.iter().position(|bin| {
        seen += bin.load(Ordering::Relaxed);
        seen >= target
    })
}

fn process_memory() -> String {
    // This benchmark adapter may inspect the host; shared runtime code never does.
    std::fs::read_to_string("/proc/self/status")
        .map(|status| {
            status
                .lines()
                .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|_| "RSS unavailable on this platform".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "opt-in hardware measurement; run explicitly in release mode"]
async fn thousand_bucket_subscribers_at_ten_thousand_events_per_second() {
    eprintln!(
        "relay_load profile: {}",
        if cfg!(debug_assertions) {
            "debug (diagnostic only)"
        } else {
            "release"
        }
    );
    let seconds: u64 = std::env::var("BEDROCK_RELAY_LOAD_SECONDS")
        .ok()
        .map(|value| {
            value
                .parse()
                .expect("BEDROCK_RELAY_LOAD_SECONDS must be an integer")
        })
        .unwrap_or(10);
    assert!((1..=3600).contains(&seconds));
    let target_rate = std::env::var("BEDROCK_RELAY_LOAD_RATE")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("BEDROCK_RELAY_LOAD_RATE must be an integer")
        })
        .unwrap_or(TARGET_RATE);
    assert!((1..=1_000_000).contains(&target_rate));
    let writer = Writer::open(
        now_ms,
        Persistence {
            factory: Arc::new(MemoryFactory::default()),
            retention: Retention::default(),
            metrics: PersistenceMetrics::default(),
        },
    )
    .await
    .unwrap();
    let writer_server = serve(Hub::writer("load-writer".into(), writer.clone(), peers())).await;
    let replay_directory = tempfile::tempdir().unwrap();
    let memory_replay = std::env::var("BEDROCK_RELAY_LOAD_MEMORY").is_ok_and(|value| value == "1");
    let replay_engine: Arc<dyn StorageEngine> = if memory_replay {
        Arc::new(MemoryEngine::new("load-replay"))
    } else {
        Arc::new(
            RocksDbAdapter::open(
                replay_directory.path(),
                "load-replay",
                &RocksDbOptions::default(),
            )
            .unwrap(),
        )
    };
    let replay_engine_name = if memory_replay { "memory" } else { "rocksdb" };
    let replay = Arc::new(
        Replay::open(
            replay_engine,
            ReplayConfig {
                max_bytes: REPLAY_BYTES,
                ..ReplayConfig::default()
            },
        )
        .unwrap(),
    );
    let relay = Hub::relay(
        "load-cell".into(),
        1,
        peers(),
        vec![writer_server.address.clone()],
        vec![writer_server.address.clone()],
        Some(replay.clone()),
    );
    let upstream = relay.start();
    let relay_server = serve(relay).await;
    let stats = Arc::new(Stats {
        ready: AtomicU64::new(0),
        delivered: AtomicU64::new(0),
        wire_bytes: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        covered: (0..SUBSCRIBERS).map(|_| AtomicU64::new(0)).collect(),
        lag: (0..=5000).map(|_| AtomicU64::new(0)).collect(),
    });
    let mut subscribers = Vec::new();
    for subscriber in 0..SUBSCRIBERS {
        subscribers.push(subscribe(&relay_server.address, subscriber, stats.clone()).await);
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while stats.ready.load(Ordering::Relaxed) < SUBSCRIBERS as u64 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("not all subscribers established coverage before measurement");
    eprintln!(
        "relay_load warmup: subscribers={SUBSCRIBERS}, replay_engine={replay_engine_name}, {}, selectors=bucket_mod_1000, buckets_per_selector=1048_or_1049",
        process_memory()
    );
    // One key per owner makes payload traffic exactly round-robin across all selectors.
    let mut keys = std::collections::BTreeMap::new();
    let mut candidate = 0u64;
    while keys.len() < SUBSCRIBERS {
        let key = candidate.to_be_bytes().to_vec();
        let owner = bucket_of(DATASET, NAMESPACE, &key) as usize % SUBSCRIBERS;
        keys.entry(owner).or_insert(key);
        candidate += 1;
    }
    let keys: Vec<_> = keys.into_values().collect();
    let started = Instant::now();
    let writer_mode = std::env::var("BEDROCK_RELAY_LOAD_WRITER").is_ok_and(|value| value == "1");
    eprintln!(
        "relay_load source: {}",
        if writer_mode {
            "materialized_memory_writer"
        } else {
            "committed_log_fixture"
        }
    );
    let log = writer.log();
    let mut jobs = tokio::task::JoinSet::new();
    let mut injected = 0u64;
    let mut committed = 0u64;
    let mut failures = 0u64;
    while started.elapsed() < Duration::from_secs(seconds) {
        if jobs.len() >= IN_FLIGHT {
            if matches!(jobs.join_next().await, Some(Ok(Ok(_)))) {
                committed += 1;
            } else {
                failures += 1;
            }
            continue;
        }
        let due = started + Duration::from_micros(injected * 1_000_000 / target_rate);
        tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
        if started.elapsed() >= Duration::from_secs(seconds) {
            break;
        }
        let source = writer.clone();
        let key = keys[injected as usize % SUBSCRIBERS].clone();
        if writer_mode {
            jobs.spawn(async move {
                source
                    .commit(Write {
                        dataset: DATASET.into(),
                        namespace: NAMESPACE.into(),
                        key,
                        operation: Operation::Set(vec![42; PAYLOAD]),
                        idempotency_key: None,
                    })
                    .await
            });
        } else {
            // Isolate relay capacity from the separately tested writer persistence path.
            match log.append(Event {
                dataset: DATASET.into(),
                namespace: NAMESPACE.into(),
                stream: "quicksilver-0".into(),
                sequence: injected + 1,
                epoch: 1,
                committed_at: now_ms(),
                key,
                operation: Operation::Set(vec![42; PAYLOAD]),
            }) {
                Ok(()) => committed += 1,
                Err(_) => failures += 1,
            }
        }
        injected += 1;
    }
    let injection_elapsed = started.elapsed();
    while let Some(result) = jobs.join_next().await {
        if matches!(result, Ok(Ok(_))) {
            committed += 1;
        } else {
            failures += 1;
        }
    }
    let commit_elapsed = started.elapsed();
    let head = writer.log().head(DATASET, NAMESPACE).get();
    let wait_started = Instant::now();
    let all_caught_up = tokio::time::timeout(Duration::from_secs(30), async {
        while stats
            .covered
            .iter()
            .any(|covered| covered.load(Ordering::Relaxed) < head)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let rate = committed as f64 / commit_elapsed.as_secs_f64();
    let delivery_rate =
        stats.delivered.load(Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64();
    let covered = stats
        .covered
        .iter()
        .map(|value| value.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    eprintln!(
        "relay_load result: target_rate={target_rate}/s, injected={injected}, committed={committed}, failures={failures}, injection_seconds={:.3}, commit_seconds={:.3}, actual_commit_rate={rate:.1}/s, producer_target_met={}, actual_delivery_rate={delivery_rate:.1}/s, payload_bytes={PAYLOAD}, selected_deliveries={}, serialized_frame_bytes={}, subscriber_errors={}, all_caught_up={all_caught_up}, catchup_wait_seconds={:.3}, head={head}, coverage_min={}, coverage_max={}, lag_p50_ms={:?}, lag_p95_ms={:?}, lag_p99_ms={:?}, lag_overflow_bin_ms=5000, replay_logical_bytes={}, replay_limit={REPLAY_BYTES}, per_subscriber_queue_limit_bytes=4194304, per_subscriber_queue_frames=1024, {}",
        injection_elapsed.as_secs_f64(),
        commit_elapsed.as_secs_f64(),
        rate >= target_rate as f64,
        stats.delivered.load(Ordering::Relaxed),
        stats.wire_bytes.load(Ordering::Relaxed),
        stats.errors.load(Ordering::Relaxed),
        wait_started.elapsed().as_secs_f64(),
        covered.iter().min().unwrap(),
        covered.iter().max().unwrap(),
        percentile(&stats.lag, 50),
        percentile(&stats.lag, 95),
        percentile(&stats.lag, 99),
        replay.bytes(),
        process_memory()
    );
    eprintln!(
        "relay_load limits: loopback transport; writer_mode={writer_mode}; replay_engine={replay_engine_name}; no S3 or deployment-network claim; queue limits are configured bounds, not observed occupancy; RSS includes source history and all 1000 client connections; keys=1000, selected_payload_distribution=exact_round_robin."
    );
    for task in subscribers {
        task.abort();
    }
    upstream.abort();
    writer.close().await.unwrap();
    assert!(replay.healthy(), "replay persistence was disabled");
    assert!(replay.bytes() <= REPLAY_BYTES);
}
