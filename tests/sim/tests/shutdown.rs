//! B-23a: the real `bedrock-quicksilver`, `bedrock-kv`, `bedrock-cdn`, `bedrock-relay` and `bedrock-api` entry points boot
//! as simulated nodes, keep running, and each stops cleanly on a simulated Ctrl-C.
#![cfg(madsim)]

use std::time::Duration;

use bedrock_lib::datasets::Dataset;
use bedrock_lib::node::{
    ApiConfig, Budget, Location, NodeConfig, RelayConfig, Role, ServiceConfig, Tier,
};
use bedrock_lib::s3::S3Config;
use madsim::runtime::Handle;
use tokio::time::sleep;

const S3: &str = "http://10.0.1.9:9000";

fn node_config(name: &str, ip: &str) -> NodeConfig {
    NodeConfig {
        node_id: name.into(),
        location: Location {
            region: "eu".into(),
            dc: "eu-1".into(),
            cell: "eu-1a".into(),
            failure_domain: String::new(),
        },
        listen: format!("{ip}:7000").parse().unwrap(),
        advertise: None,
        admin_listen: format!("{ip}:7001").parse().unwrap(),
        data_dir: format!("/data/{name}").into(),
        budget: Budget::default(),
        tls: None,
    }
}

fn service_config(node: NodeConfig, datasets: &[Dataset]) -> ServiceConfig {
    ServiceConfig {
        node,
        roles: vec![Role::L1],
        datasets: datasets.iter().map(|d| d.name.to_string()).collect(),
        relays: vec!["10.0.1.3:7000".into()],
        api: "10.0.1.4:7002".into(),
        s3_endpoint: S3.into(),
    }
}

#[madsim::test]
async fn every_binary_boots_and_stops_on_ctrl_c() {
    let handle = Handle::current();
    let services = [
        ("kv", "10.0.1.1"),
        ("cdn", "10.0.1.2"),
        ("relay", "10.0.1.3"),
        ("api", "10.0.1.4"),
        ("quicksilver", "10.0.1.5"),
    ];
    let running: Vec<_> = services
        .iter()
        .map(|&(name, ip)| {
            let node = node_config(name, ip);
            let fut: std::pin::Pin<Box<dyn Future<Output = _> + Send>> = match name {
                "kv" => Box::pin(bedrock_kv::run(service_config(node, &[]))),
                "quicksilver" => Box::pin(bedrock_quicksilver::run(service_config(
                    node,
                    bedrock_quicksilver::DATASETS,
                ))),
                "cdn" => Box::pin(bedrock_cdn::run(service_config(
                    node,
                    bedrock_cdn::DATASETS,
                ))),
                "relay" => Box::pin(bedrock_relay::run(RelayConfig {
                    node,
                    tier: Tier::Cell,
                    upstreams: vec!["10.0.1.4:7000".into()],
                    replay_peers: Vec::new(),
                    replay: Default::default(),
                    membership: None,
                    write_token: None,
                })),
                _ => Box::pin(bedrock_api::run(ApiConfig {
                    node,
                    write_listen: format!("{ip}:7002").parse().unwrap(),
                    write_token: "simulation-writer-token".into(),
                    retention: Default::default(),
                    s3: S3Config {
                        endpoint: S3.into(),
                        bucket: "bedrock".into(),
                        region: "us-east-1".into(),
                        access_key_id: "sim".into(),
                        secret_access_key: "sim".into(),
                        max_attempts: 3,
                        attempt_timeout_ms: 30_000,
                    },
                })),
            };
            let node = handle
                .create_node()
                .name(name)
                .ip(ip.parse().unwrap())
                .build();
            node.spawn(async move { fut.await.unwrap() })
        })
        .collect();

    sleep(Duration::from_secs(5)).await;
    // A `run` that returned early must fail here, not pass as a clean shutdown.
    for (task, (name, _)) in running.iter().zip(services) {
        assert!(!task.is_finished(), "{name} stopped before Ctrl-C");
    }
    for (name, _) in services {
        handle.send_ctrl_c(name);
    }
    for (task, (name, _)) in running.into_iter().zip(services) {
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .unwrap_or_else(|_| panic!("{name} did not stop on Ctrl-C"))
            .unwrap();
    }
}
