//! Three voters sharing one host elect a leader over mutual TLS and forward an operator's drain write (#15 / B-12, B-55). The
//! transport is `bedrock-lib`'s; the cell control group is `bedrock-relay`'s. Real TLS does not run under madsim.
#![cfg(not(madsim))]

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::admin::Admin;
use bedrock_lib::node::TlsConfig;
use bedrock_lib::reconciliation::drain::{Drain, Scope, State};
use bedrock_lib::replication::pb;
use bedrock_lib::storage::memory::MemoryEngine;
use bedrock_lib::transport::tls::{Identity, Tls};
use bedrock_lib::transport::{Class, Peers};
use bedrock_relay::membership::control::{Config, Control, Server};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};

struct Ca {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl Ca {
    fn new() -> Ca {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let pem = params.self_signed(&key).unwrap().pem();
        Ca {
            issuer: Issuer::new(params, key),
            pem,
        }
    }

    /// A node certificate and key for `uri`, valid for 127.0.0.1.
    fn leaf(&self, uri: &str) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![
            SanType::URI(uri.try_into().unwrap()),
            SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

fn identity(service: &str, cell: &str, node_id: &str) -> Identity {
    Identity {
        service: service.into(),
        cell: cell.into(),
        node_id: node_id.into(),
        host: "127.0.0.1".into(),
    }
}

// Test fixture files, outside the simulated runtime.
#[allow(clippy::disallowed_methods)]
async fn tls(ca: &Ca, service: &str, cell: &str, node_id: &str) -> Arc<Tls> {
    let dir = tempfile::tempdir().unwrap().keep();
    let (cert, key) = ca.leaf(&format!("bedrock://{service}/{cell}/{node_id}"));
    let config = TlsConfig {
        cert: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        ca: dir.join("ca.crt"),
    };
    std::fs::write(&config.cert, cert).unwrap();
    std::fs::write(&config.key, key).unwrap();
    std::fs::write(&config.ca, &ca.pem).unwrap();
    Tls::start(&config, identity(service, cell, node_id), &Admin::new())
        .await
        .unwrap()
}

fn peers(tls: Option<Arc<Tls>>) -> Peers {
    Peers::new(&Admin::new()).with_tls(tls)
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_voters_on_one_host_elect_and_forward_operator_writes_over_tls() {
    let ca = Ca::new();
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let voters: Vec<_> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();
    let voter_ids: std::collections::BTreeMap<_, _> = voters
        .iter()
        .enumerate()
        .map(|(id, address)| (address.clone(), format!("r{id}")))
        .collect();
    drop(listeners);
    let identities: std::collections::BTreeMap<_, _> = voter_ids
        .iter()
        .map(|(address, id)| (address.clone(), identity("relay", "c1", id)))
        .collect();
    let mut controls = Vec::new();
    let mut tasks = Vec::new();
    for (id, address) in voters.iter().enumerate() {
        let tls = tls(&ca, "relay", "c1", &format!("r{id}")).await;
        let peers = Arc::new(peers(Some(tls.clone())).with_identities(identities.clone()));
        let (control, actor) = Control::start(
            Arc::new(MemoryEngine::new("control")),
            peers,
            Config {
                region: "eu".into(),
                dc: "dc".into(),
                cell: "c1".into(),
                me: address.clone(),
                voters: voters.clone(),
                voter_ids: voter_ids.clone(),
                clock: || 1000,
                operator_token: Some("operator".into()),
                bootstrap: true,
            },
        )
        .unwrap();
        let router = bedrock_lib::transport::server(Some(&tls)).add_service(
            pb::cell_control_server::CellControlServer::new(Server(Some(control.clone()))),
        );
        let listen = address.parse().unwrap();
        tasks.push(actor);
        tasks.push(tokio::spawn(async move {
            bedrock_lib::transport::serve(router, listen, Some(tls))
                .await
                .unwrap();
        }));
        controls.push(control);
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while !controls.iter().any(|control| control.is_leader()) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let follower = controls
            .iter()
            .position(|control| !control.is_leader())
            .unwrap();
        let operator =
            peers(Some(tls(&ca, "relay", "c1", "operator").await)).with_identities(identities);
        let channel = operator.channel(&voters[follower], Class::Control).unwrap();
        let mut client = pb::cell_control_client::CellControlClient::new(channel);
        let request = || {
            let mut request = tonic::Request::new(pb::ControlWrite {
                namespace: "cdn.c1".into(),
                key: b"drain/cell".to_vec(),
                value: Some(
                    Drain {
                        scope: Scope::Cell("c1".into()),
                        datasets: Vec::new(),
                        namespace: None,
                        state: State::Draining,
                        rate: 1,
                    }
                    .encode(),
                ),
                expected: None,
                expected_term: 0,
                idempotency_key: "1000:operator".into(),
            });
            request
                .metadata_mut()
                .insert("authorization", "Bearer operator".parse().unwrap());
            request
        };
        // A follower learns the leader from the leader's first message and answers `Unavailable` until then, so the
        // operator retries; the idempotency key keeps a retry from writing twice.
        let revision = loop {
            match client.write(request()).await {
                Ok(reply) => break reply.into_inner().revision,
                Err(status) if status.code() == tonic::Code::Unavailable => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(status) => panic!("operator write failed: {status}"),
            }
        };
        assert_eq!(revision, 1);
        while controls
            .iter()
            .any(|control| control.head("cdn.c1").unwrap() != 1)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for task in tasks {
        task.abort();
    }
}
