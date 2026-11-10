use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::pki_types::pem::PemObject;
use tonic::Code;

use super::*;
use crate::node::TlsConfig;
use crate::relays::{Hub, siblings};
use crate::replication::pb;
use crate::transport::{Class, Peers};

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
        self.issue(uri, false)
    }

    fn issue(&self, uri: &str, expired: bool) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        if expired {
            params.not_before = rcgen::date_time_ymd(2020, 1, 1);
            params.not_after = rcgen::date_time_ymd(2021, 1, 1);
        }
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

/// Writes `uri`'s files from `ca` into `dir` and returns their config.
// Test fixture files, outside the simulated runtime.
#[allow(clippy::disallowed_methods)]
fn files(dir: &Path, ca: &Ca, uri: &str) -> TlsConfig {
    let (cert, key) = ca.leaf(uri);
    let config = TlsConfig {
        cert: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        ca: dir.join("ca.crt"),
    };
    std::fs::write(&config.cert, cert).unwrap();
    std::fs::write(&config.key, key).unwrap();
    std::fs::write(&config.ca, &ca.pem).unwrap();
    config
}

fn identity(service: &str, cell: &str, node_id: &str) -> Identity {
    Identity {
        service: service.into(),
        cell: cell.into(),
        node_id: node_id.into(),
        host: "127.0.0.1".into(),
    }
}

async fn tls(ca: &Ca, service: &str, cell: &str, node_id: &str) -> Arc<Tls> {
    let dir = tempfile::tempdir().unwrap().keep();
    let config = files(&dir, ca, &format!("bedrock://{service}/{cell}/{node_id}"));
    Tls::start(&config, identity(service, cell, node_id), &Admin::new())
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn startup_rejects_a_certificate_for_another_node() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().unwrap();
    let config = files(dir.path(), &ca, "bedrock://kv/c1/n1");
    for (wrong, expected) in [
        (
            identity("kv", "c1", "n2"),
            "this node is bedrock://kv/c1/n2",
        ),
        (
            identity("cdn", "c1", "n1"),
            "this node is bedrock://cdn/c1/n1",
        ),
        (
            Identity {
                host: "10.0.0.9".into(),
                ..identity("kv", "c1", "n1")
            },
            "no SAN for the advertised host",
        ),
    ] {
        let error = Tls::start(&config, wrong, &Admin::new())
            .await
            .err()
            .unwrap();
        assert!(error.0.contains(expected), "{}", error.0);
    }
    let expired = tempfile::tempdir().unwrap();
    let expired = TlsConfig {
        cert: expired.path().join("tls.crt"),
        key: expired.path().join("tls.key"),
        ..config.clone()
    };
    let (cert, key) = ca.issue("bedrock://kv/c1/n1", true);
    #[allow(clippy::disallowed_methods)]
    {
        std::fs::write(&expired.cert, cert).unwrap();
        std::fs::write(&expired.key, key).unwrap();
    }
    let error = Tls::start(&expired, identity("kv", "c1", "n1"), &Admin::new())
        .await
        .err()
        .unwrap();
    assert!(error.0.contains("expired"), "{}", error.0);
    // A key from another certificate.
    let other = tempfile::tempdir().unwrap();
    let other = files(other.path(), &ca, "bedrock://kv/c1/n1");
    let mismatched = TlsConfig {
        key: other.key,
        ..config.clone()
    };
    let error = Tls::start(&mismatched, identity("kv", "c1", "n1"), &Admin::new())
        .await
        .err()
        .unwrap();
    assert!(error.0.contains("key does not match"), "{}", error.0);
    Tls::start(&config, identity("kv", "c1", "n1"), &Admin::new())
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn startup_waits_for_files_and_never_falls_back() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().unwrap();
    let config = TlsConfig {
        cert: dir.path().join("tls.crt"),
        key: dir.path().join("tls.key"),
        ca: dir.path().join("ca.crt"),
    };
    let error = Tls::start(&config, identity("kv", "c1", "n1"), &Admin::new())
        .await
        .err()
        .unwrap();
    assert!(error.0.starts_with("tls: "), "{}", error.0);

    let path = dir.path().to_owned();
    let issued = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        files(&path, &ca, "bedrock://kv/c1/n1");
    };
    let admin = Admin::new();
    let (started, ()) = tokio::join!(
        Tls::start(&config, identity("kv", "c1", "n1"), &admin),
        issued
    );
    started.unwrap();
}

#[tokio::test(start_paused = true)]
async fn renewals_apply_and_broken_ones_keep_the_current_certificate() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().unwrap();
    let config = files(dir.path(), &ca, "bedrock://kv/c1/n1");
    let tls = Tls::start(&config, identity("kv", "c1", "n1"), &Admin::new())
        .await
        .unwrap();
    assert!(!tls.reload_once(), "unchanged files");
    files(dir.path(), &ca, "bedrock://kv/c1/n1");
    assert!(tls.reload_once(), "a renewal applies");
    files(dir.path(), &ca, "bedrock://kv/c1/n2");
    assert!(
        !tls.reload_once(),
        "a certificate for another node is rejected"
    );
    #[allow(clippy::disallowed_methods)]
    std::fs::write(&config.cert, "not a certificate").unwrap();
    assert!(!tls.reload_once(), "a corrupt renewal is rejected");
}

/// Kubernetes swaps a mounted Secret by repointing a `..data` symlink.
#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn renewals_follow_a_symlink_swap() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().unwrap();
    let mount = dir.path();
    let version = |name: &str| {
        let versioned = mount.join(name);
        #[allow(clippy::disallowed_methods)]
        std::fs::create_dir(&versioned).unwrap();
        files(&versioned, &ca, "bedrock://kv/c1/n1");
        versioned
    };
    std::os::unix::fs::symlink(version("v1"), mount.join("..data")).unwrap();
    for file in ["tls.crt", "tls.key", "ca.crt"] {
        std::os::unix::fs::symlink(mount.join("..data").join(file), mount.join(file)).unwrap();
    }
    let config = TlsConfig {
        cert: mount.join("tls.crt"),
        key: mount.join("tls.key"),
        ca: mount.join("ca.crt"),
    };
    let tls = Tls::start(&config, identity("kv", "c1", "n1"), &Admin::new())
        .await
        .unwrap();
    std::os::unix::fs::symlink(version("v2"), mount.join("..tmp")).unwrap();
    #[allow(clippy::disallowed_methods)]
    std::fs::rename(mount.join("..tmp"), mount.join("..data")).unwrap();
    assert!(tls.reload_once());
}

fn hub(cell: &str, peers: Arc<Peers>) -> Hub {
    Hub::relay("r1".into(), 1, peers, Vec::new(), Vec::new(), None).with_cell(cell.into())
}

/// Serves `hub`'s relay and sibling services over `tls` on a free local port.
async fn serve(tls: Arc<Tls>, hub: Hub) -> String {
    let addr: SocketAddr = {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        probe.local_addr().unwrap()
    };
    let router = crate::transport::server(Some(&tls))
        .add_service(pb::relay_server::RelayServer::new(hub.clone()))
        .add_service(pb::siblings_server::SiblingsServer::new(siblings::Server(
            hub,
        )));
    tokio::spawn(crate::transport::serve(router, addr, Some(tls)));
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    addr.to_string()
}

async fn watch(peers: &Peers, addr: &str) -> Result<(), tonic::Status> {
    let channel = peers.channel(addr, Class::Control).unwrap();
    let mut stream = pb::siblings_client::SiblingsClient::new(channel)
        .watch(pb::SiblingWatch::default())
        .await?
        .into_inner();
    stream.message().await?;
    Ok(())
}

fn peers(tls: Option<Arc<Tls>>) -> Peers {
    Peers::new(&Admin::new()).with_tls(tls)
}

#[tokio::test(flavor = "multi_thread")]
async fn servers_admit_only_authorized_callers() {
    let ca = Ca::new();
    let relay = tls(&ca, "relay", "c1", "r1").await;
    let addr = serve(relay.clone(), hub("c1", Arc::new(peers(Some(relay))))).await;

    watch(&peers(Some(tls(&ca, "relay", "c1", "r2").await)), &addr)
        .await
        .unwrap();
    for (service, cell) in [("relay", "c2"), ("kv", "c1")] {
        let status = watch(&peers(Some(tls(&ca, service, cell, "x").await)), &addr)
            .await
            .unwrap_err();
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{service} in {cell}: {status}"
        );
    }
    // A certificate from another CA, or no TLS at all, never reaches a handler.
    let foreign = tls(&Ca::new(), "relay", "c1", "r3").await;
    assert_ne!(
        watch(&peers(Some(foreign)), &addr)
            .await
            .unwrap_err()
            .code(),
        Code::Ok
    );
    assert_ne!(
        watch(&peers(None), &addr).await.unwrap_err().code(),
        Code::Ok
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upper_tier_siblings_may_sit_in_different_cells() {
    let ca = Ca::new();
    let relay = tls(&ca, "relay", "c1", "r1").await;
    let dc = Hub::relay(
        "r1".into(),
        2,
        Arc::new(peers(Some(relay.clone()))),
        Vec::new(),
        Vec::new(),
        None,
    )
    .with_cell("c1".into());
    let addr = serve(relay, dc).await;

    watch(&peers(Some(tls(&ca, "relay", "c2", "r2").await)), &addr)
        .await
        .unwrap();
    let status = watch(&peers(Some(tls(&ca, "kv", "c1", "x").await)), &addr)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn clients_reject_servers_that_are_not_relays() {
    let ca = Ca::new();
    let impostor = tls(&ca, "kv", "c1", "n1").await;
    let addr = serve(impostor.clone(), hub("c1", Arc::new(peers(Some(impostor))))).await;
    let status = watch(&peers(Some(tls(&ca, "relay", "c1", "r2").await)), &addr)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unavailable, "{status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_control_peers_require_the_configured_process_identity() {
    let ca = Ca::new();
    let relay = tls(&ca, "relay", "c1", "r1").await;
    let addr = serve(relay.clone(), hub("c1", Arc::new(peers(Some(relay))))).await;
    let client = tls(&ca, "relay", "c1", "r2").await;
    // All these certificates cover the same host; only the configured URI identity is valid.
    for (service, cell, node_id, accepted) in [
        ("relay", "c1", "r1", true),
        ("relay", "c1", "r3", false),
        ("relay", "c2", "r1", false),
        ("api", "c1", "r1", false),
    ] {
        let peers = peers(Some(client.clone())).with_identities(std::collections::BTreeMap::from(
            [(addr.clone(), identity(service, cell, node_id))],
        ));
        assert_eq!(watch(&peers, &addr).await.is_ok(), accepted);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_voters_on_one_host_elect_and_forward_operator_writes_over_tls() {
    use crate::membership::control::{Config, Control, Server};
    use crate::reconciliation::drain::{Drain, Scope, State};
    use crate::storage::memory::MemoryEngine;

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
        let router = crate::transport::server(Some(&tls)).add_service(
            pb::cell_control_server::CellControlServer::new(Server(Some(control.clone()))),
        );
        let listen = address.parse().unwrap();
        tasks.push(actor);
        tasks.push(tokio::spawn(async move {
            crate::transport::serve(router, listen, Some(tls))
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
        assert_eq!(
            client.write(request).await.unwrap().into_inner().revision,
            1
        );
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

#[tokio::test(flavor = "multi_thread")]
async fn a_leaf_may_announce_only_itself() {
    let ca = Ca::new();
    let relay = tls(&ca, "relay", "c1", "r1").await;
    let addr = serve(relay.clone(), hub("c1", Arc::new(peers(Some(relay))))).await;
    let leaf = peers(Some(tls(&ca, "kv", "c1", "n7").await));
    let session = |node_id: &str| {
        let hello = pb::RelayRequest {
            kind: Some(pb::relay_request::Kind::Hello(pb::Hello {
                version: 1,
                tier: 0,
                member: Some(pb::Member {
                    node_id: node_id.into(),
                    service: "kv".into(),
                    cell: "c1".into(),
                    address: "127.0.0.1:7000".into(),
                    ..Default::default()
                }),
                moved: false,
            })),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(hello).unwrap();
        let channel = leaf.channel(&addr, Class::Control).unwrap();
        async move {
            let _keep_open = tx;
            let mut frames = pb::relay_client::RelayClient::new(channel)
                .session(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await?
                .into_inner();
            frames
                .message()
                .await
                .map(|frame| frame.and_then(|f| f.kind))
        }
    };
    let status = session("n8").await.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    assert!(matches!(
        session("n7").await.unwrap(),
        Some(pb::relay_frame::Kind::Welcome(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_write_listener_accepts_callers_without_a_certificate() {
    let ca = Ca::new();
    let api = tls(&ca, "api", "core", "a1").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepted = || async {
        let (tcp, _) = listener.accept().await.unwrap();
        api.accept_optional(tcp).await.map(drop)
    };
    // A node presents its certificate.
    let relay = tls(&ca, "relay", "c1", "r1").await;
    let (dialed, served) = tokio::join!(relay.dial(&addr), accepted());
    dialed.unwrap();
    served.unwrap();
    // Deploy tooling only trusts the CA and authenticates with the bearer token.
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(ca.pem.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connect = async {
        let tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio_rustls::TlsConnector::from(Arc::new(client))
            .connect("127.0.0.1".try_into().unwrap(), tcp)
            .await
    };
    let (dialed, served) = tokio::join!(connect, accepted());
    dialed.unwrap();
    served.unwrap();
}
