//! Node-to-node RPC over tonic (B-03). Modules define their own gRPC services, wrap [`Peers::channel`] in the generated
//! client and bound every unary call and stream read with [`Peers::call`], which also feeds the peer's circuit breaker
//! ([`Peers::health`], B-13).
//!
//! Under madsim, tonic's own timeouts, limits and keepalive do nothing, so timeouts and traffic separation live here
//! where the simulation can test them.
//!
//! With `tls` configured, connections use mutual TLS ([`tls`], B-55) and servers learn each verified [`Caller`].

pub mod tls;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use tokio::io::{AsyncRead, AsyncWrite};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Status};

use crate::admin::Admin;
use crate::health::Health;
use tls::Tls;

/// HTTP/2 pings on idle connections, so a dead peer behind a quiet persistent stream is noticed (B-13).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
/// TLS servers begin a graceful close of older connections, so peers whose calls finish handshake again with current
/// certificates. Without a grace limit, long-lived streams such as relay sessions keep running.
const MAX_CONNECTION_AGE: Duration = Duration::from_secs(24 * 3600);

/// Traffic class. Each class has its own connection per peer, so bulk transfers fill their own HTTP/2 flow-control
/// windows instead of delaying control and replication messages (`cdn.md`, Artifact Transfer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, EncodeLabelValue)]
pub enum Class {
    Control,
    Bulk,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct CallLabels {
    class: Class,
    outcome: Outcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
enum Outcome {
    Ok,
    Error,
    Timeout,
}

/// Outbound connections, one per `(peer, class)`, shared by every caller on the node.
pub struct Peers {
    /// Departed cell members are dropped through [`Peers::forget`] (B-12).
    channels: Mutex<BTreeMap<(String, Class), Channel>>,
    calls: Family<CallLabels, Counter>,
    health: Health,
    tls: Option<Arc<Tls>>,
    identities: BTreeMap<String, tls::Identity>,
}

impl Peers {
    pub fn new(admin: &Admin) -> Self {
        let calls = Family::default();
        admin.register(
            "transport_calls",
            "Outbound unary calls and stream reads by traffic class and outcome",
            calls.clone(),
        );
        Peers {
            channels: Mutex::default(),
            calls,
            health: Health::new(admin),
            tls: None,
            identities: BTreeMap::new(),
        }
    }

    /// Dials peers over mutual TLS when `tls` is set (B-55).
    pub fn with_tls(mut self, tls: Option<Arc<Tls>>) -> Self {
        self.tls = tls;
        self
    }

    /// Pins configured control peers to their certificate identities, including across reconnects.
    pub fn with_identities(mut self, identities: BTreeMap<String, tls::Identity>) -> Self {
        self.channels.get_mut().unwrap().clear();
        self.identities = identities;
        self
    }

    /// Per-peer circuit breakers fed by [`Peers::call`]; route through [`Health::route`].
    pub fn health(&self) -> &Health {
        &self.health
    }

    /// The connection to `addr` (`host:port`) for `class`. It connects on first use, reconnects after failures and is
    /// reused by every later caller.
    pub fn channel(&self, addr: &str, class: Class) -> Result<Channel, tonic::transport::Error> {
        let mut channels = self.channels.lock().unwrap();
        if let Some(channel) = channels.get(&(addr.to_owned(), class)) {
            return Ok(channel.clone());
        }
        // The scheme only labels requests; with TLS the connector encrypts.
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))?
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true);
        let channel = match &self.tls {
            Some(tls) => tls.channel(endpoint, self.identities.get(addr).cloned()),
            None => endpoint.connect_lazy(),
        };
        channels.insert((addr.to_owned(), class), channel.clone());
        Ok(channel)
    }

    /// Drops `addr`'s connections and breaker, e.g. once it left the cell's membership. Later use starts afresh.
    pub fn forget(&self, addr: &str) {
        self.channels
            .lock()
            .unwrap()
            .retain(|(peer, _), _| peer != addr);
        self.health.forget(addr);
    }

    /// Fails `fut` (a unary call or one `Streaming::message` to `addr`) with `DEADLINE_EXCEEDED` after `timeout`, counts
    /// it and reports it to `addr`'s breaker.
    pub async fn call<T>(
        &self,
        addr: &str,
        class: Class,
        timeout: Duration,
        fut: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let mut probe = Probe {
            health: &self.health,
            peer: addr,
            owned: self.health.begin(addr, timeout),
        };
        #[cfg(madsim)]
        let fut = crate::sim::around(addr, fut);
        let (outcome, result) = match tokio::time::timeout(timeout, fut).await {
            Ok(Ok(v)) => (Outcome::Ok, Ok(v)),
            Ok(Err(status)) => (Outcome::Error, Err(status)),
            Err(_) => (
                Outcome::Timeout,
                Err(Status::deadline_exceeded(format!(
                    "no response within {timeout:?}"
                ))),
            ),
        };
        self.calls
            .get_or_create(&CallLabels { class, outcome })
            .inc();
        self.health.record(
            addr,
            probe.owned.take(),
            !result.as_ref().is_err_and(peer_failed),
        );
        result
    }
}

/// A call's claim on its peer's probe, given back if the call is dropped before it has an outcome.
struct Probe<'a> {
    health: &'a Health,
    peer: &'a str,
    owned: Option<Arc<()>>,
}

impl Drop for Probe<'_> {
    fn drop(&mut self) {
        if let Some(probe) = &self.owned {
            self.health.abandon(self.peer, probe);
        }
    }
}

/// A byte stream from [`Peers::dial`].
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

/// A gRPC server builder; with TLS, connections start a graceful close after [`MAX_CONNECTION_AGE`]. Serve it with
/// [`serve`].
pub fn server(tls: Option<&Arc<Tls>>) -> Server {
    let server = Server::builder();
    match tls {
        Some(_) => server.max_connection_age(MAX_CONNECTION_AGE),
        None => server,
    }
}

/// Serves `router` on `addr`, over mutual TLS when `tls` is set.
pub async fn serve(
    router: tonic::transport::server::Router,
    addr: std::net::SocketAddr,
    tls: Option<Arc<Tls>>,
) -> Result<(), String> {
    match tls {
        Some(tls) => tls.serve(router, addr).await,
        None => router.serve(addr).await.map_err(|e| e.to_string()),
    }
}

/// The node a verified client certificate names (B-55). TLS servers put it in every request's extensions; plaintext
/// servers have none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub service: String,
    pub cell: String,
    pub node_id: String,
    /// The certificate's DNS and IP SANs.
    pub hosts: Vec<String>,
}

impl Caller {
    pub fn of<T>(request: &tonic::Request<T>) -> Option<&Caller> {
        request.extensions().get::<Caller>()
    }

    /// Fails unless the caller runs one of `services`, in `cell` when given.
    pub fn require(&self, services: &[&str], cell: Option<&str>) -> Result<(), Status> {
        if !services.contains(&self.service.as_str()) || cell.is_some_and(|cell| cell != self.cell)
        {
            return Err(Status::permission_denied(format!(
                "{} {} in cell {} may not call this",
                self.service, self.node_id, self.cell
            )));
        }
        Ok(())
    }

    /// Whether `host` (a DNS name or IP address) is one of the certificate's SANs.
    pub fn has_host(&self, host: &str) -> bool {
        match host.parse::<std::net::IpAddr>() {
            Ok(ip) => self.hosts.iter().any(|h| h.parse() == Ok(ip)),
            Err(_) => self.hosts.iter().any(|h| h.eq_ignore_ascii_case(host)),
        }
    }
}

/// `bedrock://{service}/{cell}/{node_id}`, the URI SAN that names a node, as `(service, cell, node_id)`.
pub fn parse_identity(uri: &str) -> Option<(&str, &str, &str)> {
    let mut parts = uri.strip_prefix("bedrock://")?.split('/');
    let identity = (parts.next()?, parts.next()?, parts.next()?);
    let plain = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    };
    (parts.next().is_none() && plain(identity.0) && plain(identity.1) && plain(identity.2))
        .then_some(identity)
}

/// The host of `host:port`, without IPv6 brackets.
pub fn host_of(addr: &str) -> &str {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Whether `status` means the peer failed, rather than answered the request with an error (`NOT_FOUND`, a rejected
/// bucket, ...).
fn peer_failed(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Unknown
            | Code::Internal
            | Code::ResourceExhausted
            | Code::DataLoss
    )
}

#[cfg(test)]
#[path = "../tests/unit/transport.rs"]
mod tests;
