//! Node-to-node RPC over tonic (B-03). Modules define their own gRPC services, wrap [`Peers::channel`] in the generated
//! client and bound every unary call and stream read with [`Peers::call`], which also feeds the peer's circuit breaker
//! ([`Peers::health`], B-13).
//!
//! Under madsim, tonic's own timeouts, limits and keepalive do nothing, so timeouts and traffic separation live here
//! where the simulation can test them.

pub mod admin;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::health::Health;
use admin::Admin;

/// HTTP/2 pings on idle connections, so a dead peer behind a quiet persistent stream is noticed (B-13).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

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
    // ponytail: entries are never evicted; drop departed peers once membership exists (B-12) if churn makes this grow.
    channels: Mutex<BTreeMap<(String, Class), Channel>>,
    calls: Family<CallLabels, Counter>,
    health: Health,
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
        }
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
        let channel = Endpoint::from_shared(format!("http://{addr}"))?
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect_lazy();
        channels.insert((addr.to_owned(), class), channel.clone());
        Ok(channel)
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
        self.health.begin(addr, timeout);
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
        self.health
            .record(addr, !result.as_ref().is_err_and(peer_failed));
        result
    }
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
mod tests {
    use super::*;
    use crate::health::{CONSECUTIVE_FAILURES, State};

    #[tokio::test]
    async fn peer_failures_feed_the_breaker() {
        let peers = Peers::new(&Admin::new());
        let timeout = Duration::from_secs(1);
        for _ in 0..CONSECUTIVE_FAILURES {
            let _ = peers
                .call::<()>("a", Class::Control, timeout, async {
                    Err(Status::unavailable("down"))
                })
                .await;
            // An answer that is an error is not a peer failure.
            let _ = peers
                .call::<()>("b", Class::Control, timeout, async {
                    Err(Status::not_found("no key"))
                })
                .await;
        }
        assert_eq!(peers.health().state("a"), State::Open);
        assert_eq!(peers.health().state("b"), State::Closed);
        assert_eq!(peers.health().route(&["a", "b"]), [&"b"]);
    }
}
