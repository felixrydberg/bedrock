//! Node-to-node RPC over tonic (B-03). Modules define their own gRPC services, wrap [`Peers::channel`] in the generated
//! client and bound every unary call and stream read with [`Peers::call`].
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
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use admin::Admin;

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
        }
    }

    /// The connection to `addr` (`host:port`) for `class`. It connects on first use, reconnects after failures and is
    /// reused by every later caller.
    pub fn channel(&self, addr: &str, class: Class) -> Result<Channel, tonic::transport::Error> {
        let mut channels = self.channels.lock().unwrap();
        if let Some(channel) = channels.get(&(addr.to_owned(), class)) {
            return Ok(channel.clone());
        }
        let channel = Endpoint::from_shared(format!("http://{addr}"))?.connect_lazy();
        channels.insert((addr.to_owned(), class), channel.clone());
        Ok(channel)
    }

    /// Fails `fut` (a unary call or one `Streaming::message`) with `DEADLINE_EXCEEDED` after `timeout`, and counts it.
    pub async fn call<T>(
        &self,
        class: Class,
        timeout: Duration,
        fut: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
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
        result
    }
}
