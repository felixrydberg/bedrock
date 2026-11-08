//! All simulated network failure (#25 / B-23). Madsim's tonic channels are reliable, so a clogged link only stalls: that
//! exercises the real `Peers::call` timeouts, but a call never fails on its own. Errors come from [`hook`], and the
//! disruptions madsim does offer are scheduled by the functions below.

use std::sync::Arc;
use std::time::Duration;

use bedrock_lib::sim::RpcFuture;
use bedrock_lib::storage::sim::Fingerprint;
use madsim::net::NetSim;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use madsim::task::NodeId;
use tokio::time::sleep;
use tonic::Status;

/// What happens to one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcFault {
    /// The call fails and the peer never sees it.
    RequestLost,
    /// The peer handles the call, but the caller only learns that it failed.
    ResponseLost,
    Delay(Duration),
}

/// Whether the next call fails; the harness draws this from its seed and counts it.
pub type Strikes = Arc<dyn Fn() -> bool>;

fn draw() -> RpcFault {
    let mut rng = thread_rng();
    match rng.gen_range(0..3) {
        0 => RpcFault::RequestLost,
        1 => RpcFault::ResponseLost,
        _ => RpcFault::Delay(Duration::from_millis(rng.gen_range(10..2_000))),
    }
}

/// The hook for `bedrock_lib::sim::Hooks::rpc`. Rates are per destination and shared by every caller: madsim's task
/// context does not expose the calling node. A lost response is returned by value, so concurrent calls to one peer
/// cannot swap decisions.
pub fn hook(strikes: Strikes, fingerprint: Fingerprint) -> impl Fn(&str) -> RpcFuture {
    move |addr| {
        let fault = strikes().then(draw);
        fingerprint.mix((addr, fault));
        Box::pin(async move {
            match fault {
                None => Ok(None),
                Some(RpcFault::RequestLost) => Err(Status::unavailable("request lost")),
                Some(RpcFault::ResponseLost) => Ok(Some(Status::unavailable("response lost"))),
                Some(RpcFault::Delay(delay)) => {
                    sleep(delay).await;
                    Ok(None)
                }
            }
        })
    }
}

/// Cuts `a` from `b` for `time`, in one direction if `one_way`.
pub async fn partition(a: NodeId, b: NodeId, one_way: bool, time: Duration) {
    let net = NetSim::current();
    net.clog_link(a, b);
    if !one_way {
        net.clog_link(b, a);
    }
    sleep(time).await;
    net.unclog_link(a, b);
    net.unclog_link(b, a);
}

/// Stalls `id` for `time`, as a long garbage-collection or I/O pause would.
pub async fn pause(id: NodeId, time: Duration) {
    let handle = Handle::current();
    handle.pause(id);
    sleep(time).await;
    handle.resume(id);
}

/// Kills `id` and starts it again once its sockets are released, which takes the old tasks a moment to drop.
pub async fn bounce(id: NodeId) {
    let handle = Handle::current();
    handle.kill(id);
    sleep(Duration::from_millis(10)).await;
    handle.restart(id);
}
