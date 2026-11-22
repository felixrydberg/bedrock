//! The host's `Actors` service (#116 / S-08, `statefull/v2.md` §6 step 4): a cell Gateway delivers an event here, and
//! [`ActorHost::acquire`] serves the running actor, claims and wakes it, or names the live holder in a redirect.

use bedrock_lib::datasets::capabilities::GATEWAY;
use bedrock_lib::transport::Caller;
use bedrock_lib::volume as pb;
use tonic::{Request, Response, Status};

use super::host::ActorHost;
use crate::runtime::Error;

pub struct Actors {
    host: ActorHost,
    /// This host, as its claims name it.
    holder: pb::Holder,
}

impl Actors {
    pub fn new(host: ActorHost, holder: pb::Holder) -> Self {
        Self { host, holder }
    }
}

/// A failure once this host took the event, carried in the reply so no Gateway re-sends it. A volume failure or lost
/// lease is retryable by the caller; the event may have committed.
fn failed(holder: &pb::Holder, error: Error) -> pb::InvokeReply {
    let status = match error {
        Error::Redirect(_) => unreachable!("redirects are answers"),
        Error::Volume(error) => Status::unavailable(format!("volume: {error}")),
        Error::Unsupported(detail) => Status::unimplemented(detail),
        Error::Handler(detail) => Status::aborted(detail),
    };
    pb::InvokeReply {
        holder: Some(holder.clone()),
        code: status.code() as i32,
        message: status.message().to_owned(),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl pb::actors_server::Actors for Actors {
    async fn invoke(
        &self,
        request: Request<pb::InvokeRequest>,
    ) -> Result<Response<pb::InvokeReply>, Status> {
        // Only its own cell's Gateways deliver events here.
        if let Some(caller) = Caller::of(&request) {
            caller.require(&[GATEWAY], Some(&self.holder.cell))?;
        }
        let request = request.into_inner();
        let actor = request
            .actor
            .ok_or_else(|| Status::invalid_argument("missing actor"))?;
        let reply = match self.host.acquire(&actor).await {
            Ok(awake) => match awake.invoke(request.payload).await {
                Ok(payload) => pb::InvokeReply {
                    holder: Some(self.holder.clone()),
                    payload,
                    ..Default::default()
                },
                Err(error) => failed(&self.holder, error),
            },
            Err(Error::Redirect(holder)) => pb::InvokeReply {
                holder: Some(holder),
                redirect: true,
                ..Default::default()
            },
            Err(error) => failed(&self.holder, error),
        };
        Ok(Response::new(reply))
    }
}
