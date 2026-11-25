//! The host's `Actors` service (#116 / S-08, `statefull/v2.md` §6 step 4): a cell Gateway delivers an event here, and
//! [`ActorHost::acquire`] serves the running actor, claims and wakes it, or names the live holder in a redirect.

use bedrock_lib::datasets::capabilities::GATEWAY;
use bedrock_lib::transport::Caller;
use bedrock_lib::volume as pb;
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::host::ActorHost;
use crate::runtime::Error;

pub struct Actors {
    host: ActorHost,
    /// This host, as its claims name it.
    holder: pb::Holder,
    kv: Option<Arc<bedrock_lib::kv::DirectoryView>>,
}

impl Actors {
    pub fn new(host: ActorHost, holder: pb::Holder) -> Self {
        Self {
            host,
            holder,
            kv: None,
        }
    }

    /// Enables namespace administrative lifecycle admission against the authoritative directory.
    pub fn with_kv(mut self, directory: Arc<bedrock_lib::kv::DirectoryView>) -> Self {
        self.kv = Some(directory);
        self
    }

    async fn admit_purge(
        &self,
        actor: &pb::Actor,
        request: &bedrock_lib::kv::Request,
    ) -> Result<(), Status> {
        use bedrock_lib::{actors, kv};
        let directory = self
            .kv
            .as_ref()
            .ok_or_else(|| Status::unimplemented("KV lifecycle admission is not configured"))?;
        if request.operation != Some(kv::request::Operation::Purge(true)) {
            return Err(Status::invalid_argument("invalid purge request"));
        }
        let record = directory
            .record(&actor.namespace, request.directory_min_revision)
            .await?;
        if request.tenant != record.tenant {
            return Err(Status::permission_denied(
                "namespace belongs to another tenant",
            ));
        }
        if request.purge_grant.len() != 32
            || record.purge_grant_hash.is_empty()
            || kv::purge_grant_hash(&request.purge_grant) != record.purge_grant_hash
        {
            return Err(Status::permission_denied(
                "namespace-management purge grant required",
            ));
        }
        const GRACE_MS: u64 = 20 * 60 * 1000;
        if record.state != kv::Lifecycle::Deleting as i32
            || record.deleting_at == 0
            || record.deleting_at.saturating_add(GRACE_MS) > super::storage::now_ms()
        {
            return Err(Status::failed_precondition(
                "namespace purge grace has not elapsed",
            ));
        }
        let (topology, _) = actors::Topology::read(&directory.client, &record.home_region, 0)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let regions = std::collections::BTreeMap::from([(record.home_region.clone(), topology)]);
        let canonical = kv::actor(&regions, &record.home_region, &record.id)
            .map_err(|e| Status::data_loss(e.to_string()))?;
        if *actor != canonical {
            return Err(Status::permission_denied("noncanonical KV actor identity"));
        }
        Ok(())
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
        if actor.kind == bedrock_lib::actors::Kind::Kv as i32
            && let Ok(kv) = bedrock_lib::kv::Request::decode(request.payload.as_slice())
            && matches!(
                kv.operation,
                Some(bedrock_lib::kv::request::Operation::Purge(_))
            )
        {
            if let Err(error) = self.admit_purge(&actor, &kv).await {
                return Ok(Response::new(pb::InvokeReply {
                    holder: Some(self.holder.clone()),
                    code: error.code() as i32,
                    message: error.message().into(),
                    ..Default::default()
                }));
            }
            let reply = match self.host.delete(&actor).await {
                Ok(()) => pb::InvokeReply {
                    holder: Some(self.holder.clone()),
                    payload: bedrock_lib::kv::Reply::default().encode_to_vec(),
                    ..Default::default()
                },
                Err(Error::Redirect(holder)) => pb::InvokeReply {
                    holder: Some(holder),
                    redirect: true,
                    ..Default::default()
                },
                Err(error) => failed(&self.holder, error),
            };
            return Ok(Response::new(reply));
        }
        let reply = match self.host.acquire(&actor).await {
            Ok(awake) => match match request.alarm_ms {
                Some(at_ms) => awake.alarm(at_ms).await.map(|()| Vec::new()),
                None => awake.invoke(request.payload).await,
            } {
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
