# S-18 namespace admission model

`kv_admission.tla` models #126 / S-18's purge authorization and directory revision
floors above the existing authorized CAS and Actors transport. The namespace
already exists at abstract revision 0; revision 1 marks it `Deleting`, and
revision 2 publishes the management workflow's purge grant hash. Grace passage is
one abstract step. The two clients are authenticated namespace management and an
ordinary binding. Tenant and canonical actor-ID checks are Boolean abstractions.

Only the bearer-authenticated namespace-management workflow issues a grant,
through the authorized directory CAS after deletion grace. Production creates a
secret random nonce, publishes its SHA-256 hash in the `Deleting` record, and
sends the nonce and publication minimum revision through Actors. Compute checks
the hash, tenant, state, grace and canonical actor ID before deletion. The model
represents possession with `grantIssued` and the management client identity;
reading a directory snapshot never grants possession. It assumes an ordinary
binding cannot recover a nonce from its published hash. It does not prove
cryptography, bearer authentication, random generation, wire validation, CAS or
transport authorization; Rust checks cover those boundaries.

`knownFloor` represents the shared `DirectoryView`'s observed infrastructure
revision. Every successful directory read must be at least the maximum of that
floor and the caller's minimum. Only the returned, observed revision can raise
the shared floor. A failed read cannot advance it; a caller's request alone does
not publish progress. Management purge requests carry at least the grant's
publication revision. `Attempt` combines a successful directory read and its
admission decision, so floor checks apply at that read, without asserting a
barrier against concurrent future observations.

`PurgeAuthorized` requires every accepted purge to have the issued secret grant,
its matching published record, tenant, `Deleting` state, elapsed grace, canonical
actor ID and publication minimum. `KnownDeletionFences` requires successful
directory results to meet both observed and caller floors. An `Active` result at
revision 0 can therefore still be admitted after an unobserved deletion when
neither floor requires revision 1. That ordinary propagation delay is allowed;
this model adds no authoritative global read barrier or replication protocol.
There is no liveness claim, data deletion protocol, grant rotation, namespace
recreation or multi-namespace interaction in this model.

`kv_admission.cfg` has two clients and revisions 0 through 2. The deliberate
`admin-bypass` bug accepts purge without grant verification and must violate
`PurgeAuthorized`; `stale-floor` accepts directory results below the required
floor and must violate `KnownDeletionFences`. Both are registered in
`scripts/tlc.sh`. Run
`TLC_WORKERS=1 TLC_HEAP=4g scripts/tlc.sh kv_admission` for the normal model and
both isolated bug checks. On 2026-11-24 it passed all 8 reachable states at depth 5
with one worker and a 4 GiB heap; both bugs broke their named invariants.
