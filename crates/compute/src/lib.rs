//! `bedrock-compute`: Compute hosts (`statefull/v2.md` §3, §9). It depends only on `bedrock-lib`. It holds the actor
//! host and the volume client of stateful actors (#112 / S-04, #115 / S-07) and the one execution seam ([`runtime`]).
//! The binary and its relay membership follow once #114 / S-06 lands (user decision 2026-11-17); the sandbox is S-10.

pub mod actors;
pub mod runtime;
