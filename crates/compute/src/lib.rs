//! `bedrock-compute`: Compute hosts (`statefull/v2.md` §3, §9). It depends only on `bedrock-lib`. It holds the actor
//! host and the volume client of stateful actors (#112 / S-04, #115 / S-07) and the one execution seam ([`runtime`]).
//! [`bindings`] sends a Worker's actor calls to the cell Gateway, and [`actors::serve`] takes them from it (#116 / S-08).
//! The binary and its relay membership as `compute` are a follow-up (user decisions 2026-11-17 and 2026-11-22); the
//! sandbox is S-10.

pub mod actors;
pub mod bindings;
pub mod runtime;
