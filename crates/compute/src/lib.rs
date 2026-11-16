//! `bedrock-compute`: Compute hosts (`statefull/v2.md` §3, §9). It depends only on `bedrock-lib`. So far it holds the
//! volume client of stateful actors (#112 / S-04); the actor host, the binary and the sandbox follow in S-07.

pub mod actors;
