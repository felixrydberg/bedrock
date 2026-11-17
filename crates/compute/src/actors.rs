//! Stateful actors on a Compute host (`statefull/v2.md` §5, §9). [`volume`] is the holder's client of an actor's three
//! `bedrock-volume` slots (#112 / S-04); [`host`] keeps one instance per actor and [`storage`] gives it key/value and
//! SQLite storage over the client (#115 / S-07).

pub mod host;
pub mod storage;
pub mod volume;
