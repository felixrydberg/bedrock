//! Stateful actors on a Compute host (`statefull/v2.md` §5, §9). [`volume`] is the holder's client of an actor's three
//! `bedrock-volume` slots (#112 / S-04); [`host`] keeps one instance per actor and [`storage`] gives it key/value and
//! SQLite storage over the client (#115 / S-07); [`serve`] takes events from the cell's Gateways (#116 / S-08).

pub mod host;
pub mod serve;
pub mod storage;
pub mod volume;
