//! kv-node library surface: the real driver and its plumbing around the pure
//! `raft-core`. The binary in `main.rs` is a thin CLI wrapper; integration
//! tests drive these modules directly.

pub mod application;
pub mod config;
pub mod crc;
pub mod driver;
pub mod durability;
pub mod http;
pub mod kv;
pub mod shutdown;
pub mod snapshot;
pub mod storage;
pub mod transport;

pub mod applied_store;

pub mod admin;
pub mod bootstrap;
pub mod membership_runtime;
