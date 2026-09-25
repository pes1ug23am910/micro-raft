//! kv-node library surface: the real driver and its plumbing around the pure
//! `raft-core`. The binary in `main.rs` is a thin CLI wrapper; integration
//! tests drive these modules directly.

pub mod crc;
pub mod driver;
pub mod http;
pub mod kv;
pub mod storage;
pub mod transport;
