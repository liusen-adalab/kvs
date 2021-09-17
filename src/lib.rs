#![deny(missing_docs)]
//! A simple key/value store
mod kv;
mod error;

pub use error::{Result, KvsError};
pub use kv::KvStore;