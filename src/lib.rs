#![deny(missing_docs)]
//! A simple key/value store
mod engines;
mod error;
mod client;
mod server;
mod common;
pub mod thread_pool;

pub use error::{Result, KvsError};
pub use client::KvsClient;
pub use server::KvsServer;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};