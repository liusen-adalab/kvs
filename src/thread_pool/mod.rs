//! This module provide various thread pools. All thread pool should implement
//! the `ThreadPool` trait

mod rayon;
mod shared_queue;
mod naive;

pub use self::naive::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;
use crate::Result;

/// The trait that all thread pool should implement.
pub trait ThreadPool {
    /// Create a thread pool. Immediately spawn the specify number of threads
    ///
    /// Return an error if any thread fails to spawn.
    /// All previously-spawned threads are terminated.
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// Spawns a function into the thread pool.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
