mod kvs;
mod sled;
use crate::Result;

/// Trait for key value storage engine
pub trait KvsEngine: Clone + Send + 'static{
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exsists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Gets the string value of the given string key
    ///
    /// Returns `None` if the given key does not exsist.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a given key
    ///
    /// # Errors
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    fn remove(&self, key: String) -> Result<()>;
}

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
