use std::collections::HashMap;

/// The `KvStore` store string key/value pairs
/// 
/// key/value pairs are stored in a `HashMap` in memory and not persisted to disk
/// 
/// Example:
/// ```rust 
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let value = store.get("key".to_owned());
/// assert_eq!(value, Some("value".to_owned()));
pub struct KvStore {
    map: HashMap<String, String>
}

impl KvStore {
    /// Creates a KvStore
    pub fn new() -> Self {
        Self {
            map: HashMap::new()
        }
    }

    /// Sets the string value of a string key to a string
    ///
    /// If the key already exsist, the previous value will be overwritten
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Gets the string value of a string key
    /// 
    /// Returns `None` if given string does not exsist
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// remove the given key
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
