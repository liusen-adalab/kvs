use std::{cell::RefCell, collections::HashMap};

pub struct KvStore {
    map: RefCell<HashMap<String, String>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: RefCell::new(HashMap::new()),
        }
    }
    pub fn set(&self, key: String, value: String) {
        self.map.borrow_mut().insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        if let Some(value) = self.map.borrow().get(&key) {
            Some(value.to_owned())
        }else {
            None
        }
    }

    pub fn remove(&self, key: String) {
        self.map.borrow_mut().remove(&key);
    }
}
