use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::Result;

/// A thread-safe, in-memory key-value store that wraps a `HashMap`.
///
/// It uses `Arc<RwLock<...>>` to allow for concurrent, thread-safe access.
/// Cloning is a cheap operation as it only increments the atomic reference count.
#[derive(Clone, Default)]
pub struct KvStore {
    map: Arc<RwLock<HashMap<String, String>>>,
}

impl KvStore {

    /// Creates a new, empty `KvStore`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the value of a string key to a string.
    pub fn set(&self, key: String, value: String) -> Result<()> {
        // Acquire a write lock. This will block until no other read or write locks are held.
        let mut map = self.map.write().map_err(|_| {
            // If another thread panics while holding the lock, it becomes "poisoned".
            // We treat this as an internal error rather than trying to recover.
            crate::KvsError::Internal("RwLock poisoned".into())
        })?;

        map.insert(key, value);
        Ok(())
    }

    /// Gets the string value of a given string key.
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        // Acquire a read lock, allowing multiple concurrent readers.
        let map = self.map.read().map_err(|_| {
            crate::KvsError::Internal("RwLock poisoned".into())
        })?;
        Ok(map.get(&key).cloned())
    }

    /// Removes a given key.
    pub fn remove(&self, key: String) -> Result<()> {
        let mut map = self.map.write().map_err(|_| {
            crate::KvsError::Internal("RwLock poisoned".into())
        })?;

        if map.remove(&key).is_none() {
            return Err(crate::KvsError::KeyNotFound);
            // Explicitly return an error if the key does not exist.
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;

    #[test]
    fn test_thread_saftey() {

        let store = KvStore::new();
        let mut handles = vec![];

        // Spawn 10 threads to perform concurrent writes.
        for i in 0..10 {
            let store_clone = store.clone();
            let handel = thread::spawn(move || {
                store_clone.set(format!("Key{}", i), format!("value{}", i)).unwrap();
            });
            handles.push(handel);
        }
        
        // Wait for all threads to complete their work.
        for handel in handles {
            handel.join().unwrap();
        }

        // Verify that all data was written correctly.
        for i in 0..10 {
            assert_eq!(store.get(format!("Key{}", i)).unwrap(), Some(format!("value{}", i)));
        } 
    }
}
