use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};


#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set {key : String, value : String},
    Remove {key: String}
}

/// A thread-safe, in-memory key-value store that wraps a `HashMap`.
///
/// It uses `Arc<RwLock<...>>` to allow for concurrent, thread-safe access.
/// Cloning is a cheap operation as it only increments the atomic reference count.
#[derive(Clone)]
pub struct KvStore {
    map: Arc<RwLock<HashMap<String, String>>>,
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl KvStore {
    
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let writer = BufWriter::new(file.try_clone()?); // Clone file handle for writing

        let map = Arc::new(RwLock::new(HashMap::new()));
        

        let reader = BufReader::new(File::open(&path)?);

        Self::load(reader, &map)?;

        Ok(KvStore{
            map,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    fn load(mut reader: BufReader<File>, map: &Arc<RwLock<HashMap<String, String>>>) -> Result<()> {
        let mut map_guard = map.write().map_err(|_| KvsError::Internal("RwLock poisoned".into()))?;

        loop {
            
            let cmd: std::result::Result<Command, _> = bincode::deserialize_from(&mut reader);

            match cmd {
                Ok(Command::Set {key, value}) => {
                    map_guard.insert(key, value);
                }
                Ok(Command::Remove {key}) => {
                    map_guard.remove(&key);
                }
                Err(e) => {
                    if let bincode::ErrorKind::Io(ref io_err) = *e {
                        if io_err.kind() == io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(KvsError::from(e));
                }
            } 
        }
        Ok(())
    }

    /// Sets the value of a string key to a string.
    pub fn set(&self, key: String, value: String) -> Result<()> {
        // Acquire a write lock. This will block until no other read or write locks are held.
        let cmd = Command::Set {key: key.clone(), value: value.clone()};
        
        {
            let mut writer = self.writer.lock().map_err(|_| KvsError::Internal("Mutex poisoned".into()))?;
            bincode::serialize_into(&mut *writer, &cmd)?;
            writer.flush()?;
        }

        let mut map = self.map.write().map_err(|_| KvsError::Internal("RwLock poisoned".into()))?;
        map.insert(key, value);

        Ok(())
    }

    /// Gets the string value of a given string key.
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let map = self.map.read().map_err(|_| KvsError::Internal("RwLock poisoned".into()))?;
        Ok(map.get(&key).cloned())
    }

    /// Removes a given key.
    pub fn remove(&self, key: String) -> Result<()> {
        let cmd = Command::Remove {key: key.clone()};

        {
            let mut writer = self.writer.lock().map_err(|_| KvsError::Internal("Mutex poisoned".into()))?;
            bincode::serialize_into(&mut *writer, &cmd)?;
            writer.flush()?;
        }

        let mut map = self.map.write().map_err(|_| KvsError::Internal("RwLock poisoned".into()))?;
        if map.remove(&key).is_none() {
            return Err(KvsError::KeyNotFound);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::thread;

    #[test]
    fn test_crud() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path().join("db.kvs")).expect("unable to open store");

        store.set("key1".to_owned(), "value1".to_owned()).unwrap();
        store.set("key2".to_owned(), "value2".to_owned()).unwrap();

        assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
        assert_eq!(store.get("key2".to_owned()).unwrap(), Some("value2".to_owned()));

        store.remove("key1".to_owned()).unwrap();
        assert_eq!(store.get("key1".to_owned()).unwrap(), None);
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kvs.db");

        {
            // 1. Open a store
            let store = KvStore::open(&db_path).unwrap();
            // 2. Set key 'foo' to 'bar'
            store.set("foo".to_owned(), "bar".to_owned()).unwrap();
        }

        // 4. Open a new store at the same path.
        let new_store = KvStore::open(&db_path).unwrap();
        // 5. Assert 'foo' is still 'bar'.
        assert_eq!(new_store.get("foo".to_owned()).unwrap(), Some("bar".to_owned()));
    }

    #[test]
    fn test_cuncurrent_writes() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path().join("db.kvs")).unwrap();

        let mut handles = vec![];

        for i in 0..10 {
            let store_clone = store.clone();
            let handle = thread::spawn(move || {
                store_clone.set(format!("key{}", i), format!("value{}", i)).unwrap();
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let store_reloaded = KvStore::open(temp_dir.path().join("db.kvs")).unwrap();

        for i in 0..10 {
            assert_eq!(
                store_reloaded.get(format!("key{}", i)).unwrap(),
                Some(format!("value{}", i))
            );
        }
    }
}
