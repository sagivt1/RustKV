use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};


// Represents the commands that can be written to the log.
// This allows us to rebuild the state of the KvStore by replaying the log.
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set {key : String, value : String},
    Remove {key: String}
}

/// A simple, persistent, thread-safe key-value store.
///
/// It stores key-value pairs in memory for fast lookups and appends every
/// write operation to a log file on disk to ensure durability. The log is replayed
/// on startup to restore the in-memory state.
///
/// Cloning is a cheap, lightweight operation as it only increments an atomic reference count.
#[derive(Clone)]
pub struct KvStore {
    // The in-memory cache of key-value pairs for fast reads.
    map: Arc<RwLock<HashMap<String, String>>>,
    // The writer for the on-disk write-ahead log (WAL).
    // A Mutex is used to ensure that writes to the log are sequential.
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl KvStore {
    /// Opens a `KvStore` and loads its data from the given path.
    /// If the log file doesn't exist, it will be created.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // Clone the file handle for a separate writer. This allows us to read and write
        // to the same file concurrently (reading for startup, writing for operations).
        let writer = BufWriter::new(file.try_clone()?);

        let map = Arc::new(RwLock::new(HashMap::new()));
        
        let reader = BufReader::new(File::open(&path)?);
        
        // Replay the write-ahead log to restore the in-memory state.
        Self::load(reader, &map)?;

        Ok(KvStore{
            map,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    // Rebuilds the in-memory map by reading and applying all commands from the log file.
    fn load(mut reader: BufReader<File>, map: &Arc<RwLock<HashMap<String, String>>>) -> Result<()> {
        // A write lock is held during the entire load process to prevent any other access.
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
                        // `UnexpectedEof` is a normal condition, indicating the end of the log file.
                        // Any other I/O error during deserialization is a corruption issue.
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

    /// Sets a key-value pair.
    ///
    /// This operation is persisted to the on-disk log before updating the in-memory map.
    pub fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {key: key.clone(), value: value.clone()};
        
        {
            // Lock the writer, serialize the command, and flush to disk.
            // This implements the write-ahead log (WAL) pattern for durability.
            // The lock is released immediately after the write.
            let mut writer = self.writer.lock().map_err(|_| KvsError::Internal("Mutex poisoned".into()))?;
            bincode::serialize_into(&mut *writer, &cmd)?;
            writer.flush()?;
        }

        let mut map = self.map.write().map_err(|_| KvsError::Internal("RwLock poisoned".into()))?;
        map.insert(key, value);

        Ok(())
    }

    /// Gets the value associated with a key.
    ///
    /// Returns `None` if the key is not found. Reads are served from the in-memory
    /// map for high performance.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        // Acquire a read lock, which allows for concurrent reads.
        let map = self.map.read().map_err(|_| KvsError::Internal("RwLock poisoned".into()))?;
        Ok(map.get(&key).cloned())
    }

    /// Removes a key-value pair.
    ///
    /// Errors if the key does not exist. This operation is persisted to the log.
    pub fn remove(&self, key: String) -> Result<()> {
        let cmd = Command::Remove {key: key.clone()};

        {
            // Similar to `set`, log the removal command first for durability.
            let mut writer = self.writer.lock().map_err(|_| KvsError::Internal("Mutex poisoned".into()))?;
            bincode::serialize_into(&mut *writer, &cmd)?;
            writer.flush()?;
        }

        // Enforce that the key must exist for a remove operation to be valid.
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
