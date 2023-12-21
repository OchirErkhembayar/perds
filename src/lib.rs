#![warn(missing_docs, clippy::overflow_check_conditional)]

//! Prototyping
//! I am starting off an initial prototype to only
//! work with [HashMaps](std::collections::HashMap) and will
//! move on to other data structures later
//!
//! TODO:
//! - Error handling, currently there are just units for Ok and Err variants of Result
//! - Format for the append only file
//!   - Representing data types and their sizes
//!   - Representing cut off for when the state snapshots were taken
//! - How to compress the file
//! - ...
//!

use std::{
    any::TypeId,
    collections::HashMap,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::mpsc::{channel, Sender},
    thread,
    thread::JoinHandle,
};

/// The persistent container for a std library collection type
#[derive(Debug)]
pub struct Perds<K, V> {
    strategy: Strategy,
    inner: Data<K, V>,
}

/// The representation of the persistence mechanism of the
/// inner data structure
#[derive(Debug)]
struct Data<K, V> {
    task: JoinHandle<()>,
    inner: HashMap<K, V>,
    tx: Sender<Cmd>,
    writer: BufWriter<File>,
}

/// The persistence strategy for a Perds instance
#[derive(Debug, PartialEq, Clone)]
pub enum Strategy {
    /// A Perds with this strategy will not persist
    InMemory,
    /// Save on every state change
    Stream(PathBuf),
    /// Save only when calling ___
    Manual(PathBuf),
    /// Save at a specified interval in ms
    ///
    /// This increases chance of data loss and is more dependent
    /// on a graceful shutdown but can be much more performant
    /// when updated are very frequent
    Interval(PathBuf, u32),
}

#[derive(Debug)]
enum Cmd {
    /// Add an entry to the append only file
    Append,
}

/// All errors related to the Perds crate
///
/// These will typically wrap an inner error type
#[derive(Debug)]
pub enum Error {
    /// All errors related to file IO
    FileError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::FileError(err)
    }
}

impl<K, V> Data<K, V> {
    /// This will start a background worker which will listen to
    /// IO events.
    ///
    /// The most common use case for it will be to check the size of the
    /// append only file and update the snapshot then compress the file
    /// once it reaches a large enough size
    fn start(value: HashMap<K, V>, writer: BufWriter<File>) -> Self {
        let (tx, rx) = channel::<Cmd>();
        let task = thread::spawn(move || loop {
            if let Ok(cmd) = rx.recv() {
                println!("Command: {:?}", cmd);
            }
        });
        Self {
            task,
            writer,
            inner: value,
            tx,
        }
    }

    fn save(&self) -> Result<(), ()> {
        Ok(())
    }

    fn append(&mut self, key: K, val: V) -> Result<(), Error> {
        self.writer.write_all(&[1])?;
        self.writer.flush()?;
        Ok(())
    }
}

impl<K, V> Data<K, V>
where
    K: Hash + Eq,
{
    fn set(&mut self, key: K, value: V, strategy: Strategy) -> Result<(), ()> {
        self.inner.insert(key, value);
        if strategy != Strategy::InMemory {
            self.save()?;
        }
        Ok(())
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }
}

impl<K, V> Drop for Perds<K, V> {
    /// We need to shut down our persistence thread
    /// gracefully and make sure any resources are cleaned up
    fn drop(&mut self) {}
}

impl<K, V> Perds<K, V>
where
    K: Hash + Eq,
{
    /// Instantiate a new Perds instance with a given strategy
    ///
    /// Depending on the strategy this may start a new thread
    ///
    /// <div class="warning">A file must not exist at the given path</div>
    pub fn new(value: HashMap<K, V>, strategy: Strategy) -> Result<Self, Error> {
        let writer = match &strategy {
            Strategy::Stream(path) => {
                // TODO: Remove this hack to replace the file
                std::fs::write(path, &[]).unwrap();
                let f = OpenOptions::new().read(true).append(true).open(path)?;
                BufWriter::new(f)
            }
            _ => todo!(),
        };
        Ok(Self {
            strategy,
            inner: Data::start(value, writer),
        })
    }

    /// Hydrate a Perds from data in a provided file path
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the append only file we want to hydrate from
    pub fn from_file(path: &Path) -> Result<Self, Error> {
        let mut f = File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let mut _map: HashMap<K, V> = HashMap::new();
        buf.iter().for_each(|_l| {
            // First byte +/- (insert or delete)
            // Next byte data type of key
            // Next byte data type of value
            // From this we know how much to seek for each one
            // For now assume no padding between statements
        });
        todo!();
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    fn set(&mut self, key: K, value: V) -> Result<(), ()> {
        self.inner.append(key, value);
        Ok(())
    }

    fn save(&mut self) -> Result<(), ()> {
        match self.strategy {
            // TODO: Handle these properly
            Strategy::Stream(_) => {
                todo!();
            }
            Strategy::Manual(_) | Strategy::Interval(_, _) => {
                todo!();
            }
            Strategy::InMemory => {}
        };
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    const TEST_FILE: &str = "./test/test.data";
    const APPEND_FILE: &str = "./test/append.data";

    #[test]
    fn test_get_hashmap() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let perds = Perds::new(
            map.clone(),
            Strategy::Stream(PathBuf::from_str(TEST_FILE).unwrap()),
        )
        .unwrap();

        assert_eq!(perds.get(&"key"), Some(&"value"));
    }

    #[test]
    fn test_start() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let perds = Perds::new(
            map.clone(),
            Strategy::Stream(PathBuf::from_str(TEST_FILE).unwrap()),
        )
        .unwrap();

        assert_eq!(perds.get(&"key"), Some(&"value"));
    }

    #[test]
    fn test_append() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let mut perds = Perds::new(
            map.clone(),
            Strategy::Stream(PathBuf::from_str(APPEND_FILE).unwrap()),
        )
        .unwrap();

        perds.set("new key", "new value").unwrap();

        println!("File: {:?}", std::fs::read(APPEND_FILE).unwrap());
        assert_eq!(&[1], std::fs::read(APPEND_FILE).unwrap().as_slice())
    }
}
