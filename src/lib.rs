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
//! Plan:
//! - Keys are strings
//! - Values are serializable data format not chosen
//!

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{BufWriter, Read, Write},
    path::PathBuf,
};

/// The persistent container for a std library collection type
#[derive(Debug)]
pub struct Perds<K, V> {
    strategy: Strategy,
    inner: HashMap<K, V>,
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
}

#[derive(Serialize, Deserialize)]
enum Operation {
    Insert,
    Delete,
}

/// All errors related to the Perds crate
///
/// These will typically wrap an inner error type
#[derive(Debug)]
pub enum Error {
    /// All errors related to file IO
    FileError(std::io::Error),
    /// Serialization/Deserialization failure
    SerError(postcard::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::FileError(err)
    }
}

impl From<postcard::Error> for Error {
    fn from(value: postcard::Error) -> Self {
        Error::SerError(value)
    }
}

impl<K, V> Drop for Perds<K, V> {
    /// We need to shut down our persistence thread
    /// gracefully and make sure any resources are cleaned up
    fn drop(&mut self) {}
}

impl<K, V> Perds<K, V>
where
    K: Eq + Hash + DeserializeOwned,
    V: DeserializeOwned,
{
    /// Hydrate a Perds from data in a provided file path
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the append only file we want to hydrate from
    ///
    /// # Example
    ///
    /// ```
    ///  use perds::Perds;
    /// ```
    pub fn from_file(strategy: Strategy) -> Result<Self, Error> {
        let path = match &strategy {
            Strategy::Manual(path) | Strategy::Stream(path) => path,
            _ => panic!("Cannot use non persistent strategy"),
        };
        let mut f = File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let mut map = HashMap::new();
        let mut buf = buf.as_slice();
        loop {
            let (op, rest) = postcard::take_from_bytes::<Operation>(&buf)?;
            buf = rest;
            let (k, rest) = postcard::take_from_bytes::<K>(&buf)?;
            buf = rest;
            match op {
                Operation::Delete => map.remove(&k),
                Operation::Insert => {
                    let (v, rest) = postcard::take_from_bytes::<V>(&buf)?;
                    buf = rest;
                    map.insert(k, v)
                }
            };
            if buf.is_empty() {
                break;
            }
        }
        Ok(Self {
            strategy,
            inner: map,
            writer: BufWriter::new(f),
        })
    }
}

impl<K, V> Perds<K, V>
where
    K: Hash + Eq + Serialize + Clone,
    V: Serialize + Clone,
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
            inner: value,
            writer,
        })
    }

    /// Get the value from the HashMap
    ///
    /// This is a simple wrapper around `HashMap::get(...)`
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Set a value in the HashMap
    ///
    /// This will use the persistence strategy chosen for the instance of `Perds`
    pub fn set(&mut self, k: K, v: V) -> Result<(), Error> {
        match self.strategy {
            Strategy::Stream(_) => {
                let pair = postcard::to_stdvec(&(Operation::Insert, k.clone(), v.clone()))?;
                self.writer.write_all(pair.as_slice())?;
                self.writer.flush()?;
            }
            Strategy::Manual(_) | Strategy::InMemory => {}
        };
        self.inner.insert(k, v);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    const TEST_FILE: &str = "./test/test.postcard";
    const APPEND_FILE: &str = "./test/append.postcard";
    const HYDRATE_FILE: &str = "./test/hydrate.postcard";

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
        let map: HashMap<&str, &str> = HashMap::new();

        let mut perds = Perds::new(
            map.clone(),
            Strategy::Stream(PathBuf::from_str(APPEND_FILE).unwrap()),
        )
        .unwrap();

        perds.set("abc", "def").unwrap();

        println!("File: {:?}", std::fs::read(APPEND_FILE).unwrap());
        assert_eq!(
            &[0, 3, 97, 98, 99, 3, 100, 101, 102],
            std::fs::read(APPEND_FILE).unwrap().as_slice()
        )
    }

    #[test]
    fn test_hydrate() {
        {
            let mut perds = Perds::new(
                HashMap::new(),
                Strategy::Stream(PathBuf::from_str(HYDRATE_FILE).unwrap()),
            )
            .unwrap();
            perds.set("hello".to_string(), "world".to_string()).unwrap();
            perds.set("bye".to_string(), "world".to_string()).unwrap();
        }
        let perds =
            Perds::from_file(Strategy::Stream(PathBuf::from_str(HYDRATE_FILE).unwrap())).unwrap();

        assert_eq!(perds.get(&"hello".to_string()), Some(&"world".to_string()));
        assert_eq!(perds.get(&"bye".to_string()), Some(&"world".to_string()));
    }
}
