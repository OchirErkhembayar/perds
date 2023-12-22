#![warn(
    missing_docs,
    clippy::overflow_check_conditional,
    clippy::perf,
    clippy::needless_lifetimes
)]

//! Prototyping
//! I am starting off an initial prototype to only
//! work with [HashMaps](std::collections::HashMap) and will
//! move on to other data structures later
//!
//! TODO:
//! - Format for the append only file
//!   - Representing cut off for when the state snapshots were taken
//!   - Blocking? Non blocking?
//! - How to compress the file

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    hash::Hash,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

/// The persistent container for a std library collection type
#[derive(Debug)]
pub struct Perds<K, V> {
    strategy: Strategy,
    inner: HashMap<K, V>,
    writer: BufWriter<File>,
    path: PathBuf,
}

/// The persistence strategy for a Perds instance
#[derive(Debug, PartialEq, Clone)]
pub enum Strategy {
    /// Flush on every update
    Stream,
    /// Flush only when [flush](Perds::flush()) is explicitly called
    Manual,
}

#[derive(Serialize, Deserialize, Debug)]
enum Operation {
    Insert,
    Delete,
}

/// All errors related to the Perds crate
///
/// These will typically wrap an inner error type
#[derive(Debug)]
pub enum Error {
    /// Wrapper around [std::io::Error]
    FileError(std::io::Error),
    /// Serialization/Deserialization error
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

impl<K, V> Perds<K, V> {
    /// Get the path of the append only file of this instance
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
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
    ///  use perds::{Perds, Strategy};
    ///  use std::str::FromStr;
    ///
    ///  let path = std::path::PathBuf::from_str("./examples/doc.postcard").unwrap();
    ///  let p: Perds<String, String> = Perds::from_file(Strategy::Stream, path).unwrap();
    ///
    ///  assert_eq!(p.get(&"foo".to_string()), None);
    /// ```
    pub fn from_file(strategy: Strategy, path: PathBuf) -> Result<Self, Error> {
        let mut f = File::open(&path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let mut map = HashMap::new();
        let mut buf = buf.as_slice();
        while !buf.is_empty() {
            let (op, rest) = postcard::take_from_bytes::<Operation>(buf)?;
            buf = rest;
            let (k, rest) = postcard::take_from_bytes::<K>(buf)?;
            buf = rest;
            match op {
                Operation::Delete => map.remove(&k),
                Operation::Insert => {
                    let (v, rest) = postcard::take_from_bytes::<V>(buf)?;
                    buf = rest;
                    map.insert(k, v)
                }
            };
        }
        Ok(Self {
            strategy,
            inner: map,
            writer: BufWriter::new(f),
            path,
        })
    }

    /// Hydrate a Perds from data in a provided file path
    ///
    /// This function will create a file if one does no exist
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the append only file we want to hydrate from
    ///
    /// # Example
    ///
    /// ```
    ///  use perds::{Perds, Strategy};
    ///  use std::str::FromStr;
    ///
    ///  let path = std::path::PathBuf::from_str("./examples/doc.postcard").unwrap();
    ///  let p: Perds<String, String> = Perds::try_from_file(Strategy::Stream, path).unwrap();
    ///
    ///  assert_eq!(p.get(&"foo".to_string()), None);
    /// ```
    pub fn try_from_file(strategy: Strategy, path: PathBuf) -> Result<Self, Error> {
        let mut f = File::options().write(true).read(true).open(&path)?;
        let mut buf = Vec::new();
        eprintln!("Here? f: {:?}", f);
        f.read_to_end(&mut buf)?;
        let mut inner = HashMap::new();
        let mut buf = buf.as_slice();
        while !buf.is_empty() {
            let (op, rest) = postcard::take_from_bytes::<Operation>(buf)?;
            buf = rest;
            let (k, rest) = postcard::take_from_bytes::<K>(buf)?;
            buf = rest;
            match op {
                Operation::Delete => inner.remove(&k),
                Operation::Insert => {
                    let (v, rest) = postcard::take_from_bytes::<V>(buf)?;
                    buf = rest;
                    inner.insert(k, v)
                }
            };
        }
        Ok(Self {
            strategy,
            inner,
            writer: BufWriter::new(f),
            path,
        })
    }
}

impl<K, V> Perds<K, V>
where
    K: Hash + Eq + Serialize,
    V: Serialize,
{
    /// Instantiate a new Perds instance with a given strategy
    ///
    /// This will create a new file and write the contents of the given [HashMap] into it
    ///
    /// <div class="warning">Existing files in this path will be overwritten</div>
    pub fn new(value: HashMap<K, V>, strategy: Strategy, path: PathBuf) -> Result<Self, Error> {
        let mut writer = {
            let f = File::options()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&path)?;
            BufWriter::new(f)
        };
        if !value.is_empty() {
            // TODO: Fix this horrific thing
            let mut cmds = vec![];
            for (k, v) in value.iter() {
                // I know the size of the Operation but need to know the size of
                // k and v in order to use postcard::to_slice and not have an
                // allocation for every single operation
                let mut pc = postcard::to_stdvec(&(Operation::Insert, k, v))?;
                cmds.append(&mut pc)
            }
            writer.write_all(cmds.as_slice())?;
            writer.flush()?;
        }
        Ok(Self {
            strategy,
            inner: value,
            writer,
            path,
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
    pub fn insert(&mut self, k: K, v: V) -> Result<Option<V>, Error> {
        let cmd = postcard::to_stdvec(&(Operation::Insert, &k, &v))?;
        self.writer.write_all(cmd.as_slice())?;
        if let Strategy::Stream = self.strategy {
            self.writer.flush()?;
        }
        // Update in memory DS after successful disk write
        Ok(self.inner.insert(k, v))
    }

    /// Remove a value from the `HashMap`
    ///
    /// This will use the persistence strategy chosen for the instance of `Perds`
    pub fn remove(&mut self, k: K) -> Result<Option<V>, Error> {
        let cmd = postcard::to_stdvec(&(Operation::Delete, &k))?;
        self.writer.write_all(cmd.as_slice())?;
        if let Strategy::Stream = self.strategy {
            self.writer.flush()?;
        }
        Ok(self.inner.remove(&k))
    }

    /// Flush the [BufWriter]
    ///
    /// If [Strategy::Manual] was chosen this function should
    /// be called in order to ensure that the state changes were saved to disk
    pub fn flush(&mut self) -> Result<(), Error> {
        Ok(self.writer.flush()?)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_stream() {
        const TEST_FILE: &str = "./test/test.postcard";

        let path = PathBuf::from_str(TEST_FILE).unwrap();
        let mut perds = Perds::new(HashMap::new(), Strategy::Stream, path.clone()).unwrap();

        let val = perds.insert("abc", "fed").unwrap();
        assert_eq!(val, None);
        let val = perds.insert("abc", "def").unwrap();
        assert_eq!(val, Some("fed"));
        let val = perds.remove("abc").unwrap();
        assert_eq!(val, Some("def"));

        perds.insert("hello", "world").unwrap();

        assert_eq!(
            &[
                0, 3, b'a', b'b', b'c', 3, b'f', b'e', b'd', 0, 3, b'a', b'b', b'c', 3, b'd', b'e',
                b'f', 1, 3, b'a', b'b', b'c', 0, 5, b'h', b'e', b'l', b'l', b'o', 5, b'w', b'o',
                b'r', b'l', b'd'
            ],
            std::fs::read(TEST_FILE).unwrap().as_slice()
        );
        drop(perds);

        let perds =
            Perds::from_file(Strategy::Stream, PathBuf::from_str(TEST_FILE).unwrap()).unwrap();

        assert_eq!(perds.get(&"hello".to_string()), Some(&"world".to_string()));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_file_created() {
        let map = HashMap::from_iter([("foo", "bar")]);
        let path = PathBuf::from_str("./test/test_new.postcard").unwrap();
        Perds::new(map, Strategy::Stream, path.clone()).unwrap();

        let perds = Perds::from_file(Strategy::Stream, path.clone()).unwrap();

        assert_eq!(perds.get(&"foo".to_string()), Some(&"bar".to_string()));

        std::fs::remove_file(&path).unwrap();
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct Foo {
        x: i32,
        y: i32,
    }
    const TEST_STRUCT: &str = "./test/test_struct.postcard";

    #[test]
    fn test_struct() {
        let my_foo = Foo { x: 2, y: 3 };

        let path = PathBuf::from_str(TEST_STRUCT).unwrap();
        let mut perds = Perds::new(HashMap::new(), Strategy::Stream, path.clone()).unwrap();
        perds.insert("my_foo", my_foo.clone()).unwrap();
        drop(perds);
        let perds =
            Perds::from_file(Strategy::Stream, PathBuf::from_str(TEST_STRUCT).unwrap()).unwrap();

        assert_eq!(perds.get(&"my_foo".to_string()), Some(&my_foo));

        std::fs::remove_file(&path).unwrap();
    }
}
