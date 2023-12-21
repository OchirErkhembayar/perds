#![warn(missing_docs, clippy::overflow_check_conditional)]

//! Prototyping
//! I am starting off an initial prototype to only
//! work with [std::collections::HashMap] and will
//! move on to other data structures later
//!
//! TODO: Error handling, currently there are just units for Ok and Err variants of Result

use std::{
    collections::HashMap,
    hash::Hash,
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
    tx: Sender<Event>,
}

/// The persistence strategy for a Perds instance
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Strategy {
    /// A Perds with this strategy will not persist
    InMemory,
    /// Save on every state change
    Stream,
    /// Save only when calling ___
    Manual,
    /// Save at a specified interval in ms
    ///
    /// This increases chance of data loss and is more dependent
    /// on a graceful shutdown but can be much more performant
    /// when updated are very frequent
    Interval(u32),
}

#[derive(Debug)]
enum Event {
    /// Add an entry to the append only file
    Append,
}

impl<K, V> Data<K, V> {
    /// This will start a background worker which will listen to
    /// IO events.
    ///
    /// The most common use case for it will be to check the size of the
    /// append only file and update the snapshot then compress the file
    /// once it reaches a large enough size
    fn start(value: HashMap<K, V>) -> Self {
        let (tx, rx) = channel::<Event>();
        let task = thread::spawn(move || loop {
            if let Ok(cmd) = rx.recv() {
                println!("Command: {:?}", cmd);
            }
        });
        Self {
            task,
            inner: value,
            tx,
        }
    }

    fn save(&self) -> Result<(), ()> {
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

impl<K, V> From<HashMap<K, V>> for Perds<K, V> {
    /// Instantiate a volatile Perds from a HashMap
    ///
    /// If you want to choose a strategy use ___ instead
    fn from(value: HashMap<K, V>) -> Self {
        Self {
            strategy: Strategy::InMemory,
            inner: Data::start(value),
        }
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
    pub fn new(value: HashMap<K, V>, strategy: Strategy) -> Self {
        Self {
            strategy,
            inner: Data::start(value),
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    fn set(&mut self, key: K, value: V) -> Result<(), ()> {
        self.inner.set(key, value, self.strategy)?;
        Ok(())
    }

    fn save(&mut self) -> Result<(), ()> {
        match self.strategy {
            // TODO: Handle these differently
            Strategy::Stream | Strategy::Manual | Strategy::Interval(_) => self.inner.save(),
            Strategy::InMemory => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_hashmap() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let perds = Perds::from(map.clone());

        assert_eq!(map, perds.inner.inner);
        assert_eq!(Strategy::InMemory, perds.strategy);
    }

    #[test]
    fn test_get_hashmap() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let perds = Perds::from(map.clone());

        assert_eq!(perds.get(&"key"), Some(&"value"));
    }

    #[test]
    fn test_start() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let perds = Perds::new(map.clone(), Strategy::InMemory);

        assert_eq!(perds.get(&"key"), Some(&"value"));
    }
}
