#![warn(missing_docs, clippy::overflow_check_conditional)]

//! Prototyping
//! I am starting off an initial prototype to only
//! work with [std::collections::HashMap] and will
//! move on to other data structures later
//!
//! TODO: Error handling, currently there are just units for Ok and Err variants of Result

use std::{collections::HashMap, hash::Hash, sync::mpsc::channel, thread, thread::JoinHandle};

/// The persistent container for a std library collection type
#[derive(Debug)]
pub struct Perds<K, V> {
    inner: HashMap<K, V>,
    strategy: Strategy,
    // TODO: This is not ideal because we have to unwrap. Probably best to just make it always
    // available and have a dummy variant or something
    data: Option<Data>,
}

/// The representation of the persistence mechanism of the
/// inner data structure
#[derive(Debug)]
struct Data {
    task: JoinHandle<()>,
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
}

#[derive(Debug)]
struct Command;

impl Data {
    fn start() -> Self {
        let (tx, rx) = channel::<Command>();
        let task = thread::spawn(move || loop {
            if let Ok(cmd) = rx.recv() {
                println!("Command: {:?}", cmd);
            }
        });
        Self { task }
    }

    fn save(&self) -> Result<(), ()> {
        Ok(())
    }
}

impl<K, V> From<HashMap<K, V>> for Perds<K, V> {
    /// Instantiate a volatile Perds from a HashMap
    ///
    /// If you want to choose a strategy use ___ instead
    fn from(value: HashMap<K, V>) -> Self {
        Self {
            inner: value,
            strategy: Strategy::InMemory,
            data: None,
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
    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    fn set(&mut self, key: K, value: V) -> Result<(), ()> {
        self.inner.insert(key, value);
        if self.strategy != Strategy::InMemory {
            let data = &mut self.data;
            data.as_mut().unwrap().save()?;
        }
        Ok(())
    }

    fn save(&mut self) -> Result<(), ()> {
        match self.strategy {
            Strategy::Stream | Strategy::Manual => self.data.as_mut().unwrap().save(),
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

        assert_eq!(map, perds.inner);
        assert_eq!(Strategy::InMemory, perds.strategy);
    }

    #[test]
    fn test_get_hashmap() {
        let map: HashMap<&str, &str> = HashMap::from_iter([("key", "value")]);

        let perds = Perds::from(map.clone());

        assert_eq!(perds.get(&"key"), Some(&"value"));
    }
}
