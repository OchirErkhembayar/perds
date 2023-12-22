//! Hydrate a [HashMap](std::collections::HashMap) from an append only file

use std::str::FromStr;
use std::{collections::HashMap, path::PathBuf};

use perds::{Perds, Strategy};

const FILE: &str = "./examples/data/hydrate.postcard";

fn main() -> Result<(), ()> {
    let path = PathBuf::from_str(FILE).unwrap();
    {
        let mut perds = Perds::new(HashMap::new(), Strategy::Stream, path.clone()).unwrap();

        let val = perds.insert("abc", "fed").unwrap();
        assert_eq!(val, None);
        let val = perds.insert("abc", "def").unwrap();
        assert_eq!(val, Some("fed"));
        let val = perds.remove("abc").unwrap();
        assert_eq!(val, Some("def"));

        perds.insert("Hello", "World!").unwrap();
    }

    let perds: Perds<String, String> = Perds::from_file(Strategy::Stream, path.clone()).unwrap();

    println!("Hello, {}", perds.get(&"Hello".to_string()).unwrap());

    std::fs::remove_file(&path).unwrap();
    Ok(())
}
