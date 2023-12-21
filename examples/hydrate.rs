//! Hydrate a [HashMap](std::collections::HashMap) from an append only file
//!
//! The file is located in `src/examples/data`

use std::{collections::HashMap, path::PathBuf};

use perds::{Perds, Strategy};

fn main() -> Result<(), ()> {
    let perds: Perds<String, String> =
        Perds::new(HashMap::new(), Strategy::Manual(PathBuf::new())).unwrap();
    Ok(())
}
