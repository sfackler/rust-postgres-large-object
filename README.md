# rust-postgres-large-object

[![Build Status](https://travis-ci.org/sfackler/rust-postgres-large-object.svg?branch=master)](https://travis-ci.org/sfackler/rust-postgres-large-object)

A crate providing access to the Postgres large object API.

Documentation is available [here](https://sfackler.github.io/rust-postgres-large-object/doc/postgres_large_object).

# Example

```rust
extern crate postgres;
extern crate postgres_large_object;

use std::old_path::Path;
use std::old_io::fs::File;
use std::old_io::util;

use postgres::{Connection, SslMode};
use postgres_large_object::{LargeObjectExt, LargeObjectTransactionExt, Mode};

fn main() {
    let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();

    let mut file = File::open(&Path::new("vacation_photos.tar.gz")).unwrap();
    let trans = conn.transaction().unwrap();
    let oid = trans.create_large_object().unwrap();
    {
        let mut large_object = trans.open_large_object(oid, Mode::Write).unwrap();
        util::copy(&mut file, &mut large_object).unwrap();
    }
    trans.commit().unwrap();

    let mut file = File::create(&Path::new("vacation_photos_copy.tar.gz")).unwrap();
    let trans = conn.transaction().unwrap();
    let mut large_object = trans.open_large_object(oid, Mode::Read).unwrap();
    util::copy(&mut large_object, &mut file).unwrap();
}
```
