# rust-postgres-large-object

A crate providing access to the Postgres large object API.

[![Build Status](https://travis-ci.org/sfackler/rust-postgres-large-object.svg?branch=master)](https://travis-ci.org/sfackler/rust-postgres-large-object)

[Documentation](https://sfackler.github.io/rust-postgres-large-object/doc/v0.4.0/postgres_large_object)

# Example

```rust
extern crate postgres;
extern crate postgres_large_object;

use std::fs::File;
use std::io;

use postgres::{Connection, TlsMode};
use postgres_large_object::{LargeObjectExt, LargeObjectTransactionExt, Mode};

fn main() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();

    let mut file = File::open("vacation_photos.tar.gz").unwrap();
    let trans = conn.transaction().unwrap();
    let oid = trans.create_large_object().unwrap();
    {
        let mut large_object = trans.open_large_object(oid, Mode::Write).unwrap();
        io::copy(&mut file, &mut large_object).unwrap();
    }
    trans.commit().unwrap();

    let mut file = File::create("vacation_photos_copy.tar.gz").unwrap();
    let trans = conn.transaction().unwrap();
    let mut large_object = trans.open_large_object(oid, Mode::Read).unwrap();
    io::copy(&mut large_object, &mut file).unwrap();
}
```

## License

Licensed under either of
 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.
