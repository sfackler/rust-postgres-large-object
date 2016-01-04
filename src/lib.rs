//! A crate providing access to the Postgres large object API.
//!
//! # Example
//!
//! ```rust,no_run
//! extern crate postgres;
//! extern crate postgres_large_object;
//!
//! use std::fs::File;
//! use std::io;
//!
//! use postgres::{Connection, SslMode};
//! use postgres_large_object::{LargeObjectExt, LargeObjectTransactionExt, Mode};
//!
//! fn main() {
//!     let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
//!
//!     let mut file = File::open("vacation_photos.tar.gz").unwrap();
//!     let trans = conn.transaction().unwrap();
//!     let oid = trans.create_large_object().unwrap();
//!     {
//!         let mut large_object = trans.open_large_object(oid, Mode::Write).unwrap();
//!         io::copy(&mut file, &mut large_object).unwrap();
//!     }
//!     trans.commit().unwrap();
//!
//!     let mut file = File::create("vacation_photos_copy.tar.gz").unwrap();
//!     let trans = conn.transaction().unwrap();
//!     let mut large_object = trans.open_large_object(oid, Mode::Read).unwrap();
//!     io::copy(&mut large_object, &mut file).unwrap();
//! }
//! ```
#![doc(html_root_url="https://sfackler.github.io/rust-postgres-large-object/doc/v0.3.4")]

extern crate postgres;

use postgres::{Result, Transaction, GenericConnection};
use postgres::error::Error;
use postgres::types::Oid;
use std::cmp;
use std::fmt;
use std::i32;
use std::io::{self, Write};

/// An extension trait adding functionality to create and delete large objects.
pub trait LargeObjectExt {
    /// Creates a new large object, returning its `Oid`.
    fn create_large_object(&self) -> Result<Oid>;

    /// Deletes the large object with the specified `Oid`.
    fn delete_large_object(&self, oid: Oid) -> Result<()>;
}

impl<T: GenericConnection> LargeObjectExt for T {
    fn create_large_object(&self) -> Result<Oid> {
        let stmt = try!(self.prepare_cached("SELECT pg_catalog.lo_create(0)"));
        let r = stmt.query(&[]).map(|r| r.iter().next().unwrap().get(0));
        r
    }

    fn delete_large_object(&self, oid: Oid) -> Result<()> {
        let stmt = try!(self.prepare_cached("SELECT pg_catalog.lo_unlink($1)"));
        stmt.execute(&[&oid]).map(|_| ())
    }
}

/// Large object access modes.
///
/// Note that Postgres currently does not make any distinction between the
/// `Write` and `ReadWrite` modes.
#[derive(Debug)]
pub enum Mode {
    /// An object opened in this mode may only be read from.
    Read,
    /// An object opened in this mode may be written to.
    Write,
    /// An object opened in this mode may be read from or written to.
    ReadWrite,
}

impl Mode {
    fn to_i32(&self) -> i32 {
        match *self {
            Mode::Read => 0x00040000,
            Mode::Write => 0x00020000,
            Mode::ReadWrite => 0x00040000 | 0x00020000,
        }
    }
}

/// An extension trait adding functionality to open large objects.
pub trait LargeObjectTransactionExt {
    /// Opens the large object with the specified `Oid` in the specified `Mode`.
    fn open_large_object<'a>(&'a self, oid: Oid, mode: Mode) -> Result<LargeObject<'a>>;
}

impl<'conn> LargeObjectTransactionExt for Transaction<'conn> {
    fn open_large_object<'a>(&'a self, oid: Oid, mode: Mode) -> Result<LargeObject<'a>> {
        let version = self.connection().parameter("server_version").unwrap();
        let mut version = version.split('.');
        let major: i32 = version.next().unwrap().parse().unwrap();
        let minor: i32 = version.next().unwrap().parse().unwrap();
        let has_64 = major > 9 || (major == 9 && minor >= 3);

        let stmt = try!(self.prepare_cached("SELECT pg_catalog.lo_open($1, $2)"));
        let fd = try!(stmt.query(&[&oid, &mode.to_i32()])).iter().next().unwrap().get(0);
        Ok(LargeObject {
            trans: self,
            fd: fd,
            has_64: has_64,
            finished: false,
        })
    }
}

macro_rules! try_io {
    ($e:expr) => {
        match $e {
            Ok(ok) => ok,
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e))
        }
    }
}

/// Represents an open large object.
pub struct LargeObject<'a> {
    trans: &'a Transaction<'a>,
    fd: i32,
    has_64: bool,
    finished: bool,
}

impl<'a> fmt::Debug for LargeObject<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("LargeObject")
           .field("fd", &self.fd)
           .field("transaction", &self.trans)
           .finish()
    }
}

impl<'a> Drop for LargeObject<'a> {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl<'a> LargeObject<'a> {
    /// Returns the file descriptor of the opened object.
    pub fn fd(&self) -> i32 {
        self.fd
    }

    /// Truncates the object to the specified size.
    ///
    /// If `len` is larger than the size of the object, it will be padded with
    /// null bytes to the specified size.
    pub fn truncate(&mut self, len: i64) -> Result<()> {
        if self.has_64 {
            let stmt = try!(self.trans.prepare_cached("SELECT pg_catalog.lo_truncate64($1, $2)"));
            stmt.execute(&[&self.fd, &len]).map(|_| ())
        } else {
            let len = if len <= i32::max_value() as i64 {
                len as i32
            } else {
                return Err(Error::Io(io::Error::new(io::ErrorKind::InvalidInput,
                                                    "The database does not support objects larger \
                                                     than 2GB")));
            };
            let stmt = try!(self.trans.prepare_cached("SELECT pg_catalog.lo_truncate($1, $2)"));
            stmt.execute(&[&self.fd, &len]).map(|_| ())
        }
    }

    fn finish_inner(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        self.finished = true;
        let stmt = try!(self.trans.prepare_cached("SELECT pg_catalog.lo_close($1)"));
        stmt.execute(&[&self.fd]).map(|_| ())
    }

    /// Consumes the `LargeObject`, cleaning up server side state.
    ///
    /// Functionally identical to the `Drop` implementation on `LargeObject`
    /// except that it returns any errors to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }
}

impl<'a> io::Read for LargeObject<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.loread($1, $2)"));
        let cap = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let rows = try_io!(stmt.query(&[&self.fd, &cap]));
        buf.write(rows.get(0).get_bytes(0).unwrap())
    }
}

impl<'a> io::Write for LargeObject<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lowrite($1, $2)"));
        let cap = cmp::min(buf.len(), i32::MAX as usize);
        try_io!(stmt.execute(&[&self.fd, &&buf[..cap]]));
        Ok(cap)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> io::Seek for LargeObject<'a> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (kind, pos) = match pos {
            io::SeekFrom::Start(pos) => {
                let pos = if pos <= i64::max_value as u64 {
                    pos as i64
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                              "cannot seek more than 2^63 bytes"));
                };
                (0, pos)
            }
            io::SeekFrom::Current(pos) => (1, pos),
            io::SeekFrom::End(pos) => (2, pos),
        };

        if self.has_64 {
            let stmt = try_io!(self.trans
                                   .prepare_cached("SELECT pg_catalog.lo_lseek64($1, $2, $3)"));
            let rows = try_io!(stmt.query(&[&self.fd, &pos, &kind]));
            let pos: i64 = rows.iter().next().unwrap().get(0);
            Ok(pos as u64)
        } else {
            let pos = if pos <= i32::max_value() as i64 {
                pos as i32
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                          "cannot seek more than 2^31 bytes"));
            };
            let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lo_lseek($1, $2, $3)"));
            let rows = try_io!(stmt.query(&[&self.fd, &pos, &kind]));
            let pos: i32 = rows.iter().next().unwrap().get(0);
            Ok(pos as u64)
        }
    }
}

#[cfg(test)]
mod test {
    use postgres::{Connection, SslMode};
    use postgres::error::{Error, SqlState};

    use {LargeObjectExt, LargeObjectTransactionExt, Mode};

    #[test]
    fn test_create_delete() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let oid = conn.create_large_object().unwrap();
        conn.delete_large_object(oid).unwrap();
    }

    #[test]
    fn test_delete_bogus() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        match conn.delete_large_object(0) {
            Ok(()) => panic!("unexpected success"),
            Err(Error::Db(ref e)) if e.code == SqlState::UndefinedObject => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_open_bogus() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        match trans.open_large_object(0, Mode::Read) {
            Ok(_) => panic!("unexpected success"),
            Err(Error::Db(ref e)) if e.code == SqlState::UndefinedObject => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        };
    }

    #[test]
    fn test_open_finish() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let lo = trans.open_large_object(oid, Mode::Read).unwrap();
        lo.finish().unwrap();
    }

    #[test]
    fn test_write_read() {
        use std::io::{Write, Read};

        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Read).unwrap();
        let mut out = vec![];
        lo.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello world!!!");
    }

    #[test]
    fn test_seek_tell() {
        use std::io::{Write, Read, Seek, SeekFrom};

        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();

        assert_eq!(14, lo.seek(SeekFrom::Current(0)).unwrap());
        assert_eq!(1, lo.seek(SeekFrom::Start(1)).unwrap());
        let mut buf = [0];
        assert_eq!(1, lo.read(&mut buf).unwrap());
        assert_eq!(b'e', buf[0]);
        assert_eq!(2, lo.seek(SeekFrom::Current(0)).unwrap());
        assert_eq!(10, lo.seek(SeekFrom::End(-4)).unwrap());
        assert_eq!(1, lo.read(&mut buf).unwrap());
        assert_eq!(b'd', buf[0]);
        assert_eq!(8, lo.seek(SeekFrom::Current(-3)).unwrap());
        assert_eq!(1, lo.read(&mut buf).unwrap());
        assert_eq!(b'r', buf[0]);
    }

    #[test]
    fn test_write_with_read_fd() {
        use std::io::Write;

        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Read).unwrap();
        assert!(lo.write_all(b"hello world!!!").is_err());
    }

    #[test]
    fn test_truncate() {
        use std::io::{Seek, SeekFrom, Write, Read};

        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();

        lo.truncate(5).unwrap();
        lo.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![];
        lo.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello");
        lo.truncate(10).unwrap();
        lo.seek(SeekFrom::Start(0)).unwrap();
        buf.clear();
        lo.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello\0\0\0\0\0");
    }
}
