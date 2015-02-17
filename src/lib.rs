//! A crate providing access to the Postgres large object API.
//!
//! # Example
//!
//! ```rust,no_run
//! extern crate postgres;
//! extern crate postgres_large_object;
//!
//! use std::old_path::Path;
//! use std::old_io::fs::File;
//! use std::old_io::util;
//!
//! use postgres::{Connection, SslMode};
//! use postgres_large_object::{LargeObjectExt, LargeObjectTransactionExt, Mode};
//!
//! fn main() {
//!     let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
//!
//!     let mut file = File::open(&Path::new("vacation_photos.tar.gz")).unwrap();
//!     let trans = conn.transaction().unwrap();
//!     let oid = trans.create_large_object().unwrap();
//!     {
//!         let mut large_object = trans.open_large_object(oid, Mode::Write).unwrap();
//!         util::copy(&mut file, &mut large_object).unwrap();
//!     }
//!     trans.commit().unwrap();
//!
//!     let mut file = File::create(&Path::new("vacation_photos_copy.tar.gz")).unwrap();
//!     let trans = conn.transaction().unwrap();
//!     let mut large_object = trans.open_large_object(oid, Mode::Read).unwrap();
//!     util::copy(&mut large_object, &mut file).unwrap();
//! }
//! ```
#![feature(unsafe_destructor, io, core)]
#![doc(html_root_url="https://sfackler.github.io/rust-postgres-large-object/doc")]

extern crate postgres;

use std::cmp;
use std::fmt;
use std::i32;
use std::num::FromPrimitive;
use std::old_io::{self, IoResult, IoError, IoErrorKind, SeekStyle};
use std::slice::bytes;

use postgres::{Oid, Error, Result, Transaction, GenericConnection};

/// An extension trait adding functionality to create and delete large objects.
pub trait LargeObjectExt: GenericConnection {
    /// Creates a new large object, returning its `Oid`.
    fn create_large_object(&self) -> Result<Oid>;

    /// Deletes the large object with the specified `Oid`.
    fn delete_large_object(&self, oid: Oid) -> Result<()>;
}

impl<T: GenericConnection> LargeObjectExt for T {
    fn create_large_object(&self) -> Result<Oid> {
        let stmt = try!(self.prepare_cached("SELECT pg_catalog.lo_create(0)"));
        stmt.query(&[]).map(|mut r| r.next().unwrap().get(0))
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
        let fd = try!(stmt.query(&[&oid, &mode.to_i32()])).next().unwrap().get(0);
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
            Err(e) => return Err(IoError {
                kind: IoErrorKind::OtherIoError,
                desc: "error communicating with server",
                detail: Some(format!("{}", e)),
            })
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
        write!(fmt, "LargeObject {{ fd: {:?}, transaction: {:?} }}", self.fd, self.trans)
    }
}

#[unsafe_destructor]
impl<'a> Drop for LargeObject<'a> {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl<'a> LargeObject<'a> {
    /// Truncates the object to the specified size.
    ///
    /// If `len` is larger than the size of the object, it will be padded with
    /// null bytes to the specified size.
    pub fn truncate(&mut self, len: i64) -> Result<()> {
        if self.has_64 {
            let stmt = try!(self.trans.prepare_cached("SELECT pg_catalog.lo_truncate64($1, $2)"));
            stmt.execute(&[&self.fd, &len]).map(|_| ())
        } else {
            let len: i32 = match FromPrimitive::from_i64(len) {
                Some(len) => len,
                None => return Err(Error::IoError(IoError {
                    kind: IoErrorKind::InvalidInput,
                    desc: "The database does not support objects larger than 2GB",
                    detail: None,
                })),
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

impl<'a> Reader for LargeObject<'a> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.loread($1, $2)"));
        let cap = cmp::min(buf.len(), i32::MAX as usize) as i32;
        let out: Vec<u8> = try_io!(stmt.query(&[&self.fd, &cap])).next().unwrap().get(0);

        if !buf.is_empty() && out.is_empty() {
            return Err(old_io::standard_error(IoErrorKind::EndOfFile));
        }

        bytes::copy_memory(buf, &out);
        Ok(out.len())
    }
}

impl<'a> Writer for LargeObject<'a> {
    fn write_all(&mut self, mut buf: &[u8]) -> IoResult<()> {
        let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lowrite($1, $2)"));

        while !buf.is_empty() {
            let cap = cmp::min(buf.len(), i32::MAX as usize);
            try_io!(stmt.execute(&[&self.fd, &&buf[..cap]]));
            buf = &buf[cap..];
        }

        Ok(())
    }
}

impl<'a> Seek for LargeObject<'a> {
    fn tell(&self) -> IoResult<u64> {
        if self.has_64 {
            let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lo_tell64($1)"));
            Ok(try_io!(stmt.query(&[&self.fd])).next().unwrap().get::<_, i64>(0) as u64)
        } else {
            let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lo_tell($1)"));
            Ok(try_io!(stmt.query(&[&self.fd])).next().unwrap().get::<_, i32>(0) as u64)
        }
    }

    fn seek(&mut self, pos: i64, style: SeekStyle) -> IoResult<()> {
        let kind = match style {
            SeekStyle::SeekSet => 0,
            SeekStyle::SeekCur => 1,
            SeekStyle::SeekEnd => 2,
        };

        if self.has_64 {
            let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lo_lseek64($1, $2, $3)"));
            try_io!(stmt.execute(&[&self.fd, &pos, &kind]));
        } else {
            let pos: i32 = match FromPrimitive::from_i64(pos) {
                Some(pos) => pos,
                None => return Err(IoError {
                    kind: IoErrorKind::InvalidInput,
                    desc: "The database does not support seeks larger than 2GB",
                    detail: None,
                }),
            };
            let stmt = try_io!(self.trans.prepare_cached("SELECT pg_catalog.lo_lseek($1, $2, $3)"));
            try_io!(stmt.execute(&[&self.fd, &pos, &kind]));
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::old_io::SeekStyle;
    use postgres::{Connection, SslMode, SqlState, Error};

    use {LargeObjectExt, LargeObjectTransactionExt, Mode};

    #[test]
    fn test_create_delete() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let oid = conn.create_large_object().unwrap();
        conn.delete_large_object(oid).unwrap();
    }

    #[test]
    fn test_delete_bogus() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        match conn.delete_large_object(0) {
            Ok(()) => panic!("unexpected success"),
            Err(Error::DbError(ref e)) if e.code() == &SqlState::UndefinedObject => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_open_bogus() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        match trans.open_large_object(0, Mode::Read) {
            Ok(_) => panic!("unexpected success"),
            Err(Error::DbError(ref e)) if e.code() == &SqlState::UndefinedObject => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        };
    }

    #[test]
    fn test_open_finish() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let lo = trans.open_large_object(oid, Mode::Read).unwrap();
        lo.finish().unwrap();
    }

    #[test]
    fn test_write_read() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Read).unwrap();
        assert_eq!(b"hello world!!!", lo.read_to_end().unwrap());
    }

    #[test]
    fn test_seek_tell() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();

        assert_eq!(14, lo.tell().unwrap());
        lo.seek(1, SeekStyle::SeekSet).unwrap();
        assert_eq!(1, lo.tell().unwrap());
        assert_eq!(b'e', lo.read_u8().unwrap());
        assert_eq!(2, lo.tell().unwrap());
        lo.seek(-4, SeekStyle::SeekEnd).unwrap();
        assert_eq!(10, lo.tell().unwrap());
        assert_eq!(b'd', lo.read_u8().unwrap());
        lo.seek(-3, SeekStyle::SeekCur).unwrap();
        assert_eq!(8, lo.tell().unwrap());
        assert_eq!(b'r', lo.read_u8().unwrap());
    }

    #[test]
    fn test_write_with_read_fd() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Read).unwrap();
        assert!(lo.write_all(b"hello world!!!").is_err());
    }

    #[test]
    fn test_truncate() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        let trans = conn.transaction().unwrap();
        let oid = trans.create_large_object().unwrap();
        let mut lo = trans.open_large_object(oid, Mode::Write).unwrap();
        lo.write_all(b"hello world!!!").unwrap();

        lo.truncate(5).unwrap();
        lo.seek(0, SeekStyle::SeekSet).unwrap();
        assert_eq!(b"hello", lo.read_to_end().unwrap());
        lo.truncate(10).unwrap();
        lo.seek(0, SeekStyle::SeekSet).unwrap();
        assert_eq!(b"hello\0\0\0\0\0", lo.read_to_end().unwrap());
    }
}
