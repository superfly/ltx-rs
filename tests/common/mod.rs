use litetx as ltx;
use rand::prelude::*;
use rusqlite::{Connection, OpenFlags};
use std::{
    env, ffi, fs,
    io::{self, Read},
    ops, path, process,
};
use uuid::Uuid;

pub struct Tempfile(path::PathBuf);

impl Drop for Tempfile {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_file(&self.0) {
            if err.kind() != io::ErrorKind::NotFound {
                panic!("delete {}", self.0.to_string_lossy());
            }
        }
    }
}

impl AsRef<path::Path> for Tempfile {
    fn as_ref(&self) -> &path::Path {
        self.0.as_ref()
    }
}

impl ops::Deref for Tempfile {
    type Target = path::Path;

    fn deref(&self) -> &Self::Target {
        self.0.as_path()
    }
}

#[allow(dead_code)]
pub fn temp_file() -> Tempfile {
    let mut file = env::temp_dir();
    file.push(format!("{}", Uuid::new_v4()));

    Tempfile(file)
}

pub struct TestDb {
    pub path: Tempfile,
    pub page_size: ltx::PageSize,
    pub page_count: ltx::PageNum,
}

#[allow(dead_code)]
pub fn setup_test_db() -> TestDb {
    let path = temp_file();

    let conn = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .expect("open SQLite DB");

    conn.execute(
        "CREATE TABLE test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data BLOB
        )",
        (),
    )
    .expect("create test schema");

    let entries = 30 + random::<i32>() % 30;
    for _ in 0..entries {
        let mut buf = vec![0; 128 + random::<usize>() % 256];
        thread_rng().fill_bytes(&mut buf);
        conn.execute("INSERT INTO test (data) VALUES (?)", [buf])
            .expect("insert test row");
    }

    let page_size = conn
        .query_row("SELECT page_size FROM pragma_page_size", [], |row| {
            row.get(0)
        })
        .expect("query page_size");
    let page_count = conn
        .query_row("SELECT page_count FROM pragma_page_count", [], |row| {
            row.get(0)
        })
        .expect("query page_count");

    TestDb {
        path,
        page_size: ltx::PageSize::new(page_size).unwrap(),
        page_count: ltx::PageNum::new(page_count).unwrap(),
    }
}

#[allow(dead_code)]
pub fn run_ltx<T: AsRef<ffi::OsStr>>(args: &[T]) {
    let ltx_bin = env::var("LTX_BIN").expect("LTX_BIN env var required");
    let status = process::Command::new(ltx_bin)
        .args(args)
        .status()
        .expect("execute LTX binary");
    assert!(status.success(), "LTX binary non-zero exit code");
}

#[allow(dead_code)]
pub fn compare_files<P1, P2>(f1: P1, f2: P2)
where
    P1: AsRef<path::Path>,
    P2: AsRef<path::Path>,
{
    let f1 = fs::File::open(f1).expect("open first file");
    let f2 = fs::File::open(f2).expect("open second file");

    assert!(
        f1.bytes()
            .map(|b| b.unwrap())
            .eq(f2.bytes().map(|b| b.unwrap())),
        "files are different"
    );
}
