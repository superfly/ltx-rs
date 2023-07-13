use ltx::PageChecksum;
use std::{
    ffi::OsString,
    fs,
    io::{Read, Write},
    mem, time,
};

mod common;

#[test]
#[cfg_attr(not(feature = "compat"), ignore)]
fn encode_uncompressed() {
    encode(ltx::HeaderFlags::empty());
}

#[test]
#[cfg_attr(not(feature = "compat"), ignore)]
fn encode_compressed() {
    encode(ltx::HeaderFlags::COMPRESS_LZ4);
}

#[cfg_attr(not(feature = "compat"), ignore)]
fn encode(flags: ltx::HeaderFlags) {
    // Setup test DB
    let test_db = common::setup_test_db();
    let ltx_out = common::temp_file();
    let db_out = common::temp_file();

    // Create an LTX file
    let w = fs::File::create(&ltx_out).expect("create LTX file");
    let mut enc = ltx::Encoder::new(
        &w,
        &ltx::Header {
            flags,
            page_size: test_db.page_size,
            commit: test_db.page_count,
            min_txid: ltx::TXID::ONE,
            max_txid: ltx::TXID::ONE,
            timestamp: time::SystemTime::now(),
            pre_apply_checksum: None,
        },
    )
    .expect("create LTX encoder");

    let mut r = fs::File::open(&test_db.path).expect("open DB file");
    let mut buf = vec![0; test_db.page_size.into_inner() as usize];
    let mut checksum = ltx::Checksum::new(0);
    for pgno in 1..=test_db.page_count.into_inner() {
        let pgno = ltx::PageNum::new(pgno).unwrap();
        r.read_exact(&mut buf).expect("read DB page");
        enc.encode_page(pgno, buf.as_slice()).expect("encode page");
        checksum = checksum ^ buf.page_checksum(pgno);
    }
    enc.finish(checksum).expect("finish LTX encoder");
    w.sync_all().expect("sync LTX file");
    mem::drop(w);

    // Decode using Go's decoder
    common::run_ltx(&[
        "apply",
        "-db",
        &db_out.to_string_lossy(),
        &ltx_out.to_string_lossy(),
    ]);

    common::compare_files(&test_db.path, &db_out);
}

#[test]
#[cfg_attr(not(feature = "compat"), ignore)]
fn decode_uncompressed() {
    decode(false);
}

#[test]
#[cfg_attr(not(feature = "compat"), ignore)]
fn decode_compressed() {
    decode(true);
}

#[cfg_attr(not(feature = "compat"), ignore)]
fn decode(compressed: bool) {
    // Setup test DB
    let test_db = common::setup_test_db();
    let ltx_out = common::temp_file();
    let db_out = common::temp_file();

    // Encode using Go's decoder
    let mut args: Vec<OsString> = vec!["encode-db".into(), "-o".into(), ltx_out.as_os_str().into()];
    if compressed {
        args.push("-c".into())
    }
    args.push(test_db.path.as_os_str().into());
    common::run_ltx(&args);

    // Decode LTX file
    let r = fs::File::open(&ltx_out).expect("open LTX file");
    let (mut dec, _) = ltx::Decoder::new(r).expect("create LTX decoder");
    let mut w = fs::File::create(&db_out).expect("create DB file");

    let mut buf = vec![0; test_db.page_size.into_inner() as usize];
    let mut checksum = ltx::Checksum::new(0);
    while let Some(pgno) = dec.decode_page(buf.as_mut_slice()).expect("decode DB page") {
        w.write_all(buf.as_slice()).expect("write DB page");
        checksum = checksum ^ buf.page_checksum(pgno);
    }
    let trailer = dec.finish().expect("finish LTX decoder");

    assert_eq!(checksum, trailer.post_apply_checksum);

    common::compare_files(&test_db.path, &db_out);
}
