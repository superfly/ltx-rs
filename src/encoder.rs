use crate::{
    ltx::{PageHeader, CRC64},
    Checksum, Header, HeaderFlags, PageNum, PageSize, Result, Trailer,
};
use lz4_flex::frame::{BlockSize, FrameEncoder, FrameInfo};
use std::{
    io::{self, Write},
    result,
};

#[derive(thiserror::Error, Debug)]
pub enum PageEncodeError {
    #[error("cannot encode lock page: {0}")]
    LockPage(PageNum),
    #[error("snapshot transaction file must start with page number 1")]
    FirstSnapshotPage,
    #[error("nonsequential page numbers in snapshot transaction: {0}, {1}")]
    NonsequentialPages(PageNum, PageNum),
    #[error("out-of-order page numbers: {0}, {1}")]
    OutOfOrderPage(PageNum, PageNum),
    #[error("invalid page buffer size: {0}, expected {1}")]
    InvalidBufferSize(usize, PageSize),
    #[error("write error")]
    Write(#[from] io::Error),
}

/// Encoder implements an encoder for LTX files.
pub struct Encoder<'a, W>
where
    W: io::Write,
{
    w: LTXWriter<W>,
    digest: crc::Digest<'a, u64>,
    page_size: PageSize,
    is_snapshot: bool,
    last_page_num: Option<PageNum>,
}

impl<'a, W> Encoder<'a, W>
where
    W: io::Write,
{
    pub fn new(mut w: W, hdr: &Header) -> Result<Encoder<'a, W>> {
        let mut digest = CRC64.digest();
        {
            let writer = CrcDigestWrite::new(&mut w, &mut digest);
            hdr.encode_into(writer)?;
        }

        Ok(Encoder {
            w: LTXWriter::new(w, hdr.flags.contains(HeaderFlags::COMPRESS_LZ4)),
            digest,
            page_size: hdr.page_size,
            is_snapshot: hdr.is_snapshot(),
            last_page_num: None,
        })
    }

    fn validate_page_num(&self, page_num: PageNum) -> result::Result<(), PageEncodeError> {
        let lock = PageNum::lock_page(self.page_size);

        if page_num == lock {
            return Err(PageEncodeError::LockPage(page_num));
        }
        if self.is_snapshot {
            if self.last_page_num.is_none() && page_num != PageNum::ONE {
                return Err(PageEncodeError::FirstSnapshotPage);
            } else if let Some(last) = self.last_page_num {
                if last + 1 != Some(page_num)
                    || last + 1 != Some(lock) && last + 2 != Some(page_num)
                {
                    return Err(PageEncodeError::NonsequentialPages(last, page_num));
                }
            }
        } else if let Some(last) = self.last_page_num {
            if last >= page_num {
                return Err(PageEncodeError::OutOfOrderPage(last, page_num));
            }
        }

        Ok(())
    }

    pub fn encode_page(&mut self, page_num: PageNum, data: &[u8]) -> Result<()> {
        self.validate_page_num(page_num)?;
        if data.len() != self.page_size.into_inner() as usize {
            return Err(PageEncodeError::InvalidBufferSize(data.len(), self.page_size).into());
        }

        let mut writer = CrcDigestWrite::new(&mut self.w, &mut self.digest);
        PageHeader(Some(page_num)).encode_into(&mut writer)?;
        writer.write_all(data).map_err(PageEncodeError::Write)?;

        self.last_page_num = Some(page_num);

        Ok(())
    }

    pub fn finish(mut self, post_apply_checksum: Checksum) -> Result<Trailer> {
        let mut writer = CrcDigestWrite::new(&mut self.w, &mut self.digest);
        PageHeader(None).encode_into(&mut writer)?;

        let writer = self.w.finish().map_err(PageEncodeError::Write)?;
        self.digest
            .update(&post_apply_checksum.into_inner().to_be_bytes());

        let trailer = Trailer {
            post_apply_checksum,
            file_checksum: Checksum::new(self.digest.finalize()),
        };

        trailer.encode_into(writer)?;

        Ok(trailer)
    }
}

struct LTXWriter<W>
where
    W: io::Write,
{
    enc: FrameEncoder<W>,
    compressed: bool,
}

impl<W> LTXWriter<W>
where
    W: io::Write,
{
    fn new(w: W, compressed: bool) -> LTXWriter<W> {
        LTXWriter {
            enc: FrameEncoder::with_frame_info(FrameInfo::new().block_size(BlockSize::Max64KB), w),
            compressed,
        }
    }

    fn finish(self) -> io::Result<W> {
        if self.compressed {
            self.enc
                .finish()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        } else {
            Ok(self.enc.into_inner())
        }
    }
}

impl<W> io::Write for LTXWriter<W>
where
    W: io::Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.compressed {
            self.enc.write(buf)
        } else {
            self.enc.get_mut().write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.compressed {
            self.enc.flush()?;
        }
        self.enc.get_mut().flush()
    }
}

/// An [`io::Write`] computing a digest on the bytes written.
struct CrcDigestWrite<'a, 'b, W>
where
    W: 'a + io::Write,
{
    inner: &'a mut W,
    digest: &'a mut crc::Digest<'b, u64>,
}

impl<'a, 'b, W> CrcDigestWrite<'a, 'b, W>
where
    W: io::Write,
{
    fn new(inner: &'a mut W, digest: &'a mut crc::Digest<'b, u64>) -> Self {
        CrcDigestWrite { inner, digest }
    }
}

impl<'a, 'b, W> io::Write for CrcDigestWrite<'a, 'b, W>
where
    W: io::Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.digest.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::{CrcDigestWrite, Encoder, PageEncodeError};
    use crate::{
        ltx::{self, CRC64},
        Checksum, Error, Header, HeaderFlags, PageNum, PageSize, TXID,
    };
    use std::{io::Write, time};

    #[test]
    fn crc_digest_write() {
        let mut buf = Vec::new();
        let mut digest = CRC64.digest();
        let mut writer = CrcDigestWrite::new(&mut buf, &mut digest);

        assert!(matches!(
            writer.write(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            Ok(10)
        ));
        assert_eq!(6672316476627126589, digest.finalize());
    }

    #[test]
    fn encoder() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(5).unwrap(),
                max_txid: TXID::new(6).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: Some(Checksum::new(5)),
            },
        )
        .expect("failed to create encoder");

        let page1: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();
        let page2: Vec<u8> = (0..4096).map(|_| rand::random::<u8>()).collect();

        enc.encode_page(PageNum::new(1).unwrap(), page1.as_slice())
            .expect("failed to encode page1");
        enc.encode_page(PageNum::new(2).unwrap(), page2.as_slice())
            .expect("failed to encode page2");

        let trailer = enc
            .finish(Checksum::new(6))
            .expect("failed to finish encoder");

        assert_eq!(Checksum::new(6), trailer.post_apply_checksum);
        assert_eq!(
            ltx::HEADER_SIZE + (4096 + 4) * 2 + 4 + ltx::TRAILER_SIZE,
            buf.len()
        );
    }

    #[test]
    fn encoder_compressed() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::COMPRESS_LZ4,
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(5).unwrap(),
                max_txid: TXID::new(6).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: Some(Checksum::new(5)),
            },
        )
        .expect("failed to create encoder");

        let page1: Vec<u8> = (0..4096).map(|_| 1).collect();
        let page2: Vec<u8> = (0..4096).map(|_| 2).collect();

        enc.encode_page(PageNum::new(1).unwrap(), page1.as_slice())
            .expect("failed to encode page1");
        enc.encode_page(PageNum::new(2).unwrap(), page2.as_slice())
            .expect("failed to encode page2");

        let trailer = enc
            .finish(Checksum::new(6))
            .expect("failed to finish encoder");
        assert_eq!(Checksum::new(6), trailer.post_apply_checksum);
        assert!(ltx::HEADER_SIZE + (4096 + 4) * 2 + 4 + ltx::TRAILER_SIZE > buf.len());
    }

    #[test]
    fn encoder_lock_page() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(1).unwrap(),
                max_txid: TXID::new(1).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: None,
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (
            PageNum::lock_page(PageSize::new(4096).unwrap()),
            vec![0; 4096],
        );

        assert!(matches!(
            enc.encode_page(page1_num, page1.as_slice()),
            Err(Error::PageEncode(PageEncodeError::LockPage(p))) if p == page1_num
        ));
    }

    #[test]
    fn encoder_non_sequential() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(1).unwrap(),
                max_txid: TXID::new(1).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: None,
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (PageNum::new(1).unwrap(), vec![0; 4096]);
        let (page3_num, page3) = (PageNum::new(3).unwrap(), vec![0; 4096]);

        enc.encode_page(page1_num, page1.as_slice())
            .expect("failed to encode page1");
        assert!(matches!(
            enc.encode_page(page3_num, page3.as_slice()),
            Err(Error::PageEncode(PageEncodeError::NonsequentialPages(a, b))) if a == page1_num && b == page3_num
        ));
    }

    #[test]
    fn encoder_out_of_order() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(2).unwrap(),
                max_txid: TXID::new(5).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: Some(Checksum::new(1)),
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (PageNum::new(1).unwrap(), vec![0; 4096]);
        let (page3_num, page3) = (PageNum::new(3).unwrap(), vec![0; 4096]);

        enc.encode_page(page3_num, page3.as_slice())
            .expect("failed to encode page3");
        assert!(matches!(
            enc.encode_page(page1_num, page1.as_slice()),
            Err(Error::PageEncode(PageEncodeError::OutOfOrderPage(a, b))) if a == page3_num && b == page1_num
        ));
    }

    #[test]
    fn encoder_snapshot() {
        let mut buf = Vec::new();

        let mut enc = Encoder::new(
            &mut buf,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: PageSize::new(4096).unwrap(),
                commit: PageNum::new(3).unwrap(),
                min_txid: TXID::new(1).unwrap(),
                max_txid: TXID::new(1).unwrap(),
                timestamp: time::SystemTime::now(),
                pre_apply_checksum: None,
            },
        )
        .expect("failed to create encoder");

        let (page1_num, page1) = (PageNum::new(2).unwrap(), vec![0; 4096]);

        assert!(matches!(
            enc.encode_page(page1_num, page1.as_slice()),
            Err(Error::PageEncode(PageEncodeError::FirstSnapshotPage))
        ));
    }
}
