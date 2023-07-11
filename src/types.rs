use std::{fmt, num, ops};

// TXID represents a transaction ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TXID(num::NonZeroU64);

impl TXID {
    pub const ONE: TXID = TXID(num::NonZeroU64::MIN);

    /// new constructs new TXID.
    pub const fn new(id: u64) -> Result<Self, TXIDError> {
        if let Some(id) = num::NonZeroU64::new(id) {
            Ok(Self(id))
        } else {
            Err(TXIDError)
        }
    }

    /// into_inner returns underlying integer representation of TXID.
    pub const fn into_inner(&self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for TXID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:016x}", self.0.get())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("transaction ID must be non-zero")]
pub struct TXIDError;

/// Checksum represents a database or file checksum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checksum(u64);

impl Checksum {
    const NON_ZERO_FLAG: u64 = 1 << 63;

    // new constructs new valid checksum.
    pub const fn new(s: u64) -> Self {
        Self(s | Self::NON_ZERO_FLAG)
    }

    // into_inner returns underlying integer representation of checksum.
    pub const fn into_inner(&self) -> u64 {
        self.0
    }
}

impl ops::BitXor<Checksum> for Checksum {
    type Output = Checksum;

    fn bitxor(self, rhs: Checksum) -> Self::Output {
        Checksum::new(self.0 ^ rhs.0)
    }
}

/// PageSize represents a database page size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageSize(u32);

impl PageSize {
    const MIN_PAGE_SIZE: u32 = 512;
    const MAX_PAGE_SIZE: u32 = 65536;

    // new constructs new valid page size
    pub const fn new(s: u32) -> Result<PageSize, PageSizeError> {
        if s < Self::MIN_PAGE_SIZE || s > Self::MAX_PAGE_SIZE || (s & (s - 1)) != 0 {
            Err(PageSizeError(s))
        } else {
            Ok(Self(s))
        }
    }

    pub const fn into_inner(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for PageSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

#[derive(thiserror::Error, Debug)]
#[error("unsupported page size: {0}")]
pub struct PageSizeError(u32);

/// PageNum represents a database page number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageNum(num::NonZeroU32);

impl PageNum {
    pub const ONE: PageNum = PageNum(num::NonZeroU32::MIN);

    pub const fn new(n: u32) -> Result<Self, PageNumError> {
        if let Some(n) = num::NonZeroU32::new(n) {
            Ok(PageNum(n))
        } else {
            Err(PageNumError)
        }
    }

    /// into_inner returns underlying integer representation of TXID.
    pub const fn into_inner(&self) -> u32 {
        self.0.get()
    }

    /// lock_page returns lock_page number for the given page size.
    pub const fn lock_page(page_size: PageSize) -> PageNum {
        PageNum(unsafe { num::NonZeroU32::new_unchecked(0x40000000 / page_size.into_inner() + 1) })
    }
}

impl fmt::Display for PageNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.get())
    }
}

impl ops::Add<u32> for PageNum {
    type Output = Option<PageNum>;

    fn add(self, rhs: u32) -> Self::Output {
        PageNum::new(self.into_inner() + rhs).ok()
    }
}

#[derive(thiserror::Error, Debug)]
#[error("transaction ID must be non-zero")]
pub struct PageNumError;

#[cfg(test)]
mod tests {
    use super::{Checksum, PageNum, PageNumError, PageSize, PageSizeError, TXIDError, TXID};

    #[test]
    fn txid() {
        assert_eq!(10, TXID::new(10).unwrap().into_inner());
        assert!(matches!(TXID::new(0), Err(TXIDError)));
        assert_eq!("000000000000000a", format!("{}", TXID::new(10).unwrap()))
    }

    #[test]
    fn checksum() {
        assert_eq!(1 | Checksum::NON_ZERO_FLAG, Checksum::new(1).into_inner());
        assert_eq!(Checksum::NON_ZERO_FLAG, Checksum::new(0).into_inner());
    }

    #[test]
    fn page_size() {
        assert_eq!(512, PageSize::new(512).unwrap().into_inner());
        assert_eq!(65536, PageSize::new(65536).unwrap().into_inner());
        assert!(matches!(PageSize::new(513), Err(PageSizeError(513))));
        assert!(matches!(PageSize::new(256), Err(PageSizeError(256))));
        assert!(matches!(PageSize::new(131072), Err(PageSizeError(131072))));
    }

    #[test]
    fn page_num() {
        assert_eq!(10, PageNum::new(10).unwrap().into_inner());
        assert!(matches!(PageNum::new(0), Err(PageNumError)));
    }
}
