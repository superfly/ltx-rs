mod encoder;
mod ltx;
mod types;

pub use ltx::{Header, HeaderFlags, Trailer};
pub use types::{Checksum, PageNum, PageSize, TXID};

/// Error represents an error that can be returned from this crate.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("header encoding")]
    HeaderEncode(#[from] ltx::HeaderEncodeError),
    #[error("trailer encoding")]
    TrailerEncode(#[from] ltx::TrailerEncodeError),
    #[error("page header encoding")]
    PageHeaderEncode(#[from] ltx::PageHeaderEncodeError),
    #[error("page encoding")]
    PageEncode(#[from] encoder::PageEncodeError),
}

/// Result is returned from any fallible function of this crate.
pub type Result<T> = std::result::Result<T, Error>;

pub use encoder::Encoder;
