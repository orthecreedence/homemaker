//! Handles creation/conversion of errors.

use nom::error::{Error as NomError};
use thiserror::Error;

/// This is our error enum. It contains an entry for any part of the system in
/// which an expectation is not met or a problem occurs.
#[derive(Error, Debug)]
pub enum Error<'a> {
    /// A bad channel name was given.
    #[error("Bad channel name")]
    BadChannel,
    /// There was a problem converting bytes, which are expected to be utf8-encoded,
    /// into a string.
    #[error("Error converting bytes to utf8 string")]
    Conversion(#[from] std::str::Utf8Error),

    /// AN IO ERROR
    #[error("IO error")]
    Io(#[from] std::io::Error),

    /// There was an error parsing the protocol
    #[error("Error parsing the protocol")]
    Parse(NomError<&'a [u8]>),

    /// The parser received incomplete data
    #[error("Incomplete data")]
    ParseIncomplete(nom::Needed),

    /// An unknown command was encountered
    #[error("Unknown command: {0}")]
    UnknownRequest(&'a str),
}

impl<'a> PartialEq for Error<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BadChannel, Self::BadChannel) => true,
            (Self::Conversion(x), Self::Conversion(y)) => x == y,
            (Self::Io(..), _) => false,
            (Self::Parse(x), Self::Parse(y)) => x == y,
            (Self::UnknownRequest(x), Self::UnknownRequest(y)) => x == y,
            _ => false,
        }
    }
}

impl<'a> From<NomError<&'a [u8]>> for Error<'a> {
    fn from(err: NomError<&'a [u8]>) -> Self {
        Self::Parse(err)
    }
}

/// Wraps our precious [`Error`] type in an `std::result::Result`.
pub type Result<'a, T> = std::result::Result<T, Error<'a>>;

