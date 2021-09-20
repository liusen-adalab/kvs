use failure::Fail;
use serde_json;
use std::io;
use std::string::FromUtf8Error;

/// Error type for kvs
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Io error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// error of serde
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    /// Key not found error
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// unexpected command
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),

    /// Sled error
    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),

    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::Sled(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> Self {
        KvsError::Utf8(err)
    }
}

/// custume Result type for kvs
pub type Result<T> = std::result::Result<T, KvsError>;
