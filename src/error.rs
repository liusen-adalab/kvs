use failure::Fail;
use serde_json;
use std::io;

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

/// custume Result type for kvs
pub type Result<T> = std::result::Result<T, KvsError>;