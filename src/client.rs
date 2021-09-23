use crate::common::{GetResponse, Request, RmResponse, SetResponse};
use crate::{KvsError, Result};
use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpStream, ToSocketAddrs},
};

/// key value store client
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Connect to `addr` to access `KvsServer`
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;

        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        })
    }

    /// Get the value of the given key from the server
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;

        let resp = GetResponse::deserialize(&mut self.reader)?;
        match resp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    /// Set the value of a string key in the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;

        let resp = SetResponse::deserialize(&mut self.reader)?;
        match resp {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    /// Remove a string key in the server.
    pub fn rm(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;

        let resp = RmResponse::deserialize(&mut self.reader)?;
        match resp {
            RmResponse::Ok(_) => Ok(()),
            RmResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }
}
