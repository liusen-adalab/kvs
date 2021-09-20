use crate::common::{GetResponse, Request, RmResponse, SetResponse};
use crate::engines::KvsEngine;
use crate::Result;
use serde_json::Deserializer;
use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use log::{debug, error};

/// The server for key value store
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

/// connect backend, and serve the client
impl<E: KvsEngine> KvsServer<E> {
    /// create a `KvsServer` with given engine 
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    /// Run the serve listening on the given address 
    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream){
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(err) => {
                    error!("Connection failed: {}", err);
                }
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);
        let request_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                debug!("Response sent to {}: {:?}", peer_addr, resp);
            };};
        }
        for request in request_reader {
            let request = request?;
            debug!("Receive request from {}: {:?}", peer_addr, request);

            match request {
                Request::Set { key, value } => send_resp!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(err) => SetResponse::Err(format!("{}", err)),
                }),
                Request::Get { key } => send_resp!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(err) => GetResponse::Err(format!("{}", err)),
                }),
                Request::Remove { key } => send_resp!(match self.engine.remove(key) {
                    Ok(()) => RmResponse::Ok(()),
                    Err(err) => RmResponse::Err(format!("{}", err)),
                }),
            }
        }

        Ok(())
    }
}
