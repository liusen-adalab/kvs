use crate::common::{GetResponse, Request, RmResponse, SetResponse};
use crate::engines::KvsEngine;
use crate::thread_pool::ThreadPool;
use crate::Result;
use log::{debug, error};
use serde_json::Deserializer;
use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

/// The server for key value store
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

/// connect backend, and serve the client
impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// create a `KvsServer` with given engine
    pub fn new(engine: E, pool: P) -> Self {
        Self { engine, pool }
    }

    /// Run the serve listening on the given address
    pub fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(|| match stream {
                Ok(stream) => {
                    if let Err(e) = serve(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(err) => {
                    error!("Connection failed: {}", err);
                }
            })
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(engine: E, tcp: TcpStream) -> Result<()> {
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
            Request::Set { key, value } => send_resp!(match engine.set(key, value) {
                Ok(_) => SetResponse::Ok(()),
                Err(err) => SetResponse::Err(format!("{}", err)),
            }),
            Request::Get { key } => send_resp!(match engine.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(err) => GetResponse::Err(format!("{}", err)),
            }),
            Request::Remove { key } => send_resp!(match engine.remove(key) {
                Ok(()) => RmResponse::Ok(()),
                Err(err) => RmResponse::Err(format!("{}", err)),
            }),
        }
    }

    Ok(())
}
