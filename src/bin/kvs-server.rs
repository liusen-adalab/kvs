use clap::arg_enum;
use kvs::{KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};
use log::LevelFilter;
use log::{error, info, warn};
use core::num;
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;
use kvs::thread_pool::{ThreadPool, SharedQueueThreadPool};

const DEFAUTL_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::Kvs;

#[derive(Debug, StructOpt)]
struct Command {
    #[structopt(
        long,
        default_value = DEFAUTL_ADDR,
        help = "Set the listening address"
        )]
    addr: SocketAddr,

    #[structopt(
        long, 
        possible_values = &Engine::variants(), 
        help = "Set the storage engine",
        value_name = "ENGINE-NAME", case_insensitive = true)] 
    engine: Option<Engine>, 
}
        
arg_enum! {
    #[derive(Debug, PartialEq, Clone, Copy)]
    enum Engine{
        Kvs,
        Sled,
    }
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let mut cmd = Command::from_args();
    let result = get_pre_engine().and_then(|op| {
        if cmd.engine.is_none() {
            cmd.engine = op;
        }
        if op.is_some() && op != cmd.engine {
            error!("wrong engine!");
            exit(1);
        }
        run(cmd)
    });

    if let Err(e) = result {
        error!("{}", &e);
        exit(1);
    }
}

fn run(cmd: Command) -> Result<()> {
    let engine = cmd.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", cmd.addr);

    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::Kvs => run_with_engine(KvStore::open(current_dir()?)?, cmd.addr),
        Engine::Sled => run_with_engine(SledKvsEngine::new(sled::open(current_dir()?)?), cmd.addr),
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let thread_pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
    let server = KvsServer::new(engine, thread_pool);
    server.run(addr)
}

fn get_pre_engine() -> Result<Option<Engine>> {
    let engine_path = current_dir()?.join("engine");
    if !engine_path.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_path)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
