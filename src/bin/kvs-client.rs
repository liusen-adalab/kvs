use kvs::{KvsClient, Result};
use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;

const DEFAUTL_ADDR: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "set")]
    Set {
        key: String,
        value: String,
        #[structopt(
            long,
            default_value = DEFAUTL_ADDR,
            value_name = ADDRESS_FORMAT,
            help = "Sets the server address"
        )]
        addr: SocketAddr,
    },

    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
            long,
            default_value = DEFAUTL_ADDR,
            value_name = ADDRESS_FORMAT,
            help = "Sets the server address"
        )]
        addr: SocketAddr,
    },

    #[structopt(name = "rm")]
    Remove {
        key: String,
        #[structopt(
            long,
            default_value = DEFAUTL_ADDR,
            value_name = ADDRESS_FORMAT,
            help = "Sets the server address"
        )]
        addr: SocketAddr,
    },
}

fn main() {
    let command = Command::from_args();

    if let Err(err) = run(command) {
        eprintln!("{}", &err);
        exit(1);
    }
}

fn run(command: Command) -> Result<()> {
    match command {
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Remove { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.rm(key)?;
        }
    }

    Ok(())
}
