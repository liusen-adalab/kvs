use clap::AppSettings;
use clap::{crate_version, App, SubCommand};
use kvs::Result;
use kvs::{KvStore, KvsError};
use std::env::current_dir;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequired)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg_from_usage("<key> 'a string key'")
                .arg_from_usage("<value> 'a string value'"),
        )
        .subcommands([
            SubCommand::with_name("get").arg_from_usage("<key>"),
            SubCommand::with_name("rm").arg_from_usage("<key>"),
        ])
        .get_matches();

    match matches.subcommand() {
        ("set", Some(sub_matches)) => {
            let key = sub_matches.value_of("key").unwrap();
            let value = sub_matches.value_of("value").unwrap();
            let mut kvs = KvStore::open(current_dir()?)?;
            kvs.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(sub_matches)) => {
            let key = sub_matches.value_of("key").unwrap();
            let mut kvs = KvStore::open(current_dir()?)?;

            if let Some(value) = kvs.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("{}", "Key not found");
            }
        }
        ("rm", Some(sub_matches)) => {
            let key = sub_matches.value_of("key").unwrap();
            let mut kvs = KvStore::open(current_dir()?)?;
            match kvs.remove(key.to_string()) {
                Ok(_) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("{}", "Key not found");
                    exit(1);
                }
                Err(err) => return Err(err),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
