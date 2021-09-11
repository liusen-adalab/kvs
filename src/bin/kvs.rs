use std::process::exit;

use clap::{crate_version, App, SubCommand};
use kvs::KvStore;

fn main() {
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
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", Some(sub_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", Some(sub_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
