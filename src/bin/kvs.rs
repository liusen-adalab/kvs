use clap::{crate_version, App, SubCommand};
use kvs::KvStore;

fn main() {
    let matches = App::new("kvs")
        .version(crate_version!())
        .setting(clap::AppSettings::SubcommandRequired)
        .subcommand(
            SubCommand::with_name("set")
                .arg_from_usage("<key>")
                .arg_from_usage("<value>"),
        )
        .subcommands([
            SubCommand::with_name("get").arg_from_usage("<key>"),
            SubCommand::with_name("rm").arg_from_usage("<key>"),
        ])
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("key").unwrap();
        let kvs = KvStore::new();
        if let Some(value) = kvs.get(key.to_string()) {
            println!("{}", value);
        } else {
            println!("cannnot find the value");
        }
    }

    if let Some(sub_match) = matches.subcommand_matches("set") {
        let key = sub_match.value_of("key").unwrap();
        let value = sub_match.value_of("value").unwrap();
        let kvs = KvStore::new();
        kvs.set(key.to_string(), value.to_string());
    }

    if let Some(sub_match) = matches.subcommand_matches("rm") {
        let key = sub_match.value_of("key").unwrap();
        let kvs = KvStore::new();
        kvs.remove(key.to_string());
    }
}
