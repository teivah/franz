mod kafka;

use crate::kafka::create_replicators;
use clap::{App as ClapApp, Arg};
use futures::executor::block_on;
use std::collections::HashMap;
use tokio::task;

#[tokio::main]
pub async fn main() {
    let args = ClapApp::new("franz")
        .arg(
            Arg::with_name("kafka_hosts")
                .short("k")
                .long("kafka_hosts")
                .takes_value(true)
                .help("Kafka hosts separated by a colon")
                .required(true),
        )
        .arg(
            Arg::with_name("replication")
                .short("r")
                .long("replication")
                .takes_value(true)
                .help("Replication configuration: e.g. \"a=b;c=d\"")
                .required(true),
        )
        .get_matches();

    let kafka_hosts_args = args.value_of("kafka_hosts").unwrap();
    let kafka_hosts = parse_kafka_hosts(kafka_hosts_args);

    let replication_args = args.value_of("replication").unwrap();
    let replication = parse_replication(replication_args).expect("configuration error");

    task::spawn_blocking(|| create_replicators(kafka_hosts, replication)).await;
}

// TODO reuse?
fn parse_kafka_hosts(kafka_hosts: &str) -> Vec<String> {
    kafka_hosts
        .split(",")
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
}

fn parse_replication(s: &str) -> Result<HashMap<String, String>, &'static str> {
    if s.len() == 0 {
        return Err("no elements");
    }

    let mut map: HashMap<String, String> = HashMap::new();
    let configurations = s.split(";");

    for configuration in configurations {
        let topics: Vec<&str> = configuration.split("=").collect();
        if topics.len() != 2 {
            return Err("syntax error");
        }
        map.insert(
            topics.get(0).unwrap().to_string(),
            topics.get(1).unwrap().to_string(),
        );
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_replication_single_element() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(String::from("a"), String::from("b"));
        assert_eq!(parse_replication("a=b"), Ok(map));
    }

    #[test]
    fn test_parse_replication_multiple_elements() {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert(String::from("a"), String::from("b"));
        map.insert(String::from("c"), String::from("d"));
        map.insert(String::from("e"), String::from("f"));
        assert_eq!(parse_replication("a=b;c=d;e=f"), Ok(map));
    }

    #[test]
    fn test_parse_replication_error_no_elements() {
        assert_eq!(parse_replication(""), Err("no elements"));
    }

    #[test]
    fn test_parse_replication_error_syntax() {
        assert_eq!(parse_replication("a=b=c"), Err("syntax error"));
    }
}
