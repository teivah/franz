mod kafka;

use crate::kafka::{create_replicators, KafkaConfig};
use clap::{App as ClapApp, Arg};
use futures::executor::block_on;
use std::collections::HashMap;
use std::time::Duration;
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
        .arg(
            Arg::with_name("offset")
                .short("o")
                .long("offset")
                .takes_value(true)
                .help("Offset: 0 (earliest) or 1 (latest)")
                .required(true),
        )
        .arg(
            Arg::with_name("required-acks")
                .short("a")
                .long("required-acks")
                .takes_value(true)
                .help("Number of required acks for the producer: 0 (none), 1 (one), -1 (all)")
                .required(true),
        )
        .arg(
            Arg::with_name("group")
                .short("g")
                .long("group")
                .takes_value(true)
                .help("Consumer group")
                .required(true),
        )
        .arg(
            Arg::with_name("producer-timeout")
                .short("t")
                .long("producer-timeout")
                .takes_value(true)
                .help("Producer timeout in ms")
                .required(true),
        )
        .get_matches();

    let kafka_hosts_args = args.value_of("kafka_hosts").unwrap();
    let kafka_hosts = parse_kafka_hosts(kafka_hosts_args);

    let replication_args = args.value_of("replication").unwrap();
    let replication = parse_replication(replication_args).expect("configuration error");

    let offset_args = args.value_of("offset").unwrap();
    let offset = offset_args.parse::<i8>().expect("unable to parse offset");

    let required_ack_args = args.value_of("required-acks").unwrap();
    let required_acks = required_ack_args
        .parse::<i8>()
        .expect("unable to parse required-acks");

    let group = args.value_of("group").unwrap().to_owned();

    let producer_timeout_args = args.value_of("producer-timeout").unwrap();
    let producer_timeout = Duration::from_millis(
        producer_timeout_args
            .parse::<u64>()
            .expect("unable to parse producer-timeout"),
    );

    task::spawn_blocking(move || {
        create_replicators(
            kafka_hosts,
            replication,
            KafkaConfig {
                // TODO
                offset,
                group: group,
                required_acks,
                producer_timeout: producer_timeout,
            },
        )
    })
    .await;
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
