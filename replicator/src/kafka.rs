use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::collections::HashMap;
use std::time::Duration;
use tokio::task;

pub struct KafkaConfig {
    pub offset: i8,
    pub group: String,
    pub required_acks: i8,
    pub producer_timeout: Duration,
}

pub fn create_replicators(
    kafka_hosts: Vec<String>,
    replication: HashMap<String, String>,
    kafka_config: KafkaConfig,
) -> Result<(), &'static str> {
    let required_acks = parse_required_acks(kafka_config.required_acks)?;
    let offset = parse_offset(kafka_config.offset)?;

    for (source, target) in &replication {
        let consumer = Consumer::from_hosts(kafka_hosts.to_owned())
            .with_topic(source.to_owned())
            .with_fallback_offset(offset)
            .with_group(kafka_config.group.to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .expect("consumer creation error");

        let producer = Producer::from_hosts(kafka_hosts.to_owned())
            .with_ack_timeout(kafka_config.producer_timeout)
            .with_required_acks(required_acks)
            .create()
            .expect("producer creation error");

        task::spawn(consume(consumer, producer, target.to_owned()));
    }

    Ok(())
}

pub fn parse_required_acks(
    required_acks: i8,
) -> Result<kafka::producer::RequiredAcks, &'static str> {
    match required_acks {
        0 => Ok(RequiredAcks::None),
        1 => Ok(RequiredAcks::One),
        -1 => Ok(RequiredAcks::All),
        _ => Err("unknown required acks"),
    }
}

pub fn parse_offset(fallback_offset: i8) -> Result<kafka::consumer::FetchOffset, &'static str> {
    match fallback_offset {
        0 => Ok(FetchOffset::Earliest),
        1 => Ok(FetchOffset::Latest),
        _ => Err("unknown fallback offset"),
    }
}

pub async fn consume(mut consumer: Consumer, mut producer: Producer, target: String) {
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                producer
                    .send(&Record::from_key_value(target.as_str(), m.key, m.value))
                    .unwrap();
            }
            consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
    }
}
