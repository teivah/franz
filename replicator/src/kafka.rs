use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::collections::HashMap;
use std::time::Duration;
use tokio::task;

pub fn create_replicators(
    kafka_hosts: Vec<String>,
    replication: HashMap<String, String>,
) -> Result<(), &'static str> {
    for (source, target) in &replication {
        let consumer = Consumer::from_hosts(kafka_hosts.to_owned())
            .with_topic(source.to_owned())
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group("my-group".to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .expect("consumer creation error");

        let producer = Producer::from_hosts(kafka_hosts.to_owned())
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .expect("producer creation error");

        task::spawn(consume(consumer, producer, target.to_owned()));
    }

    Ok(())
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
