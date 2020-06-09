use kafka::producer::{Producer, Record, RequiredAcks};
use std::borrow::Borrow;
use std::fmt::Write;
use uuid::Uuid;

struct KafkaCfg {
    hosts: Vec<String>,
    requested_required_acks: i8,
}

struct SendCfg {
    topic: String,
    payload: String,
    number_of_messages: i32,
}

struct Tester {
    campaign_id: Uuid,
    producer: Producer,
    send_cfg: SendCfg,
}

impl Tester {
    pub fn new(kafka_cfg: KafkaCfg, send_cfg: SendCfg) -> Result<Self, String> {
        Ok(Tester {
            campaign_id: Uuid::new_v4(),
            producer: create_producer(kafka_cfg)?,
            send_cfg,
        })
    }

    fn produce(&mut self) {
        for i in 0..self.send_cfg.number_of_messages {
            let result = self.producer.send(&Record::from_value(
                self.send_cfg.topic.as_str(),
                self.send_cfg.payload.as_str(),
            ));
        }
    }
}

fn create_producer(kafka_cfg: KafkaCfg) -> Result<Producer, String> {
    let required_acks = match kafka_cfg.requested_required_acks {
        1 => RequiredAcks::One,
        0 => RequiredAcks::None,
        -1 => RequiredAcks::All,
        _ => {
            return Err(format_args!(
                "unknown required acks option ({}), it should be either 1, 0 or -1",
                kafka_cfg.requested_required_acks
            )
            .to_string())
        }
    };

    let producer = Producer::from_hosts(kafka_cfg.hosts)
        .with_required_acks(required_acks)
        .create();

    return match producer {
        Err(e) => Err(format_args!("unable to create producer: {}", e).to_string()),
        Ok(p) => Ok(p),
    };
}
