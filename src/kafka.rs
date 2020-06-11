use kafka::producer::{Producer, Record, RequiredAcks};
use std::borrow::Borrow;
use std::fmt::Write;
use uuid::Uuid;

pub struct SendCfg {
    pub topic: String,
    pub payload: String,
    pub number_of_messages: i32,
    pub requested_required_acks: i8,
}

pub struct Worker {
    id: Uuid,
    producer: Producer,
    send_cfg: SendCfg,
}

impl Worker {
    pub fn new(hosts: Vec<String>, send_cfg: SendCfg) -> Result<Self, String> {
        Ok(Worker {
            id: Uuid::new_v4(),
            producer: create_producer(hosts, send_cfg.requested_required_acks)?,
            send_cfg,
        })
    }

    pub fn produce(mut self) {
        for _ in 0..self.send_cfg.number_of_messages {
            let result = self.producer.send(&Record::from_value(
                self.send_cfg.topic.as_str(),
                self.send_cfg.payload.as_str(),
            ));
            result.expect("error");
        }
    }

    pub fn id(&self) -> String {
        self.id.to_string()
    }
}

fn create_producer(hosts: Vec<String>, requested_required_acks: i8) -> Result<Producer, String> {
    let required_acks = match requested_required_acks {
        1 => RequiredAcks::One,
        0 => RequiredAcks::None,
        -1 => RequiredAcks::All,
        _ => {
            return Err(format_args!(
                "unknown required acks option ({}), it should be either 1, 0 or -1",
                requested_required_acks
            )
            .to_string())
        }
    };

    let producer = Producer::from_hosts(hosts)
        .with_required_acks(required_acks)
        .create();

    return match producer {
        Err(e) => Err(format_args!("unable to create producer: {}", e).to_string()),
        Ok(p) => Ok(p),
    };
}
