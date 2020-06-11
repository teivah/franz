use kafka::producer::{Producer, Record, RequiredAcks};
use log::warn;
use std::borrow::Borrow;
use std::process::Output;
use tokio::task;
use uuid::Uuid;

pub struct SendCfg {
    pub topic: String,
    pub payload: String,
    pub producer_count: u32,
    pub total_number_of_messages: u32,
    pub requested_required_acks: i8,
}

pub struct Job {
    id: Uuid,
    producers: Vec<Producer>,
    send_cfg: SendCfg,
}

pub struct JobResult {
    pub messages_sent: i32,
}

struct ProducerState {
    messages_sent: i32,
}

impl Job {
    pub fn new(hosts: Vec<String>, send_cfg: SendCfg) -> Result<Self, String> {
        let mut producers: Vec<Producer> = Vec::with_capacity(send_cfg.producer_count as usize);
        for _ in 0..send_cfg.producer_count as usize {
            producers.push(create_producer(
                hosts.clone(),
                send_cfg.requested_required_acks,
            )?);
        }

        Ok(Job {
            id: Uuid::new_v4(),
            producers,
            send_cfg,
        })
    }

    pub fn produce(self) -> JobResult {
        let messages_per_producer =
            self.send_cfg.total_number_of_messages / self.send_cfg.producer_count;
        let mut joins: Vec<task::JoinHandle<ProducerState>> = vec![];

        for mut producer in self.producers.into_iter() {
            let topic = self.send_cfg.topic.clone();
            let payload = self.send_cfg.payload.clone();
            let join = task::spawn(async move {
                for _ in 0..messages_per_producer {
                    let result =
                        producer.send(&Record::from_value(topic.as_str(), payload.as_str()));
                    if let Err(e) = result {
                        warn!["unable to produce message: {}", e];
                    }
                }
                ProducerState { messages_sent: 4 }
            });
            joins.push(join);
        }

        JobResult { messages_sent: 1 }
        // async {
        //     let mut messages_sent = 0;
        //     for join in joins {
        //         let producer_state: Result<ProducerState, task::JoinError> = join.await;
        //
        //         match producer_state {
        //             Ok(s) => messages_sent += s.messages_sent,
        //             Err(e) => (),
        //         }
        //     }
        //     JobResult { messages_sent }
        // }
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
