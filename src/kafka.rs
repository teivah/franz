use kafka::producer::{Producer, Record, RequiredAcks};
use log::warn;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobResult {
    pub messages_sent: i32,
    pub average_latency: f32,
}

struct ProducerState {
    messages_sent: i32,
    duration: Duration,
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

    pub async fn produce(self) -> JobResult {
        let messages_per_producer =
            self.send_cfg.total_number_of_messages / self.send_cfg.producer_count;
        let mut joins: Vec<task::JoinHandle<ProducerState>> = vec![];

        for mut producer in self.producers.into_iter() {
            let topic = self.send_cfg.topic.clone();
            let payload = self.send_cfg.payload.clone();
            let join = task::spawn_blocking(move || {
                let mut sent = 0;
                let start = Instant::now();
                for _ in 0..messages_per_producer {
                    let result =
                        producer.send(&Record::from_value(topic.as_str(), payload.as_str()));
                    sent += 1;
                    if let Err(e) = result {
                        warn!["unable to produce message: {}", e];
                    }
                }
                ProducerState {
                    messages_sent: sent,
                    duration: start.elapsed(),
                }
            });
            joins.push(join);
        }

        let mut messages_sent = 0;
        let mut total_time: u128 = 0;
        for join in joins {
            let producer_state: Result<ProducerState, task::JoinError> = join.await;

            match producer_state {
                Ok(s) => {
                    messages_sent += s.messages_sent;
                    total_time += s.duration.as_millis();
                }
                Err(e) => warn!["unable to produce message: {}", e],
            }
        }
        JobResult {
            messages_sent,
            average_latency: total_time as f32 / messages_sent as f32,
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
