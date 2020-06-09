use kafka::producer::{Producer, Record, RequiredAcks};
use std::borrow::Borrow;
use std::fmt::Write;
use uuid::Uuid;

struct KafkaConfiguration {
    hosts: Vec<String>,
    requested_required_acks: i8,
}

struct TestConfiguration<'a> {
    topic: &'a str,
    payload: &'a str,
}

struct Tester<'a> {
    campaignID: &'a str,
    producer: Producer,
}

impl Tester<'_> {
    pub fn new(hosts: Vec<String>, requested_required_acks: i8) -> Result<Self, String> {
        let result = create_producer(hosts, requested_required_acks)?;

        Ok(Tester {
            campaignID: Uuid::new_v4().to_string().as_str(),
            producer: result,
        })
    }

    fn produce(&self) {
        let mut buf = String::with_capacity(2);
        for i in 0..10 {
            let _ = write!(&mut buf, "{}", i); // some computation of the message data to be sent
            self.producer
                .send(&Record::from_value("my-topic", buf.as_bytes()))
                .unwrap();
            buf.clear();
        }
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

    let result_producer = Producer::from_hosts(hosts)
        .with_required_acks(required_acks)
        .create();
    match result_producer {
        Err(e) => return Err(format_args!("unable to create producer: {}", e).to_string()),
        Ok(p) => return Ok(p),
    }
}
