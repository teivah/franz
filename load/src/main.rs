mod kafka;

#[macro_use]
extern crate lazy_static;

use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use clap::{App as ClapApp, Arg};
use dashmap::DashMap;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use tokio::task;

lazy_static! {
    static ref STATE: DashMap<String, kafka::JobResult> = DashMap::new();
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
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
            Arg::with_name("http_port")
                .short("p")
                .long("http_port")
                .takes_value(true)
                .help("HTTP port")
                .required(true),
        )
        .get_matches();

    let kafka_hosts_args = args
        .value_of("kafka_hosts")
        .expect("kafka_hosts argument not set");
    let http_port = args
        .value_of("http_port")
        .expect("http_port argument not set");

    let kafka_hosts = parse_kafka_hosts(kafka_hosts_args);

    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    HttpServer::new(move || {
        App::new()
            .data(kafka_hosts.clone())
            .wrap(middleware::Logger::default())
            .service(web::resource("/produce").route(web::post().to(produce)))
            .service(web::resource("/status/{id}").route(web::get().to(get_status)))
    })
    .bind(format!("0.0.0.0:{}", http_port))?
    .run()
    .await
}

fn parse_kafka_hosts(kafka_hosts: &str) -> Vec<String> {
    kafka_hosts
        .split(",")
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
}

#[derive(Debug, Serialize, Deserialize)]
struct JobRequest {
    topic: String,
    payload: String,
    expected_total: u32,
    producers: u32,
    requested_required_acks: i8,
}

#[derive(Debug, Serialize, Deserialize)]
struct JobCreated {
    links: Link,
}

#[derive(Debug, Serialize, Deserialize)]
struct Link {
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct JobResult {
    pub messages_sent: i32,
    pub average_latency_ms: f32,
}

async fn produce(kafka_hosts: web::Data<Vec<String>>, req: web::Json<JobRequest>) -> HttpResponse {
    let worker_result = kafka::Job::new(
        kafka_hosts.to_vec(),
        kafka::SendCfg {
            topic: (&req.topic).to_string(),
            payload: (&req.payload).to_string(),
            total_number_of_messages: req.expected_total,
            requested_required_acks: req.requested_required_acks,
            producer_count: req.producers,
        },
    );

    let worker = match worker_result {
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("unable to create worker: {}", e))
        }
        Ok(e) => e,
    };
    STATE.insert(
        worker.id().clone(),
        kafka::JobResult {
            on_going: true,
            messages_sent: 0,
            average_latency_ms: 0.0,
        },
    );

    let task_id = worker.id().clone();
    let response_id = worker.id().clone();
    task::spawn_blocking(|| STATE.insert(task_id, block_on(worker.produce())));
    HttpResponse::Created().json(JobCreated {
        links: Link {
            status: format!["/status/{}", response_id],
        },
    })
}

async fn get_status(path: web::Path<(String,)>) -> HttpResponse {
    let id = &path.0;
    let status = STATE.get(id);
    match status {
        Some(s) => {
            return match s.on_going {
                true => HttpResponse::NoContent().finish(),
                false => HttpResponse::Ok().json(JobResult {
                    messages_sent: s.messages_sent,
                    average_latency_ms: s.average_latency_ms,
                }),
            }
        }
        None => HttpResponse::NotFound().finish(),
    }
}
