mod kafka;

use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer};
use clap::{App as ClapApp, Arg, ArgMatches};
use futures::Future;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use tokio::task;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let args = ClapApp::new("franz")
        .arg(
            Arg::with_name("kafka_hosts")
                .short("k")
                .long("kafka_hosts")
                .takes_value(true)
                .help("Kafka hosts separated by a colon"),
        )
        .get_matches();
    let kafka_hosts_args = args
        .value_of("kafka_hosts")
        .expect("kafka_hosts argument not set");

    let kafka_hosts = parse_kafka_hosts(kafka_hosts_args);

    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    HttpServer::new(move || {
        App::new()
            .data(kafka_hosts.clone())
            .wrap(middleware::Logger::default())
            .service(web::resource("/produce").route(web::post().to(produce)))
    })
    .bind("127.0.0.1:8080")?
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
struct Request {
    topic: String,
    payload: String,
    number_of_messages: i32,
    requested_required_acks: i8,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    id: String,
}

async fn produce<'a>(kafka_hosts: web::Data<Vec<String>>, req: web::Json<Request>) -> HttpResponse {
    let worker_result = kafka::Worker::new(
        kafka_hosts.to_vec(),
        kafka::SendCfg {
            topic: (&req.topic).to_string(),
            payload: (&req.payload).to_string(),
            number_of_messages: req.number_of_messages,
            requested_required_acks: req.requested_required_acks,
        },
    );

    let worker = match worker_result {
        Err(e) => return HttpResponse::InternalServerError().body("unable to create worker"),
        Ok(e) => e,
    };
    let id = worker.id();

    task::spawn(async move { worker.produce() });
    HttpResponse::Ok().json(Response { id })
}
