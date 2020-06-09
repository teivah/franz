mod kafka;

use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer};
use clap::{App as ClapApp, Arg, ArgMatches};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let args = setupApplication();
    let kafka_hosts = args
        .value_of("kafka_hosts")
        .expect("kafka_hosts argument not set");

    // let server = ;
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let req = Server::new(kafka_hosts);
    let f1 = |x: web::Json<Request>| Server::produce(req, x);

    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(web::resource("/produce").route(web::post().to(f1)))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

fn setupApplication() -> &'static ArgMatches<'static> {
    &ClapApp::new("franz")
        .arg(
            Arg::with_name("kafka_hosts")
                .short("k")
                .long("kafka_hosts")
                .takes_value(true)
                .help("Kafka hosts separated by a colon"),
        )
        .get_matches()
}

struct Server {
    kafka_hosts: Vec<String>,
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

impl Server {
    fn new(kafka_hosts: &str) -> &'static Self {
        &Server {
            kafka_hosts: kafka_hosts
                .split(",")
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        }
    }

    async fn produce(&self, req: web::Json<Request>) -> HttpResponse {
        let worker_result = kafka::Worker::new(
            self.kafka_hosts.to_vec(),
            kafka::SendCfg {
                topic: (&req.topic).to_string(),
                payload: (&req.payload).to_string(),
                number_of_messages: req.number_of_messages,
                requested_required_acks: req.requested_required_acks,
            },
        );

        let mut worker = match worker_result {
            Err(e) => return HttpResponse::InternalServerError().body(e),
            Ok(e) => e,
        };

        let _ = worker.produce();
        HttpResponse::Ok().json(Response { id: worker.id() })
    }
}
