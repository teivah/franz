mod kafka;

use actix_web::{middleware, web, App, HttpRequest, HttpServer};
use clap::{App as ClapApp, Arg};

async fn index(req: HttpRequest) -> &'static str {
    println!("REQ: {:?}", req);
    "Hello world!"
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    setup();

    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(web::resource("/index.html").to(|| async { "Hello world!" }))
            .service(web::resource("/").to(index))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

fn setup() {
    let matches = ClapApp::new("franz")
        .arg(
            Arg::with_name("foo")
                .short("f")
                .long("foo")
                .takes_value(true)
                .help("help"),
        )
        .get_matches();

    let foo = matches.value_of("foo").unwrap();
    println!("{}", foo)
}
