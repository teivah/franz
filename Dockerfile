FROM rust:latest as cargo-build
WORKDIR /usr/src/franz
COPY Cargo.toml Cargo.toml
RUN mkdir src/
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/franz*
COPY . .
RUN cargo build --release
RUN cargo install --path .

FROM alpine:latest
COPY --from=cargo-build /usr/local/cargo/bin/franz /usr/local/bin/franz
CMD ["franz"]