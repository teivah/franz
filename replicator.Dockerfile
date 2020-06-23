FROM rust:1.44.0 as build
WORKDIR /app
COPY replicator/Cargo.toml /app/
COPY replicator/src/ /app/src
RUN cargo build --release

FROM rust:1.44.0
WORKDIR /app
COPY --from=build /app/target/release/franz /app/
ENTRYPOINT ["/app/franz"]