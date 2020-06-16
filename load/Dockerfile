FROM rust:1.44.0 as build
WORKDIR /app
COPY Cargo.toml /app/
COPY src/ /app/src
RUN cargo build --release

FROM rust:1.44.0
WORKDIR /app
COPY --from=build /app/target/release/franz /app/
ENTRYPOINT ["/app/franz"]