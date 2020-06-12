FROM rust:1.23
COPY ./ ./
RUN cargo build --release
CMD ["./target/release/franz"]