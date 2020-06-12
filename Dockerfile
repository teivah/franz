FROM rustlang/rust:nightly-alpine3.10
COPY ./ ./
RUN cargo build --release
CMD ["./target/release/franz"]