FROM rust:1.65 as builder

WORKDIR /usr/src/kon

COPY Cargo.toml Cargo.lock ./
COPY ./src ./src

RUN cargo build --release
RUN cargo test --release

FROM debian:bullseye-slim

COPY --from=builder /usr/src/kon/target/release/kon ./

USER 1000

CMD ["./kon"]
