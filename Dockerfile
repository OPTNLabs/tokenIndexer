FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/target/release/tokenindex /usr/local/bin/tokenindex
COPY migrations ./migrations
EXPOSE 8080
CMD ["/usr/local/bin/tokenindex"]
