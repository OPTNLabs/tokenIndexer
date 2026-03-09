# syntax=docker/dockerfile:1.7

FROM rust:1.88-bookworm AS build
WORKDIR /app
ARG CARGO_PROFILE=docker-release

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/app/target \
    cargo build --profile "${CARGO_PROFILE}" --locked && \
    rm -rf src

COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/app/target \
    cargo build --profile "${CARGO_PROFILE}" --locked && \
    cp "/app/target/${CARGO_PROFILE}/tokenindex" /tmp/tokenindex && \
    strip /tmp/tokenindex

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /tmp/tokenindex /usr/local/bin/tokenindex
COPY migrations ./migrations
EXPOSE 8080
CMD ["/usr/local/bin/tokenindex"]
