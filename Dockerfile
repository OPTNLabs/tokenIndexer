# syntax=docker/dockerfile:1.7

FROM rust:1.88-bookworm AS build
WORKDIR /app
ARG CARGO_PROFILE=docker-release

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/tmp/target-warm \
    CARGO_TARGET_DIR=/tmp/target-warm cargo build --profile "${CARGO_PROFILE}" --locked && \
    rm -rf src

COPY src ./src
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    cargo build --profile "${CARGO_PROFILE}" --locked && \
    "/app/target/${CARGO_PROFILE}/tokenindex" --help >/tmp/tokenindex-help.txt && \
    grep -q "Usage:" /tmp/tokenindex-help.txt && \
    cp "/app/target/${CARGO_PROFILE}/tokenindex" /tmp/tokenindex && \
    strip /tmp/tokenindex

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /tmp/tokenindex /usr/local/bin/tokenindex
COPY --from=build /app/migrations ./migrations
COPY --chmod=755 docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []
