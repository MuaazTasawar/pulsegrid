# syntax=docker/dockerfile:1.7

# One image holds both binaries; Compose chooses which one runs with `command:`
# (api-coordinator or fanout-worker).
#
# The builder image's Rust version must match rust-toolchain.toml (1.97).

FROM rust:1.97-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*
COPY . .
# The image already contains the pinned compiler. Without this line rustup
# would read rust-toolchain.toml and download its components again.
RUN rm -f rust-toolchain.toml
# BuildKit cache mounts keep the downloaded crates and the compiled dependencies
# between builds, so a rebuild only recompiles what changed. The mounts are not
# part of the image, so the binaries are copied out inside the same step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p api-coordinator -p fanout-worker \
 && mkdir -p /out \
 && cp target/release/api-coordinator target/release/fanout-worker /out/

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin pulsegrid
COPY --from=builder /out/api-coordinator /usr/local/bin/api-coordinator
COPY --from=builder /out/fanout-worker /usr/local/bin/fanout-worker
# Neither binary needs root: ports are above 1024 and nothing is written to disk.
USER pulsegrid
EXPOSE 8090 8081
CMD ["api-coordinator"]
