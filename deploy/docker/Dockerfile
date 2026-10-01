FROM rust:1.96.0 AS builder
ENV CARGO_HOME=/cargo-home CARGO_TARGET_DIR=/target RUSTUP_TOOLCHAIN=1.96.0
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release --locked -p kv-node --bin kv-node

# Match the builder's Debian generation and libc.
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 raft \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin raft \
    && mkdir -p /data \
    && chown 10001:10001 /data
COPY --from=builder /target/release/kv-node /usr/local/bin/kv-node
ENV HTTP_HEALTH_PORT=8100
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail --silent --show-error --max-time 2 "http://127.0.0.1:${HTTP_HEALTH_PORT}/status" > /dev/null || exit 1
# The application drains accepted work before the container stop grace expires.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/kv-node"]
