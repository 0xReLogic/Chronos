# Multi-stage build for Chronos

# 1) Builder image
FROM rust:1.91 as builder

# Install protoc for prost/tonic codegen
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create a dummy project to cache dependencies
RUN USER=root cargo new chronos-build
WORKDIR /app/chronos-build

# Copy manifest and lockfile from host
COPY Cargo.toml Cargo.lock ./

RUN mkdir -p benches && touch benches/storage_bench.rs

# Pre-build dependencies (will be cached as long as Cargo.toml/Cargo.lock don't change)
RUN cargo build --release || true

# 2) Real build with source
WORKDIR /app
COPY . ./

RUN cargo build --release --bin chronos

# 3) Runtime image
FROM debian:12-slim

# Install minimal runtime deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/chronos /usr/local/bin/chronos

# Default data directory inside container
ENV CHRONOS_DATA_DIR=/data

# Expose default gRPC port (configurable via CLI)
EXPOSE 8000

# Simple entrypoint: pass all args to chronos
ENTRYPOINT ["chronos"]
