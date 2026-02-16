# Stage 1: Builder
FROM rust:latest AS builder

WORKDIR /usr/src/smartcopy

# Install build dependencies
RUN apt-get update && apt-get install -y \
    liburing-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/smartcopy/target \
    cargo build --release 2>/dev/null || true
RUN rm -rf src

# Copy actual source and build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/smartcopy/target \
    cargo build --release --all-features && \
    cp target/release/smartcopy /usr/local/bin/smartcopy

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    liburing2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r smartcopy && useradd -r -g smartcopy -m smartcopy

COPY --from=builder /usr/local/bin/smartcopy /usr/local/bin/smartcopy

USER smartcopy
WORKDIR /data

ENTRYPOINT ["smartcopy"]
CMD ["--help"]
