# One image per binary: docker build -f deploy/Dockerfile --build-arg BIN=bedrock-quicksilver -t bedrock-quicksilver .
FROM rust:1.92-bookworm AS build
ARG BIN
# librocksdb-sys generates its bindings with bindgen, which needs libclang.
RUN apt-get update && apt-get install -y --no-install-recommends libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin "$BIN" && cp "target/release/$BIN" /out

FROM debian:bookworm-slim
COPY --from=build /out /usr/local/bin/bedrock
ENV BEDROCK_CONFIG=/etc/bedrock/config.toml BEDROCK_DATA_DIR=/data
VOLUME /data
ENTRYPOINT ["/usr/local/bin/bedrock"]
