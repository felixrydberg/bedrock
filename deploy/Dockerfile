# One image per binary: docker build -f deploy/Dockerfile --build-arg BIN=bedrock-kv -t bedrock-kv .
FROM rust:1.92-bookworm AS build
ARG BIN
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
