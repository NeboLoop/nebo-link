# oal-relay: one static binary on an empty image.
#
#   docker build -t oal-relay -f crates/oal-relay/Dockerfile .
#   docker run -d -p 8480:8480 -v oal-relay:/data oal-relay
#
# Build from the repository root (the workspace). The relay keeps its store
# and admin token in /data; mount a volume there.

FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
ARG CARGO_BUILD_JOBS
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p oal-relay \
 && mkdir -p /out/data \
 && cp target/release/oal-relay /out/oal-relay

FROM scratch
COPY --from=build /out/oal-relay /oal-relay
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV OAL_RELAY_DATA_DIR=/data \
    OAL_RELAY_LISTEN=0.0.0.0:8480
EXPOSE 8480
VOLUME ["/data"]
ENTRYPOINT ["/oal-relay"]
CMD ["serve"]
