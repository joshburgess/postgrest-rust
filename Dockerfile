FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY pg-schema-cache-types/ pg-schema-cache-types/
COPY pg-schema-cache-tokio-postgres/ pg-schema-cache-tokio-postgres/
COPY pg-schema-cache-resolute/ pg-schema-cache-resolute/
COPY pg-query-engine/ pg-query-engine/
COPY pg-rest-server-tokio-postgres-pg-wired/ pg-rest-server-tokio-postgres-pg-wired/
COPY pg-rest-server-tokio-postgres-deadpool/ pg-rest-server-tokio-postgres-deadpool/
COPY pg-rest-server-resolute/ pg-rest-server-resolute/
COPY test/ test/

ARG BACKEND=pg-rest-server-tokio-postgres-pg-wired
RUN cargo build --release -p ${BACKEND}
RUN cp /src/target/release/${BACKEND} /src/target/release/pg-rest-server

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /src/target/release/pg-rest-server /usr/local/bin/pg-rest-server

EXPOSE 3000

ENTRYPOINT ["pg-rest-server"]
CMD ["--config", "/etc/pg-rest/pg-rest.toml"]
