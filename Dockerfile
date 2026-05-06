FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY pg-schema-cache/ pg-schema-cache/
COPY pg-query-engine/ pg-query-engine/
COPY pg-rest-server-tokio-postgres/ pg-rest-server-tokio-postgres/

RUN cargo build --release -p pg-rest-server-tokio-postgres

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /src/target/release/pg-rest-server-tokio-postgres /usr/local/bin/pg-rest-server

EXPOSE 3000

ENTRYPOINT ["pg-rest-server"]
CMD ["--config", "/etc/pg-rest/pg-rest.toml"]
