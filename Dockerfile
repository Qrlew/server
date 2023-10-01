FROM rust:slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --locked --release --package qrlew-server && \
  cp ./target/release/qrlew-server /app

FROM debian:stable-slim AS final
RUN adduser \
  --disabled-password \
  --gecos "" \
  --home "/nonexistent" \
  --shell "/sbin/nologin" \
  --no-create-home \
  --uid "10001" \
  appuser
COPY --from=builder /app/qrlew-server /usr/local/bin
RUN chown appuser /usr/local/bin/qrlew-server
USER appuser
WORKDIR /opt/qrlew-server
ENTRYPOINT ["qrlew-server"]
EXPOSE 3000/tcp