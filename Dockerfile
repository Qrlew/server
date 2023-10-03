FROM rust:slim AS builder
ARG SECRET_KEY
WORKDIR /app
COPY . .
RUN \
  --mount=type=cache,target=/app/target/ \
  --mount=type=cache,target=/usr/local/cargo/registry/ \
  /bin/bash -c \
  'cargo build --locked --release --package qrlew-server && \
  cp ./target/release/qrlew-server /app && \
  echo ${SECRET_KEY} > /app/secret_key.pem'

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
COPY --from=builder /app/secret_key.pem /usr/local/bin
RUN chown appuser /usr/local/bin/secret_key.pem
USER appuser
WORKDIR /opt/qrlew-server
ENTRYPOINT ["qrlew-server"]
EXPOSE 3000/tcp