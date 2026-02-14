# FDB client library stage - extract from community image with headers
FROM celrenheit/foundationdb:7.3.46 AS fdb

# Build stage
FROM golang:1.25 AS builder

# Copy FDB client library and headers for CGO
COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
COPY --from=fdb /usr/include/foundationdb /usr/include/foundationdb

WORKDIR /workspace

# Copy f4a source
COPY f4a ./f4a

WORKDIR /workspace/f4a

RUN go mod download

RUN CGO_ENABLED=1 GOOS=linux go build -o /dispatch ./cmd/dispatch

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy FDB client library
COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so

WORKDIR /app

COPY --from=builder /dispatch .

EXPOSE 8082

ENTRYPOINT ["/app/dispatch"]
