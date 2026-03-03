# FDB client library stage - extract from community image with headers
FROM celrenheit/foundationdb:7.3.46 AS fdb

# Build stage
FROM public.ecr.aws/docker/library/golang:1.26.0-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates ccache \
    && rm -rf /var/lib/apt/lists/*

# Copy FDB client library and headers for CGO
COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
COPY --from=fdb /usr/include/foundationdb /usr/include/foundationdb

WORKDIR /workspace/f4a

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/.cache/ccache \
    CCACHE_DIR=/root/.cache/ccache \
    CC="ccache gcc" CGO_ENABLED=1 GOOS=linux go build -o /dispatch ./cmd/dispatch

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
