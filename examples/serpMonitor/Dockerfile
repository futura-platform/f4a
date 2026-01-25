# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Remove the local replace directive for Docker build
RUN sed -i '/replace github.com\/futura-platform\/futura/d' go.mod

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the serpMonitor binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /serpmonitor ./examples/serpMonitor

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /serpmonitor .

EXPOSE 8080

ENTRYPOINT ["/app/serpmonitor"]
