# ---- Build Stage ----
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /raft-kv-server ./cmd/server

# ---- Runtime Stage ----
FROM scratch

COPY --from=builder /raft-kv-server /raft-kv-server

EXPOSE 8080

ENTRYPOINT ["/raft-kv-server"]
CMD ["--id=0", "--http=:8080", "--cluster=3"]
