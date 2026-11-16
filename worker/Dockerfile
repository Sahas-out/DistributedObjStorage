# Build context is the repository root, because the worker module depends on
# the root module (pkg/protocol) through `replace ... => ../` in worker/go.mod:
#   docker build -f worker/Dockerfile -t dos-worker .

FROM golang:1.26-alpine AS build
WORKDIR /src
# Root module (shared pkg/protocol), then the worker module's dependency list,
# so `go mod download` stays cached until go.mod/go.sum change.
COPY go.mod ./
COPY pkg/ ./pkg/
COPY worker/go.mod worker/go.sum ./worker/
WORKDIR /src/worker
RUN go mod download
COPY worker/ ./
# The SQLite driver (modernc.org/sqlite) is pure Go, so the binary stays static.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.20
RUN addgroup -S app && adduser -S -G app app \
    && mkdir -p /data && chown app:app /data
COPY --from=build /out/worker /usr/local/bin/worker
USER app
ENV PORT=8080 DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/worker"]
