# Multi-stage build: compile with the Go toolchain, ship a ~20 MB image that
# contains nothing but the static binaries.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl && adduser -D -u 10001 edgekv \
    && mkdir -p /data && chown edgekv:edgekv /data
COPY --from=build /out/edgekv /out/edgekv-edge /out/edgekv-bench /usr/local/bin/
USER edgekv
# Declared after chown so a fresh named volume copies the ownership.
VOLUME /data
EXPOSE 8080 9090
HEALTHCHECK --interval=5s --timeout=2s --retries=3 CMD curl -fsS http://localhost:8080/healthz || exit 1
ENTRYPOINT ["edgekv"]
