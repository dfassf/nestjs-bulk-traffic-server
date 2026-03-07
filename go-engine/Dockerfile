FROM golang:1.24-bookworm AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY proto/ ./proto/

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /engine \
    ./cmd/engine/

FROM busybox:1.36 AS tools

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=tools /bin/wget /usr/bin/wget
COPY --from=builder /engine /engine

EXPOSE 50051 9090

USER nonroot:nonroot

ENTRYPOINT ["/engine"]
