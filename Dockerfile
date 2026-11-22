# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/lora-mqtt-bridge ./cmd/lora-mqtt-bridge

# Minimal scratch image (~8 MB) with TLS root CAs and non-root UID
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /out/lora-mqtt-bridge /lora-mqtt-bridge

EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/lora-mqtt-bridge"]
