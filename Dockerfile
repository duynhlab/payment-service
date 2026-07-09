FROM docker.io/library/golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/payment-service ./cmd/main.go

# Pinned base (not :latest) for reproducibility; busybox provides the wget the
# compose healthcheck uses. Runs as a non-root user (least privilege; also
# satisfies the cluster PSS-restricted runAsNonRoot policy).
FROM alpine:3.21
RUN apk --no-cache add ca-certificates \
    && adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /app/payment-service .
USER 10001
EXPOSE 8080
# ENTRYPOINT (not CMD) so the migrate init container/compose can pass the
# `migrate` subcommand via args while the main container serves with no args.
ENTRYPOINT ["./payment-service"]
