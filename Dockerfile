# Dockerfile for a go container serving HTTP on 8080
####################################################################################################
## Builder
####################################################################################################
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY . .
# No UPX here, unlike the older services: a mangled binary only fails at
# startup, which is a bad trade for a few megabytes.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/server -ldflags="-w -s" ./cmd/server.go

####################################################################################################
## Final image
####################################################################################################
FROM alpine AS release
# TLS roots, needed to reach the gateway over https
RUN apk add --no-cache ca-certificates
# The runtime user, having no home dir nor password
RUN adduser -HD -s /bin/ash appuser

WORKDIR /app
# Copy the built app, only allowing our app user to execute it
COPY --from=builder --chmod=0500 --chown=appuser:appuser /app/bin/server ./
USER appuser
EXPOSE 8080
ENTRYPOINT [ "/app/server" ]
