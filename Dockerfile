# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

ARG GO_VERSION=1.27.1
ARG GO_IMAGE_DIGEST=sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125
ARG ALPINE_VERSION=3.24

FROM golang:${GO_VERSION}-alpine@${GO_IMAGE_DIGEST} AS build
RUN apk add --no-cache ca-certificates git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/openclaw/discrawl/internal/cli.version=${VERSION}" \
    -o /out/discrawl ./cmd/discrawl

FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache ca-certificates git openssh-client tzdata \
    && adduser -D -u 10001 -h /home/discrawl discrawl \
    && mkdir -p /data/config /data/data /data/cache /data/state \
    && chown -R discrawl:discrawl /data
ENV HOME=/home/discrawl \
    XDG_CONFIG_HOME=/data/config \
    XDG_DATA_HOME=/data/data \
    XDG_CACHE_HOME=/data/cache \
    XDG_STATE_HOME=/data/state
VOLUME ["/data"]
WORKDIR /data
COPY --from=build /out/discrawl /usr/local/bin/discrawl
USER discrawl
ENTRYPOINT ["discrawl"]
CMD ["--help"]
