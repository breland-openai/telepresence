FROM --platform=$BUILDPLATFORM golang:1.26.3-alpine@sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d AS builder

ENV GOTOOLCHAIN=local

RUN apk add --no-cache --virtual .build-deps gcc libc-dev
WORKDIR /docker-network-teleroute
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /go/bin/docker-network-teleroute

FROM alpine
RUN apk add --no-cache bash
COPY --from=builder /go/bin/docker-network-teleroute /bin/
