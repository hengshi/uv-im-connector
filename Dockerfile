# syntax=docker/dockerfile:1

FROM golang:1.24-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=
ARG BUILD_TIME=

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
  -ldflags="-s -w -X github.com/hengshi/uv-im-connector.Version=${VERSION} -X github.com/hengshi/uv-im-connector.GitCommit=${COMMIT} -X github.com/hengshi/uv-im-connector.BuildTime=${BUILD_TIME}" \
  -o /out/uv-im-connector ./cmd/uv-im-connector

FROM alpine:3.22

RUN addgroup -S -g 10001 uvim \
  && adduser -S -u 10001 -G uvim -h /var/lib/uv-im-connector uvim \
  && apk add --no-cache ca-certificates tzdata

WORKDIR /var/lib/uv-im-connector

COPY --from=build /out/uv-im-connector /usr/local/bin/uv-im-connector
RUN chown -R uvim:uvim /var/lib/uv-im-connector

ENV UV_IM_ADDR=0.0.0.0:8787
ENV UV_IM_STATE_DIR=/var/lib/uv-im-connector

EXPOSE 8787

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/uv-im-connector"]
