# syntax = docker/dockerfile:experimental

## (1) Build the app binary
FROM golang:1.20 AS build-app

ARG VERSION

# Build the app binary in /app
RUN mkdir /app
WORKDIR /app

COPY . .

# Keep Go's build cache between builds.
# https://github.com/golang/go/issues/27719#issuecomment-514747274
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GO111MODULE=on GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags "-extldflags -static -X github.com/onflow/flow-storage-tracker/build.version=${VERSION}" \
    -o ./app ./cmd/tracker

RUN chmod a+x /app/app

## (2) Add the statically linked binary to a distroless image
FROM gcr.io/distroless/base

COPY --from=build-app /app/app /bin/app
COPY ./migrations /migrations

# Metrics port
EXPOSE 2112

ENTRYPOINT ["/bin/app"]