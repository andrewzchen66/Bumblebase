# FROM ubuntu:bionic
FROM golang:1.16-alpine

# Set workdir.
WORKDIR /app

# Install Go dependencies.
COPY ./go.mod ./
RUN go mod download

# Copy files.
COPY ./cmd ./cmd
COPY ./pkg ./pkg

# Build.
RUN go build ./cmd/bumble

# Enable the following past the Concurrency assignment.
# RUN go build ./cmd/bumble_client

# Run.
CMD ["./bumble"]