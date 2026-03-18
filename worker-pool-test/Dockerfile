FROM golang:1.25-alpine as builder
WORKDIR /app
# Initialize a go module
COPY worker-pool-test/main.go ./
COPY detectors/gcp ./detectors/gcp
RUN go mod init test-app && \
    go mod edit -replace github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp=./detectors/gcp && \
    go get github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp && \
    go build -v -o myapp

# Use a lightweight alpine image for the runtime
FROM alpine:latest
COPY --from=builder /app/myapp /myapp
ENTRYPOINT ["/myapp"]
