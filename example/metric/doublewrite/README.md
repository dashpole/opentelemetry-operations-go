# Metric Double-Writing Migration Example

This sample demonstrates how to configure double-writing of metrics during migration by registering both the legacy Google Cloud Monitoring exporter (`mexporter.New`) and the standard OpenTelemetry OTLP metric exporter (`otlpmetricgrpc.New`) on the same `MeterProvider`.

This allows your application to send metrics simultaneously to both Google Cloud Monitoring custom metrics (`workload.googleapis.com/`) and the Telemetry API (`prometheus.googleapis.com/`), ensuring no disruption to existing dashboards while new dashboards/alerts are validated.

#### Prerequisites

Get Google credentials on your machine:

```sh
gcloud auth application-default login
```

#### Run the Sample

```sh
# export necessary OTEL environment variables
export PROJECT_ID=<project-id>
export OTEL_EXPORTER_OTLP_ENDPOINT=https://telemetry.googleapis.com
export OTEL_RESOURCE_ATTRIBUTES="gcp.project_id=$PROJECT_ID,service.name=doublewrite-sample,service.instance.id=1"
export OTEL_EXPORTER_OTLP_HEADERS=X-Goog-User-Project=$PROJECT_ID

# from the repository root
cd example/metric/doublewrite && go run .
```
