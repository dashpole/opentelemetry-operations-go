# OTLP Metric with Views Prefixing Example

This sample demonstrates how to use OpenTelemetry Views (`sdkmetric.WithView`) to preserve `workload.googleapis.com/` metric name prefixes when migrating to standard OTLP metric exporters.

Google Cloud's Telemetry API natively recognizes metric names starting with `workload.googleapis.com/` and routes them directly to Cloud Monitoring custom metrics, preserving existing dashboards and alerting policies without converting them to `prometheus.googleapis.com/` metrics.

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
export OTEL_RESOURCE_ATTRIBUTES="gcp.project_id=$PROJECT_ID,service.name=views-sample,service.instance.id=1"
export OTEL_EXPORTER_OTLP_HEADERS=X-Goog-User-Project=$PROJECT_ID

# from the repository root
cd example/metric/views && go run .
```
