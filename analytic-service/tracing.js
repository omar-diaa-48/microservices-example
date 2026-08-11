import { NodeSDK } from "@opentelemetry/sdk-node"
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc"
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node"

// Service name and OTLP endpoint come from standard OpenTelemetry env vars,
// set per-service in docker-compose.yml:
//   OTEL_SERVICE_NAME             e.g. "analytic-service"
//   OTEL_EXPORTER_OTLP_ENDPOINT   default http://localhost:4317 (host) / http://jaeger:4317 (compose)
//
// Loaded via `node --import ./tracing.js index.js` so the CommonJS libraries we
// instrument (kafkajs, http, pino) are patched before index.js loads them.
export const otelSdk = new NodeSDK({
    traceExporter: new OTLPTraceExporter(),
    instrumentations: [getNodeAutoInstrumentations()],
})

otelSdk.start()
