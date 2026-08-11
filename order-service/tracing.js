import { register } from "node:module"
import { NodeSDK } from "@opentelemetry/sdk-node"
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc"
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node"

// ESM apps must register the import-in-the-middle loader hook so libraries loaded
// via `import` (kafkajs) get patched. `node --import ./tracing.js` runs this before
// index.js imports kafkajs. Without it only CommonJS `require` loads are patched
// (http, express/router, grpc) and the kafkajs producer/consumer spans are missing.
register("import-in-the-middle/hook.mjs", import.meta.url)

// Jaeger ingests traces only — not OTLP logs or metrics. Disable those signal
// pipelines so the SDK doesn't ship them to the wrong endpoint (the default
// http/protobuf logs/metrics exporter would hit the gRPC port 4317 and fail).
// Trace-context injection into the pino stdout logs is unaffected.
process.env.OTEL_LOGS_EXPORTER ??= "none"
process.env.OTEL_METRICS_EXPORTER ??= "none"

// Service name and OTLP endpoint come from standard OpenTelemetry env vars,
// set per-service in docker-compose.yml:
//   OTEL_SERVICE_NAME             e.g. "order-service"
//   OTEL_EXPORTER_OTLP_ENDPOINT   default http://localhost:4317 (host) / http://jaeger:4317 (compose)
//
// Loaded via `node --import ./tracing.js index.js` so the CommonJS libraries we
// instrument (kafkajs, express, http, pino) are patched before index.js loads them.
export const otelSdk = new NodeSDK({
    traceExporter: new OTLPTraceExporter(),
    instrumentations: [getNodeAutoInstrumentations()],
})

otelSdk.start()
