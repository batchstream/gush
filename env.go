package gush

import "os"

const (
	envTemporalEndpoint  = "TEMPORAL_ENDPOINT"
	envTemporalAddress   = "TEMPORAL_ADDRESS"
	envTemporalNamespace = "TEMPORAL_NAMESPACE"

	envLogGRPCEndpoint = "OTEL_ENDPOINT"
	envLogHTTPEndpoint = "OTEL_HTTP_ENDPOINT"

	envOTLPLogsEndpoint = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
	envOTLPEndpoint     = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPLogsProtocol = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"
	envOTLPProtocol     = "OTEL_EXPORTER_OTLP_PROTOCOL"
)

// firstEnv returns the first non-empty value among the named environment variables.
func firstEnv(names ...string) string {
	for _, name := range names {
		value := os.Getenv(name)
		if value != "" {
			return value
		}
	}
	return ""
}

func temporalEndpointFromEnv() string {
	return firstEnv(envTemporalEndpoint, envTemporalAddress)
}
