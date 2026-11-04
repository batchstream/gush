package gush

import "os"

const (
	envTemporalAddress   = "TEMPORAL_ADDRESS"
	envTemporalNamespace = "TEMPORAL_NAMESPACE"

	envOTLPLogsEndpoint = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
	envOTLPEndpoint     = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPLogsProtocol = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"
	envOTLPProtocol     = "OTEL_EXPORTER_OTLP_PROTOCOL"
)

// firstEnv returns the first of the named environment variables that has a value, and that
// value.
func firstEnv(names ...string) (name, value string) {
	for _, candidate := range names {
		value := os.Getenv(candidate)
		if value != "" {
			return candidate, value
		}
	}
	return "", ""
}
