package gush

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const (
	logInstrumentationName = "github.com/batchstream/gush"
	logShutdownTimeout     = 30 * time.Second
)

// otelPlaceholderPackage holds the type of OpenTelemetry's global LoggerProvider before
// anyone sets one. Package order decides whether a host's init runs before gush's, so the
// placeholder has to be recognized by type rather than remembered at startup.
const otelPlaceholderPackage = "go.opentelemetry.io/otel/log/internal/global"

// logProviderConfigured reports whether a LoggerProvider has been set as the global one.
func logProviderConfigured(provider log.LoggerProvider) bool {
	providerType := reflect.TypeOf(provider)
	if providerType == nil || providerType.Kind() != reflect.Pointer {
		return true
	}
	return providerType.Elem().PkgPath() != otelPlaceholderPackage
}

var stderrLogEmitter = sync.OnceValue(func() log.Logger {
	processor := sdklog.NewSimpleProcessor(NewStderrLogExporter())
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor))
	return provider.Logger(logInstrumentationName)
})

// activeLogEmitter resolves the destination for each record, so a Logger created before
// logging was configured still reaches the configured destination.
func activeLogEmitter() log.Logger {
	provider := global.GetLoggerProvider()
	if !logProviderConfigured(provider) {
		return stderrLogEmitter()
	}
	return provider.Logger(logInstrumentationName)
}

var logExport struct {
	mu       sync.Mutex
	provider *sdklog.LoggerProvider
	previous log.LoggerProvider
	users    int
}

// InitLogExport starts exporting logs over OTLP when OTEL_EXPORTER_OTLP_LOGS_ENDPOINT or
// OTEL_EXPORTER_OTLP_ENDPOINT is set, and returns a function that flushes them and stops.
// The endpoint must be an http:// or https:// URL. OpenTelemetry's other standard variables
// set the protocol, headers and TLS, and the default protocol is http/protobuf. Dial calls
// it, so call it directly only for a process that logs without a Client.
//
// A LoggerProvider the host already set with OpenTelemetry's global.SetLoggerProvider
// always wins, and InitLogExport leaves it alone. Importing gush never changes the global
// provider; InitLogExport sets it only when it starts exporting. With no provider and no
// endpoint, logs go to standard error.
//
// Export is shared by the whole process: it stops when the last returned function has run.
func InitLogExport() (shutdown func(), err error) {
	logExport.mu.Lock()
	defer logExport.mu.Unlock()

	if logExport.provider != nil {
		logExport.users++
		return sync.OnceFunc(releaseLogExport), nil
	}
	current := global.GetLoggerProvider()
	if logProviderConfigured(current) {
		return func() {}, nil
	}

	exporter, err := newLogExporterFromEnv(context.Background())
	if err != nil {
		return nil, err
	}
	if exporter == nil {
		return func() {}, nil
	}

	processor := sdklog.NewBatchProcessor(exporter)
	logExport.provider = sdklog.NewLoggerProvider(sdklog.WithProcessor(processor))
	logExport.previous = current
	logExport.users = 1
	global.SetLoggerProvider(logExport.provider)

	return sync.OnceFunc(releaseLogExport), nil
}

// releaseLogExport stops the export once nothing uses it, giving records in the batch a
// bounded time to reach the collector.
func releaseLogExport() {
	logExport.mu.Lock()
	defer logExport.mu.Unlock()

	logExport.users--
	if logExport.users > 0 {
		return
	}

	provider := logExport.provider
	logExport.provider = nil
	if global.GetLoggerProvider() == provider {
		global.SetLoggerProvider(logExport.previous)
	}

	ctx, cancel := context.WithTimeout(context.Background(), logShutdownTimeout)
	defer cancel()
	err := errors.Join(provider.ForceFlush(ctx), provider.Shutdown(ctx))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gush: flush logs: %v\n", err)
	}
}

// newLogExporterFromEnv returns nil when no endpoint is configured. Otherwise the exporter
// reads OpenTelemetry's standard variables itself. It accepts a malformed endpoint, such as
// collector:4318, and then loses every record, so that is an error here.
func newLogExporterFromEnv(ctx context.Context) (sdklog.Exporter, error) {
	endpoint := firstEnv(envOTLPLogsEndpoint, envOTLPEndpoint)
	if endpoint == "" {
		return nil, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return nil, fmt.Errorf("gush: OTLP log endpoint %q must be an http:// or https:// URL with a host", endpoint)
	}

	if firstEnv(envOTLPLogsProtocol, envOTLPProtocol) == "grpc" {
		exporter, err := otlploggrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("gush: create OTLP/gRPC log exporter: %w", err)
		}
		return exporter, nil
	}

	// The exporter appends /v1/logs to the general variable as written, so a trailing slash
	// would post to //v1/logs. The logs variable is used as written, slash included.
	var options []otlploghttp.Option
	if os.Getenv(envOTLPLogsEndpoint) == "" && strings.HasSuffix(parsed.Path, "/") {
		logsPath := strings.TrimRight(parsed.Path, "/") + "/v1/logs"
		options = append(options, otlploghttp.WithURLPath(logsPath))
	}

	exporter, err := otlploghttp.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("gush: create OTLP/HTTP log exporter: %w", err)
	}
	return exporter, nil
}
