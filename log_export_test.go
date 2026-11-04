package gush

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	logcollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// clearLogEnv isolates a test from endpoints set in the environment running it.
func clearLogEnv(t *testing.T) {
	t.Helper()
	names := []string{envOTLPLogsEndpoint, envOTLPEndpoint, envOTLPLogsProtocol, envOTLPProtocol}
	for _, name := range names {
		t.Setenv(name, "")
	}
}

// collectedLogs is what a fake OTLP collector received, as body to attributes.
type collectedLogs struct {
	mu     sync.Mutex
	bodies map[string]map[string]string
}

func newCollectedLogs() *collectedLogs {
	collected := &collectedLogs{bodies: make(map[string]map[string]string)}
	return collected
}

func (collected *collectedLogs) add(request *logcollector.ExportLogsServiceRequest) {
	collected.mu.Lock()
	defer collected.mu.Unlock()

	for _, resourceLogs := range request.GetResourceLogs() {
		for _, scopeLogs := range resourceLogs.GetScopeLogs() {
			for _, record := range scopeLogs.GetLogRecords() {
				attributes := make(map[string]string)
				for _, attr := range record.GetAttributes() {
					attributes[attr.GetKey()] = attr.GetValue().GetStringValue()
				}
				collected.bodies[record.GetBody().GetStringValue()] = attributes
			}
		}
	}
}

func (collected *collectedLogs) attributesOf(body string) (map[string]string, bool) {
	collected.mu.Lock()
	defer collected.mu.Unlock()

	attributes, ok := collected.bodies[body]
	return attributes, ok
}

// startHTTPCollector runs a fake OTLP/HTTP collector that records the paths it is sent.
func startHTTPCollector(t *testing.T) (*httptest.Server, *collectedLogs, *[]string) {
	t.Helper()

	server, collected, paths := newHTTPCollector(t)
	server.Start()
	return server, collected, paths
}

// newHTTPCollector returns a fake OTLP/HTTP collector for the caller to start.
func newHTTPCollector(t *testing.T) (*httptest.Server, *collectedLogs, *[]string) {
	t.Helper()

	collected := newCollectedLogs()
	paths := &[]string{}
	var pathsMu sync.Mutex
	handler := func(writer http.ResponseWriter, request *http.Request) {
		pathsMu.Lock()
		*paths = append(*paths, request.URL.Path)
		pathsMu.Unlock()

		body, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		export := &logcollector.ExportLogsServiceRequest{}
		err = proto.Unmarshal(body, export)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		collected.add(export)

		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return server, collected, paths
}

type grpcCollector struct {
	logcollector.UnimplementedLogsServiceServer
	collected *collectedLogs
}

func (collector *grpcCollector) Export(_ context.Context, request *logcollector.ExportLogsServiceRequest) (*logcollector.ExportLogsServiceResponse, error) {
	collector.collected.add(request)
	response := &logcollector.ExportLogsServiceResponse{}
	return response, nil
}

// startGRPCCollector runs a fake OTLP/gRPC collector and returns its host:port.
func startGRPCCollector(t *testing.T, options ...grpc.ServerOption) (string, *collectedLogs) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	collected := newCollectedLogs()
	server := grpc.NewServer(options...)
	collector := &grpcCollector{collected: collected}
	logcollector.RegisterLogsServiceServer(server, collector)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-done
	})

	return listener.Addr().String(), collected
}

// trustCertificate makes the exporter trust a test collector's certificate.
func trustCertificate(t *testing.T, certificate *x509.Certificate) {
	t.Helper()

	block := &pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}
	path := filepath.Join(t.TempDir(), "collector.pem")
	err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600)
	require.NoError(t, err)
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", path)
}

// exportOneRecord logs through a running export and flushes it to the collector.
func exportOneRecord(t *testing.T, body string) {
	t.Helper()

	shutdown, err := InitLogExport()
	require.NoError(t, err)
	NewLogger(Attr("flow_id", "flow-123")).Print(body)
	shutdown()
}

func assertCollected(t *testing.T, collected *collectedLogs, body string) {
	t.Helper()

	attributes, ok := collected.attributesOf(body)
	require.True(t, ok, "the collector did not receive %q", body)
	assert.Equal(t, "flow-123", attributes["flow_id"])
}

// unconfiguredLogProvider is the global provider these tests start with and restore.
var unconfiguredLogProvider = global.GetLoggerProvider()

func TestImportLeavesTheGlobalProviderAlone(t *testing.T) {
	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
	assert.Contains(t, fmt.Sprintf("%T", global.GetLoggerProvider()), "global.")
}

// A host's provider may be set before gush's package variables are initialized, so the
// placeholder is recognized by its type. This fails if OpenTelemetry moves that type.
func TestLogProviderConfiguredRecognizesThePlaceholder(t *testing.T) {
	assert.False(t, logProviderConfigured(unconfiguredLogProvider))

	host := sdklog.NewLoggerProvider()
	assert.True(t, logProviderConfigured(host))
}

func TestTimeEndUsesTheLoggersClock(t *testing.T) {
	hostLogs := captureHostLogs(t)

	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	logger := NewLogger()
	logger.now = func() time.Time { return current }

	logger.Time("step")
	current = current.Add(2 * time.Second)
	logger.TimeEnd("step")

	assert.Contains(t, hostLogs.bodies(), "step: 2s")
}

func TestExportsOverHTTP(t *testing.T) {
	clearLogEnv(t)
	server, collected, paths := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)

	exportOneRecord(t, "over http")

	assertCollected(t, collected, "over http")
	assert.Contains(t, *paths, "/v1/logs")
}

func TestExportsOverGRPC(t *testing.T) {
	clearLogEnv(t)
	endpoint, collected := startGRPCCollector(t)
	t.Setenv(envOTLPLogsEndpoint, "http://"+endpoint)
	t.Setenv(envOTLPLogsProtocol, "grpc")

	exportOneRecord(t, "over grpc")

	assertCollected(t, collected, "over grpc")
}

func TestExportsOverHTTPS(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := newHTTPCollector(t)
	server.StartTLS()
	t.Setenv(envOTLPEndpoint, server.URL)
	trustCertificate(t, server.Certificate())

	exportOneRecord(t, "over https")

	assertCollected(t, collected, "over https")
}

// This is the configuration the README shows.
func TestExportsOverGRPCWithTLS(t *testing.T) {
	clearLogEnv(t)
	certificates := httptest.NewTLSServer(http.NotFoundHandler())
	defer certificates.Close()
	serverTLS := credentials.NewServerTLSFromCert(&certificates.TLS.Certificates[0])
	endpoint, collected := startGRPCCollector(t, grpc.Creds(serverTLS))
	t.Setenv(envOTLPEndpoint, "https://"+endpoint)
	t.Setenv(envOTLPProtocol, "grpc")
	trustCertificate(t, certificates.Certificate())

	exportOneRecord(t, "over grpc with tls")

	assertCollected(t, collected, "over grpc with tls")
}

func TestExportsWithAnUppercaseScheme(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, "HTTP://"+server.Listener.Addr().String())

	exportOneRecord(t, "uppercase scheme")

	assertCollected(t, collected, "uppercase scheme")
}

func TestGeneralEndpointGetsTheLogsPath(t *testing.T) {
	tests := map[string]struct {
		suffix string
		path   string
	}{
		"no path":                {suffix: "", path: "/v1/logs"},
		"only a slash":           {suffix: "/", path: "/v1/logs"},
		"a path":                 {suffix: "/base", path: "/base/v1/logs"},
		"a path ending in slash": {suffix: "/base/", path: "/base/v1/logs"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			clearLogEnv(t)
			server, collected, paths := startHTTPCollector(t)
			t.Setenv(envOTLPEndpoint, server.URL+test.suffix)

			exportOneRecord(t, "general endpoint")

			assertCollected(t, collected, "general endpoint")
			assert.Equal(t, []string{test.path}, *paths)
		})
	}
}

func TestLogsEndpointWinsAndIsUsedAsWritten(t *testing.T) {
	clearLogEnv(t)
	general, generalCollected, _ := startHTTPCollector(t)
	logs, logsCollected, paths := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, general.URL)
	t.Setenv(envOTLPLogsEndpoint, logs.URL+"/ingest/")

	exportOneRecord(t, "logs endpoint")

	assertCollected(t, logsCollected, "logs endpoint")
	assert.Equal(t, []string{"/ingest/"}, *paths)
	_, reachedGeneral := generalCollected.attributesOf("logs endpoint")
	assert.False(t, reachedGeneral)
}

// The exporter never reads the general variable here, so its value does not matter.
func TestOnlyTheEndpointInUseIsChecked(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, "collector:4318")
	t.Setenv(envOTLPLogsEndpoint, server.URL+"/v1/logs")

	exportOneRecord(t, "endpoint in use")

	assertCollected(t, collected, "endpoint in use")
}

func TestLogsProtocolWinsOverTheGeneralOne(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)
	t.Setenv(envOTLPProtocol, "grpc")
	t.Setenv(envOTLPLogsProtocol, "http/protobuf")

	exportOneRecord(t, "logs protocol")

	assertCollected(t, collected, "logs protocol")
}

func TestRejectsAnEndpointThatIsNotAURL(t *testing.T) {
	tests := map[string]struct {
		variable string
		protocol string
		endpoint string
	}{
		"no scheme":                {variable: envOTLPEndpoint, endpoint: "collector:4318"},
		"an address":               {variable: envOTLPEndpoint, endpoint: "127.0.0.1:4318"},
		"no scheme over grpc":      {variable: envOTLPEndpoint, protocol: "grpc", endpoint: "collector:4317"},
		"no scheme, logs variable": {variable: envOTLPLogsEndpoint, endpoint: "collector:4318"},
		"another scheme":           {variable: envOTLPEndpoint, endpoint: "ftp://collector:4318"},
		"no host":                  {variable: envOTLPEndpoint, endpoint: "http://"},
		"no host over https":       {variable: envOTLPEndpoint, endpoint: "https://"},
		"only a port":              {variable: envOTLPEndpoint, endpoint: "http://:4318"},
		"a bad port":               {variable: envOTLPEndpoint, endpoint: "http://collector:43x8"},
		"a space in the host":      {variable: envOTLPEndpoint, endpoint: "http://coll ector:4317"},
		"a trailing newline":       {variable: envOTLPEndpoint, endpoint: "http://collector:4318\n"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			clearLogEnv(t)
			t.Setenv(test.variable, test.endpoint)
			t.Setenv(envOTLPProtocol, test.protocol)

			_, err := InitLogExport()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be an http:// or https:// URL")
			assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
		})
	}
}

func TestNoEndpointLeavesGlobalProviderAlone(t *testing.T) {
	clearLogEnv(t)

	shutdown, err := InitLogExport()
	require.NoError(t, err)
	defer shutdown()

	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
}

func TestExportSetsTheGlobalProviderUntilItStops(t *testing.T) {
	clearLogEnv(t)
	server, _, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)

	shutdown, err := InitLogExport()
	require.NoError(t, err)
	assert.NotSame(t, unconfiguredLogProvider, global.GetLoggerProvider())

	shutdown()
	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
}

func TestExportIsSharedUntilTheLastUserStops(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)

	first, err := InitLogExport()
	require.NoError(t, err)
	second, err := InitLogExport()
	require.NoError(t, err)

	first()
	first() // running a shutdown twice must not count twice
	assert.NotSame(t, unconfiguredLogProvider, global.GetLoggerProvider())

	NewLogger(Attr("flow_id", "flow-123")).Print("after first stop")
	second()

	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
	assertCollected(t, collected, "after first stop")
}

func TestHostProviderWinsOverEndpoints(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)

	hostLogs := captureHostLogs(t)
	host := global.GetLoggerProvider()

	shutdown, err := InitLogExport()
	require.NoError(t, err)
	NewLogger().Print("to the host")
	shutdown()

	assert.Same(t, host, global.GetLoggerProvider())
	assert.Equal(t, []string{"to the host"}, hostLogs.bodies())
	_, reachedCollector := collected.attributesOf("to the host")
	assert.False(t, reachedCollector)
}

func TestHostProviderSetAfterExportStartedTakesOver(t *testing.T) {
	clearLogEnv(t)
	server, _, _ := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)

	// Runs last, because captureHostLogs restores the export's stopped provider.
	t.Cleanup(func() { global.SetLoggerProvider(unconfiguredLogProvider) })

	shutdown, err := InitLogExport()
	require.NoError(t, err)

	hostLogs := captureHostLogs(t)
	host := global.GetLoggerProvider()
	NewLogger().Print("late host")
	shutdown()

	assert.Same(t, host, global.GetLoggerProvider())
	assert.Equal(t, []string{"late host"}, hostLogs.bodies())
}

// hostLogExporter stands in for a host's own logging pipeline.
type hostLogExporter struct {
	mu       sync.Mutex
	messages []string
}

func (exporter *hostLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	for _, record := range records {
		exporter.messages = append(exporter.messages, record.Body().AsString())
	}
	return nil
}

func (exporter *hostLogExporter) Shutdown(context.Context) error { return nil }

func (exporter *hostLogExporter) ForceFlush(context.Context) error { return nil }

func (exporter *hostLogExporter) bodies() []string {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return append([]string(nil), exporter.messages...)
}

func captureHostLogs(t *testing.T) *hostLogExporter {
	t.Helper()

	exporter := &hostLogExporter{}
	processor := sdklog.NewSimpleProcessor(exporter)
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor))

	previous := global.GetLoggerProvider()
	global.SetLoggerProvider(provider)
	t.Cleanup(func() { global.SetLoggerProvider(previous) })

	return exporter
}
