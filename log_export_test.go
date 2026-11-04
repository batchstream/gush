package gush

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	logcollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// clearLogEnv isolates a test from endpoints set in the environment running it.
func clearLogEnv(t *testing.T) {
	t.Helper()
	names := []string{
		envLogGRPCEndpoint, envLogHTTPEndpoint, envLogHTTPEndpointLegacy,
		envOTLPLogsEndpoint, envOTLPEndpoint, envOTLPLogsProtocol, envOTLPProtocol,
	}
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

	server := httptest.NewServer(http.HandlerFunc(handler))
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
func startGRPCCollector(t *testing.T) (string, *collectedLogs) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	collected := newCollectedLogs()
	server := grpc.NewServer()
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

func TestImportLeavesTheGlobalProviderAlone(t *testing.T) {
	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
	assert.Contains(t, fmt.Sprintf("%T", global.GetLoggerProvider()), "global.")
}

func TestExportsOverHTTP(t *testing.T) {
	clearLogEnv(t)
	server, collected, paths := startHTTPCollector(t)
	t.Setenv(envLogHTTPEndpoint, server.URL)

	exportOneRecord(t, "over http")

	assertCollected(t, collected, "over http")
	assert.Contains(t, *paths, "/v1/logs")
}

func TestExportsOverHTTPThroughTheLegacyAlias(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envLogHTTPEndpointLegacy, server.URL)

	exportOneRecord(t, "over legacy alias")

	assertCollected(t, collected, "over legacy alias")
}

func TestExportsOverGRPCToABareHostPort(t *testing.T) {
	clearLogEnv(t)
	endpoint, collected := startGRPCCollector(t)
	t.Setenv(envLogGRPCEndpoint, endpoint)

	exportOneRecord(t, "over grpc")

	assertCollected(t, collected, "over grpc")
}

func TestExportsOverGRPCToAURL(t *testing.T) {
	clearLogEnv(t)
	endpoint, collected := startGRPCCollector(t)
	t.Setenv(envLogGRPCEndpoint, "http://"+endpoint)

	exportOneRecord(t, "over grpc url")

	assertCollected(t, collected, "over grpc url")
}

func TestExportsThroughStandardVariables(t *testing.T) {
	clearLogEnv(t)
	server, collected, paths := startHTTPCollector(t)
	t.Setenv(envOTLPEndpoint, server.URL)

	exportOneRecord(t, "over standard http")

	assertCollected(t, collected, "over standard http")
	assert.Contains(t, *paths, "/v1/logs")
}

func TestExportsThroughStandardVariablesOverGRPC(t *testing.T) {
	clearLogEnv(t)
	endpoint, collected := startGRPCCollector(t)
	t.Setenv(envOTLPLogsEndpoint, "http://"+endpoint)
	t.Setenv(envOTLPLogsProtocol, "grpc")

	exportOneRecord(t, "over standard grpc")

	assertCollected(t, collected, "over standard grpc")
}

func TestGRPCEndpointWinsOverHTTP(t *testing.T) {
	clearLogEnv(t)
	endpoint, grpcCollected := startGRPCCollector(t)
	server, httpCollected, _ := startHTTPCollector(t)
	t.Setenv(envLogGRPCEndpoint, endpoint)
	t.Setenv(envLogHTTPEndpoint, server.URL)

	exportOneRecord(t, "precedence")

	assertCollected(t, grpcCollected, "precedence")
	_, reachedHTTP := httpCollected.attributesOf("precedence")
	assert.False(t, reachedHTTP)
}

func TestLegacyAliasWinsOverHTTPEndpoint(t *testing.T) {
	clearLogEnv(t)
	legacy, legacyCollected, _ := startHTTPCollector(t)
	current, currentCollected, _ := startHTTPCollector(t)
	t.Setenv(envLogHTTPEndpointLegacy, legacy.URL)
	t.Setenv(envLogHTTPEndpoint, current.URL)

	exportOneRecord(t, "alias precedence")

	assertCollected(t, legacyCollected, "alias precedence")
	_, reachedCurrent := currentCollected.attributesOf("alias precedence")
	assert.False(t, reachedCurrent)
}

func TestRejectsAnHTTPEndpointThatIsNotAURL(t *testing.T) {
	clearLogEnv(t)
	t.Setenv(envLogHTTPEndpoint, "collector:4318")

	_, err := InitLogExport()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "must start with http")
	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
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
	t.Setenv(envLogHTTPEndpoint, server.URL)

	shutdown, err := InitLogExport()
	require.NoError(t, err)
	assert.NotSame(t, unconfiguredLogProvider, global.GetLoggerProvider())

	shutdown()
	assert.Same(t, unconfiguredLogProvider, global.GetLoggerProvider())
}

func TestExportIsSharedUntilTheLastUserStops(t *testing.T) {
	clearLogEnv(t)
	server, collected, _ := startHTTPCollector(t)
	t.Setenv(envLogHTTPEndpoint, server.URL)

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
	t.Setenv(envLogHTTPEndpoint, server.URL)

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
	t.Setenv(envLogHTTPEndpoint, server.URL)

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
