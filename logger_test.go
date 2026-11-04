package gush_test

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/gush"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// memoryLogExporter keeps exported records for assertions.
type memoryLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (exporter *memoryLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	for _, record := range records {
		exporter.records = append(exporter.records, record.Clone())
	}
	return nil
}

func (exporter *memoryLogExporter) Shutdown(context.Context) error { return nil }

func (exporter *memoryLogExporter) ForceFlush(context.Context) error { return nil }

// matching returns the records whose body contains text.
func (exporter *memoryLogExporter) matching(text string) []sdklog.Record {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()

	var matched []sdklog.Record
	for _, record := range exporter.records {
		if strings.Contains(record.Body().AsString(), text) {
			matched = append(matched, record)
		}
	}
	return matched
}

// captureLogs points OpenTelemetry's global provider at memory for the test, the way a
// host configures its own logging.
func captureLogs(t *testing.T) *memoryLogExporter {
	t.Helper()

	exporter := &memoryLogExporter{}
	processor := sdklog.NewSimpleProcessor(exporter)
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor))

	previous := global.GetLoggerProvider()
	global.SetLoggerProvider(provider)
	t.Cleanup(func() { global.SetLoggerProvider(previous) })

	return exporter
}

func attributesOf(record sdklog.Record) map[string]string {
	attributes := make(map[string]string)
	record.WalkAttributes(func(attr gush.LogAttribute) bool {
		attributes[string(attr.Key)] = attr.Value.Emit()
		return true
	})
	return attributes
}

func TestLoggerWritesToTheHostsProvider(t *testing.T) {
	exporter := captureLogs(t)
	logger := gush.NewLogger(gush.Attr("service", "billing"))
	logger.SetAttributes(gush.Attr("region", "eu"))

	logger.Printf("charged %d", 5)
	logger.Print("plain")
	logger.Println("line", 1)
	logger.PrintWithAttributes("with extra", gush.Attr("order", "o-1"))

	charged := exporter.matching("charged 5")
	require.Len(t, charged, 1)
	attributes := attributesOf(charged[0])
	assert.Equal(t, "billing", attributes["service"])
	assert.Equal(t, "eu", attributes["region"])
	assert.Equal(t, "logger_test.go", attributes["file"][strings.LastIndex(attributes["file"], "/")+1:])
	assert.NotEmpty(t, attributes["line"])

	assert.Len(t, exporter.matching("plain"), 1)
	assert.Len(t, exporter.matching("line 1"), 1)
	extra := exporter.matching("with extra")
	require.Len(t, extra, 1)
	assert.Equal(t, "o-1", attributesOf(extra[0])["order"])
	assert.NotContains(t, attributesOf(charged[0]), "order")
}

func TestLoggerCreatedBeforeLoggingIsConfiguredFollowsIt(t *testing.T) {
	logger := gush.NewLogger()

	exporter := captureLogs(t)
	logger.Print("created early")

	assert.Len(t, exporter.matching("created early"), 1)
}

func TestLoggerWriteReceivesTheStandardLogger(t *testing.T) {
	exporter := captureLogs(t)
	standard := log.New(gush.NewLogger(), "", 0)

	standard.Print("first")
	standard.Print("second")

	assert.Len(t, exporter.matching("first"), 1)
	assert.Len(t, exporter.matching("second"), 1)
}

func TestLoggerWriteSplitsLinesAndReportsTheFullLength(t *testing.T) {
	exporter := captureLogs(t)
	input := []byte("one\r\ntwo\n\nthree")

	written, err := gush.NewLogger().Write(input)

	require.NoError(t, err)
	assert.Equal(t, len(input), written)
	assert.Len(t, exporter.matching("one"), 1)
	assert.Len(t, exporter.matching("two"), 1)
	assert.Len(t, exporter.matching("three"), 1)
}

func TestLoggerWriteKeepsRecordsWhenTheBufferIsReused(t *testing.T) {
	exporter := captureLogs(t)
	logger := gush.NewLogger()

	buffer := []byte("first")
	_, err := logger.Write(buffer)
	require.NoError(t, err)
	copy(buffer, "xxxxx")

	assert.Len(t, exporter.matching("first"), 1)
}

func TestLoggerTimer(t *testing.T) {
	exporter := captureLogs(t)
	logger := gush.NewLogger()

	logger.Time("load")
	time.Sleep(time.Millisecond)
	logger.TimeEnd("load", "rows", 3)
	logger.TimeEnd("load")
	logger.TimeEnd("never-started", "only extra")
	logger.TimeEnd("never-started")

	timed := exporter.matching("load: ")
	require.Len(t, timed, 1)
	assert.True(t, strings.HasSuffix(timed[0].Body().AsString(), "rows 3"), timed[0].Body().AsString())
	assert.Len(t, exporter.matching("only extra"), 1)
}

// captureStderr returns what writes runs while it prints to standard error.
func captureStderr(t *testing.T, writes func()) string {
	t.Helper()

	read, write, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stderr
	os.Stderr = write
	defer func() { os.Stderr = original }()

	writes()
	require.NoError(t, write.Close())

	var output bytes.Buffer
	_, err = output.ReadFrom(read)
	require.NoError(t, err)
	return output.String()
}

func TestStderrLogExporterFormat(t *testing.T) {
	processor := sdklog.NewSimpleProcessor(gush.NewStderrLogExporter())
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor))
	previous := global.GetLoggerProvider()
	global.SetLoggerProvider(provider)
	t.Cleanup(func() { global.SetLoggerProvider(previous) })

	output := captureStderr(t, func() { gush.NewLogger().Print("hello") })

	assert.Regexp(t, `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} logger_test\.go:\d+: hello\n$`, output)
}

func TestLoggerFallsBackToStderrWithoutConfiguration(t *testing.T) {
	output := captureStderr(t, func() { gush.NewLogger().Print("no provider") })

	assert.Regexp(t, `logger_test\.go:\d+: no provider\n$`, output)
}

func TestStderrLogExporterHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records := make([]sdklog.Record, 1)
	assert.ErrorIs(t, gush.NewStderrLogExporter().Export(ctx, records), context.Canceled)
}
