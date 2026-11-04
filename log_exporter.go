package gush

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// StderrLogExporter writes log records to standard error, one line per record. Gush uses it
// when no other destination is configured, and it can back a host's own LoggerProvider.
type StderrLogExporter struct{}

// NewStderrLogExporter creates a StderrLogExporter.
func NewStderrLogExporter() *StderrLogExporter {
	exporter := &StderrLogExporter{}
	return exporter
}

// Export writes records until ctx is canceled.
func (exporter *StderrLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	for _, record := range records {
		err := ctx.Err()
		if err != nil {
			return err
		}

		file := "-"
		line := int64(0)
		record.WalkAttributes(func(attr LogAttribute) bool {
			switch attr.Key {
			case "file":
				file = filepath.Base(attr.Value.AsString())
			case "line":
				line = attr.Value.AsInt64()
			}
			return true
		})

		timestamp := record.Timestamp().Format("2006/01/02 15:04:05")
		fmt.Fprintf(os.Stderr, "%s %s:%d: %s\n", timestamp, file, line, record.Body().AsString())
	}
	return nil
}

// Shutdown does nothing.
func (exporter *StderrLogExporter) Shutdown(context.Context) error {
	return nil
}

// ForceFlush does nothing.
func (exporter *StderrLogExporter) ForceFlush(context.Context) error {
	return nil
}
