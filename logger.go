package gush

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
)

// Logger writes structured log records with timing helpers. Flows and steps embed one, so
// ctx.Printf works directly in a handler, and records carry flow_id and the inherited log
// attributes wherever the flow's work runs.
//
// See InitLogExport for where records go.
type Logger struct {
	timers     sync.Map // map[string]time.Time
	attributes sync.Map // map[attribute.Key]attribute.KeyValue

	// muted drops records while it returns true. Flows set it to skip replays.
	muted func() bool
}

// NewLogger creates a Logger whose records carry the given attributes.
func NewLogger(attributes ...LogAttribute) *Logger {
	logger := &Logger{}
	logger.SetAttributes(attributes...)
	return logger
}

// output emits a record. callDepth selects the stack frame recorded as the source location.
func (logger *Logger) output(callDepth int, message string, attributes ...attribute.KeyValue) {
	if logger.muted != nil && logger.muted() {
		return
	}

	record := log.Record{}
	record.SetTimestamp(time.Now())
	record.SetBody(attribute.StringValue(message))

	// Clip so appending cannot write into the caller's slice.
	attributes = slices.Clip(attributes)
	logger.attributes.Range(func(_, value any) bool {
		attributes = append(attributes, value.(attribute.KeyValue))
		return true
	})

	_, file, line, ok := runtime.Caller(callDepth)
	if !ok {
		file = "???"
		line = 0
	}
	attributes = append(attributes, attribute.String("file", file), attribute.Int("line", line))
	record.AddAttributes(attributes...)

	activeLogEmitter().Emit(context.Background(), record)
}

// Print logs its arguments in the manner of fmt.Print.
func (logger *Logger) Print(v ...any) {
	logger.output(2, fmt.Sprint(v...))
}

// Printf logs its arguments in the manner of fmt.Printf.
func (logger *Logger) Printf(format string, v ...any) {
	logger.output(2, fmt.Sprintf(format, v...))
}

// Println logs its arguments in the manner of fmt.Println, without the trailing newline.
func (logger *Logger) Println(v ...any) {
	logger.output(2, strings.TrimSuffix(fmt.Sprintln(v...), "\n"))
}

// PrintWithAttributes logs a message with attributes on this record only.
func (logger *Logger) PrintWithAttributes(message string, attributes ...LogAttribute) {
	logger.output(2, message, attributes...)
}

// SetAttributes adds attributes to every later record, replacing any with the same key.
func (logger *Logger) SetAttributes(attributes ...LogAttribute) {
	for _, attr := range attributes {
		logger.attributes.Store(attr.Key, attr)
	}
}

// Time starts a named timer for TimeEnd.
func (logger *Logger) Time(label string) {
	logger.timers.Store(label, time.Now())
}

// TimeEnd logs the time since the matching Time call, followed by any extra values, and
// removes the timer.
func (logger *Logger) TimeEnd(label string, v ...any) {
	parts := make([]string, 0, 2)

	started, ok := logger.timers.LoadAndDelete(label)
	if ok {
		parts = append(parts, fmt.Sprintf("%s: %s", label, time.Since(started.(time.Time))))
	}
	if len(v) > 0 {
		parts = append(parts, strings.TrimSuffix(fmt.Sprintln(v...), "\n"))
	}

	if len(parts) > 0 {
		logger.output(2, strings.Join(parts, " "))
	}
}

// Write logs each non-empty line of p, so a Logger can receive the standard log package's
// output.
func (logger *Logger) Write(p []byte) (n int, err error) {
	normalized := bytes.ReplaceAll(p, []byte("\r\n"), []byte("\n"))
	for line := range bytes.SplitSeq(normalized, []byte("\n")) {
		if len(line) > 0 {
			logger.output(2, string(line))
		}
	}
	return len(p), nil
}
