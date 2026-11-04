package gush

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"go.temporal.io/sdk/client"
)

func TestTruncateFairKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "empty", key: "", want: ""},
		{name: "short", key: "tenant-123", want: "tenant-123"},
		{name: "exactly the limit", key: strings.Repeat("a", 64), want: strings.Repeat("a", 64)},
		{name: "one byte over", key: strings.Repeat("a", 65), want: strings.Repeat("a", 64)},
		{name: "much longer", key: strings.Repeat("a", 5000), want: strings.Repeat("a", 64)},
		{name: "multibyte ending at the limit", key: strings.Repeat("a", 61) + "例", want: strings.Repeat("a", 61) + "例"},
		{name: "multibyte ending at the limit with more after", key: strings.Repeat("a", 61) + "例b", want: strings.Repeat("a", 61) + "例"},
		{name: "two byte character across the limit", key: strings.Repeat("a", 63) + "ü", want: strings.Repeat("a", 63)},
		{name: "three byte character across the limit", key: strings.Repeat("a", 62) + "例", want: strings.Repeat("a", 62)},
		{name: "four byte character across the limit", key: strings.Repeat("a", 61) + "😀", want: strings.Repeat("a", 61)},
		{name: "only multibyte characters", key: strings.Repeat("例", 30), want: strings.Repeat("例", 21)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateFairKey(tt.key)
			if got != tt.want {
				t.Fatalf("truncateFairKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
			if len(got) > maxFairKeyBytes {
				t.Fatalf("truncateFairKey(%q) is %d bytes", tt.key, len(got))
			}
			if !utf8.ValidString(got) {
				t.Fatalf("truncateFairKey(%q) = %q is not valid UTF-8", tt.key, got)
			}
		})
	}
}

func TestNewActivityOptionsTruncatesFairKey(t *testing.T) {
	stepConfig := DefaultStepConfig()
	stepConfig.FairKey = strings.Repeat("a", 65)

	activityOptions := newActivityOptions(stepConfig)

	if got := activityOptions.Priority.FairnessKey; got != strings.Repeat("a", 64) {
		t.Fatalf("activity fairness key = %q", got)
	}
}

func TestNewChildWorkflowOptionsTruncatesFairKey(t *testing.T) {
	flowConfig := DefaultFlowConfig()
	flowConfig.FairKey = strings.Repeat("a", 65)

	childWorkflowOptions := newChildWorkflowOptions("target-queue", flowConfig)

	if got := childWorkflowOptions.Priority.FairnessKey; got != strings.Repeat("a", 64) {
		t.Fatalf("child flow fairness key = %q", got)
	}
}

var errStartCaptured = errors.New("start captured")

// startCaptureClient records the options of a flow start and stops there.
type startCaptureClient struct {
	client.Client
	options client.StartWorkflowOptions
}

func (c *startCaptureClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, _ any, _ ...any) (client.WorkflowRun, error) {
	c.options = options
	return nil, errStartCaptured
}

func TestRunFlowTruncatesFairKey(t *testing.T) {
	capture := &startCaptureClient{}
	flowClient := &Client{temporalClient: capture}

	request := FlowRequest{Queue: "queue", Flow: "handler", Input: "input"}
	err := flowClient.RunFlow(context.Background(), request, WithFairKey(strings.Repeat("a", 65)))
	if !errors.Is(err, errStartCaptured) {
		t.Fatalf("RunFlow() error = %v", err)
	}

	if got := capture.options.Priority.FairnessKey; got != strings.Repeat("a", 64) {
		t.Fatalf("flow start fairness key = %q", got)
	}
}
