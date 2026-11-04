package gush

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"go.temporal.io/sdk/workflow"
)

// SDK functions such as workflow.WithCancel panic on a context type they do not know, so
// passing a FlowContext to one has to stay a compile error.
func TestFlowContextIsNotAWorkflowContext(t *testing.T) {
	contextType := reflect.TypeFor[workflow.Context]()
	if reflect.TypeFor[*FlowContext]().Implements(contextType) {
		t.Errorf("FlowContext implements workflow.Context")
	}
}

func TestRunFlowSendsNoRetryTypesToTemporal(t *testing.T) {
	capture := &startCaptureClient{}
	flowClient := &Client{temporalClient: capture}

	request := FlowRequest{Queue: "queue", Flow: "handler"}
	err := flowClient.RunFlow(context.Background(), request, WithNoRetry("Declined"))
	if !errors.Is(err, errStartCaptured) {
		t.Fatalf("RunFlow() error = %v", err)
	}

	got := capture.options.RetryPolicy.NonRetryableErrorTypes
	if !reflect.DeepEqual(got, []string{"Declined"}) {
		t.Fatalf("non-retryable types = %v", got)
	}
}

func TestStartFlowRejectsAFlowThatCannotRun(t *testing.T) {
	flowClient := &Client{temporalClient: &startCaptureClient{}}

	request := FlowRequest{Queue: "queue", Flow: 123}
	_, err := flowClient.StartFlow(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "flow") {
		t.Fatalf("StartFlow() error = %v", err)
	}
}

func TestRunFlowRejectsAFlowThatCannotRun(t *testing.T) {
	flowClient := &Client{temporalClient: &startCaptureClient{}}

	request := FlowRequest{Queue: "queue", Flow: 123}
	err := flowClient.RunFlow(context.Background(), request)

	if err == nil || !strings.Contains(err.Error(), "flow must be a function") {
		t.Fatalf("RunFlow() error = %v", err)
	}
}

func TestOptionsAddNoRetryTypesOnFlowsAndSteps(t *testing.T) {
	declined := NewApplicationError("declined", "Declined")
	untyped := NewApplicationError("untyped", "")
	options := []Option{WithNoRetry("First"), WithNoRetry("Second"), WithNoRetryErrors(declined, untyped, nil)}
	want := []string{"First", "Second", "Declined"}

	flowConfig := DefaultFlowConfig()
	stepConfig := DefaultStepConfig()
	for _, option := range options {
		flowConfig.Apply(option)
		stepConfig.Apply(option)
	}

	if !reflect.DeepEqual(flowConfig.NoRetry, want) {
		t.Errorf("flow NoRetry = %v", flowConfig.NoRetry)
	}
	if !reflect.DeepEqual(stepConfig.NoRetry, want) {
		t.Errorf("step NoRetry = %v", stepConfig.NoRetry)
	}
}

func TestDefaultClientReportsAFailedConnection(t *testing.T) {
	t.Setenv(envTemporalEndpoint, "127.0.0.1:1")

	_, err := DefaultClient()
	if err == nil || !strings.Contains(err.Error(), "connect to Temporal") {
		t.Fatalf("DefaultClient() error = %v", err)
	}

	request := FlowRequest{Queue: "queue", Flow: "handler"}
	err = RunFlow(context.Background(), request)
	if err == nil {
		t.Fatalf("RunFlow() should report the failed connection")
	}
}

func TestPackageCloseWithoutAConnectionDoesNothing(t *testing.T) {
	Close()
}

func TestTemporalEndpointFromEnv(t *testing.T) {
	t.Setenv(envTemporalEndpoint, "")
	t.Setenv(envTemporalAddress, "")
	if got := temporalEndpointFromEnv(); got != "" {
		t.Errorf("no variables: %q", got)
	}

	t.Setenv(envTemporalAddress, "address:7233")
	if got := temporalEndpointFromEnv(); got != "address:7233" {
		t.Errorf("TEMPORAL_ADDRESS: %q", got)
	}

	t.Setenv(envTemporalEndpoint, "endpoint:7233")
	if got := temporalEndpointFromEnv(); got != "endpoint:7233" {
		t.Errorf("TEMPORAL_ENDPOINT should win: %q", got)
	}
}
