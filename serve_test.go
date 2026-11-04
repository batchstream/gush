package gush

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
)

func serveTestStep(*StepContext) (any, error) { return nil, nil }

func serveTestFlow(*FlowContext) (any, error) { return nil, nil }

func TestValidateWorkers(t *testing.T) {
	tests := []struct {
		name    string
		workers []*Worker
		wantErr string
	}{
		{name: "no workers", workers: nil, wantErr: "at least one worker"},
		{name: "nil worker", workers: []*Worker{nil}, wantErr: "nil worker"},
		{name: "nothing registered", workers: []*Worker{NewWorker("q")}, wantErr: "no flows or steps"},
		{name: "repeated step", workers: []*Worker{NewWorker("q").WithSteps(serveTestStep, serveTestStep)}, wantErr: "repeated step name"},
		{name: "repeated flow", workers: []*Worker{NewWorker("q").WithFlows(serveTestFlow, serveTestFlow)}, wantErr: "repeated flow name"},
		{name: "empty step name", workers: []*Worker{NewWorker("q").WithStepHandlers(NewStepHandler("", serveTestStep))}, wantErr: "empty or repeated step name"},
		{name: "nil step handler", workers: []*Worker{NewWorker("q").WithStepHandlers(nil)}, wantErr: "step with no function"},
		{name: "nil flow handler", workers: []*Worker{NewWorker("q").WithFlowHandlers(nil)}, wantErr: "flow with no function"},
		{name: "nil step function", workers: []*Worker{NewWorker("q").WithSteps(nil)}, wantErr: "step with no function"},
		{name: "nil flow function", workers: []*Worker{NewWorker("q").WithFlows(nil)}, wantErr: "flow with no function"},
		{name: "valid", workers: []*Worker{NewWorker("q").WithSteps(serveTestStep).WithFlows(serveTestFlow)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkers(tt.workers)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateWorkers() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateWorkers() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestNewWorkerDefaults(t *testing.T) {
	worker := NewWorker("q")

	if worker.StopTimeout != 30*time.Second || worker.QueueRate != 100000 || worker.StepRate != 100000 ||
		worker.MaxSteps != 1000 || worker.StepPollers != 2 || worker.MaxFlowTasks != 1000 || worker.FlowPollers != 2 {
		t.Fatalf("unexpected defaults: %+v", worker)
	}
	if worker.Context == nil {
		t.Fatalf("expected a default context")
	}
}

func TestWorkerBuilder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := NewWorker("q").
		WithStopTimeout(time.Second).
		WithQueueRate(1).
		WithStepRate(2).
		WithMaxSteps(3).
		WithStepPollers(4).
		WithMaxFlowTasks(5).
		WithFlowPollers(6).
		WithContext(ctx).
		WithSteps(serveTestStep).
		WithStepHandlers(NewStepHandler("custom-step", serveTestStep)).
		WithFlows(serveTestFlow).
		WithFlowHandlers(NewFlowHandler("custom-flow", serveTestFlow))

	if worker.StopTimeout != time.Second || worker.QueueRate != 1 || worker.StepRate != 2 || worker.MaxSteps != 3 ||
		worker.StepPollers != 4 || worker.MaxFlowTasks != 5 || worker.FlowPollers != 6 || worker.Context != ctx {
		t.Fatalf("unexpected settings: %+v", worker)
	}
	if worker.Steps[0].Name != "serveTestStep" || worker.Steps[1].Name != "custom-step" {
		t.Fatalf("unexpected steps: %+v %+v", worker.Steps[0], worker.Steps[1])
	}
	if worker.Flows[0].Name != "serveTestFlow" || worker.Flows[1].Name != "custom-flow" {
		t.Fatalf("unexpected flows: %+v %+v", worker.Flows[0], worker.Flows[1])
	}
}

func TestMaxFlowTasksHasAFloor(t *testing.T) {
	if got := NewWorker("q").WithMaxFlowTasks(1).MaxFlowTasks; got != 2 {
		t.Fatalf("MaxFlowTasks = %d", got)
	}
}

func TestWithContextIgnoresNil(t *testing.T) {
	worker := NewWorker("q")
	before := worker.Context

	//lint:ignore SA1012 This test passes a nil context on purpose.
	worker.WithContext(nil)

	if worker.Context != before {
		t.Fatalf("expected the context to stay")
	}
}

func TestServeReportsWhatTheSDKWouldPanicOn(t *testing.T) {
	var options client.Options
	lazy, err := client.NewLazyClient(options)
	if err != nil {
		t.Fatalf("NewLazyClient() error = %v", err)
	}
	defer lazy.Close()
	c := &Client{temporalClient: lazy}

	tests := []struct {
		name   string
		worker *Worker
	}{
		{name: "too few flow pollers", worker: NewWorker("q").WithSteps(serveTestStep).WithFlowPollers(1)},
		{name: "reserved queue name", worker: NewWorker("__temporal_q").WithSteps(serveTestStep)},
		{name: "reserved flow name", worker: NewWorker("q").WithFlowHandlers(NewFlowHandler("__temporal_flow", serveTestFlow))},
		{name: "reserved step name", worker: NewWorker("q").WithStepHandlers(NewStepHandler("__temporal_step", serveTestStep))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.Serve(tt.worker)
			if err == nil || !strings.Contains(err.Error(), "invalid worker") {
				t.Fatalf("Serve() error = %v", err)
			}
		})
	}
}

func TestDecodeLogAttributesKeepsNumbersExact(t *testing.T) {
	raw := json.RawMessage(`{"id":9007199254740993,"big":18446744073709551615,"ratio":2.5,"whole":3,"name":"acme","nested":{"id":9007199254740993}}`)

	attributes, err := decodeLogAttributes(raw)
	if err != nil {
		t.Fatalf("decodeLogAttributes() error = %v", err)
	}

	expected := map[string]any{
		"id":    int64(9007199254740993),
		"big":   uint64(18446744073709551615),
		"ratio": 2.5,
		"whole": int64(3),
		"name":  "acme",
	}
	for key, want := range expected {
		if attributes[key] != want {
			t.Errorf("%s = %v (%T), want %v (%T)", key, attributes[key], attributes[key], want, want)
		}
	}
	if nested := fmt.Sprint(attributes["nested"]); !strings.Contains(nested, "9007199254740993") {
		t.Errorf("nested = %s", nested)
	}
}

func TestDecodeLogAttributesHandlesNoAttributes(t *testing.T) {
	for _, raw := range []string{"", "null", "{}"} {
		attributes, err := decodeLogAttributes(json.RawMessage(raw))
		if err != nil {
			t.Errorf("decodeLogAttributes(%q) error = %v", raw, err)
		}
		if len(attributes) != 0 {
			t.Errorf("decodeLogAttributes(%q) = %v", raw, attributes)
		}
	}
}

func TestDecodeLogAttributesRejectsAnythingButAnObject(t *testing.T) {
	_, err := decodeLogAttributes(json.RawMessage(`[1,2]`))
	if err == nil {
		t.Fatal("decodeLogAttributes() accepted an array")
	}
}
