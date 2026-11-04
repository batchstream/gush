package gush_test

import (
	"testing"
	"time"

	"github.com/batchstream/gush"
)

func TestFlowConfigMarshalUsesSnakeCaseJSONTags(t *testing.T) {
	config := gush.DefaultFlowConfig()
	config.ID = "flow-123"
	config.Timeout = 5 * time.Minute
	config.Priority = gush.PriorityHigh
	config.FairKey = "tenant-a"
	config.InitialInterval = 2 * time.Second
	config.BackoffCoefficient = 3
	config.MaxInterval = 30 * time.Second
	config.MaxAttempts = 7
	config.NoRetry = []string{"ValidationError"}
	config.LogAttributes["request_id"] = "req-1"

	raw, err := gush.Encode(config)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	payload := make(map[string]any)
	err = gush.Decode(raw, &payload)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	expectedKeys := []string{
		"id",
		"timeout",
		"priority",
		"fair_key",
		"initial_interval",
		"backoff_coefficient",
		"max_interval",
		"max_attempts",
		"no_retry",
		"log_attributes",
	}

	for _, key := range expectedKeys {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected key %q in marshaled flow config, got %v", key, payload)
		}
	}
}

func TestStepConfigMarshalUsesSnakeCaseJSONTags(t *testing.T) {
	config := gush.DefaultStepConfig()
	config.Queue = "queue-a"
	config.AttemptTimeout = 3 * time.Minute
	config.PickupTimeout = 45 * time.Second
	config.Priority = gush.PriorityUrgent
	config.FairKey = "tenant-b"
	config.InitialInterval = 4 * time.Second
	config.BackoffCoefficient = 1.5
	config.MaxInterval = 40 * time.Second
	config.MaxAttempts = 9
	config.NoRetry = []string{"PermissionDenied"}
	config.LogAttributes["request_id"] = "req-2"

	raw, err := gush.Encode(config)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	payload := make(map[string]any)
	err = gush.Decode(raw, &payload)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	expectedKeys := []string{
		"queue",
		"attempt_timeout",
		"pickup_timeout",
		"heartbeat_timeout",
		"wait_for_cancel",
		"priority",
		"fair_key",
		"initial_interval",
		"backoff_coefficient",
		"max_interval",
		"max_attempts",
		"no_retry",
		"log_attributes",
	}

	for _, key := range expectedKeys {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected key %q in marshaled step config, got %v", key, payload)
		}
	}
}
