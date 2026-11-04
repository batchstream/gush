package gush_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/batchstream/gush"
)

func TestFlowConfigMarshalUsesSnakeCaseJSONTags(t *testing.T) {
	config := gush.DefaultFlowConfig()
	config.ID = "flow-123"
	config.Timeout = 5 * time.Minute
	config.Cron = "0 2 * * *"
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
		"cron",
		"priority",
		"fairness",
		"initial_interval",
		"backoff_coefficient",
		"maximum_interval",
		"maximum_attempts",
		"non_retryable_error_types",
		"log_attributes",
	}

	for _, key := range expectedKeys {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected key %q in marshaled flow config, got %v", key, payload)
		}
	}
}

// A config can wait in a queue, so it may hold keys its reader no longer has.
func TestFlowConfigIgnoresKeysItDoesNotKnow(t *testing.T) {
	var config gush.FlowConfig
	err := json.Unmarshal([]byte(`{"id":"flow-1","async":true}`), &config)
	if err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if config.ID != "flow-1" {
		t.Errorf("ID = %q", config.ID)
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
		"timeout",
		"worker_pickup_timeout",
		"priority",
		"fairness",
		"initial_interval",
		"backoff_coefficient",
		"maximum_interval",
		"maximum_attempts",
		"non_retryable_error_types",
		"log_attributes",
	}

	for _, key := range expectedKeys {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected key %q in marshaled step config, got %v", key, payload)
		}
	}
}
