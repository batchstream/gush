package gush_test

import (
	"testing"
	"time"

	"github.com/batchstream/gush"
)

func TestDefaultFlowConfig(t *testing.T) {
	config := gush.DefaultFlowConfig()

	// Test default values
	if config.Timeout != 0 {
		t.Errorf("Expected Timeout to be 0 (unlimited), got %v", config.Timeout)
	}
	if config.Priority != gush.PriorityMedium {
		t.Errorf("Expected Priority to be PriorityMedium, got %v", config.Priority)
	}
	if config.ID != "" {
		t.Errorf("Expected ID to be empty, got %v", config.ID)
	}

	// Test LogAttributes initialization
	if config.LogAttributes == nil {
		t.Errorf("Expected LogAttributes to be initialized, got nil")
	}
	if len(config.LogAttributes) != 0 {
		t.Errorf("Expected LogAttributes to be empty, got %d items", len(config.LogAttributes))
	}

	// Test retry policy defaults
	if config.InitialInterval != 1*time.Second {
		t.Errorf("Expected InitialInterval to be 1s, got %v", config.InitialInterval)
	}
	if config.BackoffCoefficient != 2.0 {
		t.Errorf("Expected BackoffCoefficient to be 2.0, got %v", config.BackoffCoefficient)
	}
	if config.MaxInterval != 100*time.Second {
		t.Errorf("Expected MaxInterval to be 100s, got %v", config.MaxInterval)
	}
	if config.MaxAttempts != 1 {
		t.Errorf("Expected MaxAttempts to be 1, got %v", config.MaxAttempts)
	}
}

func TestFlowOptions(t *testing.T) {
	retryPolicy := gush.RetryPolicy{
		InitialInterval:    2 * time.Second,
		BackoffCoefficient: 3.0,
		MaxInterval:        200 * time.Second,
		MaxAttempts:        5,
	}

	tests := []struct {
		name     string
		option   gush.FlowOption
		expected func(*gush.FlowConfig) bool
	}{
		{
			name:   "WithID",
			option: gush.WithID("test-flow-123"),
			expected: func(c *gush.FlowConfig) bool {
				return c.ID == "test-flow-123"
			},
		},
		{
			name:   "WithTimeout",
			option: gush.WithTimeout(5 * time.Minute),
			expected: func(c *gush.FlowConfig) bool {
				return c.Timeout == 5*time.Minute
			},
		},
		{
			name:   "WithPriority",
			option: gush.WithPriority(gush.PriorityHigh),
			expected: func(c *gush.FlowConfig) bool {
				return c.Priority == gush.PriorityHigh
			},
		},
		{
			name:   "WithRetryPolicy",
			option: gush.WithRetryPolicy(retryPolicy),
			expected: func(c *gush.FlowConfig) bool {
				return c.InitialInterval == 2*time.Second &&
					c.BackoffCoefficient == 3.0 &&
					c.MaxInterval == 200*time.Second &&
					c.MaxAttempts == 5
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := gush.DefaultFlowConfig()
			config.Apply(tt.option)
			if !tt.expected(config) {
				t.Errorf("Option %s did not set expected value", tt.name)
			}
		})
	}
}

func TestMultipleFlowOptions(t *testing.T) {
	config := gush.DefaultFlowConfig()

	// Apply multiple options
	options := []gush.FlowOption{
		gush.WithID("batch-process-123"),
		gush.WithTimeout(10 * time.Minute),
		gush.WithPriority(gush.PriorityLow),
		gush.WithLogAttribute("user_id", "batch-user"),
		gush.WithLogAttribute("process_type", "batch"),
	}

	for _, option := range options {
		config.Apply(option)
	}

	// Verify all options were applied
	if config.ID != "batch-process-123" {
		t.Errorf("Expected ID to be 'batch-process-123', got %v", config.ID)
	}
	if config.Timeout != 10*time.Minute {
		t.Errorf("Expected Timeout to be 10 minutes, got %v", config.Timeout)
	}
	if config.Priority != gush.PriorityLow {
		t.Errorf("Expected Priority to be PriorityLow, got %v", config.Priority)
	}
	if config.LogAttributes["user_id"] != "batch-user" {
		t.Errorf("Expected LogAttributes[user_id] to be 'batch-user', got %v", config.LogAttributes["user_id"])
	}
	if config.LogAttributes["process_type"] != "batch" {
		t.Errorf("Expected LogAttributes[process_type] to be 'batch', got %v", config.LogAttributes["process_type"])
	}
}

func TestWithConfigIgnoresNil(t *testing.T) {
	config := gush.DefaultFlowConfig()

	config.Apply(gush.WithConfig(nil))

	if config.Timeout != 0 {
		t.Errorf("Expected Timeout to remain 0, got %v", config.Timeout)
	}
	if config.Priority != gush.PriorityMedium {
		t.Errorf("Expected Priority to remain %v, got %v", gush.PriorityMedium, config.Priority)
	}
	if config.ID != "" {
		t.Errorf("Expected ID to remain empty, got %v", config.ID)
	}
}

func TestWithConfigAllowsLaterOverrides(t *testing.T) {
	config := gush.DefaultFlowConfig()
	customConfig := &gush.FlowConfig{
		CommonConfig: gush.CommonConfig{
			RetryPolicy: gush.RetryPolicy{
				InitialInterval:    5 * time.Second,
				BackoffCoefficient: 4.0,
				MaxInterval:        300 * time.Second,
				MaxAttempts:        7,
			},
			Priority: gush.PriorityHigh,
			FairKey:  "tenant-123",
			LogAttributes: map[string]any{
				"source": "config",
			},
		},
		ID:      "config-flow-id",
		Timeout: 15 * time.Minute,
	}

	options := []gush.FlowOption{
		gush.WithConfig(customConfig),
		gush.WithID("override-flow-id"),
		gush.WithTimeout(20 * time.Minute),
		gush.WithLogAttribute("override_key", "override_value"),
	}

	for _, option := range options {
		config.Apply(option)
	}

	if config.ID != "override-flow-id" {
		t.Errorf("Expected ID to be overridden, got %v", config.ID)
	}
	if config.Timeout != 20*time.Minute {
		t.Errorf("Expected Timeout to be overridden, got %v", config.Timeout)
	}
	if config.Priority != gush.PriorityHigh {
		t.Errorf("Expected Priority to come from custom config, got %v", config.Priority)
	}
	if config.LogAttributes["source"] != "config" {
		t.Errorf("Expected LogAttributes[source] to come from custom config, got %v", config.LogAttributes["source"])
	}
	if config.LogAttributes["override_key"] != "override_value" {
		t.Errorf("Expected LogAttributes[override_key] to be overridden, got %v", config.LogAttributes["override_key"])
	}

	if _, leaked := customConfig.LogAttributes["override_key"]; leaked {
		t.Errorf("Expected later options to leave the custom config alone, got %v", customConfig.LogAttributes)
	}
}

func TestWithConfigAcceptsConfigWithoutLogAttributes(t *testing.T) {
	config := gush.DefaultFlowConfig()
	customConfig := &gush.FlowConfig{ID: "decoded-from-json"}

	config.Apply(gush.WithConfig(customConfig), gush.WithLogAttribute("key", "value"))

	if config.LogAttributes["key"] != "value" {
		t.Errorf("Expected LogAttributes[key] to be set, got %v", config.LogAttributes)
	}
}

func TestFlowConfigRetryPolicyInheritance(t *testing.T) {
	config := gush.DefaultFlowConfig()

	// Test that FlowConfig properly embeds RetryPolicy
	if config.InitialInterval != 1*time.Second {
		t.Errorf("Expected InitialInterval to be 1s, got %v", config.InitialInterval)
	}
	if config.BackoffCoefficient != 2.0 {
		t.Errorf("Expected BackoffCoefficient to be 2.0, got %v", config.BackoffCoefficient)
	}
	if config.MaxInterval != 100*time.Second {
		t.Errorf("Expected MaxInterval to be 100s, got %v", config.MaxInterval)
	}
	if config.MaxAttempts != 1 {
		t.Errorf("Expected MaxAttempts to be 1, got %v", config.MaxAttempts)
	}
}

func decodedConfig(t *testing.T, raw string) *gush.FlowConfig {
	t.Helper()

	config := &gush.FlowConfig{}
	err := gush.Decode([]byte(raw), config)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	return config
}

// A struct holding a config beside its own fields, such as a queued message, decodes both.
func TestAStructEmbeddingAConfigDecodesItsOwnFields(t *testing.T) {
	type queuedFlow struct {
		gush.FlowConfig
		Tenant string `json:"tenant"`
	}
	raw := `{"id":"flow-1","max_attempts":3,"tenant":"acme","log_attributes":{"user_id":9007199254740993}}`

	var queued queuedFlow
	err := gush.Decode([]byte(raw), &queued)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if queued.Tenant != "acme" {
		t.Errorf("tenant = %q", queued.Tenant)
	}
	if queued.ID != "flow-1" || queued.MaxAttempts != 3 {
		t.Errorf("config = %q, %d", queued.ID, queued.MaxAttempts)
	}
	if queued.LogAttributes["user_id"] != int64(9007199254740993) {
		t.Errorf("user_id = %v (%T)", queued.LogAttributes["user_id"], queued.LogAttributes["user_id"])
	}
}

func TestJSONConfigKeepsLogAttributeNumbersExact(t *testing.T) {
	config := decodedConfig(t, `{"log_attributes":{"user_id":9007199254740993}}`)

	if config.LogAttributes["user_id"] != int64(9007199254740993) {
		t.Errorf("user_id = %v (%T)", config.LogAttributes["user_id"], config.LogAttributes["user_id"])
	}
}

func TestOptionsApplyToADecodedConfig(t *testing.T) {
	config := decodedConfig(t, `{"log_attributes":null}`)

	config.Apply(gush.WithLogAttribute("tenant", "acme"))

	if config.LogAttributes["tenant"] != "acme" {
		t.Errorf("attributes = %v", config.LogAttributes)
	}
}

func TestLogAttributeOptionsApplyToAZeroConfig(t *testing.T) {
	var config gush.StepConfig
	attributes := map[string]any{"b": 2}

	config.Apply(gush.WithLogAttributes(attributes), gush.WithLogAttribute("a", 1))

	if config.LogAttributes["a"] != 1 || config.LogAttributes["b"] != 2 {
		t.Errorf("attributes = %v", config.LogAttributes)
	}
}

func TestWithConfigKeepsWhatAChildInherits(t *testing.T) {
	inherited := gush.DefaultFlowConfig()
	inherited.Priority = gush.PriorityHigh
	inherited.FairKey = "tenant-1"
	inherited.LogAttributes["tenant"] = "acme"
	inherited.LogAttributes["region"] = "parent"

	config := decodedConfig(t, `{"id":"child","log_attributes":{"region":"child"}}`)
	inherited.Apply(gush.WithConfig(config))

	if inherited.ID != "child" {
		t.Errorf("id = %q", inherited.ID)
	}
	if inherited.Priority != gush.PriorityHigh || inherited.FairKey != "tenant-1" {
		t.Errorf("inherited scheduling = %q, %q", inherited.Priority, inherited.FairKey)
	}
	if inherited.LogAttributes["tenant"] != "acme" || inherited.LogAttributes["region"] != "child" {
		t.Errorf("attributes = %v", inherited.LogAttributes)
	}
}

func TestWithConfigOverridesWhatItSets(t *testing.T) {
	inherited := gush.DefaultFlowConfig()
	inherited.Priority = gush.PriorityHigh

	config := decodedConfig(t, `{"priority":"low","fair_key":"tenant-2"}`)
	inherited.Apply(gush.WithConfig(config))

	if inherited.Priority != gush.PriorityLow || inherited.FairKey != "tenant-2" {
		t.Errorf("scheduling = %q, %q", inherited.Priority, inherited.FairKey)
	}
}

func TestJSONConfigMergesLogAttributes(t *testing.T) {
	config := gush.DefaultFlowConfig()
	config.LogAttributes["kept"] = "value"

	err := gush.Decode([]byte(`{"log_attributes":{"added":1}}`), config)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if config.LogAttributes["kept"] != "value" || config.LogAttributes["added"] != int64(1) {
		t.Errorf("attributes = %v", config.LogAttributes)
	}
}

func TestJSONConfigDecodesOverExistingSettings(t *testing.T) {
	config := gush.DefaultFlowConfig()
	config.ID = "kept"
	config.MaxAttempts = 5
	config.Priority = gush.PriorityHigh

	err := gush.Decode([]byte(`{"timeout":60000000000}`), config)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if config.Timeout != time.Minute {
		t.Errorf("timeout = %v", config.Timeout)
	}
	if config.ID != "kept" || config.MaxAttempts != 5 || config.Priority != gush.PriorityHigh {
		t.Errorf("existing settings changed: %q, %d, %q", config.ID, config.MaxAttempts, config.Priority)
	}
}
