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
	if config.Cron != "" {
		t.Errorf("Expected Cron to be empty, got %v", config.Cron)
	}
	if config.Async {
		t.Errorf("Expected Async to be false, got %v", config.Async)
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
			name:   "RunFlowWithWaitResult",
			option: gush.WithAsync(),
			expected: func(c *gush.FlowConfig) bool {
				return c.Async
			},
		},
		{
			name:   "WithCron",
			option: gush.WithCron("0 2 * * *"),
			expected: func(c *gush.FlowConfig) bool {
				return c.Cron == "0 2 * * *"
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
		gush.WithAsync(),
		gush.WithCron("0 3 * * *"),
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
	if !config.Async {
		t.Errorf("Expected Async to be true, got %v", config.Async)
	}
	if config.Cron != "0 3 * * *" {
		t.Errorf("Expected Cron to be '0 3 * * *', got %v", config.Cron)
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
		Cron:    "0 4 * * *",
		Async:   true,
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
	if config.Cron != "0 4 * * *" {
		t.Errorf("Expected Cron to come from custom config, got %v", config.Cron)
	}
	if config.Priority != gush.PriorityHigh {
		t.Errorf("Expected Priority to come from custom config, got %v", config.Priority)
	}
	if !config.Async {
		t.Errorf("Expected Async to come from custom config, got %v", config.Async)
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

func TestCronScheduleValidation(t *testing.T) {
	validCronSchedules := []string{
		"0 2 * * *",   // Daily at 2 AM
		"0 */6 * * *", // Every 6 hours
		"0 9 * * 1",   // Every Monday at 9 AM
		"30 14 * * *", // Daily at 2:30 PM
		"0 0 1 * *",   // First day of every month
	}

	for _, cron := range validCronSchedules {
		t.Run("ValidCron_"+cron, func(t *testing.T) {
			config := gush.DefaultFlowConfig()
			config.Apply(gush.WithCron(cron))
			if config.Cron != cron {
				t.Errorf("Expected Cron to be %s, got %s", cron, config.Cron)
			}
		})
	}
}
