package gush_test

import (
	"testing"
	"time"

	"github.com/batchstream/gush"
)

func TestDefaultStepConfig(t *testing.T) {
	config := gush.DefaultStepConfig()

	// Test default values
	if config.AttemptTimeout != 2*time.Minute {
		t.Errorf("Expected Timeout to be 2 minutes, got %v", config.AttemptTimeout)
	}
	if config.HeartbeatTimeout != 0 {
		t.Errorf("Expected HeartbeatTimeout to be 0, got %v", config.HeartbeatTimeout)
	}
	if config.Priority != gush.PriorityMedium {
		t.Errorf("Expected Priority to be PriorityMedium, got %v", config.Priority)
	}
	if config.Queue != "" {
		t.Errorf("Expected Queue to be empty, got %v", config.Queue)
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
	if config.MaxAttempts != 10 {
		t.Errorf("Expected MaxAttempts to be 10, got %v", config.MaxAttempts)
	}
}

func TestStepOptions(t *testing.T) {
	tests := []struct {
		name     string
		option   gush.StepOption
		expected func(*gush.StepConfig) bool
	}{
		{
			name:   "WithQueue",
			option: gush.WithQueue("test-queue"),
			expected: func(c *gush.StepConfig) bool {
				return c.Queue == "test-queue"
			},
		},
		{
			name:   "WithAttemptTimeout",
			option: gush.WithAttemptTimeout(5 * time.Minute),
			expected: func(c *gush.StepConfig) bool {
				return c.AttemptTimeout == 5*time.Minute
			},
		},
		{
			name:   "WithHeartbeatTimeout",
			option: gush.WithHeartbeatTimeout(30 * time.Second),
			expected: func(c *gush.StepConfig) bool {
				return c.HeartbeatTimeout == 30*time.Second
			},
		},
		{
			name:   "WithPriority",
			option: gush.WithPriority(gush.PriorityUrgent),
			expected: func(c *gush.StepConfig) bool {
				return c.Priority == gush.PriorityUrgent
			},
		},
		{
			name:   "WithInitialInterval",
			option: gush.WithInitialInterval(2 * time.Second),
			expected: func(c *gush.StepConfig) bool {
				return c.InitialInterval == 2*time.Second
			},
		},
		{
			name:   "WithBackoffCoefficient",
			option: gush.WithBackoffCoefficient(3.0),
			expected: func(c *gush.StepConfig) bool {
				return c.BackoffCoefficient == 3.0
			},
		},
		{
			name:   "WithMaxInterval",
			option: gush.WithMaxInterval(200 * time.Second),
			expected: func(c *gush.StepConfig) bool {
				return c.MaxInterval == 200*time.Second
			},
		},
		{
			name:   "WithMaxAttempts",
			option: gush.WithMaxAttempts(5),
			expected: func(c *gush.StepConfig) bool {
				return c.MaxAttempts == 5
			},
		},
		{
			name:   "WithNoRetry",
			option: gush.WithNoRetry("Error1", "Error2"),
			expected: func(c *gush.StepConfig) bool {
				return len(c.NoRetry) == 2 &&
					c.NoRetry[0] == "Error1" &&
					c.NoRetry[1] == "Error2"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := gush.DefaultStepConfig()
			config.Apply(tt.option)
			if !tt.expected(config) {
				t.Errorf("Option %s did not set expected value", tt.name)
			}
		})
	}
}

func TestMultipleStepOptions(t *testing.T) {
	config := gush.DefaultStepConfig()

	// Apply multiple options
	options := []gush.StepOption{
		gush.WithQueue("high-priority-queue"),
		gush.WithAttemptTimeout(1 * time.Minute),
		gush.WithHeartbeatTimeout(30 * time.Second),
		gush.WithPriority(gush.PriorityHigh),
		gush.WithMaxAttempts(3),
		gush.WithLogAttribute("action_type", "high_priority"),
		gush.WithLogAttribute("user_id", "admin"),
	}

	for _, option := range options {
		config.Apply(option)
	}

	// Verify all options were applied
	if config.Queue != "high-priority-queue" {
		t.Errorf("Expected Queue to be 'high-priority-queue', got %v", config.Queue)
	}
	if config.AttemptTimeout != 1*time.Minute {
		t.Errorf("Expected Timeout to be 1 minute, got %v", config.AttemptTimeout)
	}
	if config.HeartbeatTimeout != 30*time.Second {
		t.Errorf("Expected HeartbeatTimeout to be 30 seconds, got %v", config.HeartbeatTimeout)
	}
	if config.Priority != gush.PriorityHigh {
		t.Errorf("Expected Priority to be PriorityHigh, got %v", config.Priority)
	}
	if config.MaxAttempts != 3 {
		t.Errorf("Expected MaxAttempts to be 3, got %v", config.MaxAttempts)
	}
	if config.LogAttributes["action_type"] != "high_priority" {
		t.Errorf("Expected LogAttributes[action_type] to be 'high_priority', got %v", config.LogAttributes["action_type"])
	}
	if config.LogAttributes["user_id"] != "admin" {
		t.Errorf("Expected LogAttributes[user_id] to be 'admin', got %v", config.LogAttributes["user_id"])
	}
}

func TestStepConfigRetryPolicyInheritance(t *testing.T) {
	config := gush.DefaultStepConfig()

	// Test that StepConfig properly embeds RetryPolicy
	if config.InitialInterval != 1*time.Second {
		t.Errorf("Expected InitialInterval to be 1s, got %v", config.InitialInterval)
	}
	if config.BackoffCoefficient != 2.0 {
		t.Errorf("Expected BackoffCoefficient to be 2.0, got %v", config.BackoffCoefficient)
	}
	if config.MaxInterval != 100*time.Second {
		t.Errorf("Expected MaxInterval to be 100s, got %v", config.MaxInterval)
	}
	if config.MaxAttempts != 10 {
		t.Errorf("Expected MaxAttempts to be 10, got %v", config.MaxAttempts)
	}

	// Test LogAttributes initialization
	if config.LogAttributes == nil {
		t.Errorf("Expected LogAttributes to be initialized, got nil")
	}
	if len(config.LogAttributes) != 0 {
		t.Errorf("Expected LogAttributes to be empty, got %d items", len(config.LogAttributes))
	}
}
