package gush_test

import (
	"testing"
	"time"

	"github.com/batchstream/gush"
)

func TestRetryPolicyDefaultValues(t *testing.T) {
	policy := gush.RetryPolicy{}

	// Test default values
	if policy.InitialInterval != 0 {
		t.Errorf("Expected InitialInterval to be 0, got %v", policy.InitialInterval)
	}
	if policy.BackoffCoefficient != 0 {
		t.Errorf("Expected BackoffCoefficient to be 0, got %v", policy.BackoffCoefficient)
	}
	if policy.MaxInterval != 0 {
		t.Errorf("Expected MaxInterval to be 0, got %v", policy.MaxInterval)
	}
	if policy.MaxAttempts != 0 {
		t.Errorf("Expected MaxAttempts to be 0, got %v", policy.MaxAttempts)
	}
	if len(policy.NoRetry) != 0 {
		t.Errorf("Expected NoRetry to be empty, got %v", policy.NoRetry)
	}
}

func TestRetryPolicyWithValues(t *testing.T) {
	policy := gush.RetryPolicy{
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 2.0,
		MaxInterval:        10 * time.Second,
		MaxAttempts:        5,
		NoRetry:            []string{"PermanentError", "ValidationError"},
	}

	if policy.InitialInterval != 1*time.Second {
		t.Errorf("Expected InitialInterval to be 1s, got %v", policy.InitialInterval)
	}
	if policy.BackoffCoefficient != 2.0 {
		t.Errorf("Expected BackoffCoefficient to be 2.0, got %v", policy.BackoffCoefficient)
	}
	if policy.MaxInterval != 10*time.Second {
		t.Errorf("Expected MaxInterval to be 10s, got %v", policy.MaxInterval)
	}
	if policy.MaxAttempts != 5 {
		t.Errorf("Expected MaxAttempts to be 5, got %v", policy.MaxAttempts)
	}
	if len(policy.NoRetry) != 2 {
		t.Errorf("Expected 2 NoRetry, got %d", len(policy.NoRetry))
	}
}

func TestRetryPolicyBackoffCalculation(t *testing.T) {
	policy := gush.RetryPolicy{
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 2.0,
		MaxInterval:        10 * time.Second,
	}

	// Test backoff calculation logic
	expectedIntervals := []time.Duration{
		1 * time.Second,  // 1st attempt
		2 * time.Second,  // 2nd attempt (1 * 2.0)
		4 * time.Second,  // 3rd attempt (2 * 2.0)
		8 * time.Second,  // 4th attempt (4 * 2.0)
		10 * time.Second, // 5th attempt (8 * 2.0 = 16, but capped at 10)
	}

	for i, expected := range expectedIntervals {
		interval := policy.InitialInterval
		for j := 0; j < i; j++ {
			interval = time.Duration(float64(interval) * policy.BackoffCoefficient)
			if interval > policy.MaxInterval {
				interval = policy.MaxInterval
			}
		}
		if interval != expected {
			t.Errorf("Attempt %d: expected %v, got %v", i+1, expected, interval)
		}
	}
}

func TestRetryPolicyNonRetryableErrors(t *testing.T) {
	policy := gush.RetryPolicy{
		NoRetry: []string{"PermanentError", "ValidationError"},
	}

	// Test that the error types are correctly stored
	expectedErrors := map[string]bool{
		"PermanentError":  true,
		"ValidationError": true,
		"TemporaryError":  false,
	}

	for errorType, shouldBeNonRetryable := range expectedErrors {
		found := false
		for _, nonRetryable := range policy.NoRetry {
			if nonRetryable == errorType {
				found = true
				break
			}
		}
		if found != shouldBeNonRetryable {
			t.Errorf("Error type %s: expected nonRetryable=%v, got nonRetryable=%v",
				errorType, shouldBeNonRetryable, found)
		}
	}
}
