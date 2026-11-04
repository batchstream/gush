package gush

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// RetryPolicy defines how flows and steps are retried after they fail.
type RetryPolicy struct {
	// InitialInterval is the delay before the first retry. Zero means 1s.
	InitialInterval time.Duration `json:"initial_interval"`

	// BackoffCoefficient multiplies the delay after each retry. It must be at least 1.
	// Zero means 2.0.
	BackoffCoefficient float64 `json:"backoff_coefficient"`

	// MaxInterval caps the delay between retries. Zero means 100 times InitialInterval.
	MaxInterval time.Duration `json:"maximum_interval"`

	// MaxAttempts is the total number of attempts, including the first. Zero means
	// unlimited.
	MaxAttempts int32 `json:"maximum_attempts"`

	// NoRetry lists the ApplicationError types that fail immediately.
	NoRetry []string `json:"non_retryable_error_types"`
}

func (policy RetryPolicy) temporalPolicy() *temporal.RetryPolicy {
	temporalPolicy := &temporal.RetryPolicy{
		InitialInterval:        policy.InitialInterval,
		BackoffCoefficient:     policy.BackoffCoefficient,
		MaximumInterval:        policy.MaxInterval,
		MaximumAttempts:        policy.MaxAttempts,
		NonRetryableErrorTypes: policy.NoRetry,
	}
	return temporalPolicy
}
