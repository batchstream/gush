package gush

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// UnlimitedAttempts as MaxAttempts keeps retrying until something else ends the run, such as
// a timeout.
const UnlimitedAttempts int32 = -1

const (
	defaultFlowAttempts int32 = 1
	defaultStepAttempts int32 = 10
)

// RetryPolicy defines how flows and steps are retried after they fail. Every setting left
// at zero uses its default.
type RetryPolicy struct {
	// InitialInterval is the delay before the first retry. Zero means 1s.
	InitialInterval time.Duration `json:"initial_interval"`

	// BackoffCoefficient multiplies the delay after each retry. It must be at least 1.
	// Zero means 2.0.
	BackoffCoefficient float64 `json:"backoff_coefficient"`

	// MaxInterval caps the delay between retries. Zero means 100 times InitialInterval.
	MaxInterval time.Duration `json:"max_interval"`

	// MaxAttempts is the total number of attempts, including the first. Zero means one for
	// a flow and ten for a step. Use UnlimitedAttempts to keep retrying.
	MaxAttempts int32 `json:"max_attempts"`

	// NoRetry lists the ApplicationError types that fail immediately.
	NoRetry []string `json:"no_retry"`
}

// totalBackoff returns the time spent waiting between attempts if all MaxAttempts are used,
// applying the same defaults Temporal does to zero settings.
func (policy RetryPolicy) totalBackoff() time.Duration {
	interval := policy.InitialInterval
	if interval == 0 {
		interval = time.Second
	}
	coefficient := policy.BackoffCoefficient
	if coefficient == 0 {
		coefficient = 2.0
	}
	maxInterval := policy.MaxInterval
	if maxInterval == 0 {
		maxInterval = 100 * interval
	}

	var total time.Duration
	for retries := policy.MaxAttempts - 1; retries > 0; retries-- {
		if interval >= maxInterval {
			return total + time.Duration(retries)*maxInterval
		}
		if coefficient <= 1 {
			return total + time.Duration(retries)*interval
		}
		total += interval
		interval = time.Duration(float64(interval) * coefficient)
	}
	return total
}

// temporalPolicy returns the policy Temporal runs, which reads zero attempts as unlimited.
func (policy RetryPolicy) temporalPolicy(defaultAttempts int32) *temporal.RetryPolicy {
	attempts := policy.MaxAttempts
	if attempts == 0 {
		attempts = defaultAttempts
	}

	temporalPolicy := &temporal.RetryPolicy{
		InitialInterval:        policy.InitialInterval,
		BackoffCoefficient:     policy.BackoffCoefficient,
		MaximumInterval:        policy.MaxInterval,
		MaximumAttempts:        max(attempts, 0),
		NonRetryableErrorTypes: policy.NoRetry,
	}
	return temporalPolicy
}
