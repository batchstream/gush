package gush

import (
	"fmt"
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

	// MaxInterval caps the delay between retries. Zero means 100 times InitialInterval, and a
	// cap below InitialInterval is raised to it.
	MaxInterval time.Duration `json:"max_interval"`

	// MaxAttempts is the total number of attempts, including the first. Zero means one for
	// a flow and ten for a step. Use UnlimitedAttempts to keep retrying. Any other negative
	// number is an error.
	MaxAttempts int32 `json:"max_attempts"`

	// NoRetry lists the ApplicationError types that fail immediately.
	NoRetry []string `json:"no_retry"`
}

// validate rejects settings Temporal cannot run, which would leave a flow Running with its
// tasks failing, and an attempt count that only a mistake produces, such as a budget computed
// below zero, which must not turn into retrying forever.
func (policy RetryPolicy) validate() error {
	switch {
	case policy.MaxAttempts < UnlimitedAttempts:
		return fmt.Errorf("gush: max attempts %d must be positive, zero for the default, or UnlimitedAttempts", policy.MaxAttempts)
	case policy.BackoffCoefficient != 0 && policy.BackoffCoefficient < 1:
		return fmt.Errorf("gush: backoff coefficient %v must be at least 1", policy.BackoffCoefficient)
	case policy.InitialInterval < 0:
		return fmt.Errorf("gush: initial interval %v must not be negative", policy.InitialInterval)
	case policy.MaxInterval < 0:
		return fmt.Errorf("gush: max interval %v must not be negative", policy.MaxInterval)
	}
	return nil
}

// intervals returns the first delay and the cap, applying the defaults Temporal gives zero
// settings. Temporal rejects a cap below the first delay.
func (policy RetryPolicy) intervals() (initial, maximum time.Duration) {
	initial = policy.InitialInterval
	if initial == 0 {
		initial = time.Second
	}
	maximum = policy.MaxInterval
	if maximum == 0 {
		maximum = 100 * initial
	}
	return initial, max(maximum, initial)
}

// totalBackoff returns the time spent waiting between attempts if all MaxAttempts are used,
// applying the same defaults Temporal does to zero settings.
func (policy RetryPolicy) totalBackoff() time.Duration {
	interval, maxInterval := policy.intervals()
	coefficient := policy.BackoffCoefficient
	if coefficient == 0 {
		coefficient = 2.0
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

	_, maxInterval := policy.intervals()
	temporalPolicy := &temporal.RetryPolicy{
		InitialInterval:        policy.InitialInterval,
		BackoffCoefficient:     policy.BackoffCoefficient,
		MaximumInterval:        maxInterval,
		MaximumAttempts:        max(attempts, 0),
		NonRetryableErrorTypes: policy.NoRetry,
	}
	return temporalPolicy
}
