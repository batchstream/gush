package gush

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
)

// UnlimitedAttempts as MaxAttempts keeps retrying until something else ends the run, such as
// a timeout.
const UnlimitedAttempts int32 = -1

const (
	defaultFlowAttempts int32 = 1
	defaultStepAttempts int32 = 10
)

// timeoutErrorTypePrefix marks a NoRetry entry that names a timeout rather than an
// ApplicationError type, and with more than one attempt Temporal rejects one naming an unknown
// timeout.
const timeoutErrorTypePrefix = "TemporalTimeout:"

// invalidConfigErrorType is the ApplicationError type of a flow that failed on settings
// validate rejected, which lets IsInvalidConfigError recognize it after it leaves the flow. It
// carries the package's name so that an application's own error type does not match it.
const invalidConfigErrorType = "GushInvalidConfig"

// errInvalidConfig marks settings that Temporal refuses. See IsInvalidConfigError.
var errInvalidConfig = errors.New("gush: invalid config")

// maxDuration is the longest time a time.Duration can hold.
const maxDuration = time.Duration(math.MaxInt64)

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

	// NoRetry lists the ApplicationError types that fail immediately. An entry starting with
	// TemporalTimeout: names a timeout instead, such as StepTimeoutErrorType. Naming a timeout
	// Temporal does not know is an error, unless there is only one attempt and so no retry.
	NoRetry []string `json:"no_retry"`
}

// validate rejects settings Temporal refuses, which would leave a flow Running with its tasks
// failing, and an attempt count that only a mistake produces, such as a budget computed below
// zero, which must not turn into retrying forever.
//
// Flow code calls it, so a new check must not reject what Temporal accepts: a refused command
// leaves no history for a replay to meet, while a stricter check fails a flow in flight that
// ran it. The checks on the backoff settings are older than this rule, and loosening them would
// change what replays see.
func (policy RetryPolicy) validate(defaultAttempts int32) error {
	switch {
	case policy.MaxAttempts < UnlimitedAttempts:
		return fmt.Errorf("%w: max attempts %d must be positive, zero for the default, or UnlimitedAttempts", errInvalidConfig, policy.MaxAttempts)
	case policy.BackoffCoefficient != 0 && policy.BackoffCoefficient < 1:
		return fmt.Errorf("%w: backoff coefficient %v must be at least 1", errInvalidConfig, policy.BackoffCoefficient)
	case policy.InitialInterval < 0:
		return fmt.Errorf("%w: initial interval %v must not be negative", errInvalidConfig, policy.InitialInterval)
	case policy.MaxInterval < 0:
		return fmt.Errorf("%w: max interval %v must not be negative", errInvalidConfig, policy.MaxInterval)
	}

	// With only one attempt Temporal does not look at NoRetry.
	if policy.attempts(defaultAttempts) == 1 {
		return nil
	}
	for _, errorType := range policy.NoRetry {
		timeoutName, isTimeout := strings.CutPrefix(errorType, timeoutErrorTypePrefix)
		if !isTimeout {
			continue
		}
		timeoutType, err := enums.TimeoutTypeFromString(timeoutName)
		if err != nil || timeoutType == enums.TIMEOUT_TYPE_UNSPECIFIED {
			return fmt.Errorf("%w: no retry entry %q names an unknown timeout", errInvalidConfig, errorType)
		}
	}
	return nil
}

// attempts returns MaxAttempts with zero read as the given default.
func (policy RetryPolicy) attempts(defaultAttempts int32) int32 {
	if policy.MaxAttempts == 0 {
		return defaultAttempts
	}
	return policy.MaxAttempts
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
			return addDurations(total, scaleDuration(retries, maxInterval))
		}
		if coefficient <= 1 {
			return addDurations(total, scaleDuration(retries, interval))
		}
		total = addDurations(total, interval)

		next := float64(interval) * coefficient
		if next >= float64(maxInterval) {
			interval = maxInterval
			continue
		}
		interval = time.Duration(next)
	}
	return total
}

// addDurations returns a+b, stopping at maxDuration rather than overflowing.
func addDurations(a, b time.Duration) time.Duration {
	if b > 0 && a > maxDuration-b {
		return maxDuration
	}
	return a + b
}

// scaleDuration returns count*duration, stopping at maxDuration rather than overflowing.
func scaleDuration(count int32, duration time.Duration) time.Duration {
	if duration > 0 && time.Duration(count) > maxDuration/duration {
		return maxDuration
	}
	return time.Duration(count) * duration
}

// temporalPolicy returns the policy Temporal runs, which reads zero attempts as unlimited.
func (policy RetryPolicy) temporalPolicy(defaultAttempts int32) *temporal.RetryPolicy {
	attempts := policy.attempts(defaultAttempts)

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
