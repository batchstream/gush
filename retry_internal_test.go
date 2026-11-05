package gush

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestRetryPolicyValidate(t *testing.T) {
	invalid := map[string]RetryPolicy{
		"negative attempts":    {MaxAttempts: -2},
		"a large negative":     {MaxAttempts: -100},
		"small coefficient":    {BackoffCoefficient: 0.5},
		"negative coefficient": {BackoffCoefficient: -1},
		"negative initial":     {InitialInterval: -time.Second},
		"negative maximum":     {MaxInterval: -time.Second},
		"an unknown timeout":   {NoRetry: []string{"TemporalTimeout:Soon"}},
		"no timeout name":      {NoRetry: []string{"TemporalTimeout:"}},
		"unspecified timeout":  {NoRetry: []string{"TemporalTimeout:Unspecified"}},
	}
	for name, policy := range invalid {
		err := policy.validate(defaultStepAttempts)
		if !errors.Is(err, errInvalidConfig) {
			t.Errorf("%s: validate() = %v, want an invalid config error", name, err)
		}
	}

	valid := map[string]RetryPolicy{
		"empty":              {},
		"unlimited":          {MaxAttempts: UnlimitedAttempts},
		"a number":           {MaxAttempts: 3},
		"a coefficient of 1": {BackoffCoefficient: 1},
		"a cap below first":  {InitialInterval: time.Minute, MaxInterval: time.Second},
		"an error type":      {NoRetry: []string{"Declined", ""}},
		"the step timeout":   {NoRetry: []string{StepTimeoutErrorType}},
		"another timeout":    {NoRetry: []string{"TemporalTimeout:Heartbeat"}},
	}
	for name, policy := range valid {
		if err := policy.validate(defaultStepAttempts); err != nil {
			t.Errorf("%s: validate() = %v", name, err)
		}
	}
}

// Temporal does not look at NoRetry when there is only one attempt, so flows that do this run
// today, and rejecting it would fail them on replay.
func TestRetryPolicyValidateAcceptsAnUnknownTimeoutWithOneAttempt(t *testing.T) {
	unknown := []string{"TemporalTimeout:Soon"}

	explicit := RetryPolicy{MaxAttempts: 1, NoRetry: unknown}
	if err := explicit.validate(defaultStepAttempts); err != nil {
		t.Errorf("one attempt on a step: validate() = %v", err)
	}

	byDefault := RetryPolicy{NoRetry: unknown}
	if err := byDefault.validate(defaultFlowAttempts); err != nil {
		t.Errorf("a flow's default of one attempt: validate() = %v", err)
	}
	if err := byDefault.validate(defaultStepAttempts); !errors.Is(err, errInvalidConfig) {
		t.Errorf("a step's default of ten attempts: validate() = %v, want an invalid config error", err)
	}

	retried := RetryPolicy{MaxAttempts: 2, NoRetry: unknown}
	if err := retried.validate(defaultFlowAttempts); !errors.Is(err, errInvalidConfig) {
		t.Errorf("two attempts on a flow: validate() = %v, want an invalid config error", err)
	}
}

func TestTemporalPolicyAttempts(t *testing.T) {
	tests := map[int32]int32{0: 10, 3: 3, UnlimitedAttempts: 0}
	for attempts, expected := range tests {
		policy := RetryPolicy{MaxAttempts: attempts}
		if got := policy.temporalPolicy(defaultStepAttempts).MaximumAttempts; got != expected {
			t.Errorf("attempts %d = %d, want %d", attempts, got, expected)
		}
	}
}

func TestTemporalPolicyRaisesACapBelowTheFirstDelay(t *testing.T) {
	tests := map[string]struct {
		policy   RetryPolicy
		expected time.Duration
	}{
		"above the cap":     {RetryPolicy{InitialInterval: 200 * time.Second, MaxInterval: 100 * time.Second}, 200 * time.Second},
		"below the cap":     {RetryPolicy{InitialInterval: time.Second, MaxInterval: 100 * time.Second}, 100 * time.Second},
		"above the default": {RetryPolicy{MaxInterval: time.Millisecond}, time.Second},
		"no cap":            {RetryPolicy{InitialInterval: 2 * time.Second}, 200 * time.Second},
		"nothing set":       {RetryPolicy{}, 100 * time.Second},
	}
	for name, test := range tests {
		if got := test.policy.temporalPolicy(defaultStepAttempts).MaximumInterval; got != test.expected {
			t.Errorf("%s: MaximumInterval = %v, want %v", name, got, test.expected)
		}
	}
}

func TestTotalBackoffWithACapBelowTheFirstDelay(t *testing.T) {
	policy := RetryPolicy{InitialInterval: 200 * time.Second, MaxInterval: 100 * time.Second, MaxAttempts: 3}

	got := policy.totalBackoff()

	if expected := 400 * time.Second; got != expected {
		t.Errorf("totalBackoff() = %v, want %v", got, expected)
	}
}

func TestStepConfigValidate(t *testing.T) {
	invalid := map[string]func(config *StepConfig){
		"no attempt timeout":       func(config *StepConfig) { config.AttemptTimeout = 0 },
		"negative attempt timeout": func(config *StepConfig) { config.AttemptTimeout = -time.Second },
		"negative heartbeat":       func(config *StepConfig) { config.HeartbeatTimeout = -time.Second },
		"a bad retry policy":       func(config *StepConfig) { config.MaxAttempts = -2 },
		"negative attempt timeout with a pickup timeout": func(config *StepConfig) {
			config.AttemptTimeout = -time.Second
			config.PickupTimeout = time.Minute
		},
		"no attempt timeout and no overall limit": func(config *StepConfig) {
			config.AttemptTimeout = 0
			config.PickupTimeout = time.Minute
			config.MaxAttempts = UnlimitedAttempts
		},
	}
	for name, change := range invalid {
		config := DefaultStepConfig()
		change(config)
		err := config.validate()
		if !errors.Is(err, errInvalidConfig) {
			t.Errorf("%s: validate() = %v, want an invalid config error", name, err)
		}
	}

	valid := map[string]func(config *StepConfig){
		"every timeout": func(config *StepConfig) {
			config.HeartbeatTimeout = time.Second
			config.PickupTimeout = time.Second
		},
		// Gush sends Temporal no pickup timeout for this.
		"negative pickup": func(config *StepConfig) { config.PickupTimeout = -time.Second },
		// The overall limit is the timeout Temporal needs.
		"a pickup timeout instead of an attempt timeout": func(config *StepConfig) {
			config.AttemptTimeout = 0
			config.PickupTimeout = 30 * time.Second
		},
		"attempts too many to total": func(config *StepConfig) {
			config.MaxAttempts = math.MaxInt32
			config.PickupTimeout = time.Minute
		},
		"attempts too many to total and no attempt timeout": func(config *StepConfig) {
			config.AttemptTimeout = 0
			config.MaxAttempts = math.MaxInt32
			config.PickupTimeout = 30 * time.Minute
		},
	}
	for name, change := range valid {
		config := DefaultStepConfig()
		change(config)
		err := config.validate()
		if err != nil {
			t.Errorf("%s: validate() = %v", name, err)
		}
	}
}

func TestFlowConfigValidate(t *testing.T) {
	config := DefaultFlowConfig()
	config.Timeout = -time.Second
	err := config.validate()
	if !errors.Is(err, errInvalidConfig) {
		t.Errorf("negative timeout: validate() = %v, want an invalid config error", err)
	}

	config = DefaultFlowConfig()
	config.MaxAttempts = -2
	err = config.validate()
	if !errors.Is(err, errInvalidConfig) {
		t.Errorf("a bad retry policy: validate() = %v, want an invalid config error", err)
	}

	err = DefaultFlowConfig().validate()
	if err != nil {
		t.Errorf("defaults: validate() = %v", err)
	}
}
