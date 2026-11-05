package gush

import (
	"fmt"
	"time"
)

// StepConfig holds the settings of one step run.
type StepConfig struct {
	CommonConfig

	// Queue is the task queue the step runs on. If empty, it uses its flow's queue.
	Queue string `json:"queue"`

	// AttemptTimeout limits each attempt, not the step as a whole. It also bounds how long
	// a suspended step waits for Resume. A negative one is an error, and so is zero, unless
	// PickupTimeout and a limited number of attempts give the step an overall limit.
	AttemptTimeout time.Duration `json:"attempt_timeout"`

	// HeartbeatTimeout fails an attempt that goes this long without calling Heartbeat.
	// Zero disables it, and a negative one is an error.
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`

	// PickupTimeout fails the step if no worker picks it up in time. Zero or negative means
	// unlimited.
	PickupTimeout time.Duration `json:"pickup_timeout"`

	// WaitForCancel makes RunStep wait for a canceled step to end. See WithWaitForCancel.
	WaitForCancel bool `json:"wait_for_cancel"`
}

// DefaultStepConfig returns the settings a step run starts from: ten attempts of up to
// 2 minutes each, medium priority, and a 1s to 100s backoff.
func DefaultStepConfig() *StepConfig {
	config := &StepConfig{}
	config.AttemptTimeout = 2 * time.Minute
	config.InitialInterval = 1 * time.Second
	config.BackoffCoefficient = 2.0
	config.MaxInterval = 100 * time.Second
	config.MaxAttempts = defaultStepAttempts
	config.Priority = PriorityMedium
	config.LogAttributes = make(map[string]any)
	return config
}

// Apply applies options to config in order.
func (config *StepConfig) Apply(options ...StepOption) {
	for _, option := range options {
		option.applyStep(config)
	}
}

type stepOption func(config *StepConfig)

func (option stepOption) applyStep(config *StepConfig) {
	option(config)
}

// validate rejects settings Temporal refuses, judging the timeouts it would actually receive.
// See RetryPolicy.validate for why it must not go further.
func (config *StepConfig) validate() error {
	err := config.RetryPolicy.validate(defaultStepAttempts)
	if err != nil {
		return err
	}

	options := newActivityOptions(config)
	switch {
	case options.StartToCloseTimeout < 0, options.StartToCloseTimeout == 0 && options.ScheduleToCloseTimeout == 0:
		return fmt.Errorf("%w: attempt timeout %v must be positive", errInvalidConfig, config.AttemptTimeout)
	case options.HeartbeatTimeout < 0:
		return fmt.Errorf("%w: heartbeat timeout %v must not be negative", errInvalidConfig, config.HeartbeatTimeout)
	}
	return nil
}

// WithQueue runs the step on another task queue.
func WithQueue(queue string) StepOption {
	apply := func(config *StepConfig) {
		config.Queue = queue
	}
	return stepOption(apply)
}

// WithWaitForCancel makes RunStep wait for a running step to end when the flow is canceled.
// Otherwise it returns a canceled error at once while the step carries on, so cleanup could
// overlap the step's work. The step ends sooner if it heartbeats and returns on ctx.Done(),
// and if it finishes anyway, RunStep returns its result.
func WithWaitForCancel() StepOption {
	apply := func(config *StepConfig) {
		config.WaitForCancel = true
	}
	return stepOption(apply)
}

// WithAttemptTimeout limits each attempt of the step.
func WithAttemptTimeout(timeout time.Duration) StepOption {
	apply := func(config *StepConfig) {
		config.AttemptTimeout = timeout
	}
	return stepOption(apply)
}

// WithHeartbeatTimeout fails an attempt that goes this long without calling Heartbeat.
func WithHeartbeatTimeout(timeout time.Duration) StepOption {
	apply := func(config *StepConfig) {
		config.HeartbeatTimeout = timeout
	}
	return stepOption(apply)
}

// WithPickupTimeout fails the step if no worker picks it up in time.
func WithPickupTimeout(timeout time.Duration) StepOption {
	apply := func(config *StepConfig) {
		config.PickupTimeout = timeout
	}
	return stepOption(apply)
}
