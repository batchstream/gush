package gush

import "time"

// StepConfig holds the settings of one step run.
type StepConfig struct {
	CommonConfig

	// Queue is the task queue the step runs on. If empty, it uses its flow's queue.
	Queue string `json:"queue"`

	// AttemptTimeout limits each attempt, not the step as a whole. It also bounds how long
	// a suspended step waits for Resume.
	AttemptTimeout time.Duration `json:"timeout"`

	// HeartbeatTimeout fails an attempt that goes this long without calling Heartbeat.
	// Zero disables it.
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`

	// PickupTimeout fails the step if no worker picks it up in time. Zero means unlimited.
	PickupTimeout time.Duration `json:"worker_pickup_timeout"`

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
	config.MaxAttempts = 10
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
