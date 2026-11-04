package gush

import (
	"maps"
	"slices"
	"time"
)

// FlowConfig holds the settings of one flow run. Its JSON form is stable, so a config can
// be queued and applied later with WithConfig. A setting left out of the JSON, or zero,
// uses its default, and "max_attempts": -1 means unlimited. Waiting is not part of a
// config: whoever applies it chooses RunFlow or StartFlow.
type FlowConfig struct {
	CommonConfig

	// ID is the flow ID. If empty, one is generated.
	ID string `json:"id"`

	// Timeout limits the whole execution, including every retry. Zero means unlimited.
	Timeout time.Duration `json:"timeout"`

	// ErrorIfRunning makes a client's RunFlow or StartFlow fail rather than join a flow
	// already running with this ID. See WithErrorIfRunning.
	ErrorIfRunning bool `json:"error_if_running"`
}

// DefaultFlowConfig returns the settings a flow run starts from: one attempt, medium
// priority, and a 1s to 100s backoff if more attempts are allowed.
func DefaultFlowConfig() *FlowConfig {
	config := &FlowConfig{}
	config.InitialInterval = 1 * time.Second
	config.BackoffCoefficient = 2.0
	config.MaxInterval = 100 * time.Second
	config.MaxAttempts = defaultFlowAttempts
	config.Priority = PriorityMedium
	config.LogAttributes = make(map[string]any)
	return config
}

// Apply applies options to config in order.
func (config *FlowConfig) Apply(options ...FlowOption) {
	for _, option := range options {
		option.applyFlow(config)
	}
}

type flowOption func(config *FlowConfig)

func (option flowOption) applyFlow(config *FlowConfig) {
	option(config)
}

// WithConfig replaces the settings accumulated so far with a copy of config, so options
// after it refine config. An empty Priority or FairKey keeps the current one, and
// LogAttributes merge over the current ones, so a child flow keeps what it inherits. A nil
// config changes nothing.
func WithConfig(config *FlowConfig) FlowOption {
	apply := func(target *FlowConfig) {
		if config == nil {
			return
		}
		current := *target

		*target = *config
		target.NoRetry = slices.Clone(config.NoRetry)
		if target.Priority == "" {
			target.Priority = current.Priority
		}
		if target.FairKey == "" {
			target.FairKey = current.FairKey
		}
		target.LogAttributes = make(map[string]any, len(current.LogAttributes)+len(config.LogAttributes))
		maps.Copy(target.LogAttributes, current.LogAttributes)
		maps.Copy(target.LogAttributes, config.LogAttributes)
	}
	return flowOption(apply)
}

// WithID sets the flow ID. Without it one is generated, which StartFlow returns. While a
// flow with this ID is running, a client's RunFlow or StartFlow joins it instead of starting
// another and ignores this run's input, so concurrent callers can share one run. Use
// WithErrorIfRunning to be told instead. A child flow always gets an error, because a flow
// cannot wait for a flow it did not start; check it with IsFlowRunningError.
func WithID(flowID string) FlowOption {
	apply := func(config *FlowConfig) {
		config.ID = flowID
	}
	return flowOption(apply)
}

// WithErrorIfRunning makes a client's RunFlow or StartFlow return an error, which
// IsFlowRunningError recognizes, rather than join a flow already running with this ID and
// ignore this run's input.
func WithErrorIfRunning() FlowOption {
	apply := func(config *FlowConfig) {
		config.ErrorIfRunning = true
	}
	return flowOption(apply)
}

// WithTimeout limits the whole flow execution, including every retry.
func WithTimeout(timeout time.Duration) FlowOption {
	apply := func(config *FlowConfig) {
		config.Timeout = timeout
	}
	return flowOption(apply)
}
