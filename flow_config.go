package gush

import (
	"maps"
	"slices"
	"time"
)

// FlowConfig holds the settings of one flow run. Its JSON form is stable, so a config can
// be queued and applied later with WithConfig.
type FlowConfig struct {
	CommonConfig

	// ID is the flow ID. If empty, the server generates one.
	ID string `json:"id"`

	// Timeout limits the whole execution, including every retry. Zero means unlimited.
	Timeout time.Duration `json:"timeout"`

	// Cron is a schedule in UTC, such as "0 2 * * *" for daily at 2 AM. A run starts only
	// after the previous one ends, and the schedule continues until canceled or terminated.
	Cron string `json:"cron"`

	// Async makes RunFlow return as soon as the flow has started.
	Async bool `json:"async"`
}

// DefaultFlowConfig returns the settings a flow run starts from: one attempt, medium
// priority, and a 1s to 100s backoff if more attempts are allowed.
func DefaultFlowConfig() *FlowConfig {
	config := &FlowConfig{}
	config.InitialInterval = 1 * time.Second
	config.BackoffCoefficient = 2.0
	config.MaxInterval = 100 * time.Second
	config.MaxAttempts = 1
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
// after it refine config. A nil config changes nothing.
func WithConfig(config *FlowConfig) FlowOption {
	apply := func(target *FlowConfig) {
		if config == nil {
			return
		}
		*target = *config
		target.NoRetry = slices.Clone(config.NoRetry)
		target.LogAttributes = make(map[string]any, len(config.LogAttributes))
		maps.Copy(target.LogAttributes, config.LogAttributes)
	}
	return flowOption(apply)
}

// WithID sets the flow ID instead of letting the server generate one.
func WithID(flowID string) FlowOption {
	apply := func(config *FlowConfig) {
		config.ID = flowID
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

// WithCron runs the flow on a schedule in UTC, such as "0 2 * * *" for daily at 2 AM.
func WithCron(cron string) FlowOption {
	apply := func(config *FlowConfig) {
		config.Cron = cron
	}
	return flowOption(apply)
}

// WithAsync makes RunFlow return as soon as the flow has started. A child flow started
// this way also outlives its parent.
func WithAsync() FlowOption {
	apply := func(config *FlowConfig) {
		config.Async = true
	}
	return flowOption(apply)
}
