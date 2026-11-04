package gush

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"time"
)

// FlowConfig holds the settings of one flow run. Its JSON form is stable, so a config can
// be queued and applied later with WithConfig. A field left out of the JSON keeps its
// default, while a zero written out is taken as it is, so "maximum_attempts": 0 still
// means unlimited.
type FlowConfig struct {
	CommonConfig

	// ID is the flow ID. If empty, the server generates one.
	ID string `json:"id"`

	// Timeout limits the whole execution, including every retry and every scheduled run.
	// Zero means unlimited.
	Timeout time.Duration `json:"timeout"`

	// Cron is a schedule in UTC, such as "0 2 * * *" for daily at 2 AM. A run starts only
	// after the previous one ends, and the schedule continues until canceled, terminated
	// or stopped by Timeout.
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

// UnmarshalJSON starts from DefaultFlowConfig, because an attempt count left out of the JSON
// would otherwise read as zero, which means unlimited retries. Priority stays empty unless
// the JSON sets it, so that WithConfig keeps what a child flow inherits.
func (config *FlowConfig) UnmarshalJSON(data []byte) error {
	type plainFlowConfig FlowConfig
	decoded := plainFlowConfig(*DefaultFlowConfig())
	decoded.Priority = ""

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(&decoded)
	if err != nil {
		return err
	}
	resolveNumbers(decoded.LogAttributes)

	*config = FlowConfig(decoded)
	return nil
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

// WithID sets the flow ID. Set one to refer to the flow later, because a generated ID is
// not returned. While a flow with this ID is running, RunFlow joins it instead of starting
// another, and this run's input is ignored.
func WithID(flowID string) FlowOption {
	apply := func(config *FlowConfig) {
		config.ID = flowID
	}
	return flowOption(apply)
}

// WithTimeout limits the whole flow execution, including every retry and every scheduled
// run of a WithCron schedule, so it also ends the schedule.
func WithTimeout(timeout time.Duration) FlowOption {
	apply := func(config *FlowConfig) {
		config.Timeout = timeout
	}
	return flowOption(apply)
}

// WithCron runs the flow on a schedule in UTC, such as "0 2 * * *" for daily at 2 AM. Add
// WithAsync, because RunFlow otherwise waits across every run of the schedule.
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
