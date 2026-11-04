package gush

import (
	"maps"
	"time"
)

// CommonConfig holds the settings flows and steps share.
type CommonConfig struct {
	RetryPolicy

	// Priority controls scheduling order; higher priority runs first.
	Priority Priority `json:"priority"`

	// FairKey groups work for fair scheduling across tenants or user groups. Keys are
	// cut to the 64 bytes Temporal accepts, so keys that agree up to there share a group.
	FairKey string `json:"fair_key"`

	// LogAttributes are attached to every log record and inherited by child flows and steps.
	LogAttributes LogAttributes `json:"log_attributes"`
}

// FlowOption customizes a flow run.
type FlowOption interface {
	applyFlow(config *FlowConfig)
}

// StepOption customizes a step run.
type StepOption interface {
	applyStep(config *StepConfig)
}

// Option customizes a flow run or a step run.
type Option interface {
	FlowOption
	StepOption
}

type commonOption func(config *CommonConfig)

func (option commonOption) applyFlow(config *FlowConfig) {
	option(&config.CommonConfig)
}

func (option commonOption) applyStep(config *StepConfig) {
	option(&config.CommonConfig)
}

// WithRetryPolicy replaces the whole retry policy.
func WithRetryPolicy(policy RetryPolicy) Option {
	apply := func(config *CommonConfig) {
		config.RetryPolicy = policy
	}
	return commonOption(apply)
}

// WithInitialInterval sets the delay before the first retry.
func WithInitialInterval(interval time.Duration) Option {
	apply := func(config *CommonConfig) {
		config.InitialInterval = interval
	}
	return commonOption(apply)
}

// WithBackoffCoefficient sets the multiplier applied to the delay after each retry. It
// must be at least 1.
func WithBackoffCoefficient(coefficient float64) Option {
	apply := func(config *CommonConfig) {
		config.BackoffCoefficient = coefficient
	}
	return commonOption(apply)
}

// WithMaxInterval caps the delay between retries.
func WithMaxInterval(interval time.Duration) Option {
	apply := func(config *CommonConfig) {
		config.MaxInterval = interval
	}
	return commonOption(apply)
}

// WithMaxAttempts sets the total number of attempts, including the first. Zero means one for
// a flow and ten for a step. Use UnlimitedAttempts to keep retrying.
func WithMaxAttempts(attempts int32) Option {
	apply := func(config *CommonConfig) {
		config.MaxAttempts = attempts
	}
	return commonOption(apply)
}

// WithNoRetry adds ApplicationError types that fail immediately instead of retrying.
func WithNoRetry(errorTypes ...string) Option {
	apply := func(config *CommonConfig) {
		config.NoRetry = append(config.NoRetry, errorTypes...)
	}
	return commonOption(apply)
}

// WithNoRetryErrors adds the types of the given errors to the ones that fail immediately.
func WithNoRetryErrors(errs ...*ApplicationError) Option {
	apply := func(config *CommonConfig) {
		for _, err := range errs {
			if err == nil || err.Type() == "" {
				continue
			}
			config.NoRetry = append(config.NoRetry, err.Type())
		}
	}
	return commonOption(apply)
}

// WithPriority sets the scheduling priority.
func WithPriority(priority Priority) Option {
	apply := func(config *CommonConfig) {
		config.Priority = priority
	}
	return commonOption(apply)
}

// WithFairKey sets the group this work is scheduled fairly against, such as a tenant ID.
// Keys are cut to the 64 bytes Temporal accepts, so keys that agree up to there share a group.
func WithFairKey(key string) Option {
	apply := func(config *CommonConfig) {
		config.FairKey = key
	}
	return commonOption(apply)
}

// WithLogAttribute adds one attribute to every log record of this run and its children.
func WithLogAttribute(key string, value any) Option {
	apply := func(config *CommonConfig) {
		if config.LogAttributes == nil {
			config.LogAttributes = make(map[string]any)
		}
		config.LogAttributes[key] = value
	}
	return commonOption(apply)
}

// WithLogAttributes merges attributes into every log record of this run and its children.
func WithLogAttributes(attributes map[string]any) Option {
	apply := func(config *CommonConfig) {
		if config.LogAttributes == nil {
			config.LogAttributes = make(map[string]any)
		}
		maps.Copy(config.LogAttributes, attributes)
	}
	return commonOption(apply)
}
