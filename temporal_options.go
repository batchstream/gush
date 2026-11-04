package gush

import (
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// defaultCommandVersioningIntent routes new commands by assignment rules, so a step or
// child flow does not inherit the Build ID of a parent whose workers have been retired.
//
//lint:ignore SA1019 Temporal deprecated this intent, but it still prevents tasks being pinned to a retired Build ID while workers run unversioned.
func defaultCommandVersioningIntent() temporal.VersioningIntent {
	//lint:ignore SA1019 See the comment on this function.
	return temporal.VersioningIntentUseAssignmentRules
}

func newTemporalPriority(config *CommonConfig) temporal.Priority {
	priority := temporal.Priority{
		PriorityKey: config.Priority.Int(),
		FairnessKey: truncateFairKey(config.FairKey),
	}
	return priority
}

func newStartWorkflowOptions(queue string, config *FlowConfig) client.StartWorkflowOptions {
	options := client.StartWorkflowOptions{
		ID:                       config.ID,
		TaskQueue:                queue,
		RetryPolicy:              config.temporalPolicy(),
		CronSchedule:             config.Cron,
		Priority:                 newTemporalPriority(&config.CommonConfig),
		WorkflowExecutionTimeout: config.Timeout,
	}
	return options
}

func newChildWorkflowOptions(queue string, config *FlowConfig) workflow.ChildWorkflowOptions {
	options := workflow.ChildWorkflowOptions{
		WorkflowID:               config.ID,
		TaskQueue:                queue,
		RetryPolicy:              config.temporalPolicy(),
		CronSchedule:             config.Cron,
		Priority:                 newTemporalPriority(&config.CommonConfig),
		WorkflowExecutionTimeout: config.Timeout,
		VersioningIntent:         defaultCommandVersioningIntent(),
	}
	if config.Async {
		options.ParentClosePolicy = enums.PARENT_CLOSE_POLICY_ABANDON
	}
	return options
}

func newActivityOptions(config *StepConfig) workflow.ActivityOptions {
	options := workflow.ActivityOptions{
		TaskQueue:           config.Queue,
		StartToCloseTimeout: config.AttemptTimeout,
		HeartbeatTimeout:    config.HeartbeatTimeout,
		RetryPolicy:         config.temporalPolicy(),
		Priority:            newTemporalPriority(&config.CommonConfig),
		VersioningIntent:    defaultCommandVersioningIntent(),
	}

	if config.PickupTimeout > 0 {
		options.ScheduleToStartTimeout = config.PickupTimeout

		// The overall limit stops a task lingering once no worker is left. It must not cut
		// retries short, so it covers every attempt's pickup wait and run plus the delays
		// between them. Unlimited attempts have no such total, so they get no overall limit.
		if config.MaxAttempts > 0 {
			perAttempt := config.PickupTimeout + config.AttemptTimeout
			options.ScheduleToCloseTimeout = time.Duration(config.MaxAttempts)*perAttempt + config.totalBackoff()
		}
	}
	return options
}
