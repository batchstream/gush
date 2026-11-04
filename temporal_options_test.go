//lint:file-ignore SA1019 These regression tests verify the Assignment Rules routing that stops tasks being pinned to a retired Build ID.
package gush

import (
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
)

func TestDefaultCommandVersioningIntent(t *testing.T) {
	intent := defaultCommandVersioningIntent()

	if intent != temporal.VersioningIntentUseAssignmentRules {
		t.Fatalf("expected default command versioning intent %v, got %v", temporal.VersioningIntentUseAssignmentRules, intent)
	}
}

func TestNewActivityOptionsUsesAssignmentRules(t *testing.T) {
	stepConfig := DefaultStepConfig()
	stepConfig.Queue = "target-queue"
	stepConfig.HeartbeatTimeout = 30 * time.Second

	activityOptions := newActivityOptions(stepConfig)

	if activityOptions.VersioningIntent != temporal.VersioningIntentUseAssignmentRules {
		t.Fatalf("expected activity versioning intent %v, got %v", temporal.VersioningIntentUseAssignmentRules, activityOptions.VersioningIntent)
	}
	if activityOptions.HeartbeatTimeout != stepConfig.HeartbeatTimeout {
		t.Fatalf("expected activity heartbeat timeout %v, got %v", stepConfig.HeartbeatTimeout, activityOptions.HeartbeatTimeout)
	}
}

func TestNewChildWorkflowOptionsUsesAssignmentRules(t *testing.T) {
	flowConfig := DefaultFlowConfig()

	childWorkflowOptions := newChildWorkflowOptions("target-queue", flowConfig)

	if childWorkflowOptions.VersioningIntent != temporal.VersioningIntentUseAssignmentRules {
		t.Fatalf("expected child workflow versioning intent %v, got %v", temporal.VersioningIntentUseAssignmentRules, childWorkflowOptions.VersioningIntent)
	}
}

func TestRetryPolicyReachesTemporalOnEveryPath(t *testing.T) {
	policy := RetryPolicy{
		InitialInterval:    2 * time.Second,
		BackoffCoefficient: 3,
		MaxInterval:        time.Minute,
		MaxAttempts:        4,
		NoRetry:            []string{"Declined"},
	}

	flowConfig := DefaultFlowConfig()
	flowConfig.RetryPolicy = policy
	stepConfig := DefaultStepConfig()
	stepConfig.RetryPolicy = policy

	policies := map[string]*temporal.RetryPolicy{
		"flow start": newStartWorkflowOptions("queue", flowConfig).RetryPolicy,
		"child flow": newChildWorkflowOptions("queue", flowConfig).RetryPolicy,
		"step":       newActivityOptions(stepConfig).RetryPolicy,
	}

	for path, got := range policies {
		if got.InitialInterval != policy.InitialInterval || got.BackoffCoefficient != policy.BackoffCoefficient ||
			got.MaximumInterval != policy.MaxInterval || got.MaximumAttempts != policy.MaxAttempts {
			t.Errorf("%s: retry settings = %+v, want %+v", path, got, policy)
		}
		if len(got.NonRetryableErrorTypes) != 1 || got.NonRetryableErrorTypes[0] != "Declined" {
			t.Errorf("%s: non-retryable types = %v", path, got.NonRetryableErrorTypes)
		}
	}
}

func TestPickupTimeoutCoversEveryAttempt(t *testing.T) {
	config := DefaultStepConfig()
	config.PickupTimeout = time.Minute
	config.AttemptTimeout = time.Minute
	config.MaxAttempts = 3

	options := newActivityOptions(config)

	if options.ScheduleToStartTimeout != time.Minute {
		t.Errorf("schedule to start = %v", options.ScheduleToStartTimeout)
	}
	if options.ScheduleToCloseTimeout != 8*time.Minute {
		t.Errorf("schedule to close = %v", options.ScheduleToCloseTimeout)
	}
}

func TestPickupTimeoutWithUnlimitedAttemptsHasNoOverallLimit(t *testing.T) {
	config := DefaultStepConfig()
	config.PickupTimeout = time.Minute
	config.MaxAttempts = 0

	options := newActivityOptions(config)

	if options.ScheduleToCloseTimeout != 0 {
		t.Errorf("schedule to close = %v", options.ScheduleToCloseTimeout)
	}
}

func TestAsyncChildFlowOutlivesItsParent(t *testing.T) {
	config := DefaultFlowConfig()

	if newChildWorkflowOptions("queue", config).ParentClosePolicy != enums.PARENT_CLOSE_POLICY_UNSPECIFIED {
		t.Errorf("a synchronous child must keep the default parent close policy")
	}

	config.Async = true
	if newChildWorkflowOptions("queue", config).ParentClosePolicy != enums.PARENT_CLOSE_POLICY_ABANDON {
		t.Errorf("an async child must be abandoned when its parent closes")
	}
}
