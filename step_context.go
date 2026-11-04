package gush

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// abortedStepErrorType is the ApplicationError type of a step's Abort.
const abortedStepErrorType = "StepAborted"

// StepContext is what a step receives. It is a context.Context, so pass it to anything
// that takes one, and the step's cancellation and deadline follow.
//
//	func ProcessOrder(ctx *gush.StepContext) (any, error) {
//	    var order Order
//	    err := ctx.Decode(&order)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return processOrder(ctx, order)
//	}
type StepContext struct {
	context.Context
	StepInfo
	*Logger

	// Input is the raw input; Decode deserializes it.
	Input []byte

	// LogAttributes are inherited from the parent flow.
	LogAttributes map[string]any
}

// NewStepContext creates a StepContext from any context. Outside a running step, such as
// in a test, its step information is empty.
func NewStepContext(ctx context.Context, logAttributes map[string]any) *StepContext {
	stepCtx := &StepContext{Context: ctx, Logger: NewLogger(), LogAttributes: logAttributes}

	if activity.IsActivity(ctx) {
		info := activity.GetInfo(ctx)
		stepCtx.ID = info.ActivityID
		stepCtx.FlowID = info.WorkflowExecution.ID
		stepCtx.Queue = info.TaskQueue
		stepCtx.Priority = NewPriority(info.Priority.PriorityKey)
		stepCtx.FairKey = info.Priority.FairnessKey
		stepCtx.ResumeToken = encodeResumeToken(info.TaskToken)
		stepCtx.Attempt = info.Attempt
	}

	for key, value := range logAttributes {
		stepCtx.Logger.SetAttributes(Attr(key, value))
	}
	if stepCtx.FlowID != "" {
		stepCtx.Logger.SetAttributes(Attr("flow_id", stepCtx.FlowID))
	}

	return stepCtx
}

// AsStepContext returns ctx as a StepContext, wrapping it if it is not one. It lets helpers
// that take a plain context.Context log with the step's context.
func AsStepContext(ctx context.Context) *StepContext {
	stepCtx, ok := ctx.(*StepContext)
	if ok {
		return stepCtx
	}
	return NewStepContext(ctx, nil)
}

// Suspend ends the attempt without finishing the step, freeing the worker while something
// outside the flow, such as a person or a webhook, decides the result and calls Resume with
// ctx.ResumeToken.
//
// The attempt timeout keeps running while the step waits. If it expires first, the step
// retries and runs again, which by default happens after 2 minutes. So set
// WithAttemptTimeout to the longest acceptable wait, and WithMaxAttempts(1) unless running
// the step's code twice is safe.
//
//	func ApproveExpense(ctx *gush.StepContext) (any, error) {
//	    err := sendApprovalRequest(ctx, ctx.ResumeToken, ctx.Input)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return ctx.Suspend()
//	}
//
//	err := ctx.RunStep(ApproveExpense, expense, &decision,
//	    gush.WithAttemptTimeout(24*time.Hour),
//	    gush.WithMaxAttempts(1),
//	)
func (stepCtx *StepContext) Suspend() (any, error) {
	return nil, activity.ErrResultPending
}

// Heartbeat reports that the step is alive, which WithHeartbeatTimeout requires. Details are
// recorded for the next attempt to read with the SDK's activity.GetHeartbeatDetails. Outside
// a running step it does nothing.
func (stepCtx *StepContext) Heartbeat(details ...any) {
	if activity.IsActivity(stepCtx.Context) {
		activity.RecordHeartbeat(stepCtx.Context, details...)
	}
}

// Decode deserializes the step's Input into target, aborting the step if it cannot.
func (stepCtx *StepContext) Decode(target any) error {
	err := Decode(stepCtx.Input, target)
	if err != nil {
		return stepCtx.Abort(err)
	}
	return nil
}

// Abort returns an error that fails the step permanently, with no further retries.
func (stepCtx *StepContext) Abort(cause ...error) error {
	return abortError(abortedStepErrorType, cause)
}
