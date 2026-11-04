package gush

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
)

// StepTimeoutErrorType is the error type of an attempt that exceeded its timeout. Pass it
// to WithNoRetry to stop retrying timed-out steps.
const StepTimeoutErrorType = "TemporalTimeout:StartToClose"

// StepHandlerFunc is the signature of a step. Temporal limits its encoded output to 2MB,
// so return a reference to anything larger.
type StepHandlerFunc func(ctx *StepContext) (output any, err error)

// StepHandler is a step registered under an explicit name.
type StepHandler struct {
	Name string
	Func StepHandlerFunc
}

// NewStepHandler names a step. Use it when the function's own name is not unique or not
// stable, such as for a closure.
//
//	handler := gush.NewStepHandler("process-data", ProcessData)
//	worker := gush.NewWorker("my-queue").WithStepHandlers(handler)
func NewStepHandler(name string, fn StepHandlerFunc) *StepHandler {
	handler := &StepHandler{Name: name, Func: fn}
	return handler
}

// StepInfo describes a step execution.
type StepInfo struct {
	// ID identifies this step within its flow.
	ID string

	// FlowID is the flow that started this step.
	FlowID string

	// Queue is the task queue this step runs on.
	Queue string

	// Priority is this step's scheduling priority.
	Priority Priority

	// FairKey is this step's fairness group.
	FairKey string

	// ResumeToken is what Resume needs to finish this step after it suspends.
	ResumeToken string

	// Attempt counts from 1.
	Attempt int32
}

// Resume finishes a suspended step with a result or, if stepErr is set, an error. See
// StepContext.Suspend.
func (c *Client) Resume(ctx context.Context, resumeToken string, result any, stepErr error) error {
	token, err := decodeResumeToken(resumeToken)
	if err != nil {
		return err
	}
	payload, err := encodePayload(result)
	if err != nil {
		return err
	}
	return c.temporalClient.CompleteActivity(ctx, token, payload, stepErr)
}

// IsResumeTokenError reports whether Resume failed because its token is invalid or the
// step no longer exists, such as after it timed out or was already resumed.
func IsResumeTokenError(err error) bool {
	var invalidArgument *serviceerror.InvalidArgument
	if errors.As(err, &invalidArgument) {
		return true
	}

	var namespaceNotFound *serviceerror.NamespaceNotFound
	if errors.As(err, &namespaceNotFound) {
		return true
	}

	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}
