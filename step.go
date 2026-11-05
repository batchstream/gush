package gush

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

// errInvalidResumeToken marks a token that could not be decoded.
var errInvalidResumeToken = errors.New("gush: invalid resume token")

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

	// ResumeToken is what Resume needs to finish this step after it suspends. It belongs to
	// this attempt, and a retry gets a new one.
	ResumeToken string

	// Attempt counts from 1.
	Attempt int32
}

// Resume finishes a suspended step with a result or, if stepErr is set, an error. See
// StepContext.Suspend.
//
// An error fails the step for good, because running its code again would repeat whatever
// it asked for, such as a person's approval. To run the code again under the step's retry
// policy, pass a NewApplicationError, which is a request to retry. A canceled error cancels
// the step if it is being canceled, and otherwise fails it for good too. An ApplicationError
// caused by a cancellation is recorded without that cause or its details.
//
// A result over Temporal's blob size limit, 2MiB by default, fails the step on the server while
// Resume returns nil, because the server accepts the request and then refuses the result. One
// over gRPC's 4MiB message limit makes Resume return an error IsInvalidConfigError recognizes,
// and the step stays suspended with its token still valid: resume it again with a smaller
// result or an error, or it waits until its attempt times out and is retried under its policy.
func (c *Client) Resume(ctx context.Context, resumeToken string, result any, stepErr error) error {
	token, err := decodeResumeToken(resumeToken)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidResumeToken, err)
	}
	payload, err := EncodePayload(result)
	if err != nil {
		return err
	}
	if !isCancellation(stepErr) {
		return c.temporalClient.CompleteActivity(ctx, token, payload, resumeError(stepErr))
	}

	err = c.temporalClient.CompleteActivity(ctx, token, payload, stepErr)
	var rejected *serviceerror.InvalidArgument
	if !errors.As(err, &rejected) {
		return err
	}
	// The server accepts a cancellation only from a step that was asked for one. Only the
	// text goes along, because the SDK would read a wrapped cause as a cancellation again.
	failure := errors.New(stepErr.Error())
	return c.temporalClient.CompleteActivity(ctx, token, payload, resumeError(failure))
}

// isCancellation reports whether an error passed to Resume asks to cancel the step.
func isCancellation(err error) bool {
	if err == nil || temporal.IsApplicationError(err) {
		return false
	}
	return temporal.IsCanceledError(err) || errors.Is(err, context.Canceled)
}

// resumeError returns what Temporal records for an error passed to Resume that does not
// cancel the step. An ApplicationError says for itself whether to retry, and anything else
// ends the step like Abort.
func resumeError(err error) error {
	if err == nil {
		return nil
	}
	if !temporal.IsApplicationError(err) {
		return abortError(abortedStepErrorType, []error{err})
	}
	return withoutCanceledCause(temporalError(err))
}

// withoutCanceledCause drops the cause and details of an ApplicationError caused by a
// cancellation. The SDK would report it as a cancellation, which the server refuses unless
// the step is being canceled, and the error would lose its say over retrying.
func withoutCanceledCause(err error) error {
	if !temporal.IsCanceledError(err) && !errors.Is(err, context.Canceled) {
		return err
	}
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		return err
	}
	options := temporal.ApplicationErrorOptions{
		NonRetryable:   appErr.NonRetryable(),
		NextRetryDelay: appErr.NextRetryDelay(),
		Category:       appErr.Category(),
	}
	return temporal.NewApplicationErrorWithOptions(appErr.Message(), appErr.Type(), options)
}

// IsResumeTokenError reports whether Resume failed because its token is invalid, or stale as
// IsStaleResumeTokenError describes.
func IsResumeTokenError(err error) bool {
	if errors.Is(err, errInvalidResumeToken) {
		return true
	}

	var invalidArgument *serviceerror.InvalidArgument
	if errors.As(err, &invalidArgument) {
		return true
	}

	var namespaceNotFound *serviceerror.NamespaceNotFound
	if errors.As(err, &namespaceNotFound) {
		return true
	}

	return IsStaleResumeTokenError(err)
}

// IsStaleResumeTokenError reports whether Resume failed because its token's attempt is over:
// the step ended, or it is retrying under a new token. That makes it a late callback rather
// than a bad one, though the server cannot tell it from a well-formed token that matches no
// step, such as a corrupted one. Every such error is also an IsResumeTokenError.
func IsStaleResumeTokenError(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}
