package gush

import (
	"errors"
	"regexp"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

// ApplicationError is an error with a type name that retry policies can match.
type ApplicationError = temporal.ApplicationError

// PanicError is an unrecovered panic during a step.
type PanicError = temporal.PanicError

// NewApplicationError creates a retryable error. Pass errType to WithNoRetry to stop
// retrying errors of this type on a particular run.
func NewApplicationError(message, errType string, details ...any) *ApplicationError {
	return temporal.NewApplicationError(message, errType, details...).(*ApplicationError)
}

// NewPermanentError creates an error that fails its flow or step immediately, whatever
// the retry policy says.
func NewPermanentError(message, errType string, cause error, details ...any) *ApplicationError {
	return temporal.NewNonRetryableApplicationError(message, errType, cause, details...).(*ApplicationError)
}

// IsApplicationError reports whether err is an ApplicationError.
func IsApplicationError(err error) bool {
	return temporal.IsApplicationError(err)
}

// IsPermanentError reports whether err is an ApplicationError that is not retried.
func IsPermanentError(err error) bool {
	var applicationError *ApplicationError
	if errors.As(err, &applicationError) {
		return applicationError.NonRetryable()
	}
	return false
}

// IsFlowRunningError reports whether a flow did not start because one with the same ID is
// running. A child flow gets this error, and so does a client using WithErrorIfRunning. A
// step using such a client must check the error itself, because a flow cannot recognize it
// after it crosses the step.
func IsFlowRunningError(err error) bool {
	return temporal.IsWorkflowExecutionAlreadyStartedError(err)
}

// IsInvalidConfigError reports whether a request was refused as invalid, so sending it again
// cannot succeed: Gush found settings Temporal refuses, including in a flow that has since
// failed on them, or the server answered InvalidArgument. The server gives that answer for
// more than settings, such as an input over its size limit. Past a flow it sees through Abort,
// NewPermanentError and %w, but not errors.Join or several %w, and not a client's refusal inside
// a step, whose text is all that crosses the step.
func IsInvalidConfigError(err error) bool {
	if errors.Is(err, errInvalidConfig) {
		return true
	}
	var rejected *serviceerror.InvalidArgument
	if errors.As(err, &rejected) {
		return true
	}

	// A flow can wrap the failure in an error of its own before returning it, so look at every
	// ApplicationError in the chain rather than the outermost.
	var applicationError *ApplicationError
	for errors.As(err, &applicationError) {
		if applicationError.Type() == invalidConfigErrorType && applicationError.NonRetryable() {
			return true
		}
		err = applicationError.Unwrap()
	}
	return false
}

// IsPanicError reports whether err is a PanicError.
func IsPanicError(err error) bool {
	return temporal.IsPanicError(err)
}

// programError carries the short "[type] message" text of an ApplicationError while
// keeping the ApplicationError itself reachable through Unwrap, so errors.As,
// IsApplicationError and IsPermanentError still see its type, retryability and details.
type programError struct {
	message string
	appErr  *ApplicationError
}

func (e *programError) Error() string {
	return e.message
}

func (e *programError) Unwrap() error {
	return e.appErr
}

// temporalError returns the error a flow or step hands back to Temporal, which only reads
// the type and retryability of an ApplicationError it is given directly. Anything else
// would be recorded as an untyped, retryable error, so an ApplicationError found inside a
// wrapped error is passed on as one that keeps the wrapper's message. Its details stay on
// the wrapped error, which is that one's cause.
func temporalError(err error) error {
	programErr, ok := err.(*programError)
	if ok {
		return programErr.appErr
	}

	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		return err
	}
	switch err.(type) {
	case *ApplicationError, *temporal.ActivityError, *temporal.ChildWorkflowExecutionError:
		return err
	}

	options := temporal.ApplicationErrorOptions{
		NonRetryable:   appErr.NonRetryable(),
		Cause:          appErr,
		NextRetryDelay: appErr.NextRetryDelay(),
		Category:       appErr.Category(),
	}
	return temporal.NewApplicationErrorWithOptions(wrappedMessage(err, appErr), appErr.Type(), options)
}

// wrappedMessage returns err's text with the wrapped error reduced to its plain message,
// because the type is recorded separately and would otherwise appear twice.
func wrappedMessage(err error, appErr *ApplicationError) string {
	message := strings.Replace(err.Error(), appErr.Error(), appErr.Message(), 1)

	var programErr *programError
	if errors.As(err, &programErr) {
		message = strings.Replace(message, programErr.message, appErr.Message(), 1)
	}
	return message
}

// unwrapError extracts the program's own error from Temporal's error chain, which
// otherwise reads "activity error (type: ..., scheduledEventID: ...): message (type: ...,
// retryable: ...)". An ApplicationError stays reachable through errors.As.
func unwrapError(err error) error {
	if err == nil {
		return nil
	}

	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		msg := appErr.Message()
		errType := appErr.Type()
		if errType != "" {
			msg = "[" + errType + "] " + msg
		}
		programErr := &programError{message: msg, appErr: appErr}
		return programErr
	}

	current := err
	for {
		unwrapped := errors.Unwrap(current)
		if unwrapped == nil {
			return current
		}
		current = unwrapped
	}
}

// temporalErrorType matches what the SDK's errors add to their text, such as
// "(type: Declined, retryable: true)".
var temporalErrorType = regexp.MustCompile(` \(type: [^()]*\)`)

// abortError is the permanent error behind Abort on flows and steps. Its message is the
// cause's text, which keeps what the cause wraps, without the SDK's additions.
func abortError(errType string, cause []error) *ApplicationError {
	if len(cause) == 0 || cause[0] == nil {
		return NewPermanentError("unknown error", errType, nil)
	}
	message := temporalErrorType.ReplaceAllString(cause[0].Error(), "")
	return NewPermanentError(message, errType, cause[0])
}
