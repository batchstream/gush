package gush

import (
	"errors"

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

// temporalError returns the error a flow or step hands back to Temporal. An error that
// came straight from unwrapError is swapped for its ApplicationError, so the recorded
// failure keeps the type and retryability instead of becoming an untyped, retryable
// error. Wrapped errors are left alone to keep the caller's message.
func temporalError(err error) error {
	programErr, ok := err.(*programError)
	if ok {
		return programErr.appErr
	}
	return err
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

// abortError is the permanent error behind Abort on flows and steps.
func abortError(errType string, cause []error) *ApplicationError {
	if len(cause) == 0 || cause[0] == nil {
		return NewPermanentError("unknown error", errType, nil)
	}
	return NewPermanentError(cause[0].Error(), errType, cause[0])
}
