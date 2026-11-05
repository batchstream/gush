package gush

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

func TestUnwrapError(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		got := unwrapError(nil)
		if got != nil {
			t.Errorf("unwrapError(nil) = %v, want nil", got)
		}
	})

	t.Run("plain error returns itself", func(t *testing.T) {
		err := errors.New("simple error")
		got := unwrapError(err)
		if got.Error() != "simple error" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "simple error")
		}
	})

	t.Run("wrapped error returns root cause", func(t *testing.T) {
		root := errors.New("root cause")
		wrapped := fmt.Errorf("activity error (type: RunJob, scheduledEventID: 5): %w", root)
		got := unwrapError(wrapped)
		if got.Error() != "root cause" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "root cause")
		}
	})

	t.Run("deeply wrapped error returns root cause", func(t *testing.T) {
		root := errors.New("actual program error")
		mid := fmt.Errorf("exit status 1: %w", root)
		outer := fmt.Errorf("activity error (type: RunJob, scheduledEventID: 5, startedEventID: 6): %w", mid)
		got := unwrapError(outer)
		if got.Error() != "actual program error" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "actual program error")
		}
	})

	t.Run("ApplicationError includes message and type", func(t *testing.T) {
		appErr := NewApplicationError("db connection failed", "DBError")
		got := unwrapError(appErr)
		want := "[DBError] db connection failed"
		if got.Error() != want {
			t.Errorf("unwrapError = %q, want %q", got.Error(), want)
		}
	})

	t.Run("wrapped ApplicationError includes message and type", func(t *testing.T) {
		appErr := NewApplicationError("db connection failed", "DBError")
		wrapped := fmt.Errorf("activity error: %w", appErr)
		got := unwrapError(wrapped)
		want := "[DBError] db connection failed"
		if got.Error() != want {
			t.Errorf("unwrapError = %q, want %q", got.Error(), want)
		}
	})

	t.Run("NonRetryableApplicationError includes message and type", func(t *testing.T) {
		cause := errors.New("connection refused")
		appErr := NewPermanentError("db connection failed", "DBError", cause)
		wrapped := fmt.Errorf("activity error (type: RunJob, scheduledEventID: 5): %w", appErr)
		got := unwrapError(wrapped)
		want := "[DBError] db connection failed"
		if got.Error() != want {
			t.Errorf("unwrapError = %q, want %q", got.Error(), want)
		}
	})

	t.Run("ApplicationError with empty type omits type suffix", func(t *testing.T) {
		appErr := NewApplicationError("something broke", "")
		got := unwrapError(appErr)
		if got.Error() != "something broke" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "something broke")
		}
	})
}

func TestUnwrapErrorKeepsApplicationError(t *testing.T) {
	t.Run("non-retryable error stays detectable", func(t *testing.T) {
		cause := errors.New("boom")
		appErr := NewPermanentError("boom", "StepAborted", cause)
		wrapped := fmt.Errorf("activity error (type: RunJob, scheduledEventID: 5): %w", appErr)
		got := unwrapError(wrapped)
		if got.Error() != "[StepAborted] boom" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "[StepAborted] boom")
		}
		if !IsApplicationError(got) {
			t.Error("IsApplicationError = false, want true")
		}
		if !IsPermanentError(got) {
			t.Error("IsPermanentError = false, want true")
		}
		var target *ApplicationError
		if !errors.As(got, &target) || target.Type() != "StepAborted" {
			t.Errorf("errors.As found %v, want type StepAborted", target)
		}
		if !errors.Is(got, cause) {
			t.Error("errors.Is(cause) = false, want true")
		}
	})

	t.Run("retryable error stays retryable", func(t *testing.T) {
		appErr := NewApplicationError("db connection failed", "DBError")
		got := unwrapError(appErr)
		if !IsApplicationError(got) {
			t.Error("IsApplicationError = false, want true")
		}
		if IsPermanentError(got) {
			t.Error("IsPermanentError = true, want false")
		}
	})

	t.Run("unwrapping twice keeps the same message", func(t *testing.T) {
		appErr := NewApplicationError("db connection failed", "DBError")
		got := unwrapError(unwrapError(appErr))
		if got.Error() != "[DBError] db connection failed" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "[DBError] db connection failed")
		}
	})
}

func TestTemporalError(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		if got := temporalError(nil); got != nil {
			t.Errorf("temporalError(nil) = %v, want nil", got)
		}
	})

	t.Run("unwrapped error returns its ApplicationError", func(t *testing.T) {
		appErr := NewPermanentError("boom", "StepAborted", nil)
		got := temporalError(unwrapError(appErr))
		if got != error(appErr) {
			t.Errorf("temporalError = %#v, want the original ApplicationError", got)
		}
	})

	t.Run("other errors are unchanged", func(t *testing.T) {
		plain := errors.New("plain")
		if got := temporalError(plain); got != plain {
			t.Errorf("temporalError = %v, want %v", got, plain)
		}
	})
}

func TestTemporalErrorKeepsAWrappedErrorsTypeAndRetryability(t *testing.T) {
	permanent := NewPermanentError("card declined", "Declined", nil)
	retryable := NewApplicationError("try later", "Busy")

	tests := []struct {
		name         string
		err          error
		message      string
		errType      string
		nonRetryable bool
	}{
		{
			name:         "wrapped permanent error",
			err:          fmt.Errorf("charge order: %w", permanent),
			message:      "charge order: card declined",
			errType:      "Declined",
			nonRetryable: true,
		},
		{
			name:         "wrapped error from another step",
			err:          fmt.Errorf("fulfill: %w", unwrapError(permanent)),
			message:      "fulfill: card declined",
			errType:      "Declined",
			nonRetryable: true,
		},
		{
			name:    "wrapped retryable error",
			err:     fmt.Errorf("charge order: %w", retryable),
			message: "charge order: try later",
			errType: "Busy",
		},
		{
			name:         "wrapped twice",
			err:          fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", permanent)),
			message:      "outer: inner: card declined",
			errType:      "Declined",
			nonRetryable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := temporalError(tt.err)

			applicationError, ok := result.(*ApplicationError)
			if !ok {
				t.Fatalf("temporalError() = %T, want *ApplicationError", result)
			}
			if applicationError.Message() != tt.message {
				t.Errorf("message = %q, want %q", applicationError.Message(), tt.message)
			}
			if applicationError.Type() != tt.errType {
				t.Errorf("type = %q, want %q", applicationError.Type(), tt.errType)
			}
			if applicationError.NonRetryable() != tt.nonRetryable {
				t.Errorf("non-retryable = %v, want %v", applicationError.NonRetryable(), tt.nonRetryable)
			}
		})
	}
}

func TestTemporalErrorLeavesOtherErrorsAlone(t *testing.T) {
	plain := errors.New("plain")
	if temporalError(plain) != plain {
		t.Error("a plain error changed")
	}

	wrappedPlain := fmt.Errorf("outer: %w", plain)
	if temporalError(wrappedPlain) != wrappedPlain {
		t.Error("a wrapped plain error changed")
	}

	var application error = NewApplicationError("boom", "Declined")
	if temporalError(application) != application {
		t.Error("an ApplicationError changed")
	}
}

func TestWrappedErrorKeepsItsDetailsOnTheCause(t *testing.T) {
	permanent := NewPermanentError("m", "T", nil, "the-detail")

	result := temporalError(fmt.Errorf("wrap: %w", permanent))

	cause, ok := errors.Unwrap(result).(*ApplicationError)
	if !ok {
		t.Fatalf("cause = %T", errors.Unwrap(result))
	}
	var detail string
	if err := cause.Details(&detail); err != nil || detail != "the-detail" {
		t.Errorf("detail = %q, error = %v", detail, err)
	}
}

func TestAbortKeepsTheMessageReadable(t *testing.T) {
	declined := NewApplicationError("declined", "Declined")
	rejected := NewApplicationError("rejected", "Rejected")
	tests := map[string]struct {
		cause    error
		expected string
	}{
		"plain error":                 {cause: errors.New("bad input"), expected: "bad input"},
		"application error":           {cause: declined, expected: "declined"},
		"wrapped application error":   {cause: fmt.Errorf("charge: %w", declined), expected: "charge: declined"},
		"error returned from RunStep": {cause: unwrapError(declined), expected: "[Declined] declined"},
		"application error with a cause": {
			cause:    NewPermanentError("gateway rejected", "Declined", errors.New("card expired")),
			expected: "gateway rejected: card expired",
		},
		"two wrapped errors": {cause: fmt.Errorf("%w and %w", declined, rejected), expected: "declined and rejected"},
		"joined errors":      {cause: errors.Join(declined, rejected), expected: "declined\nrejected"},
		"timeout":            {cause: temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil), expected: "Test timeout"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			causes := []error{test.cause}
			aborted := abortError("StepAborted", causes)
			if aborted.Message() != test.expected {
				t.Errorf("message = %q, want %q", aborted.Message(), test.expected)
			}
			if !aborted.NonRetryable() {
				t.Errorf("an abort must not be retried")
			}
			if !errors.Is(aborted, test.cause) {
				t.Errorf("the cause is not reachable from %v", aborted)
			}
		})
	}
}

func TestIsFlowRunningErrorIgnoresOtherErrors(t *testing.T) {
	for _, err := range []error{nil, errors.New("other"), NewApplicationError("other", "Other")} {
		if IsFlowRunningError(err) {
			t.Errorf("IsFlowRunningError(%v) = true", err)
		}
	}
}

func TestResumeError(t *testing.T) {
	t.Run("no error stays nil", func(t *testing.T) {
		if err := resumeError(nil); err != nil {
			t.Errorf("resumeError(nil) = %v", err)
		}
	})

	t.Run("a plain error ends the step", func(t *testing.T) {
		got := resumeError(errors.New("rejected"))

		if !IsPermanentError(got) {
			t.Errorf("resumeError() = %v, want a permanent error", got)
		}
		if got.Error() == "" || !strings.Contains(got.Error(), "rejected") {
			t.Errorf("resumeError() lost the message: %v", got)
		}
	})

	t.Run("an application error keeps its retryability", func(t *testing.T) {
		retry := NewApplicationError("ask again", "AskAgain")
		if got := resumeError(retry); !IsApplicationError(got) || IsPermanentError(got) {
			t.Errorf("resumeError(retryable) = %v", got)
		}

		wrapped := fmt.Errorf("reviewer: %w", retry)
		if got := resumeError(wrapped); !IsApplicationError(got) || IsPermanentError(got) {
			t.Errorf("resumeError(wrapped retryable) = %v", got)
		}

		permanent := NewPermanentError("no", "No", nil)
		if got := resumeError(permanent); !IsPermanentError(got) {
			t.Errorf("resumeError(permanent) = %v", got)
		}
	})

	t.Run("a canceled error asks to cancel the step", func(t *testing.T) {
		canceled := []error{temporal.NewCanceledError(), context.Canceled, fmt.Errorf("review: %w", context.Canceled)}
		for _, err := range canceled {
			if !isCancellation(err) {
				t.Errorf("isCancellation(%v) = false", err)
			}
		}
		other := []error{nil, errors.New("rejected"), NewApplicationError("retry", "Retry")}
		for _, err := range other {
			if isCancellation(err) {
				t.Errorf("isCancellation(%v) = true", err)
			}
		}
	})
}

func TestResumeErrorDropsACanceledCause(t *testing.T) {
	permanent := NewPermanentError("rejected", "Rejected", context.Canceled)
	wrapped := fmt.Errorf("review: %w", permanent)
	options := temporal.ApplicationErrorOptions{Cause: context.Canceled}
	retryable := temporal.NewApplicationErrorWithOptions("retry", "Retry", options)
	tests := map[string]struct {
		err       error
		message   string
		permanent bool
	}{
		"permanent": {permanent, "rejected", true},
		"wrapped":   {wrapped, "review: rejected", true},
		"retryable": {retryable, "retry", false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := resumeError(test.err)

			if errors.Is(got, context.Canceled) || temporal.IsCanceledError(got) {
				t.Errorf("resumeError() = %v still reads as canceled", got)
			}
			var appErr *ApplicationError
			if !errors.As(got, &appErr) {
				t.Fatalf("resumeError() = %v", got)
			}
			if appErr.Message() != test.message || appErr.NonRetryable() != test.permanent {
				t.Errorf("message = %q, permanent = %v", appErr.Message(), appErr.NonRetryable())
			}
		})
	}
}

func TestResumeErrorKeepsAnotherCause(t *testing.T) {
	cause := errors.New("reviewer unavailable")
	permanent := NewPermanentError("rejected", "Rejected", cause)

	if got := resumeError(permanent); !errors.Is(got, cause) {
		t.Errorf("resumeError() = %v lost its cause", got)
	}
}

func TestIsFlowRunningError(t *testing.T) {
	started := serviceerror.NewWorkflowExecutionAlreadyStarted("running", "", "")
	if !IsFlowRunningError(started) {
		t.Errorf("IsFlowRunningError(%v) = false", started)
	}
	wrapped := fmt.Errorf("start: %w", started)
	if !IsFlowRunningError(wrapped) {
		t.Errorf("IsFlowRunningError(%v) = false", wrapped)
	}

	// What a flow sees after the error crosses a step, or when it comes from a flow that ran.
	acrossAStep := NewApplicationError("running", "WorkflowExecutionAlreadyStarted")
	if IsFlowRunningError(acrossAStep) {
		t.Errorf("IsFlowRunningError(%v) = true", acrossAStep)
	}
	if IsFlowRunningError(errors.New("plain")) {
		t.Error("IsFlowRunningError(plain) = true")
	}
}

func TestIsInvalidConfigError(t *testing.T) {
	config := DefaultFlowConfig()
	config.Timeout = -1
	client := &Client{}
	request := FlowRequest{Flow: "AnyFlow"}
	_, invalid := client.StartFlow(context.Background(), request, WithConfig(config))
	rejected := serviceerror.NewInvalidArgument("the server said no")

	recognized := []error{invalid, fmt.Errorf("start: %w", invalid), rejected, fmt.Errorf("start: %w", rejected)}
	for _, err := range recognized {
		if !IsInvalidConfigError(err) {
			t.Errorf("IsInvalidConfigError(%v) = false", err)
		}
	}

	others := []error{nil, errors.New("some other failure"), serviceerror.NewUnavailable("try later")}
	for _, err := range others {
		if IsInvalidConfigError(err) {
			t.Errorf("IsInvalidConfigError(%v) = true", err)
		}
	}
}
