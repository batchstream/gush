package gush

import (
	"errors"
	"fmt"
	"testing"
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
		appErr := NewPermanentError("boom", "ActionAborted", cause)
		wrapped := fmt.Errorf("activity error (type: RunJob, scheduledEventID: 5): %w", appErr)
		got := unwrapError(wrapped)
		if got.Error() != "[ActionAborted] boom" {
			t.Errorf("unwrapError = %q, want %q", got.Error(), "[ActionAborted] boom")
		}
		if !IsApplicationError(got) {
			t.Error("IsApplicationError = false, want true")
		}
		if !IsPermanentError(got) {
			t.Error("IsPermanentError = false, want true")
		}
		var target *ApplicationError
		if !errors.As(got, &target) || target.Type() != "ActionAborted" {
			t.Errorf("errors.As found %v, want type ActionAborted", target)
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
		appErr := NewPermanentError("boom", "ActionAborted", nil)
		got := temporalError(unwrapError(appErr))
		if got != error(appErr) {
			t.Errorf("temporalError = %#v, want the original ApplicationError", got)
		}
	})

	t.Run("wrapped unwrapped error is left alone", func(t *testing.T) {
		appErr := NewPermanentError("boom", "ActionAborted", nil)
		wrapped := fmt.Errorf("prepare: %w", unwrapError(appErr))
		got := temporalError(wrapped)
		if got != wrapped {
			t.Errorf("temporalError = %v, want the wrapped error unchanged", got)
		}
	})

	t.Run("other errors are unchanged", func(t *testing.T) {
		plain := errors.New("plain")
		if got := temporalError(plain); got != plain {
			t.Errorf("temporalError = %v, want %v", got, plain)
		}
	})
}
