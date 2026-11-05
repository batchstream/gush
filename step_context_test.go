package gush_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/gush"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
)

func TestNewStepContextOutsideARunningStep(t *testing.T) {
	stepCtx := gush.NewStepContext(context.Background(), nil)

	assert.Empty(t, stepCtx.ID)
	assert.Empty(t, stepCtx.FlowID)
	assert.NotPanics(t, func() { stepCtx.Println("logs without a running step") })
	assert.NotPanics(t, func() { stepCtx.Heartbeat("details") })
}

func TestAsStepContext(t *testing.T) {
	stepCtx := gush.NewStepContext(context.Background(), nil)

	assert.Same(t, stepCtx, gush.AsStepContext(stepCtx))
	assert.NotNil(t, gush.AsStepContext(context.Background()))
}

func TestStepContextDecode(t *testing.T) {
	stepCtx := gush.NewStepContext(context.Background(), nil)
	stepCtx.Input = []byte(`{"a":1}`)

	var decoded map[string]int
	require.NoError(t, stepCtx.Decode(&decoded))
	assert.Equal(t, 1, decoded["a"])

	stepCtx.Input = []byte(`not json`)
	err := stepCtx.Decode(&decoded)
	assert.True(t, gush.IsPermanentError(err))
}

func TestStepContextAbort(t *testing.T) {
	stepCtx := gush.NewStepContext(context.Background(), nil)
	cause := errors.New("bad input")

	err := stepCtx.Abort(cause)

	assert.True(t, gush.IsPermanentError(err))
	assert.ErrorIs(t, err, cause)
	assert.True(t, gush.IsPermanentError(stepCtx.Abort()))
}

func TestStepContextSuspendReturnsThePendingSignal(t *testing.T) {
	stepCtx := gush.NewStepContext(context.Background(), nil)

	output, err := stepCtx.Suspend()

	assert.Nil(t, output)
	assert.ErrorIs(t, err, activity.ErrResultPending)
}

func TestAsStepContextFindsTheStepBehindADerivedContext(t *testing.T) {
	attributes := map[string]any{"tenant": "acme"}
	stepCtx := gush.NewStepContext(context.Background(), attributes)
	stepCtx.Input = []byte("input")

	derived, cancel := context.WithTimeout(stepCtx, time.Hour)
	defer cancel()

	found := gush.AsStepContext(derived)

	assert.Equal(t, attributes, found.LogAttributes)
	assert.Equal(t, stepCtx.Input, found.Input)
	assert.Same(t, stepCtx.Logger, found.Logger)

	_, hasDeadline := found.Deadline()
	assert.True(t, hasDeadline, "the derived context's deadline was lost")
	_, originalHasDeadline := stepCtx.Deadline()
	assert.False(t, originalHasDeadline, "the original step context changed")
}

func TestAsStepContextWrapsAPlainContext(t *testing.T) {
	found := gush.AsStepContext(context.Background())

	assert.Nil(t, found.LogAttributes)
	assert.NotNil(t, found.Logger)
}

func TestMalformedResumeTokenIsAResumeTokenError(t *testing.T) {
	client := &gush.Client{}

	err := client.Resume(context.Background(), "!!!not-base64!!!", nil, nil)

	require.Error(t, err)
	assert.True(t, gush.IsResumeTokenError(err), "%v", err)
}

func TestStaleResumeTokenIsAlsoAResumeTokenError(t *testing.T) {
	err := fmt.Errorf("resume: %w", serviceerror.NewNotFound("activity task not found"))

	assert.True(t, gush.IsStaleResumeTokenError(err), "%v", err)
	assert.True(t, gush.IsResumeTokenError(err), "%v", err)
}

func TestBadResumeTokenIsNotStale(t *testing.T) {
	client := &gush.Client{}
	malformed := client.Resume(context.Background(), "!!!not-base64!!!", nil, nil)
	rejected := serviceerror.NewInvalidArgument("Error deserializing task token.")
	noNamespace := serviceerror.NewNamespaceNotFound("namespace")

	for _, err := range []error{malformed, rejected, noNamespace} {
		assert.True(t, gush.IsResumeTokenError(err), "%v", err)
		assert.False(t, gush.IsStaleResumeTokenError(err), "%v", err)
	}
}
