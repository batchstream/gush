package gush_test

import (
	"context"
	"errors"
	"testing"

	"github.com/batchstream/gush"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
