//go:build integration

package gush_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/gush"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logcollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

const (
	testQueue       = "gush-integration"
	secondNamespace = "gush-second"
	testTimeout     = 45 * time.Second
	retryBackoff    = 50 * time.Millisecond
)

var (
	testClient   *gush.Client
	testHostPort string

	attempts     sync.Map // map[string]*atomic.Int32, by the key a test passes as input
	resumeTokens = make(chan string, 8)
)

func TestMain(m *testing.M) {
	os.Exit(runWithTemporal(m))
}

// runWithTemporal runs the tests against a real dev server, using a temporal binary on
// PATH or else downloading one.
func runWithTemporal(m *testing.M) int {
	serverOptions := testsuite.DevServerOptions{
		LogLevel: "error",
		ExtraArgs: []string{
			"--dynamic-config-value", "matching.useNewMatcher=true",
			"--dynamic-config-value", "matching.enableFairness=true",
			"--namespace", secondNamespace,
		},
	}
	temporalPath, err := exec.LookPath("temporal")
	if err == nil {
		serverOptions.ExistingPath = temporalPath
	}

	server, err := testsuite.StartDevServer(context.Background(), serverOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start Temporal dev server: %v\n", err)
		return 1
	}
	defer func() { _ = server.Stop() }()

	testHostPort = server.FrontendHostPort()
	clientOptions := gush.Options{HostPort: testHostPort}
	testClient, err = gush.Dial(clientOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial Temporal: %v\n", err)
		return 1
	}
	defer testClient.Close()

	serveContext, stopServing := context.WithCancel(context.Background())
	testWorker := gush.NewWorker(testQueue).
		WithFlows(Double, FailingFlow, ParentOfFailingFlow, FailingStepFlow, PlainFailureFlow, AbortFlow, ApprovalFlow,
			ExpiringApprovalFlow, TimeoutFlow, HeartbeatFlow, SilentFlow, ReplayFlow, AttributesFlow, AttributesChildFlow,
			WaitingFlow, InheritanceFlow, VersionFlow, SmallFlow, BadStepFlow, ChildOnOwnQueueFlow, WrappedAbortFlow,
			WrappingFlow, ParallelFlow, CleanupFlow, CountdownFlow, AttributeEchoFlow, CancelOneStepFlow, ChargeThenRefundFlow,
			DetachingParentFlow, DetailReadingFlow, AttributeRecordingFlow, InheritingDetachingParentFlow,
			CanceledDetachingFlow, HoldingEchoFlow, InvalidChildStartFlow, DuplicateChildStartFlow,
			IdleFlow, IdleDetachingParentFlow, UnpolledChildStartFlow).
		WithSteps(DoubleStep, FailingStep, PlainFailureStep, AbortStep, ApprovalStep, SlowStep, HeartbeatStep, SilentStep,
			PurgeCacheStep, AttributesStep, InheritanceStep, WrappedAbortStep, DecliningStep, CleanupStep, AttributeEchoStep, ChargingStep, RefundStep, DetailedStep).
		WithContext(serveContext)

	served := make(chan error, 1)
	go func() { served <- testClient.Serve(testWorker) }()
	defer func() {
		stopServing()
		<-served
	}()

	return m.Run()
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

func flowRequest(flow, input, output any) gush.FlowRequest {
	request := gush.FlowRequest{Queue: testQueue, Flow: flow, Input: input, Output: output}
	return request
}

func attemptsOf(key string) *atomic.Int32 {
	fresh := &atomic.Int32{}
	counter, _ := attempts.LoadOrStore(key, fresh)
	return counter.(*atomic.Int32)
}

// uniqueKey separates tests that share a handler, and repeated runs of one test.
func uniqueKey(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func DoubleStep(ctx *gush.StepContext) (any, error) {
	var input order
	err := ctx.Decode(&input)
	if err != nil {
		return nil, err
	}
	input.Amount *= 2
	return input, nil
}

func Double(ctx *gush.FlowContext) (any, error) {
	var input order
	err := ctx.Decode(&input)
	if err != nil {
		return nil, err
	}

	var output order
	err = ctx.RunStep(DoubleStep, input, &output)
	return output, err
}

func TestFlowRunsStepsAndReturnsResult(t *testing.T) {
	input := order{ID: "o-1", Amount: 21}
	var output order

	err := testClient.RunFlow(testContext(t), flowRequest(Double, input, &output))

	require.NoError(t, err)
	assert.Equal(t, 42, output.Amount)
	assert.Equal(t, "o-1", output.ID)
}

func TestFlowCanBeNamedByString(t *testing.T) {
	input := order{Amount: 4}
	var output order

	err := testClient.RunFlow(testContext(t), flowRequest("Double", input, &output))

	require.NoError(t, err)
	assert.Equal(t, 8, output.Amount)
}

func TestNilPointerInputRunsAsEmpty(t *testing.T) {
	var input *order
	var output order

	err := testClient.RunFlow(testContext(t), flowRequest(Double, input, &output))

	require.NoError(t, err)
	assert.Zero(t, output.Amount)
}

func WaitingFlow(ctx *gush.FlowContext) (any, error) {
	return nil, workflow.Sleep(ctx.Context, 2*time.Second)
}

func TestStartFlowReturnsBeforeItFinishes(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	startedID, err := testClient.StartFlow(ctx, flowRequest(WaitingFlow, nil, nil),
		gush.WithID(flowID),
		gush.WithPriority(gush.PriorityHigh),
		gush.WithFairKey("tenant-a"),
	)
	require.NoError(t, err)
	assert.Equal(t, flowID, startedID)

	flow, err := testClient.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, flowID, flow.ID)
	assert.Equal(t, "Running", flow.Status)
	assert.Equal(t, testQueue, flow.Queue)
	assert.Equal(t, gush.PriorityHigh, flow.Priority)
	assert.Equal(t, "tenant-a", flow.FairKey)
	assert.NotNil(t, flow.StartTime)
	assert.Nil(t, flow.CloseTime)

	require.Eventually(t, func() bool {
		finished, err := testClient.GetFlow(ctx, flowID)
		return err == nil && finished.Status == "Completed" && finished.CloseTime != nil
	}, 10*time.Second, 100*time.Millisecond)
}

func FailingFlow(ctx *gush.FlowContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	return nil, gush.NewApplicationError("boom", "Declined")
}

func TestFlowRetriesUntilMaxAttempts(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(FailingFlow, key, nil),
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
	)

	require.Error(t, err)
	assert.EqualValues(t, 3, attemptsOf(key).Load())
}

func TestFlowHonorsNoRetry(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(FailingFlow, key, nil),
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
		gush.WithNoRetry("Declined"),
	)

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())

	var applicationError *gush.ApplicationError
	require.True(t, errors.As(err, &applicationError))
	assert.Equal(t, "Declined", applicationError.Type())
	assert.Equal(t, "[Declined] boom", err.Error())
}

func TestFlowHonorsNoRetryErrors(t *testing.T) {
	key := uniqueKey(t)
	declined := gush.NewApplicationError("template", "Declined")

	err := testClient.RunFlow(testContext(t), flowRequest(FailingFlow, key, nil),
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
		gush.WithNoRetryErrors(declined),
	)

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
}

func ParentOfFailingFlow(ctx *gush.FlowContext) (any, error) {
	request := gush.FlowRequest{Flow: FailingFlow, Input: string(ctx.Input)}
	err := ctx.RunFlow(request,
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
		gush.WithNoRetry("Declined"),
	)
	return nil, err
}

func TestChildFlowHonorsNoRetry(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(ParentOfFailingFlow, key, nil))

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
	assert.Equal(t, "[Declined] boom", err.Error())
}

func ChildOnOwnQueueFlow(ctx *gush.FlowContext) (any, error) {
	input := order{Amount: 10}
	var output order
	request := gush.FlowRequest{Flow: Double, Input: input, Output: &output}
	err := ctx.RunFlow(request)
	return output, err
}

func TestChildFlowWithoutAQueueRunsOnItsParents(t *testing.T) {
	var output order

	err := testClient.RunFlow(testContext(t), flowRequest(ChildOnOwnQueueFlow, nil, &output))

	require.NoError(t, err)
	assert.Equal(t, 20, output.Amount)
}

func FailingStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	return nil, gush.NewApplicationError("boom", "Declined")
}

func FailingStepFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(FailingStep, string(ctx.Input), nil,
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
		gush.WithNoRetry("Declined"),
	)
	return nil, err
}

func TestStepHonorsNoRetry(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(FailingStepFlow, key, nil))

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
	assert.Equal(t, "[Declined] boom", err.Error())
}

func PlainFailureStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	return nil, errors.New("plain failure")
}

func PlainFailureFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(PlainFailureStep, string(ctx.Input), nil,
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
	)
	return nil, err
}

func TestPlainErrorsAreRetried(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(PlainFailureFlow, key, nil))

	require.Error(t, err)
	assert.EqualValues(t, 3, attemptsOf(key).Load())
	assert.Contains(t, err.Error(), "plain failure")
}

func AbortStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	return nil, ctx.Abort(errors.New("bad input"))
}

func AbortFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(AbortStep, string(ctx.Input), nil, gush.WithMaxAttempts(3))
	return nil, err
}

func TestAbortStopsRetries(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(AbortFlow, key, nil))

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
	assert.True(t, gush.IsPermanentError(err))
	assert.Equal(t, "[StepAborted] bad input", err.Error())
}

func ApprovalStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	resumeTokens <- ctx.ResumeToken
	return ctx.Suspend()
}

func ApprovalFlow(ctx *gush.FlowContext) (any, error) {
	var decision string
	err := ctx.RunStep(ApprovalStep, string(ctx.Input), &decision,
		gush.WithAttemptTimeout(30*time.Second),
		gush.WithMaxAttempts(1),
	)
	return decision, err
}

func TestSuspendedStepFinishesWithResume(t *testing.T) {
	ctx := testContext(t)
	key := uniqueKey(t)

	result := make(chan error, 1)
	var decision string
	go func() { result <- testClient.RunFlow(ctx, flowRequest(ApprovalFlow, key, &decision)) }()

	token := <-resumeTokens
	require.NoError(t, testClient.Resume(ctx, token, "approved", nil))

	require.NoError(t, <-result)
	assert.Equal(t, "approved", decision)
	assert.EqualValues(t, 1, attemptsOf(key).Load())

	err := testClient.Resume(ctx, token, "again", nil)
	require.Error(t, err)
	assert.True(t, gush.IsResumeTokenError(err), "%v", err)
}

func TestSuspendedStepFailsWithResumeError(t *testing.T) {
	ctx := testContext(t)
	key := uniqueKey(t)

	result := make(chan error, 1)
	go func() { result <- testClient.RunFlow(ctx, flowRequest(ApprovalFlow, key, nil)) }()

	token := <-resumeTokens
	rejected := gush.NewPermanentError("rejected by reviewer", "Rejected", nil)
	require.NoError(t, testClient.Resume(ctx, token, nil, rejected))

	err := <-result
	require.Error(t, err)
	assert.Equal(t, "[Rejected] rejected by reviewer", err.Error())
}

func ExpiringApprovalFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(ApprovalStep, string(ctx.Input), nil,
		gush.WithAttemptTimeout(500*time.Millisecond),
		gush.WithMaxAttempts(2),
		gush.WithInitialInterval(retryBackoff),
	)
	return nil, err
}

// The documentation of Suspend warns about this behavior, so it is pinned here.
func TestSuspendedStepRunsAgainWhenItsAttemptTimesOut(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(ExpiringApprovalFlow, key, nil))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.EqualValues(t, 2, attemptsOf(key).Load())
	<-resumeTokens
	<-resumeTokens
}

func SlowStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TimeoutFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(SlowStep, string(ctx.Input), nil,
		gush.WithAttemptTimeout(300*time.Millisecond),
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
		gush.WithNoRetry(gush.StepTimeoutErrorType),
	)
	return nil, err
}

func TestStepTimeoutErrorTypeStopsRetries(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(TimeoutFlow, key, nil))

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
}

func HeartbeatStep(ctx *gush.StepContext) (any, error) {
	for range 15 {
		ctx.Heartbeat()
		time.Sleep(200 * time.Millisecond)
	}
	return "alive", nil
}

func HeartbeatFlow(ctx *gush.FlowContext) (any, error) {
	var output string
	err := ctx.RunStep(HeartbeatStep, nil, &output,
		gush.WithHeartbeatTimeout(time.Second),
		gush.WithMaxAttempts(1),
	)
	return output, err
}

func TestHeartbeatKeepsAStepAlive(t *testing.T) {
	var output string

	err := testClient.RunFlow(testContext(t), flowRequest(HeartbeatFlow, nil, &output))

	require.NoError(t, err)
	assert.Equal(t, "alive", output)
}

func SilentStep(*gush.StepContext) (any, error) {
	time.Sleep(3 * time.Second)
	return "too late", nil
}

func SilentFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(SilentStep, nil, nil,
		gush.WithHeartbeatTimeout(time.Second),
		gush.WithMaxAttempts(1),
	)
	return nil, err
}

func TestStepWithoutHeartbeatsTimesOut(t *testing.T) {
	err := testClient.RunFlow(testContext(t), flowRequest(SilentFlow, nil, nil))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Heartbeat")
}

func PurgeCacheStep(*gush.StepContext) (any, error) {
	worker.PurgeStickyWorkflowCache()
	return nil, nil
}

func ReplayFlow(ctx *gush.FlowContext) (any, error) {
	key := string(ctx.Input)
	attemptsOf(key).Add(1)
	ctx.Printf("flow line %s", key)

	for range 2 {
		err := ctx.RunStep(PurgeCacheStep, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func TestFlowLogsOnceThroughReplays(t *testing.T) {
	exporter := captureLogs(t)
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(ReplayFlow, key, nil))

	require.NoError(t, err)
	assert.Greater(t, attemptsOf(key).Load(), int32(1), "the flow should have replayed")
	assert.Len(t, exporter.matching("flow line "+key), 1)
}

func AttributesStep(ctx *gush.StepContext) (any, error) {
	ctx.Printf("step line %s", string(ctx.Input))
	return nil, nil
}

func AttributesChildFlow(ctx *gush.FlowContext) (any, error) {
	key := string(ctx.Input)
	ctx.Printf("child line %s", key)
	err := ctx.RunStep(AttributesStep, "of child "+key, nil)
	return nil, err
}

func AttributesFlow(ctx *gush.FlowContext) (any, error) {
	key := string(ctx.Input)
	ctx.Printf("flow line %s", key)

	err := ctx.RunStep(AttributesStep, "of parent "+key, nil, gush.WithLogAttribute("stage", "payment"))
	if err != nil {
		return nil, err
	}

	request := gush.FlowRequest{Flow: AttributesChildFlow, Input: key}
	err = ctx.RunFlow(request)
	return nil, err
}

// Logs from one logical run must be correlatable across flows, children and steps,
// wherever they execute.
func TestLogAttributesFollowTheWork(t *testing.T) {
	exporter := captureLogs(t)
	key := uniqueKey(t)
	flowID := "flow-" + key

	err := testClient.RunFlow(testContext(t), flowRequest(AttributesFlow, key, nil),
		gush.WithID(flowID),
		gush.WithLogAttribute("tenant", "acme"),
	)
	require.NoError(t, err)

	lines := []string{"flow line " + key, "step line of parent " + key, "child line " + key, "step line of child " + key}
	for _, line := range lines {
		records := exporter.matching(line)
		require.Len(t, records, 1, line)
		assert.Equal(t, "acme", attributesOf(records[0])["tenant"], line)
		assert.NotEmpty(t, attributesOf(records[0])["flow_id"], line)
	}

	parent := attributesOf(exporter.matching("flow line " + key)[0])
	step := attributesOf(exporter.matching("step line of parent " + key)[0])
	assert.Equal(t, flowID, parent["flow_id"])
	assert.Equal(t, flowID, step["flow_id"])
	assert.Equal(t, "payment", step["stage"])
	assert.NotContains(t, parent, "stage")
}

func InheritanceStep(ctx *gush.StepContext) (any, error) {
	return fmt.Sprintf("%s|%s", ctx.Priority, ctx.FairKey), nil
}

func InheritanceFlow(ctx *gush.FlowContext) (any, error) {
	var inherited string
	err := ctx.RunStep(InheritanceStep, nil, &inherited)
	if err != nil {
		return nil, err
	}

	var overridden string
	err = ctx.RunStep(InheritanceStep, nil, &overridden,
		gush.WithPriority(gush.PriorityLow),
		gush.WithFairKey("other"),
	)
	return inherited + " " + overridden, err
}

func TestStepsInheritPriorityAndFairKey(t *testing.T) {
	var output string

	err := testClient.RunFlow(testContext(t), flowRequest(InheritanceFlow, nil, &output),
		gush.WithPriority(gush.PriorityUrgent),
		gush.WithFairKey("tenant-a"),
	)

	require.NoError(t, err)
	assert.Equal(t, "urgent|tenant-a low|other", output)
}

func TestOverlongFairKeyIsCutToFit(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	err := testClient.RunFlow(ctx, flowRequest(SmallFlow, nil, nil),
		gush.WithID(flowID),
		gush.WithFairKey(strings.Repeat("k", 100)),
	)
	require.NoError(t, err)

	flow, err := testClient.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("k", 64), flow.FairKey)
}

func VersionFlow(ctx *gush.FlowContext) (any, error) {
	return ctx.Version("change", 2), nil
}

func TestVersionReturnsTheCurrentVersionForNewFlows(t *testing.T) {
	var output int

	err := testClient.RunFlow(testContext(t), flowRequest(VersionFlow, nil, &output))

	require.NoError(t, err)
	assert.Equal(t, 2, output)
}

func SmallFlow(*gush.FlowContext) (any, error) {
	return "done", nil
}

func TestConfigDecodedFromJSONRuns(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	raw := fmt.Sprintf(`{"id":%q,"priority":"high","fair_key":"tenant-json","max_attempts":1}`, flowID)
	config := &gush.FlowConfig{}
	require.NoError(t, gush.Decode([]byte(raw), config))

	var output string
	err := testClient.RunFlow(ctx, flowRequest(SmallFlow, nil, &output), gush.WithConfig(config))
	require.NoError(t, err)
	assert.Equal(t, "done", output)

	flow, err := testClient.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, gush.PriorityHigh, flow.Priority)
	assert.Equal(t, "tenant-json", flow.FairKey)
}

func TestCancelFlow(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	_, err := testClient.StartFlow(ctx, flowRequest(WaitingFlow, nil, nil), gush.WithID(flowID))
	require.NoError(t, err)
	require.NoError(t, testClient.CancelFlow(ctx, flowID))

	require.Eventually(t, func() bool {
		flow, err := testClient.GetFlow(ctx, flowID)
		return err == nil && flow.Status == "Canceled"
	}, 10*time.Second, 100*time.Millisecond)
}

func TestTerminateFlow(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	_, err := testClient.StartFlow(ctx, flowRequest(WaitingFlow, nil, nil), gush.WithID(flowID))
	require.NoError(t, err)
	require.NoError(t, testClient.TerminateFlow(ctx, flowID, "test"))

	flow, err := testClient.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, "Terminated", flow.Status)
}

func TestGetQueueStats(t *testing.T) {
	stats, err := testClient.GetQueueStats(testContext(t), testQueue)

	require.NoError(t, err)
	assert.Contains(t, stats, gush.QueueTypeFlow)
	assert.Contains(t, stats, gush.QueueTypeStep)
}

func TestRunFlowRejectsAFlowThatCannotRun(t *testing.T) {
	err := testClient.RunFlow(testContext(t), flowRequest(123, nil, nil))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "flow must be a function")
}

func BadStepFlow(ctx *gush.FlowContext) (any, error) {
	return nil, ctx.RunStep(123, nil, nil)
}

func TestRunStepAbortsWhenTheStepCannotRun(t *testing.T) {
	err := testClient.RunFlow(testContext(t), flowRequest(BadStepFlow, nil, nil))

	require.Error(t, err)
	assert.True(t, gush.IsPermanentError(err))
	assert.Contains(t, err.Error(), "step must be a function")
}

func TestServeStopsWhenItsContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stoppable := gush.NewWorker("gush-stoppable").WithFlows(SmallFlow).WithContext(ctx)

	served := make(chan error, 1)
	go func() { served <- testClient.Serve(stoppable) }()
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-served:
		assert.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not stop")
	}
}

func TestServeReportsMisconfigurationInsteadOfPanicking(t *testing.T) {
	err := testClient.Serve(gush.NewWorker("gush-empty"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no flows or steps")
}

func TestServeReportsAWorkerThatCannotStart(t *testing.T) {
	failing := gush.NewWorker("").WithFlows(SmallFlow)

	err := testClient.Serve(failing)

	require.Error(t, err)
}

func TestDialReportsAnUnreachableServer(t *testing.T) {
	options := gush.Options{HostPort: "127.0.0.1:1"}

	_, err := gush.Dial(options)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect to Temporal")
}

func TestRetryPolicyAsAWhole(t *testing.T) {
	key := uniqueKey(t)
	policy := gush.RetryPolicy{
		InitialInterval:    retryBackoff,
		BackoffCoefficient: 1,
		MaxInterval:        retryBackoff,
		MaxAttempts:        2,
	}

	err := testClient.RunFlow(testContext(t), flowRequest(FailingFlow, key, nil), gush.WithRetryPolicy(policy))

	require.Error(t, err)
	assert.EqualValues(t, 2, attemptsOf(key).Load())
	assert.False(t, temporal.IsCanceledError(err))
}

// otlpRecords runs a fake OTLP/HTTP collector and returns attributes by log body.
func otlpRecords(t *testing.T) (*httptest.Server, *sync.Map) {
	t.Helper()

	records := &sync.Map{}
	handler := func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		export := &logcollector.ExportLogsServiceRequest{}
		err = proto.Unmarshal(body, export)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		for _, resourceLogs := range export.GetResourceLogs() {
			for _, scopeLogs := range resourceLogs.GetScopeLogs() {
				for _, record := range scopeLogs.GetLogRecords() {
					attributes := make(map[string]string)
					for _, attr := range record.GetAttributes() {
						attributes[attr.GetKey()] = attr.GetValue().GetStringValue()
					}
					records.Store(record.GetBody().GetStringValue(), attributes)
				}
			}
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return server, records
}

// One run's logs reach the collector from the flow, its step and its child, tied together
// by flow_id and the inherited attributes, which is what distributed log search relies on.
func TestLogsFromOneRunReachTheCollectorCorrelated(t *testing.T) {
	server, records := otlpRecords(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)

	clientOptions := gush.Options{HostPort: testHostPort}
	exporting, err := gush.Dial(clientOptions)
	require.NoError(t, err)

	key := uniqueKey(t)
	flowID := "flow-" + key
	err = testClient.RunFlow(testContext(t), flowRequest(AttributesFlow, key, nil),
		gush.WithID(flowID),
		gush.WithLogAttribute("tenant", "acme"),
	)
	require.NoError(t, err)

	// Closing flushes the batch to the collector.
	exporting.Close()

	lines := []string{"flow line " + key, "step line of parent " + key, "child line " + key, "step line of child " + key}
	for _, line := range lines {
		stored, ok := records.Load(line)
		require.True(t, ok, "the collector did not receive %q", line)
		attributes := stored.(map[string]string)
		assert.Equal(t, "acme", attributes["tenant"], line)
		assert.NotEmpty(t, attributes["flow_id"], line)
		assert.NotEmpty(t, attributes["file"], line)
	}

	parent, _ := records.Load("flow line " + key)
	step, _ := records.Load("step line of parent " + key)
	assert.Equal(t, flowID, parent.(map[string]string)["flow_id"])
	assert.Equal(t, flowID, step.(map[string]string)["flow_id"])
}

func TestClientsRunInTheirNamespace(t *testing.T) {
	ctx := testContext(t)
	t.Setenv("TEMPORAL_NAMESPACE", secondNamespace)

	clientOptions := gush.Options{HostPort: testHostPort}
	other, err := gush.Dial(clientOptions)
	require.NoError(t, err)
	defer other.Close()

	workerContext, stopWorker := context.WithCancel(context.Background())
	otherWorker := gush.NewWorker("gush-second-queue").WithFlows(SmallFlow).WithContext(workerContext)
	served := make(chan error, 1)
	go func() { served <- other.Serve(otherWorker) }()
	defer func() {
		stopWorker()
		<-served
	}()

	flowID := uniqueKey(t)
	var output string
	request := gush.FlowRequest{Queue: "gush-second-queue", Flow: SmallFlow, Output: &output}
	err = other.RunFlow(ctx, request, gush.WithID(flowID))
	require.NoError(t, err)
	assert.Equal(t, "done", output)

	_, err = other.GetFlow(ctx, flowID)
	assert.NoError(t, err)
	_, err = testClient.GetFlow(ctx, flowID)
	assert.Error(t, err, "the flow must not exist in the default namespace")
}

func TestPackageLevelFunctionsUseTheDefaultClient(t *testing.T) {
	ctx := testContext(t)
	t.Setenv("TEMPORAL_ADDRESS", testHostPort)
	t.Setenv("TEMPORAL_NAMESPACE", "")
	defer gush.Close()

	flowID := uniqueKey(t)
	var output string
	err := gush.RunFlow(ctx, flowRequest(SmallFlow, nil, &output), gush.WithID(flowID))
	require.NoError(t, err)
	assert.Equal(t, "done", output)

	flow, err := gush.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, "Completed", flow.Status)

	stats, err := gush.GetQueueStats(ctx, testQueue)
	require.NoError(t, err)
	assert.Contains(t, stats, gush.QueueTypeStep)

	waitingID := uniqueKey(t) + "-waiting"
	startedID, err := gush.StartFlow(ctx, flowRequest(WaitingFlow, nil, nil), gush.WithID(waitingID))
	require.NoError(t, err)
	assert.Equal(t, waitingID, startedID)
	require.NoError(t, gush.CancelFlow(ctx, waitingID))

	terminatedID := uniqueKey(t) + "-terminated"
	_, err = gush.StartFlow(ctx, flowRequest(WaitingFlow, nil, nil), gush.WithID(terminatedID))
	require.NoError(t, err)
	require.NoError(t, gush.TerminateFlow(ctx, terminatedID, "test"))

	key := uniqueKey(t) + "-approval"
	result := make(chan error, 1)
	go func() { result <- gush.RunFlow(ctx, flowRequest(ApprovalFlow, key, nil)) }()
	token := <-resumeTokens
	require.NoError(t, gush.Resume(ctx, token, "approved", nil))
	require.NoError(t, <-result)

	stoppable, stop := context.WithCancel(context.Background())
	stoppableWorker := gush.NewWorker("gush-default-client").WithFlows(SmallFlow).WithContext(stoppable)
	served := make(chan error, 1)
	go func() { served <- gush.Serve(stoppableWorker) }()
	time.Sleep(500 * time.Millisecond)
	stop()
	assert.NoError(t, <-served)
}

func WrappedAbortStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	return nil, fmt.Errorf("charge order: %w", ctx.Abort(errors.New("bad input")))
}

func WrappedAbortFlow(ctx *gush.FlowContext) (any, error) {
	return nil, ctx.RunStep(WrappedAbortStep, ctx.Input, nil,
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
	)
}

func TestWrappingAnAbortKeepsItPermanent(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(WrappedAbortFlow, key, nil))

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
	assert.True(t, gush.IsPermanentError(err))
	assert.Equal(t, "[StepAborted] charge order: bad input", err.Error())
}

func DecliningStep(*gush.StepContext) (any, error) {
	return nil, gush.NewPermanentError("card declined", "Declined", nil)
}

func WrappingFlow(ctx *gush.FlowContext) (any, error) {
	attemptsOf(string(ctx.Input)).Store(ctx.Attempt)
	err := ctx.RunStep(DecliningStep, nil, nil)
	return nil, fmt.Errorf("fulfill: %w", err)
}

func TestFlowWrappingAStepErrorKeepsItsType(t *testing.T) {
	key := uniqueKey(t)

	err := testClient.RunFlow(testContext(t), flowRequest(WrappingFlow, key, nil),
		gush.WithMaxAttempts(3),
		gush.WithInitialInterval(retryBackoff),
	)

	require.Error(t, err)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
	assert.True(t, gush.IsPermanentError(err))
	assert.Equal(t, "[Declined] fulfill: card declined", err.Error())
}

func ParallelFlow(ctx *gush.FlowContext) (any, error) {
	total := 0
	group := workflow.NewWaitGroup(ctx.Context)
	for i := range 3 {
		group.Add(1)
		workflow.Go(ctx.Context, func(g workflow.Context) {
			defer group.Done()

			input := order{ID: "parallel", Amount: i + 1}
			var output order
			err := ctx.WithContext(g).RunStep(DoubleStep, input, &output)
			if err == nil {
				total += output.Amount
			}
		})
	}
	group.Wait(ctx.Context)
	return total, nil
}

func TestStartFlowReturnsAGeneratedID(t *testing.T) {
	ctx := testContext(t)

	flowID, err := testClient.StartFlow(ctx, flowRequest(WaitingFlow, nil, nil))
	require.NoError(t, err)
	require.NotEmpty(t, flowID)

	flow, err := testClient.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, flowID, flow.ID)
}

func DetachingParentFlow(ctx *gush.FlowContext) (any, error) {
	request := gush.FlowRequest{Flow: WaitingFlow}
	return ctx.StartFlow(request)
}

func DetailedStep(ctx *gush.StepContext) (any, error) {
	permanent := gush.NewPermanentError("declined", "Declined", nil, "the-detail")
	return nil, fmt.Errorf("charge: %w", permanent)
}

func DetailReadingFlow(ctx *gush.FlowContext) (any, error) {
	stepErr := ctx.RunStep(DetailedStep, nil, nil)

	var outer *gush.ApplicationError
	if !errors.As(stepErr, &outer) {
		return nil, fmt.Errorf("the step returned %v", stepErr)
	}
	var detail string
	err := outer.Details(&detail)
	if err == nil {
		return nil, fmt.Errorf("the outer error has the detail %q", detail)
	}

	var original *gush.ApplicationError
	if !errors.As(errors.Unwrap(outer), &original) {
		return nil, fmt.Errorf("the outer error wraps %v", errors.Unwrap(outer))
	}
	err = original.Details(&detail)
	return detail, err
}

func TestWrappedErrorKeepsItsDetailsOnTheOriginal(t *testing.T) {
	ctx := testContext(t)

	var detail string
	err := testClient.RunFlow(ctx, flowRequest(DetailReadingFlow, nil, &detail))
	require.NoError(t, err)
	assert.Equal(t, "the-detail", detail)
}

func TestChildBegunWithStartFlowOutlivesItsParent(t *testing.T) {
	ctx := testContext(t)

	var childID string
	err := testClient.RunFlow(ctx, flowRequest(DetachingParentFlow, nil, &childID))
	require.NoError(t, err)
	require.NotEmpty(t, childID)

	completed := func() bool {
		flow, err := testClient.GetFlow(ctx, childID)
		return err == nil && flow.Status == "Completed"
	}
	assert.Eventually(t, completed, 15*time.Second, 200*time.Millisecond)
}

var detachedAttributes sync.Map

func AttributeRecordingFlow(ctx *gush.FlowContext) (any, error) {
	detachedAttributes.Store(ctx.ID, ctx.LogAttributes["tenant"])
	return nil, nil
}

func InheritingDetachingParentFlow(ctx *gush.FlowContext) (any, error) {
	request := gush.FlowRequest{Flow: AttributeRecordingFlow}
	return ctx.StartFlow(request)
}

func TestChildBegunWithStartFlowInherits(t *testing.T) {
	ctx := testContext(t)

	var childID string
	err := testClient.RunFlow(ctx, flowRequest(InheritingDetachingParentFlow, nil, &childID),
		gush.WithPriority(gush.PriorityHigh),
		gush.WithFairKey("tenant-a"),
		gush.WithLogAttribute("tenant", "acme"),
	)
	require.NoError(t, err)

	flow, err := testClient.GetFlow(ctx, childID)
	require.NoError(t, err)
	assert.Equal(t, gush.PriorityHigh, flow.Priority)
	assert.Equal(t, "tenant-a", flow.FairKey)

	recorded := func() bool {
		tenant, ok := detachedAttributes.Load(childID)
		return ok && tenant == "acme"
	}
	assert.Eventually(t, recorded, 15*time.Second, 200*time.Millisecond)
}

func CanceledDetachingFlow(ctx *gush.FlowContext) (any, error) {
	var cleansUp bool
	err := ctx.Decode(&cleansUp)
	if err != nil {
		return nil, err
	}

	request := gush.FlowRequest{Flow: WaitingFlow}
	_, err = ctx.StartFlow(request, gush.WithID(ctx.ID+"-child"))
	if err != nil {
		return nil, err
	}

	err = workflow.Sleep(ctx.Context, time.Hour)
	if !temporal.IsCanceledError(err) {
		return nil, err
	}
	_, refused := ctx.StartFlow(request)
	if !temporal.IsCanceledError(refused) {
		return nil, fmt.Errorf("StartFlow on the canceled context returned %w", refused)
	}
	if !cleansUp {
		return nil, err
	}

	disconnected, _ := workflow.NewDisconnectedContext(ctx.Context)
	return nil, workflow.Sleep(disconnected, time.Second)
}

func TestCancelingAParentLeavesAStartedChildRunning(t *testing.T) {
	tests := map[string]struct {
		cleansUp     bool
		parentStatus string
	}{
		"parent returns at once": {cleansUp: false, parentStatus: "Canceled"},
		"parent cleans up first": {cleansUp: true, parentStatus: "Completed"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := testContext(t)
			flowID := uniqueKey(t)
			childID := flowID + "-child"

			_, err := testClient.StartFlow(ctx, flowRequest(CanceledDetachingFlow, test.cleansUp, nil), gush.WithID(flowID))
			require.NoError(t, err)

			reachesStatus := func(id, status string) func() bool {
				return func() bool {
					flow, err := testClient.GetFlow(ctx, id)
					return err == nil && flow.Status == status
				}
			}
			started := func() bool {
				_, err := testClient.GetFlow(ctx, childID)
				return err == nil
			}
			require.Eventually(t, started, 15*time.Second, 50*time.Millisecond)
			require.NoError(t, testClient.CancelFlow(ctx, flowID))

			assert.Eventually(t, reachesStatus(flowID, test.parentStatus), 15*time.Second, 200*time.Millisecond)
			assert.Eventually(t, reachesStatus(childID, "Completed"), 15*time.Second, 200*time.Millisecond)
		})
	}
}

func HoldingEchoFlow(ctx *gush.FlowContext) (any, error) {
	var input string
	err := ctx.Decode(&input)
	if err != nil {
		return nil, err
	}
	return input, workflow.Sleep(ctx.Context, 2*time.Second)
}

func TestStartFlowJoinsARunningFlow(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	first, err := testClient.StartFlow(ctx, flowRequest(HoldingEchoFlow, "first", nil), gush.WithID(flowID))
	require.NoError(t, err)
	second, err := testClient.StartFlow(ctx, flowRequest(HoldingEchoFlow, "second", nil), gush.WithID(flowID))
	require.NoError(t, err)
	assert.Equal(t, first, second)

	var output string
	err = testClient.RunFlow(ctx, flowRequest(HoldingEchoFlow, "third", &output), gush.WithID(flowID))
	require.NoError(t, err)
	assert.Equal(t, "first", output)
}

func InvalidChildStartFlow(ctx *gush.FlowContext) (any, error) {
	request := gush.FlowRequest{Flow: 123}
	return ctx.StartFlow(request)
}

func TestStartFlowAbortsOnAChildThatCannotRun(t *testing.T) {
	err := testClient.RunFlow(testContext(t), flowRequest(InvalidChildStartFlow, nil, nil))

	require.Error(t, err)
	assert.True(t, gush.IsPermanentError(err))
}

func DuplicateChildStartFlow(ctx *gush.FlowContext) (any, error) {
	var childID string
	err := ctx.Decode(&childID)
	if err != nil {
		return nil, err
	}

	request := gush.FlowRequest{Flow: WaitingFlow}
	_, err = ctx.StartFlow(request, gush.WithID(childID))
	return fmt.Sprint(err), nil
}

func TestStartFlowReportsAChildIDInUse(t *testing.T) {
	ctx := testContext(t)
	childID := uniqueKey(t)

	_, err := testClient.StartFlow(ctx, flowRequest(IdleFlow, nil, nil), gush.WithID(childID))
	require.NoError(t, err)

	var output string
	err = testClient.RunFlow(ctx, flowRequest(DuplicateChildStartFlow, childID, &output))
	require.NoError(t, err)
	assert.Equal(t, "child workflow execution already started", output)
	require.NoError(t, testClient.TerminateFlow(ctx, childID, "test"))
}

func IdleFlow(ctx *gush.FlowContext) (any, error) {
	return nil, workflow.Sleep(ctx.Context, time.Hour)
}

func IdleDetachingParentFlow(ctx *gush.FlowContext) (any, error) {
	request := gush.FlowRequest{Flow: IdleFlow}
	return ctx.StartFlow(request)
}

func TestCancelFlowStopsAStartedChild(t *testing.T) {
	ctx := testContext(t)

	var childID string
	err := testClient.RunFlow(ctx, flowRequest(IdleDetachingParentFlow, nil, &childID))
	require.NoError(t, err)
	require.NoError(t, testClient.CancelFlow(ctx, childID))

	canceled := func() bool {
		flow, err := testClient.GetFlow(ctx, childID)
		return err == nil && flow.Status == "Canceled"
	}
	assert.Eventually(t, canceled, 15*time.Second, 200*time.Millisecond)
}

func UnpolledChildStartFlow(ctx *gush.FlowContext) (any, error) {
	request := gush.FlowRequest{Queue: "gush-unpolled", Flow: "NeverRuns"}
	return ctx.StartFlow(request)
}

func TestStartFlowRunsAChildOnItsQueue(t *testing.T) {
	ctx := testContext(t)

	var childID string
	err := testClient.RunFlow(ctx, flowRequest(UnpolledChildStartFlow, nil, &childID))
	require.NoError(t, err)

	flow, err := testClient.GetFlow(ctx, childID)
	require.NoError(t, err)
	assert.Equal(t, "gush-unpolled", flow.Queue)
	require.NoError(t, testClient.TerminateFlow(ctx, childID, "test"))
}

func TestParallelStepsRunThroughWithContext(t *testing.T) {
	var total int

	err := testClient.RunFlow(testContext(t), flowRequest(ParallelFlow, nil, &total))

	require.NoError(t, err)
	assert.Equal(t, 12, total)
}

func CleanupStep(ctx *gush.StepContext) (any, error) {
	attemptsOf(string(ctx.Input)).Add(1)
	return nil, nil
}

func CleanupFlow(ctx *gush.FlowContext) (any, error) {
	err := workflow.Sleep(ctx.Context, time.Hour)
	if !temporal.IsCanceledError(err) {
		return nil, err
	}

	refused := ctx.RunStep(CleanupStep, "refused", nil)
	if !temporal.IsCanceledError(refused) {
		return nil, fmt.Errorf("a step on the canceled context returned %w", refused)
	}

	disconnected, _ := workflow.NewDisconnectedContext(ctx.Context)
	return nil, ctx.WithContext(disconnected).RunStep(CleanupStep, ctx.Input, nil)
}

func TestCanceledFlowCleansUpOnADisconnectedContext(t *testing.T) {
	ctx := testContext(t)
	key := uniqueKey(t)

	_, err := testClient.StartFlow(ctx, flowRequest(CleanupFlow, key, nil), gush.WithID(key))
	require.NoError(t, err)
	require.NoError(t, testClient.CancelFlow(ctx, key))

	require.Eventually(t, func() bool {
		flow, err := testClient.GetFlow(ctx, key)
		return err == nil && flow.Status == "Completed"
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 1, attemptsOf(key).Load())
}

func CountdownFlow(ctx *gush.FlowContext) (any, error) {
	var remaining int
	err := ctx.Decode(&remaining)
	if err != nil {
		return nil, err
	}

	if remaining > 0 {
		return nil, ctx.ContinueAsNew(remaining - 1)
	}
	return ctx.LogAttributes["tenant"], nil
}

func TestContinueAsNewCarriesTheInputAndAttributes(t *testing.T) {
	ctx := testContext(t)
	flowID := uniqueKey(t)

	var tenant string
	err := testClient.RunFlow(ctx, flowRequest(CountdownFlow, 2, &tenant),
		gush.WithID(flowID),
		gush.WithPriority(gush.PriorityHigh),
		gush.WithLogAttribute("tenant", "acme"),
	)

	require.NoError(t, err)
	assert.Equal(t, "acme", tenant)

	flow, err := testClient.GetFlow(ctx, flowID)
	require.NoError(t, err)
	assert.Equal(t, "Completed", flow.Status)
	assert.Equal(t, gush.PriorityHigh, flow.Priority)
}

func AttributeEchoStep(ctx *gush.StepContext) (any, error) {
	return ctx.LogAttributes["user_id"], nil
}

func AttributeEchoFlow(ctx *gush.FlowContext) (any, error) {
	var userID int64
	err := ctx.RunStep(AttributeEchoStep, nil, &userID)
	return userID, err
}

func TestLargeNumericAttributesSurviveEveryHop(t *testing.T) {
	const userID = int64(9007199254740993)

	var output int64
	err := testClient.RunFlow(testContext(t), flowRequest(AttributeEchoFlow, nil, &output),
		gush.WithLogAttribute("user_id", userID),
	)

	require.NoError(t, err)
	assert.Equal(t, userID, output)
}

func CancelOneStepFlow(ctx *gush.FlowContext) (any, error) {
	var stepErr error
	group := workflow.NewWaitGroup(ctx.Context)
	group.Add(1)
	workflow.Go(ctx.Context, func(g workflow.Context) {
		defer group.Done()

		canceled, cancel := workflow.WithCancel(g)
		cancel()
		stepErr = ctx.WithContext(canceled).RunStep(DoubleStep, nil, nil)
	})
	group.Wait(ctx.Context)

	if !temporal.IsCanceledError(stepErr) {
		return nil, fmt.Errorf("the step returned %v", stepErr)
	}
	return "canceled", nil
}

func TestOneParallelStepCanBeCanceledOnItsOwn(t *testing.T) {
	var output string

	err := testClient.RunFlow(testContext(t), flowRequest(CancelOneStepFlow, nil, &output))

	require.NoError(t, err)
	assert.Equal(t, "canceled", output)
}

func ChargingStep(ctx *gush.StepContext) (any, error) {
	key := string(ctx.Input)
	attemptsOf(key + "-started").Add(1)

	for ctx.Err() == nil {
		ctx.Heartbeat()
		time.Sleep(20 * time.Millisecond)
	}

	// The step takes a moment to wind down, and cleanup must not overlap it.
	time.Sleep(300 * time.Millisecond)
	attemptsOf(key + "-charged").Add(1)
	return nil, ctx.Err()
}

func RefundStep(ctx *gush.StepContext) (any, error) {
	key := string(ctx.Input)
	attemptsOf(key + "-seen").Store(attemptsOf(key + "-charged").Load())
	return nil, nil
}

func ChargeThenRefundFlow(ctx *gush.FlowContext) (any, error) {
	err := ctx.RunStep(ChargingStep, ctx.Input, nil,
		gush.WithWaitForCancel(),
		gush.WithHeartbeatTimeout(time.Second),
		gush.WithMaxAttempts(1),
	)
	if !temporal.IsCanceledError(err) {
		return nil, fmt.Errorf("the charge returned %v", err)
	}

	disconnected, _ := workflow.NewDisconnectedContext(ctx.Context)
	return nil, ctx.WithContext(disconnected).RunStep(RefundStep, ctx.Input, nil)
}

func TestWaitForCancelHoldsCleanupUntilTheStepEnds(t *testing.T) {
	ctx := testContext(t)
	key := uniqueKey(t)

	_, err := testClient.StartFlow(ctx, flowRequest(ChargeThenRefundFlow, key, nil), gush.WithID(key))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return attemptsOf(key+"-started").Load() == 1
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, testClient.CancelFlow(ctx, key))

	require.Eventually(t, func() bool {
		flow, err := testClient.GetFlow(ctx, key)
		return err == nil && flow.Status == "Completed"
	}, 15*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 1, attemptsOf(key+"-seen").Load(), "the refund started before the charge ended")
}

func TestResumeKeepsAWrappedErrorsType(t *testing.T) {
	ctx := testContext(t)
	key := uniqueKey(t)

	result := make(chan error, 1)
	go func() { result <- testClient.RunFlow(ctx, flowRequest(ApprovalFlow, key, nil)) }()

	token := <-resumeTokens
	rejected := gush.NewPermanentError("rejected by reviewer", "Rejected", nil)
	wrapped := fmt.Errorf("reviewer said no: %w", rejected)
	require.NoError(t, testClient.Resume(ctx, token, nil, wrapped))

	err := <-result
	require.Error(t, err)
	assert.True(t, gush.IsPermanentError(err))
	assert.Equal(t, "[Rejected] reviewer said no: rejected by reviewer", err.Error())
}
