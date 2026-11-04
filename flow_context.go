package gush

import (
	"maps"
	"time"

	"go.temporal.io/sdk/workflow"
)

// abortedFlowErrorType is the ApplicationError type of a flow's Abort.
const abortedFlowErrorType = "FlowAborted"

// DefaultFlowVersion is what Version returns for a flow that started before the change
// it guards.
const DefaultFlowVersion = int(workflow.DefaultVersion)

// FlowContext is what a flow receives. It is a workflow.Context, so Temporal SDK functions
// such as workflow.Sleep accept it. The exception is workflow.WithCancel, which panics on a
// context type it does not know, directly or through a parent. So pass ctx.Context to
// workflow.WithCancel and to workflow.Go, whose goroutine may call it.
//
// Temporal replays a flow's code to restore it, so flow code must be deterministic: no
// I/O, no time.Now and no time.Sleep. Do real work in steps. Logs written during a replay
// are dropped, so each line appears once.
//
//	func OrderFulfillment(ctx *gush.FlowContext) (any, error) {
//	    var order Order
//	    err := ctx.Decode(&order)
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    var payment Payment
//	    err = ctx.RunStep(ProcessPayment, order, &payment)
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    var receipt Receipt
//	    err = ctx.RunStep(ShipOrder, payment, &receipt)
//	    return receipt, err
//	}
type FlowContext struct {
	workflow.Context
	FlowInfo
	*Logger

	// Input is the raw input; Decode deserializes it.
	Input []byte

	// LogAttributes are inherited from the parent and passed on to child flows and steps.
	LogAttributes map[string]any
}

// NewFlowContext creates a FlowContext from a Temporal workflow context.
func NewFlowContext(ctx workflow.Context, logAttributes map[string]any) *FlowContext {
	info := workflow.GetInfo(ctx)

	flowCtx := &FlowContext{Context: ctx, Logger: NewLogger(), LogAttributes: logAttributes}
	flowCtx.ID = info.WorkflowExecution.ID
	flowCtx.Queue = info.TaskQueueName
	flowCtx.Priority = NewPriority(info.Priority.PriorityKey)
	flowCtx.FairKey = info.Priority.FairnessKey
	flowCtx.Attempt = info.Attempt
	startTime := info.WorkflowStartTime
	flowCtx.StartTime = &startTime
	if info.ParentWorkflowExecution != nil {
		flowCtx.ParentID = info.ParentWorkflowExecution.ID
	}

	flowCtx.Logger.muted = func() bool {
		return workflow.IsReplaying(ctx)
	}
	flowCtx.Logger.now = func() time.Time {
		return workflow.Now(ctx)
	}
	for key, value := range logAttributes {
		flowCtx.Logger.SetAttributes(Attr(key, value))
	}
	flowCtx.Logger.SetAttributes(Attr("flow_id", flowCtx.ID))

	return flowCtx
}

// WithContext returns a copy of this FlowContext that runs its steps and child flows on ctx.
// RunStep and RunFlow always use the FlowContext's own workflow.Context, so code holding a
// different one needs this, such as a goroutine started with workflow.Go or cleanup on a
// workflow.NewDisconnectedContext.
//
//	workflow.Go(ctx.Context, func(g workflow.Context) {
//	    err := ctx.WithContext(g).RunStep(Process, item, nil)
//	    ...
//	})
func (flowCtx *FlowContext) WithContext(ctx workflow.Context) *FlowContext {
	derived := *flowCtx
	derived.Context = ctx
	return &derived
}

// RunStep runs a step and waits for its result. The step inherits this flow's priority,
// fair key and log attributes unless options override them.
func (flowCtx *FlowContext) RunStep(step any, input, output any, options ...StepOption) error {
	config := DefaultStepConfig()
	config.Priority = flowCtx.Priority
	config.FairKey = flowCtx.FairKey
	maps.Copy(config.LogAttributes, flowCtx.LogAttributes)
	config.Apply(options...)

	stepName, err := handlerName("step", step)
	if err != nil {
		return flowCtx.Abort(err)
	}
	payload, err := EncodePayload(input)
	if err != nil {
		return flowCtx.Abort(err)
	}

	stepContext := workflow.WithActivityOptions(flowCtx.Context, newActivityOptions(config))
	future := workflow.ExecuteActivity(stepContext, stepName, payload, config.LogAttributes)

	var result []byte
	err = future.Get(stepContext, &result)
	if err != nil {
		return unwrapError(err)
	}

	err = DecodePayload(result, output)
	if err != nil {
		return flowCtx.Abort(err)
	}
	return nil
}

// RunFlow runs a child flow and waits for its result, unless WithAsync is set. The child
// inherits this flow's priority, fair key and log attributes unless options override them.
func (flowCtx *FlowContext) RunFlow(request FlowRequest, options ...FlowOption) error {
	config := DefaultFlowConfig()
	config.Priority = flowCtx.Priority
	config.FairKey = flowCtx.FairKey
	maps.Copy(config.LogAttributes, flowCtx.LogAttributes)
	config.Apply(options...)

	flowName, err := handlerName("flow", request.Flow)
	if err != nil {
		return flowCtx.Abort(err)
	}
	payload, err := EncodePayload(request.Input)
	if err != nil {
		return flowCtx.Abort(err)
	}

	childOptions := newChildWorkflowOptions(request.Queue, config)
	childContext := workflow.WithChildOptions(flowCtx.Context, childOptions)
	future := workflow.ExecuteChildWorkflow(childContext, flowName, payload, config.LogAttributes)

	if config.Async {
		return future.GetChildWorkflowExecution().Get(childContext, nil)
	}

	var result []byte
	err = future.Get(childContext, &result)
	if err != nil {
		return unwrapError(err)
	}

	err = DecodePayload(result, request.Output)
	if err != nil {
		return flowCtx.Abort(err)
	}
	return nil
}

// Version lets flow code change without breaking executions already running the old code.
// The first execution of a call records maxVersion in the flow's history, and replays
// return that recorded value. Flows that started before the call existed get
// DefaultFlowVersion.
//
// To replace a step:
//
//	v := ctx.Version("payment-provider", 1)
//	if v == gush.DefaultFlowVersion {
//	    err = ctx.RunStep(OldCharge, input, &output)
//	} else {
//	    err = ctx.RunStep(NewCharge, input, &output)
//	}
//
// To change the same code again, keep the tag, raise maxVersion and add a branch. Once no
// execution can reach an old branch, delete it but keep the Version call. Version accepts
// every recorded version, so a stale execution would then run the newer branch. To make one
// fail instead, call the SDK's workflow.GetVersion with a higher minimum.
func (flowCtx *FlowContext) Version(tag string, maxVersion int) int {
	minSupported := workflow.Version(DefaultFlowVersion)
	version := workflow.GetVersion(flowCtx.Context, tag, minSupported, workflow.Version(maxVersion))
	return int(version)
}

// ContinueAsNew returns an error that ends this run and starts the same flow again with the
// given input, the same flow ID and log attributes, and an empty history. Temporal limits a
// flow's history, so a flow that loops for a long time returns this every so often.
//
//	if batch.Done {
//	    return summary, nil
//	}
//	return nil, ctx.ContinueAsNew(batch.Next)
func (flowCtx *FlowContext) ContinueAsNew(input any) error {
	payload, err := EncodePayload(input)
	if err != nil {
		return flowCtx.Abort(err)
	}
	flowName := workflow.GetInfo(flowCtx.Context).WorkflowType.Name
	nextRun := workflow.WithWorkflowVersioningIntent(flowCtx.Context, defaultCommandVersioningIntent())
	return workflow.NewContinueAsNewError(nextRun, flowName, payload, flowCtx.LogAttributes)
}

// Decode deserializes the flow's Input into target, aborting the flow if it cannot.
func (flowCtx *FlowContext) Decode(target any) error {
	err := Decode(flowCtx.Input, target)
	if err != nil {
		return flowCtx.Abort(err)
	}
	return nil
}

// Abort returns an error that fails the flow permanently, with no further retries.
func (flowCtx *FlowContext) Abort(cause ...error) error {
	return abortError(abortedFlowErrorType, cause)
}
