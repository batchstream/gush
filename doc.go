// Package gush is a small Go layer over Temporal: write flows and steps as plain
// functions, and run them on a Temporal server.
//
// A flow is deterministic orchestration, a Temporal workflow. A step is the unit of
// real work, a Temporal activity: network calls, database writes and computation
// belong there, because Temporal replays flows and retries steps.
//
//	func Charge(ctx *gush.StepContext) (any, error) {
//	    var order Order
//	    err := ctx.Decode(&order)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return charge(ctx, order)
//	}
//
//	func Fulfill(ctx *gush.FlowContext) (any, error) {
//	    var receipt Receipt
//	    err := ctx.RunStep(Charge, ctx.Input, &receipt, gush.WithMaxAttempts(3))
//	    return receipt, err
//	}
//
// A StepContext is a standard context.Context. A FlowContext holds the Temporal SDK's
// workflow.Context in its Context field, for SDK functions such as workflow.Sleep.
package gush
