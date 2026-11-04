# Gush

[![CI](https://github.com/batchstream/gush/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/batchstream/gush/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/batchstream/gush.svg)](https://pkg.go.dev/github.com/batchstream/gush) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small Go layer over [Temporal](https://temporal.io): write durable flows and steps as plain functions.

Gush needs a Temporal server and does not replace one. Temporal provides the durability, retries and scaling. Gush puts a smaller surface on top:

- **One signature.** Flows and steps are `func(ctx) (any, error)` and decode their input when they need it.
- **Logs that follow the work.** Attributes set on a run reach its child flows and steps on any worker, with export over OTLP, so one `flow_id` finds every line of a flow and its steps.
- **Scheduling by name.** Priority and fair scheduling read as options, and children inherit them.
- **Compressed payloads.** Inputs and outputs are compressed with zstd, which leaves more room under Temporal's size limits.
- **Readable errors.** A failure reads `[Declined] card declined` rather than Temporal's wrapped chain, and keeps its type and retryability.

Two trade-offs come with that:

- Inputs and outputs are `any`, so the compiler does not check that a flow and its step agree on a type.
- Temporal sees compressed bytes, so its Web UI and CLI cannot show inputs and outputs as text.

The SDK stays within reach: `ctx.Context` in a flow is the SDK's own `workflow.Context`, for functions like `workflow.Sleep`.

## Install

Requires Go 1.27.3.

```sh
go get github.com/batchstream/gush
```

## Quick start

Start a local Temporal server, with [its CLI](https://docs.temporal.io/cli):

```sh
temporal server start-dev
```

Then define a step, a flow and a worker. This is [examples/quickstart](examples/quickstart/main.go), trimmed:

```go
// Charge is a step: real work, retried on failure.
func Charge(ctx *gush.StepContext) (any, error) {
    var order Order
    err := ctx.Decode(&order)
    if err != nil {
        return nil, err
    }

    ctx.Printf("charging order %s", order.ID)
    receipt := Receipt{OrderID: order.ID, Charged: order.Amount}
    return receipt, nil
}

// Fulfill is a flow: deterministic orchestration of steps.
func Fulfill(ctx *gush.FlowContext) (any, error) {
    var receipt Receipt
    err := ctx.RunStep(Charge, ctx.Input, &receipt, gush.WithMaxAttempts(3))
    return receipt, err
}

func main() {
    var options gush.Options
    client, err := gush.Dial(options)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    worker := gush.NewWorker("orders").WithSteps(Charge).WithFlows(Fulfill)
    go client.Serve(worker)

    order := Order{ID: "order-1", Amount: 42}
    var receipt Receipt
    request := gush.FlowRequest{Queue: "orders", Flow: Fulfill, Input: order, Output: &receipt}
    err = client.RunFlow(context.Background(), request, gush.WithTimeout(time.Minute))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("charged %d for %s\n", receipt.Charged, receipt.OrderID)
}
```

Run it with `go run ./examples/quickstart`.

Temporal replays flow code to restore it, so flows must be deterministic: no I/O, no `time.Now` and no `time.Sleep`. Do real work in steps.

## Running flows

`RunFlow` waits for the result. `StartFlow` returns the flow's ID once it has started.

```go
request := gush.FlowRequest{Queue: "orders", Flow: Fulfill, Input: order}
flowID, err := client.StartFlow(ctx, request)

flow, err := client.GetFlow(ctx, flowID) // flow.Status: Running, Completed, Failed, ...
err = client.CancelFlow(ctx, flowID)     // lets the flow clean up
err = client.TerminateFlow(ctx, flowID, "reason") // stops it immediately
```

A flow starts a child the same way. A child with no queue runs on its parent's, and one begun with `StartFlow` outlives its parent.

```go
request := gush.FlowRequest{Flow: Notify, Input: receipt}
err := ctx.RunFlow(request)
```

`WithCron("0 2 * * *")` runs a flow on a schedule in UTC until it is canceled, terminated or stopped by `WithTimeout`. Start it with `StartFlow`, because `RunFlow` returns an error: a schedule has no result to wait for.

Flows and steps are registered under their bare Go function names, and can be called by that name as a string. Two functions with the same name in different packages collide, and `Serve` reports it. Use `NewFlowHandler` and `NewStepHandler` to choose a name, which closures and the instantiations of a generic function need too.

`string` and `[]byte` travel as raw bytes and everything else as JSON, so read a value back as the type it was written.

### Parallel steps and cleanup

`RunStep` and `RunFlow` run on the context their `FlowContext` holds. Code on another `workflow.Context` gets a matching `FlowContext` from `WithContext`, as a goroutine does:

```go
group := workflow.NewWaitGroup(ctx.Context)
for _, item := range items {
    group.Add(1)
    workflow.Go(ctx.Context, func(g workflow.Context) {
        defer group.Done()
        err := ctx.WithContext(g).RunStep(Process, item, nil)
        if err != nil {
            ctx.Printf("process %s: %v", item, err)
        }
    })
}
group.Wait(ctx.Context)
```

After `CancelFlow`, anything on the flow's own context returns a canceled error. Cleanup needs a context that ignores the cancellation, and `WithWaitForCancel` so it starts after the step has ended rather than alongside it:

```go
err := ctx.RunStep(Charge, order, &receipt, gush.WithWaitForCancel())
if temporal.IsCanceledError(err) {
    disconnected, _ := workflow.NewDisconnectedContext(ctx.Context)
    return nil, ctx.WithContext(disconnected).RunStep(Refund, order, nil)
}
```

A step that is already running only finds out about the cancellation through `ctx.Heartbeat()`, and `WithHeartbeatTimeout` makes that prompt. It should then return on `ctx.Done()`.

### Options

| Option | Applies to | Effect |
|---|---|---|
| `WithMaxAttempts(n)` | both | Total attempts, including the first. Zero means unlimited. |
| `WithInitialInterval(d)` | both | Delay before the first retry. |
| `WithBackoffCoefficient(f)` | both | Multiplier on the delay after each retry. |
| `WithMaxInterval(d)` | both | Cap on the delay between retries. |
| `WithRetryPolicy(p)` | both | Replaces all retry settings. |
| `WithNoRetry(types...)` | both | Error types that fail immediately. |
| `WithNoRetryErrors(errs...)` | both | The same, taken from `*ApplicationError` values. |
| `WithPriority(p)` | both | Scheduling priority. |
| `WithFairKey(key)` | both | Group to schedule fairly against, such as a tenant. |
| `WithLogAttribute(k, v)` | both | Attribute on every log line of this run and its children. |
| `WithLogAttributes(m)` | both | The same, for several attributes. |
| `WithID(id)` | flow | Flow ID. Otherwise one is generated, which `StartFlow` returns. A client's `RunFlow` or `StartFlow` joins a flow already running with this ID and ignores this run's input, and a child flow gets an error. |
| `WithTimeout(d)` | flow | Limit on the whole execution, including every retry and every scheduled run. |
| `WithCron(expr)` | flow | Schedule in UTC. Use with `StartFlow`. |
| `WithConfig(c)` | flow | Starts from a `FlowConfig`, such as one decoded from a queue. Fields left out of the JSON keep their defaults. |
| `WithQueue(q)` | step | Runs on another queue. |
| `WithAttemptTimeout(d)` | step | Limit on each attempt, not the step as a whole. |
| `WithHeartbeatTimeout(d)` | step | Fails an attempt that stops calling `ctx.Heartbeat()`. |
| `WithPickupTimeout(d)` | step | Fails if no worker picks the step up in time. |
| `WithWaitForCancel()` | step | When the flow is canceled, waits for the running step to end instead of returning at once. |

## Retries and errors

| | Flow | Step |
|---|---|---|
| Attempts | 1 | 10 |
| Timeout | none | 2 minutes per attempt |
| Backoff | 1s, doubling, up to 100s | 1s, doubling, up to 100s |

Any error returned from a step or flow is retried until attempts run out, including one from `errors.New`. There are three ways to stop that:

```go
// Fail this step now, whatever the retry policy says.
return nil, ctx.Abort(err)

// The same, with a type the caller can match.
return nil, gush.NewPermanentError("card declined", "Declined", err)

// Let the caller decide, per run.
err := ctx.RunStep(Charge, order, &receipt, gush.WithNoRetry("Declined"))
```

`WithNoRetry` matches the type of a `gush.NewApplicationError`. Pass `gush.StepTimeoutErrorType` to stop retrying steps that timed out.

Errors from `RunFlow` and `RunStep` work with `errors.As`, `gush.IsApplicationError`, `gush.IsPermanentError` and `gush.IsPanicError`. Wrapping one with `fmt.Errorf` and `%w` keeps its type and retryability.

## Priority and fairness

```go
err := client.RunFlow(ctx, request,
    gush.WithPriority(gush.PriorityHigh),
    gush.WithFairKey("tenant-123"),
)
```

Priorities run from `PriorityUrgent`, `PriorityHigh`, `PriorityMedium` (the default) and `PriorityLow` to `PriorityTrivial`. A fair key stops one tenant from starving the others on a queue. Temporal rejects keys over 64 bytes, so Gush cuts longer ones to fit, between characters. Keys that agree up to the cut share a group. Child flows and steps inherit both settings.

Both depend on the server, as [Temporal's documentation](https://docs.temporal.io/develop/task-queue-priority-fairness) describes. Priority is on by default from Temporal Server 1.31; on 1.29 and 1.30, set `matching.useNewMatcher=true`. Fairness needs `matching.enableFairness=true` on a self-hosted server. `make local` starts a dev server with both.

## Suspend and resume

A step can finish later, when a person or another system decides the result. `Suspend` frees the worker in the meantime.

```go
func ApproveExpense(ctx *gush.StepContext) (any, error) {
    err := sendApprovalRequest(ctx, ctx.ResumeToken, ctx.Input)
    if err != nil {
        return nil, err
    }
    return ctx.Suspend()
}

// In the flow. The attempt timeout keeps running while the step waits.
var decision Decision
err := ctx.RunStep(ApproveExpense, expense, &decision,
    gush.WithAttemptTimeout(24*time.Hour),
    gush.WithMaxAttempts(1),
)

// Anywhere else, once decided.
err = client.Resume(ctx, resumeToken, decision, nil)
```

**Set `WithAttemptTimeout` to the longest acceptable wait.** The step's timeout keeps running while it is suspended, and when it expires the step retries and runs its code again, sending a second request. The default is 2 minutes and 10 attempts. Use `WithMaxAttempts(1)` unless running the code twice is safe.

An error passed to `Resume` counts as a failed attempt, so a plain error runs the code again too. To reject for good, pass `gush.NewPermanentError("rejected", "Rejected", nil)`.

## Changing running flows

Flows can run for a long time, and a replay must take the same path it took originally. `Version` keeps old executions on the old code.

```go
v := ctx.Version("payment-provider", 1)
if v == gush.DefaultFlowVersion {
    err = ctx.RunStep(OldCharge, input, &output)
} else {
    err = ctx.RunStep(NewCharge, input, &output)
}
```

`Version` accepts every recorded version, so once an old branch is deleted, an execution that recorded it runs the newer one. To make such an execution fail instead, call the SDK's `workflow.GetVersion` with a higher minimum.

## Long-running flows

A flow's history grows with every step, and Temporal limits its size. A flow that loops for a long time ends the run and starts again with an empty history, keeping its flow ID and log attributes:

```go
if batch.Done {
    return summary, nil
}
return nil, ctx.ContinueAsNew(batch.Next)
```

## Logging

Every flow and step context logs directly, and so does a `gush.NewLogger()` anywhere else.

```go
func ProcessData(ctx *gush.StepContext) (any, error) {
    ctx.Time("processing")
    ctx.Printf("processing %d bytes", len(ctx.Input))
    ctx.PrintWithAttributes("batch ready", gush.Attr("batch_size", 10))
    ctx.TimeEnd("processing", "done")
    return nil, nil
}
```

Each record carries `flow_id`, the source `file` and `line`, and the attributes inherited from the run. A child flow has its own `flow_id`, so set an attribute where a run starts to tie the whole tree together:

```go
err := client.RunFlow(ctx, request, gush.WithLogAttribute("tenant", "acme"))
```

Lines written while Temporal replays a flow are dropped, so each appears once. Helpers that only have a `context.Context`, even one derived with `context.WithTimeout`, can log with the step's context through `gush.AsStepContext(ctx).Printf(...)`.

### Export over OTLP

`gush.Dial` starts exporting when an endpoint is set, and `Close` flushes. A process that logs without a client calls `gush.InitLogExport()` itself. The first variable set here wins:

| Variable | Protocol |
|---|---|
| `OTEL_ENDPOINT` | OTLP/gRPC. A bare `host:port` is plaintext; a URL's scheme chooses, and `https://` means TLS. |
| `OTEL_HTTP_ENDPOINT` | OTLP/HTTP, as an `http://` or `https://` URL. Logs go to `/v1/logs`. |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT` | The [standard OpenTelemetry variables](https://opentelemetry.io/docs/specs/otel/protocol/exporter/), with their protocol, header and TLS settings. |

An application with its own logging pipeline keeps control: a provider set with OpenTelemetry's `global.SetLoggerProvider` always wins, and Gush uses it instead of starting an export. Importing Gush never changes that global. With no provider and no endpoint, logs go to standard error, as do the Temporal SDK's own messages unless `Options.Logger` says otherwise.

## Workers

```go
worker := gush.NewWorker("orders").
    WithSteps(Charge, Ship).
    WithFlows(Fulfill).
    WithMaxSteps(100).
    WithStepRate(50).
    WithStopTimeout(time.Minute)

err := client.Serve(worker)
```

`Serve` blocks until the process receives SIGINT or SIGTERM or the worker's `WithContext` is canceled. Run more workers on other machines to share a queue. `GetQueueStats` reports backlog and rates for scaling decisions:

```go
stats, err := client.GetQueueStats(ctx, "orders")
if err != nil {
    return err
}
fmt.Println(stats[gush.QueueTypeStep].ApproximateBacklogCount)
```

## Connecting

`gush.Options` is the SDK's `client.Options`, including TLS, namespaces and credentials. For Temporal Cloud with an API key:

```go
options := gush.Options{
    HostPort:    "my-namespace.a1b2c.tmprl.cloud:7233",
    Namespace:   "my-namespace.a1b2c",
    Credentials: client.NewAPIKeyStaticCredentials(apiKey),
}
c, err := gush.Dial(options)
```

An empty `HostPort` falls back to `TEMPORAL_ENDPOINT`, then `TEMPORAL_ADDRESS`, then `localhost:7233`. An empty `Namespace` falls back to `TEMPORAL_NAMESPACE`, then the default namespace.

The package-level functions, such as `gush.RunFlow` and `gush.Serve`, use a shared client configured from those variables. They suit a process that talks to one server.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
