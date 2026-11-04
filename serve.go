package gush

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Serve runs workers until the process receives SIGINT or SIGTERM or every worker's context
// is canceled, then returns once they have stopped. It returns an error if the workers are
// misconfigured or one fails, and a failed worker stops the others.
func (c *Client) Serve(workers ...*Worker) error {
	err := validateWorkers(workers)
	if err != nil {
		return err
	}

	failed := make(chan struct{})
	var failOnce sync.Once
	errs := make([]error, len(workers))

	var running sync.WaitGroup
	for i, w := range workers {
		running.Go(func() {
			err := c.runWorker(w, failed)
			if err != nil {
				errs[i] = fmt.Errorf("gush: worker on queue %q: %w", w.Queue, err)
				failOnce.Do(func() { close(failed) })
			}
		})
	}
	running.Wait()

	return errors.Join(errs...)
}

// validateWorkers reports what Temporal would otherwise panic on or never run.
func validateWorkers(workers []*Worker) error {
	if len(workers) == 0 {
		return errors.New("gush: Serve needs at least one worker")
	}

	for _, w := range workers {
		if len(w.Steps) == 0 && len(w.Flows) == 0 {
			return fmt.Errorf("gush: worker on queue %q has no flows or steps", w.Queue)
		}

		stepNames := make(map[string]bool, len(w.Steps))
		for _, step := range w.Steps {
			if step.Name == "" || stepNames[step.Name] {
				return fmt.Errorf("gush: worker on queue %q has an empty or repeated step name %q", w.Queue, step.Name)
			}
			stepNames[step.Name] = true
		}

		flowNames := make(map[string]bool, len(w.Flows))
		for _, flow := range w.Flows {
			if flow.Name == "" || flowNames[flow.Name] {
				return fmt.Errorf("gush: worker on queue %q has an empty or repeated flow name %q", w.Queue, flow.Name)
			}
			flowNames[flow.Name] = true
		}
	}
	return nil
}

func (c *Client) runWorker(w *Worker, failed <-chan struct{}) error {
	workerOptions := worker.Options{
		WorkerStopTimeout:                      w.StopTimeout,
		TaskQueueActivitiesPerSecond:           w.QueueRate,
		WorkerActivitiesPerSecond:              w.StepRate,
		MaxConcurrentActivityTaskPollers:       w.StepPollers,
		MaxConcurrentActivityExecutionSize:     w.MaxSteps,
		MaxConcurrentWorkflowTaskExecutionSize: w.MaxFlowTasks,
		MaxConcurrentWorkflowTaskPollers:       w.FlowPollers,
	}
	temporalWorker := worker.New(c.temporalClient, w.Queue, workerOptions)

	for _, flow := range w.Flows {
		registerOptions := workflow.RegisterOptions{Name: flow.Name}
		temporalWorker.RegisterWorkflowWithOptions(flowEntry(flow), registerOptions)
	}
	for _, step := range w.Steps {
		registerOptions := activity.RegisterOptions{Name: step.Name}
		temporalWorker.RegisterActivityWithOptions(stepEntry(step), registerOptions)
	}

	workerContext := w.Context
	if workerContext == nil {
		workerContext = context.Background()
	}

	stop := make(chan any)
	go func() {
		select {
		case <-worker.InterruptCh():
		case <-workerContext.Done():
		case <-failed:
		}
		close(stop)
	}()

	return temporalWorker.Run(stop)
}

// flowEntry adapts a flow to what Temporal calls. The arguments and result are the wire
// format: a zstd-compressed payload and the log attributes.
func flowEntry(flow *FlowHandler) func(workflow.Context, []byte, map[string]any) ([]byte, error) {
	return func(ctx workflow.Context, input []byte, logAttributes map[string]any) ([]byte, error) {
		flowCtx := NewFlowContext(ctx, logAttributes)

		decompressed, err := zstdDecompress(input)
		if err != nil {
			return nil, flowCtx.Abort(err)
		}
		flowCtx.Input = decompressed

		result, err := flow.Func(flowCtx)
		if err != nil {
			return nil, temporalError(err)
		}

		payload, err := encodePayload(result)
		if err != nil {
			return nil, flowCtx.Abort(err)
		}
		return payload, nil
	}
}

// stepEntry adapts a step to what Temporal calls. See flowEntry for the wire format.
func stepEntry(step *StepHandler) func(context.Context, []byte, map[string]any) ([]byte, error) {
	return func(ctx context.Context, input []byte, logAttributes map[string]any) ([]byte, error) {
		stepCtx := NewStepContext(ctx, logAttributes)

		decompressed, err := zstdDecompress(input)
		if err != nil {
			return nil, stepCtx.Abort(err)
		}
		stepCtx.Input = decompressed

		result, err := step.Func(stepCtx)
		if err != nil {
			return nil, temporalError(err)
		}

		payload, err := encodePayload(result)
		if err != nil {
			return nil, stepCtx.Abort(err)
		}
		return payload, nil
	}
}
