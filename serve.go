package gush

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

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

	// Serve hands the signals back when it returns, so the host's own handling resumes.
	stopping, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errs := make([]error, len(workers))

	var running sync.WaitGroup
	for i, w := range workers {
		running.Go(func() {
			err := c.runWorker(w, stopping.Done())
			if err != nil {
				errs[i] = fmt.Errorf("gush: worker on queue %q: %w", w.Queue, err)
				stop()
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
			if step == nil || step.Func == nil {
				return fmt.Errorf("gush: worker on queue %q has a step with no function", w.Queue)
			}
			if step.Name == "" || stepNames[step.Name] {
				return fmt.Errorf("gush: worker on queue %q has an empty or repeated step name %q", w.Queue, step.Name)
			}
			stepNames[step.Name] = true
		}

		flowNames := make(map[string]bool, len(w.Flows))
		for _, flow := range w.Flows {
			if flow == nil || flow.Func == nil {
				return fmt.Errorf("gush: worker on queue %q has a flow with no function", w.Queue)
			}
			if flow.Name == "" || flowNames[flow.Name] {
				return fmt.Errorf("gush: worker on queue %q has an empty or repeated flow name %q", w.Queue, flow.Name)
			}
			flowNames[flow.Name] = true
		}
	}
	return nil
}

// newTemporalWorker reports the settings and names the SDK rejects by panicking.
func (c *Client) newTemporalWorker(w *Worker) (temporalWorker worker.Worker, err error) {
	defer func() {
		recovered := recover()
		if recovered != nil {
			err = fmt.Errorf("invalid worker: %v", recovered)
		}
	}()

	workerOptions := worker.Options{
		WorkerStopTimeout:                      w.StopTimeout,
		TaskQueueActivitiesPerSecond:           w.QueueRate,
		WorkerActivitiesPerSecond:              w.StepRate,
		MaxConcurrentActivityTaskPollers:       w.StepPollers,
		MaxConcurrentActivityExecutionSize:     w.MaxSteps,
		MaxConcurrentWorkflowTaskExecutionSize: w.MaxFlowTasks,
		MaxConcurrentWorkflowTaskPollers:       w.FlowPollers,
	}
	temporalWorker = worker.New(c.temporalClient, w.Queue, workerOptions)

	for _, flow := range w.Flows {
		registerOptions := workflow.RegisterOptions{Name: flow.Name}
		temporalWorker.RegisterWorkflowWithOptions(flowEntry(flow), registerOptions)
	}
	for _, step := range w.Steps {
		registerOptions := activity.RegisterOptions{Name: step.Name}
		temporalWorker.RegisterActivityWithOptions(stepEntry(step), registerOptions)
	}
	return temporalWorker, nil
}

func (c *Client) runWorker(w *Worker, stopping <-chan struct{}) error {
	temporalWorker, err := c.newTemporalWorker(w)
	if err != nil {
		return err
	}

	workerContext := w.Context
	if workerContext == nil {
		workerContext = context.Background()
	}

	stop := make(chan any)
	go func() {
		select {
		case <-stopping:
		case <-workerContext.Done():
		}
		close(stop)
	}()

	return temporalWorker.Run(stop)
}

// flowEntry adapts a flow to what Temporal calls. The arguments and result are the wire
// format: a zstd-compressed payload and the log attributes as a JSON object.
func flowEntry(flow *FlowHandler) func(workflow.Context, []byte, json.RawMessage) ([]byte, error) {
	return func(ctx workflow.Context, input []byte, rawAttributes json.RawMessage) ([]byte, error) {
		logAttributes, err := decodeLogAttributes(rawAttributes)
		flowCtx := NewFlowContext(ctx, logAttributes)
		if err != nil {
			return nil, flowCtx.Abort(err)
		}

		decompressed, err := zstdDecompress(input)
		if err != nil {
			return nil, flowCtx.Abort(err)
		}
		flowCtx.Input = decompressed

		result, err := flow.Func(flowCtx)
		if err != nil {
			return nil, temporalError(err)
		}

		payload, err := EncodePayload(result)
		if err != nil {
			return nil, flowCtx.Abort(err)
		}
		return payload, nil
	}
}

// stepEntry adapts a step to what Temporal calls. See flowEntry for the wire format.
func stepEntry(step *StepHandler) func(context.Context, []byte, json.RawMessage) ([]byte, error) {
	return func(ctx context.Context, input []byte, rawAttributes json.RawMessage) ([]byte, error) {
		logAttributes, err := decodeLogAttributes(rawAttributes)
		stepCtx := NewStepContext(ctx, logAttributes)
		if err != nil {
			return nil, stepCtx.Abort(err)
		}

		decompressed, err := zstdDecompress(input)
		if err != nil {
			return nil, stepCtx.Abort(err)
		}
		stepCtx.Input = decompressed

		result, err := step.Func(stepCtx)
		if err != nil {
			return nil, temporalError(err)
		}

		payload, err := EncodePayload(result)
		if err != nil {
			return nil, stepCtx.Abort(err)
		}
		return payload, nil
	}
}
