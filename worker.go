package gush

import (
	"context"
	"time"
)

// Worker configures a process's work on one task queue. NewWorker sets the defaults.
type Worker struct {
	Queue string

	// StopTimeout is how long running tasks get to finish on shutdown. Default: 30s.
	StopTimeout time.Duration

	// QueueRate is the server-side limit of steps per second, across all workers on this
	// queue. Default: 100,000.
	QueueRate float64

	// StepRate is the limit of steps per second in this worker. Default: 100,000.
	StepRate float64

	// MaxSteps is how many steps this worker runs at once. Default: 1,000.
	MaxSteps int

	// StepPollers is how many goroutines fetch steps. Default: 2.
	StepPollers int

	// MaxFlowTasks is how many flow tasks this worker runs at once. A flow only occupies a
	// task while it advances, so far more flows than this can be in progress. Default: 1,000.
	MaxFlowTasks int

	// FlowPollers is how many goroutines fetch flow tasks. Default: 2.
	FlowPollers int

	// Steps and Flows are the registered handlers.
	Steps []*StepHandler
	Flows []*FlowHandler

	// Context stops the worker when canceled.
	Context context.Context
}

// NewWorker creates a Worker for a queue.
func NewWorker(queue string) *Worker {
	worker := &Worker{
		Queue:        queue,
		StopTimeout:  30 * time.Second,
		QueueRate:    100000,
		StepRate:     100000,
		MaxSteps:     1000,
		StepPollers:  2,
		MaxFlowTasks: 1000,
		FlowPollers:  2,
		Context:      context.Background(),
	}
	return worker
}

// WithStopTimeout sets how long running tasks get to finish on shutdown.
func (w *Worker) WithStopTimeout(timeout time.Duration) *Worker {
	w.StopTimeout = timeout
	return w
}

// WithQueueRate sets the server-side limit of steps per second across this queue's workers.
func (w *Worker) WithQueueRate(rate float64) *Worker {
	w.QueueRate = rate
	return w
}

// WithStepRate sets the limit of steps per second in this worker.
func (w *Worker) WithStepRate(rate float64) *Worker {
	w.StepRate = rate
	return w
}

// WithMaxSteps sets how many steps this worker runs at once.
func (w *Worker) WithMaxSteps(limit int) *Worker {
	w.MaxSteps = limit
	return w
}

// WithStepPollers sets how many goroutines fetch steps.
func (w *Worker) WithStepPollers(count int) *Worker {
	w.StepPollers = count
	return w
}

// WithMaxFlowTasks sets how many flow tasks this worker runs at once. Temporal needs at
// least 2, so a lower value counts as 2.
func (w *Worker) WithMaxFlowTasks(limit int) *Worker {
	w.MaxFlowTasks = max(2, limit)
	return w
}

// WithFlowPollers sets how many goroutines fetch flow tasks.
func (w *Worker) WithFlowPollers(count int) *Worker {
	w.FlowPollers = count
	return w
}

// WithSteps registers steps under their Go function names, without package or receiver.
func (w *Worker) WithSteps(steps ...StepHandlerFunc) *Worker {
	for _, step := range steps {
		w.Steps = append(w.Steps, NewStepHandler(functionName(step), step))
	}
	return w
}

// WithStepHandlers registers steps under explicit names.
func (w *Worker) WithStepHandlers(steps ...*StepHandler) *Worker {
	w.Steps = append(w.Steps, steps...)
	return w
}

// WithFlows registers flows under their Go function names, without package or receiver.
func (w *Worker) WithFlows(flows ...FlowHandlerFunc) *Worker {
	for _, flow := range flows {
		w.Flows = append(w.Flows, NewFlowHandler(functionName(flow), flow))
	}
	return w
}

// WithFlowHandlers registers flows under explicit names.
func (w *Worker) WithFlowHandlers(flows ...*FlowHandler) *Worker {
	w.Flows = append(w.Flows, flows...)
	return w
}

// WithContext sets the context that stops the worker when canceled.
func (w *Worker) WithContext(ctx context.Context) *Worker {
	if ctx != nil {
		w.Context = ctx
	}
	return w
}
