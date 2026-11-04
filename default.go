package gush

import (
	"context"
	"sync"
)

var (
	defaultClientMu sync.Mutex
	defaultClient   *Client
)

// DefaultClient returns the process-wide client behind the package-level functions,
// connecting on first use with the settings Dial reads from the environment. A failed
// connection is not remembered, so the next call tries again.
func DefaultClient() (*Client, error) {
	defaultClientMu.Lock()
	defer defaultClientMu.Unlock()

	if defaultClient != nil {
		return defaultClient, nil
	}

	var options Options
	connected, err := Dial(options)
	if err != nil {
		return nil, err
	}
	defaultClient = connected
	return defaultClient, nil
}

// Serve calls Serve on the default client.
func Serve(workers ...*Worker) error {
	c, err := DefaultClient()
	if err != nil {
		return err
	}
	return c.Serve(workers...)
}

// RunFlow calls RunFlow on the default client.
func RunFlow(ctx context.Context, request FlowRequest, options ...FlowOption) error {
	c, err := DefaultClient()
	if err != nil {
		return err
	}
	return c.RunFlow(ctx, request, options...)
}

// StartFlow calls StartFlow on the default client.
func StartFlow(ctx context.Context, request FlowRequest, options ...FlowOption) (string, error) {
	c, err := DefaultClient()
	if err != nil {
		return "", err
	}
	return c.StartFlow(ctx, request, options...)
}

// CancelFlow calls CancelFlow on the default client.
func CancelFlow(ctx context.Context, flowID string) error {
	c, err := DefaultClient()
	if err != nil {
		return err
	}
	return c.CancelFlow(ctx, flowID)
}

// TerminateFlow calls TerminateFlow on the default client.
func TerminateFlow(ctx context.Context, flowID, reason string) error {
	c, err := DefaultClient()
	if err != nil {
		return err
	}
	return c.TerminateFlow(ctx, flowID, reason)
}

// GetFlow calls GetFlow on the default client.
func GetFlow(ctx context.Context, flowID string) (*FlowInfo, error) {
	c, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return c.GetFlow(ctx, flowID)
}

// Resume calls Resume on the default client.
func Resume(ctx context.Context, resumeToken string, result any, stepErr error) error {
	c, err := DefaultClient()
	if err != nil {
		return err
	}
	return c.Resume(ctx, resumeToken, result, stepErr)
}

// GetQueueStats calls GetQueueStats on the default client.
func GetQueueStats(ctx context.Context, queue string) (map[QueueType]*QueueStats, error) {
	c, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return c.GetQueueStats(ctx, queue)
}

// Close closes the default client if it was connected. A later package-level call
// connects again.
func Close() {
	defaultClientMu.Lock()
	defer defaultClientMu.Unlock()

	if defaultClient == nil {
		return
	}
	defaultClient.Close()
	defaultClient = nil
}
