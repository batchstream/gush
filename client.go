package gush

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
)

// Options configures the connection to Temporal, including its address, namespace,
// TLS and credentials. It is the Temporal SDK's client.Options.
type Options = client.Options

// Client is a connection to a Temporal server.
type Client struct {
	temporalClient client.Client
	logShutdown    func()
	closeOnce      sync.Once
}

// Dial connects to Temporal and starts OTLP log export when it is configured; see
// InitLogExport.
//
// An empty HostPort falls back to TEMPORAL_ENDPOINT, then TEMPORAL_ADDRESS, then the
// SDK's default of localhost:7233. An empty Namespace falls back to TEMPORAL_NAMESPACE,
// then the SDK's default namespace.
func Dial(options Options) (*Client, error) {
	if options.HostPort == "" {
		options.HostPort = temporalEndpointFromEnv()
	}
	if options.Namespace == "" {
		options.Namespace = os.Getenv(envTemporalNamespace)
	}
	if options.Logger == nil {
		handlerOptions := &slog.HandlerOptions{Level: slog.LevelError}
		handler := slog.NewTextHandler(os.Stderr, handlerOptions)
		options.Logger = temporallog.NewStructuredLogger(slog.New(handler))
	}

	temporalClient, err := client.Dial(options)
	if err != nil {
		return nil, fmt.Errorf("gush: connect to Temporal: %w", err)
	}

	logShutdown, err := InitLogExport()
	if err != nil {
		temporalClient.Close()
		return nil, err
	}

	connected := &Client{temporalClient: temporalClient, logShutdown: logShutdown}
	return connected, nil
}

// Close closes the connection and flushes the logs this client started exporting.
// It is safe to call more than once.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		if c.temporalClient != nil {
			c.temporalClient.Close()
		}
		if c.logShutdown != nil {
			c.logShutdown()
		}
	})
}
