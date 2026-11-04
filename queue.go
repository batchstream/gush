package gush

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/client"
)

// QueueType separates a queue's flow tasks from its step tasks.
type QueueType = client.TaskQueueType

const (
	QueueTypeFlow QueueType = client.TaskQueueTypeWorkflow
	QueueTypeStep QueueType = client.TaskQueueTypeActivity
)

// QueueStats reports the load on a task queue, for monitoring and scaling decisions.
type QueueStats struct {
	// ApproximateBacklogCount is about how many tasks are waiting. It may count expired
	// tasks but converges to the right value.
	ApproximateBacklogCount int64

	// ApproximateBacklogAge is about how long the oldest waiting task has been queued.
	ApproximateBacklogAge time.Duration

	// BacklogIncreaseRate is TasksAddRate minus TasksDispatchRate. Positive means the
	// backlog is growing.
	BacklogIncreaseRate float32

	// TasksAddRate is about how many tasks per second are added, over the last 30 seconds.
	TasksAddRate float32

	// TasksDispatchRate is about how many tasks per second reach workers, over the last
	// 30 seconds.
	TasksDispatchRate float32
}

// GetQueueStats returns a queue's load for each task type.
//
//	stats, err := gush.GetQueueStats(ctx, "my-queue")
//	if err != nil {
//	    return err
//	}
//	log.Printf("step backlog: %d", stats[gush.QueueTypeStep].ApproximateBacklogCount)
func (c *Client) GetQueueStats(ctx context.Context, queue string) (map[QueueType]*QueueStats, error) {
	describeOptions := client.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      queue,
		TaskQueueTypes: []client.TaskQueueType{QueueTypeFlow, QueueTypeStep},
		ReportStats:    true,
	}
	description, err := c.temporalClient.DescribeTaskQueueEnhanced(ctx, describeOptions)
	if err != nil {
		return nil, err
	}

	//lint:ignore SA1019 VersionsInfo is still the only field with per-task-type queue statistics.
	versionsInfo := description.VersionsInfo
	if len(versionsInfo) == 0 {
		return nil, errors.New("gush: no queue statistics found")
	}

	//lint:ignore SA1019 TaskQueueVersionInfo is still required to read those statistics.
	var versionInfo client.TaskQueueVersionInfo
	for _, info := range versionsInfo {
		versionInfo = info
		break
	}

	stats := make(map[QueueType]*QueueStats)
	for taskType, info := range versionInfo.TypesInfo {
		typeStats := &QueueStats{
			ApproximateBacklogCount: info.Stats.ApproximateBacklogCount,
			ApproximateBacklogAge:   info.Stats.ApproximateBacklogAge,
			BacklogIncreaseRate:     info.Stats.BacklogIncreaseRate,
			TasksAddRate:            info.Stats.TasksAddRate,
			TasksDispatchRate:       info.Stats.TasksDispatchRate,
		}
		stats[taskType] = typeStats
	}
	return stats, nil
}
