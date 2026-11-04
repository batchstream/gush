// Quickstart runs a worker and one flow in the same process. Start a Temporal server first,
// for example with `temporal server start-dev`.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/batchstream/gush"
)

const queue = "orders"

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

type Receipt struct {
	OrderID string `json:"order_id"`
	Charged int    `json:"charged"`
}

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

	workerContext, stopWorker := context.WithCancel(context.Background())
	worker := gush.NewWorker(queue).
		WithSteps(Charge).
		WithFlows(Fulfill).
		WithContext(workerContext)

	served := make(chan error, 1)
	go func() { served <- client.Serve(worker) }()

	order := Order{ID: "order-1", Amount: 42}
	var receipt Receipt
	request := gush.FlowRequest{Queue: queue, Flow: Fulfill, Input: order, Output: &receipt}
	err = client.RunFlow(context.Background(), request, gush.WithTimeout(time.Minute))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("charged %d for %s\n", receipt.Charged, receipt.OrderID)

	stopWorker()
	err = <-served
	if err != nil {
		log.Fatal(err)
	}
}
