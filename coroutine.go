package rego

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
)

type Coroutine struct {
	cancel context.CancelFunc
	endCh  chan struct{}
	err    error
}

type Handler func(context.Context) error

func SimpleLoop(ctx context.Context, fn Handler) *Coroutine {
	return Go(ctx, func(ctx context.Context) error {
		for {
			err := fn(ctx)
			if err != nil {
				return err
			}
		}
	})
}

func Go(ctx context.Context, fn Handler) *Coroutine {
	ctx, cancel := context.WithCancel(ctx)
	endCh := make(chan struct{})
	c := &Coroutine{
		cancel,
		endCh,
		nil,
	}
	go func() {
		defer close(endCh)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("recovered, %v\n%s\n", r, string(debug.Stack()))
				c.err = fmt.Errorf("%w: %v", errors.New("internal error"), r)
			}
		}()
		c.err = fn(ctx)
	}()
	return c
}

func (c *Coroutine) Cancel() {
	c.cancel()
}

func (c *Coroutine) Await(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.endCh:
		return c.err
	}
}

func (c *Coroutine) EndChan() <-chan struct{} {
	return c.endCh
}
