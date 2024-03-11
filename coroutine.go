package rego

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
)

type Coroutine struct {
	ctx    context.Context
	cancel context.CancelFunc
	endCh  chan struct{}
	err    error
}

type Handler func(context.Context) error

type StateHandler[S any] func(context.Context, *S) error

func CommonLoop[S any](ctx context.Context, state *S, fnChan <-chan StateHandler[S], onErr func(error)) *Coroutine {
	return SimpleLoop(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case fn := <-fnChan:
			return fn(ctx, state)
		}
	}, onErr)
}

func SimpleLoop(ctx context.Context, fn Handler, onErr func(error)) *Coroutine {
	return Go(ctx, func(ctx context.Context) (e error) {
		defer func() {
			onErr(e)
		}()
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
		ctx,
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
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case <-c.endCh:
		return c.err
	}
}

func (c *Coroutine) EndChan() <-chan struct{} {
	return c.endCh
}

func (c *Coroutine) Ctx() context.Context {
	return c.ctx
}
