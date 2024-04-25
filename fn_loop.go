package rego

import (
	"context"
	"time"
)

type StateFn[S any] func(context.Context, *S)

type FnSink[S any] struct {
	*Sink[StateFn[S]]
}

type FnSinkLoop[S any] struct {
	*Coroutine
	*FnSink[S]
}

func NewFnSink[S any](ctx context.Context, size int) (*FnSink[S], <-chan StateFn[S]) {
	sink, ch := NewSink[StateFn[S]](ctx, size)
	return &FnSink[S]{
		sink,
	}, ch
}

func AwaitReply[S, V any](ctx context.Context, fnSink *FnSink[S], fn StateFn[S], reply *Reply[V]) (ok V, err error) {
	err = fnSink.Write(fn)
	if err != nil {
		return
	}
	return reply.Await(ctx)
}

func RetryFnLoop[S any](ctx context.Context, state *S, requestChan <-chan StateFn[S], onInit func() error, onErr func(error), sleep time.Duration) *Coroutine {
	return Retry(ctx, func(ctx context.Context) error {
		co := FnLoop(ctx, state, requestChan, onInit, onErr)
		err := co.Await(ctx)
		if err != nil {
			time.Sleep(sleep)
		}
		return err
	})
}

func FnLoop[S any](ctx context.Context, state *S, requestChan <-chan StateFn[S], onInit func() error, onErr func(error)) *Coroutine {
	return Go(ctx, func(ctx context.Context) (e error) {
		defer func() {
			if onErr != nil {
				onErr(e)
			}
		}()
		if onInit != nil {
			e = onInit()
			if e != nil {
				return
			}
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case fn := <-requestChan:
				fn(ctx, state)
			}
		}
	})
}

// retry fn util it return nil
func Retry(ctx context.Context, fn func(context.Context) error) *Coroutine {
	return Go(ctx, func(ctx context.Context) error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			err := fn(ctx)
			if err != nil {
				continue
			}
			return nil
		}
	})
}
