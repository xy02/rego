package rego

import "context"

type StateFn[S any] func(context.Context, *S) error

type FnSink[S any] struct {
	*Sink[StateFn[S]]
}

func NewFnSink[S any](ctx context.Context, size int) (*FnSink[S], <-chan StateFn[S]) {
	sink, ch := NewSink[StateFn[S]](ctx, size)
	return &FnSink[S]{
		sink,
	}, ch
}

func HandlerLoop[S any](ctx context.Context, state *S, requestChan <-chan StateFn[S], onInit func() error, onErr func(error)) *Coroutine {
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
				return fn(ctx, state)
			}
		}
	})
}

// retry fn util it return nil
func Retry(ctx context.Context, fn func(context.Context) error) *Coroutine {
	return Go(ctx, func(ctx context.Context) error {
		for {
			err := fn(ctx)
			if err != nil {
				continue
			}
			return nil
		}
	})
}
