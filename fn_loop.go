package rego

import "context"

type StateFn[S any] func(context.Context, *S) error

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
func Retry[S any](ctx context.Context, fn func() error) *Coroutine {
	return Go(ctx, func(ctx context.Context) error {
		for {
			err := fn()
			if err != nil {
				continue
			}
			return nil
		}
	})
}
