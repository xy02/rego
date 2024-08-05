package rego

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
)

type Worker[S any] struct {
	*Coroutine
	*FnSink[S]
}

var ErrInternal = errors.New("internal error")

func DoWithRecover(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered, %v\n%s\n", r, string(debug.Stack()))
			err = fmt.Errorf("%w: %v", ErrInternal, r)
		}
	}()
	fn()
	return
}

func StartSimpleWorker[S any](ctx context.Context, state S, sinkSize int) Worker[S] {
	ctx, cancel := context.WithCancel(ctx)
	sink, ch := NewFnSink[S](ctx, sinkSize)
	co := Go(ctx, func(ctx context.Context) (e error) {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case fn := <-ch:
				fn(ctx, &state)
			}
		}
	})
	return Worker[S]{
		co,
		sink,
	}
}
