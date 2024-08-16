package rego

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
)

var ErrInternal = errors.New("internal error")

type Worker[S any] struct {
	*Coroutine
	*FnSink[S]
}

type Request[S, T any] interface {
	Handle(ctx context.Context, state *S, reply *Reply[T])
}

type FnSinker[S any] interface {
	WriteWithCtx(ctx context.Context, value StateFn[S]) error
}

func SendRequest[S, T any](ctx context.Context, fnSinker FnSinker[S], req Request[S, T]) (reply *Reply[T], err error) {
	reply = NewReply[T]()
	err = fnSinker.WriteWithCtx(ctx, func(ctx context.Context, s *S) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("recovered, %v\n%s\n", r, string(debug.Stack()))
				err := fmt.Errorf("%w: %v", ErrInternal, r)
				reply.Reject(err)
			}
		}()
		req.Handle(ctx, s, reply)
	})
	return
}

func AwaitResult[S, T any](ctx context.Context, fnSinker FnSinker[S], req Request[S, T]) (ok T, err error) {
	reply, err := SendRequest(ctx, fnSinker, req)
	if err != nil {
		return
	}
	return reply.Await(ctx)
}

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
