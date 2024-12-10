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

// type RequestHandler[S, T any] func(ctx context.Context, state *S, reply *Reply[T])

type Ctx[S, T any] struct {
	context.Context
	State    *S
	Reply    *Reply[T]
	FnSinker FnSinker[S]
}

type CtxHandler[S, T any] func(ctx Ctx[S, T])
type CtxHandlerWithErr[S, T any] func(ctx Ctx[S, T]) error

type FnSinker[S any] interface {
	WriteWithCtx(ctx context.Context, value StateFn[S]) error
}

func AssignWithReply[S, T any](ctx context.Context, fnSinker FnSinker[S], handler CtxHandlerWithErr[S, T], reply *Reply[T]) (err error) {
	if reply == nil {
		return errors.New("reply is nil")
	}
	return fnSinker.WriteWithCtx(ctx, func(ctx context.Context, s *S) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("recovered, %v\n%s\n", r, string(debug.Stack()))
				err := fmt.Errorf("%w: %v", ErrInternal, r)
				reply.Reject(err)
			}
		}()
		err := handler(Ctx[S, T]{
			ctx,
			s,
			reply,
			fnSinker,
		})
		if err != nil {
			reply.Reject(err)
		}
	})
}

func Assign[S, T any](ctx context.Context, fnSinker FnSinker[S], handler CtxHandlerWithErr[S, T]) (reply *Reply[T], err error) {
	reply = NewReply[T]()
	err = AssignWithReply(ctx, fnSinker, handler, reply)
	return
}

func AwaitV2[S, T any](ctx context.Context, fnSinker FnSinker[S], handler CtxHandlerWithErr[S, T]) (ok T, err error) {
	reply := NewReply[T]()
	err = AssignWithReply(ctx, fnSinker, handler, reply)
	if err != nil {
		return
	}
	return reply.Await(ctx)
}

func SendWithReply[S, T any](ctx context.Context, fnSinker FnSinker[S], handler CtxHandler[S, T], reply *Reply[T]) (err error) {
	if reply == nil {
		return errors.New("reply is nil")
	}
	return fnSinker.WriteWithCtx(ctx, func(ctx context.Context, s *S) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("recovered, %v\n%s\n", r, string(debug.Stack()))
				err := fmt.Errorf("%w: %v", ErrInternal, r)
				reply.Reject(err)
			}
		}()
		handler(Ctx[S, T]{
			ctx,
			s,
			reply,
			fnSinker,
		})
	})
}

func Send[S, T any](ctx context.Context, fnSinker FnSinker[S], handler CtxHandler[S, T]) (reply *Reply[T], err error) {
	reply = NewReply[T]()
	err = SendWithReply(ctx, fnSinker, handler, reply)
	return
}

func Await[S, T any](ctx context.Context, fnSinker FnSinker[S], handler CtxHandler[S, T]) (ok T, err error) {
	reply, err := Send(ctx, fnSinker, handler)
	if err != nil {
		return
	}
	return reply.Await(ctx)
}

// old
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

// old
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
