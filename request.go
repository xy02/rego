package rego

import (
	"context"
	"sync"
)

type Request[T, V any] struct {
	msg  T
	ok   *V
	err  error
	ch   chan struct{}
	once *sync.Once
}

func NewRequest[T, V any](msg T) *Request[T, V] {
	return &Request[T, V]{
		msg,
		nil,
		nil,
		make(chan struct{}),
		&sync.Once{},
	}
}

func (req *Request[T, V]) Msg() T {
	return req.msg
}

func (req *Request[T, V]) Resolve(value V) {
	req.once.Do(func() {
		req.ok = &value
		close(req.ch)
	})
}

func (req *Request[T, V]) Reject(err error) {
	req.once.Do(func() {
		req.err = err
		close(req.ch)
	})
}

func (req *Request[T, V]) Await(ctx context.Context) (ok V, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-req.ch:
		ok = *req.ok
		err = req.err
		return
	}
}
