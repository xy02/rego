package rego

import (
	"context"
	"sync"
)

type Reply[V any] struct {
	ok   *V
	err  error
	ch   chan struct{}
	once *sync.Once
}

func NewReply[V any]() *Reply[V] {
	return &Reply[V]{
		nil,
		nil,
		make(chan struct{}),
		&sync.Once{},
	}
}

func (req *Reply[V]) Resolve(value V) {
	req.once.Do(func() {
		req.ok = &value
		close(req.ch)
	})
}

func (req *Reply[V]) Reject(err error) {
	req.once.Do(func() {
		req.err = err
		close(req.ch)
	})
}

func (req *Reply[V]) Await(ctx context.Context) (ok V, err error) {
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
