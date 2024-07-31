package rego

import (
	"context"
	"fmt"
	"sync"
)

type Stream[T any] struct {
	ctx       context.Context
	handler   Handler
	subChan   chan *subscription[T]
	sink      *Sink[T]
	once      *sync.Once
	generator func(ctx context.Context, sink *Sink[T]) error
	sinkSize  int
}

type subscription[T any] struct {
	key     string
	writeCh chan<- T
	take    int
	taken   int
}

func NewStream[T any](ctx context.Context, sinkSize int, generator func(ctx context.Context, sink *Sink[T]) error) *Stream[T] {
	subChan := make(chan *subscription[T], 1)
	sink, sinkCh := NewSink[T](ctx, sinkSize)
	handler := func(ctx context.Context) (err error) {
		subMap := map[string]*subscription[T]{}
		for {
			select {
			case <-ctx.Done():
				return
			case sub := <-subChan:
				if sub.writeCh == nil {
					delete(subMap, sub.key)
					continue
				}
				subMap[sub.key] = sub
			case data := <-sinkCh:
				for _, sub := range subMap {
					if sub.take != 0 && sub.take == sub.taken {
						delete(subMap, sub.key)
						continue
					}
					sub.writeCh <- data
					sub.taken++
				}
			}
		}
	}
	return &Stream[T]{
		ctx,
		handler,
		subChan,
		sink,
		&sync.Once{},
		generator,
		sinkSize,
	}
}

func (s *Stream[T]) Start() (co *Coroutine) {
	s.once.Do(func() {
		ctx := Go(s.ctx, func(ctx context.Context) error {
			return s.generator(ctx, s.sink)
		}).Ctx()
		co = Go(ctx, s.handler)
	})
	return
}

// count 0: infinite
func (s *Stream[T]) Take(count int) (result <-chan T) {
	ch := make(chan T, s.sinkSize)
	result = ch
	key := fmt.Sprintf("%p", result)
	s.subChan <- &subscription[T]{
		key,
		ch,
		count,
		0,
	}
	return
}
