package rego

import (
	"context"
	"time"
)

type Sink[T any] struct {
	ch chan T
}

func (s *Sink[T]) Write(value T, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- value:
		return nil
	}
}

func (s *Sink[T]) WriteWithCtx(ctx context.Context, value T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- value:
		return nil
	}
}

func NewSink[T any](size int) (*Sink[T], <-chan T) {
	ch := make(chan T, size)
	return &Sink[T]{
		ch,
	}, ch
}
