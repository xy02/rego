package rego

import (
	"context"
	"time"
)

type Sink[T any] struct {
	ctx context.Context
	ch  chan T
}

func (s *Sink[T]) WriteWithTimeout(value T, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- value:
		return nil
	}
}

func (s *Sink[T]) Write(value T) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.ch <- value:
		return nil
	}
}

func (s *Sink[T]) WriteWithCtx(ctx context.Context, value T) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- value:
		return nil
	}
}

func NewSink[T any](ctx context.Context, size int) (*Sink[T], <-chan T) {
	ch := make(chan T, size)
	return &Sink[T]{
		ctx,
		ch,
	}, ch
}
