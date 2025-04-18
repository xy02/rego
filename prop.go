package rego

import (
	"context"
)

type Prop[T any] struct {
	co       *Coroutine
	updateCh chan T
}

func NewProp[T any](ctx context.Context, sinkSize int) *Prop[T] {
	updateCh := make(chan T, sinkSize)
	getStateCh := make(chan *State[T], 1)
	co := Go(ctx, func(ctx context.Context) error {
		state := &State[T]{
			done: make(chan struct{}),
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v := <-updateCh:
				state.value = &v
				state.next = &State[T]{
					done: make(chan struct{}),
				}
				close(state.done)
				state = state.next

			}
		}
	})
	return &Prop[T]{
		co,
		updateCh,
	}
}

func (p *Prop[T]) Update(value T) {
	p.updateCh <- value
}

func (p *Prop[T]) State() *State[T] {

}

type State[T any] struct {
	value *T
	next  *State[T]
	done  chan struct{}
}

func (s *State[T]) Done() <-chan struct{} {
	return s.done
}

func (s *State[T]) Value() (ok T) {
	if s.value != nil {
		ok = *s.value
	}
	return
}

func (s *State[T]) Next() *State[T] {
	return s.next
}

func (s *State[T]) Await(ctx context.Context) (ok T, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.done:
		if s.value != nil {
			ok = *s.value
		}
		return
	}
}
