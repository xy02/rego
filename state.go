package rego

type state[T any] struct {
	value    T
	next     *state[T]
	nextDone chan struct{}
}

func newState[T any](value T) *state[T] {
	return &state[T]{
		value:    value,
		nextDone: make(chan struct{}),
	}
}

func (s *state[T]) update(value T) *state[T] {
	s.next = newState(value)
	close(s.nextDone)
	return s.next
}
