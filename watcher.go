package rego

import (
	"context"
)

type Watcher[T any] struct {
	id        string
	state     *state[T]
	threshold int
	consumed  int
	prop      *Property[T]
}

type consumedACK struct {
	watcherID string
	consumed  int
}

func (w *Watcher[T]) ID() string {
	return w.id
}

func (w *Watcher[T]) Value() T {
	return w.state.value
}

func (w *Watcher[T]) GetNext() T {
	<-w.state.nextDone
	w.next()
	return w.state.value
}

func (w *Watcher[T]) next() {
	w.state = w.state.next
	w.consumed++
	if w.consumed > w.threshold {
		w.prop.ackConsumed(consumedACK{
			watcherID: w.id,
			consumed:  w.consumed,
		})
		w.consumed = 0
	}
}

func (w *Watcher[T]) GetNextWithContext(ctx context.Context) (result T, ok bool) {
	select {
	case <-ctx.Done():
		ok = false
		return
	case <-w.state.nextDone:
		w.next()
		ok = true
		result = w.state.value
	}
	return
}
