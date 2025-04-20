package rego

type Watcher[T any] struct {
	id        string
	state     *state[T]
	value     T
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
	return w.value
}

func (w *Watcher[T]) NextDone() <-chan struct{} {
	return w.state.nextDone
}

func (w *Watcher[T]) Next() bool {
	if w.state == nil || w.state.next == nil {
		return false
	}
	w.state = w.state.next
	w.value = w.state.value
	w.consumed++
	if w.consumed > w.threshold {
		w.prop.ackConsumed(consumedACK{
			watcherID: w.id,
			consumed:  w.consumed,
		})
		w.consumed = 0
	}
	return true
}

func (w *Watcher[T]) Release() {
	if w.prop == nil {
		return
	}
	w.prop.unwatch(w.id)
	w.state = nil
	w.prop = nil
}

func (w *Watcher[T]) Released() bool {
	return w.prop == nil
}
