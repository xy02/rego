package rego

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

func (w *Watcher[T]) NextDone() <-chan struct{} {
	return w.state.nextDone
}

func (w *Watcher[T]) Next() {
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

func (w *Watcher[T]) Close() {
	w.prop.unwatchCh <- w.id
	w.state = nil
	w.prop = nil
}
