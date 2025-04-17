package rego

type watchRequest[T any] struct {
	// watcherID string
	replyCh chan<- *Watcher[T]
}
