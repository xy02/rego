package rego

import (
	"context"
	"fmt"
	"math"
)

type Property[T any] struct {
	readCh     chan chan<- T
	writeCh    chan T
	watchCh    chan watchRequest[T]
	unwatchCh  chan string
	consumedCh chan consumedACK
}

func NewProperty[T any](value T, maxUnconsumed int) *Property[T] {
	prop := &Property[T]{
		readCh:     make(chan chan<- T, 1),
		writeCh:    make(chan T, maxUnconsumed),
		watchCh:    make(chan watchRequest[T], 1),
		unwatchCh:  make(chan string, 1),
		consumedCh: make(chan consumedACK, maxUnconsumed),
	}
	go func() {
		state := newState(value)
		propSN := 0
		slowestWatcherSN := 0
		slowestWatcherID := ""
		watcherSnMap := map[string]int{}
		fakeUpdateCh := make(chan T)
		getUpdateCh := func() chan T {
			//若有慢消则返回假通道
			if slowestWatcherSN < propSN-maxUnconsumed {
				return fakeUpdateCh
			}
			return prop.writeCh
		}
		for {
			select {
			case replyCh := <-prop.readCh:
				replyCh <- state.value
			case v := <-getUpdateCh():
				state = state.update(v)
				propSN++
			case id := <-prop.unwatchCh:
				delete(watcherSnMap, id)
				if id == slowestWatcherID {
					slowestWatcherSN = math.MaxInt
					for id, sn := range watcherSnMap {
						if sn < slowestWatcherSN {
							slowestWatcherSN = sn
							slowestWatcherID = id
						}
					}
				}
			case req := <-prop.watchCh:
				w := &Watcher[T]{
					state:     state,
					threshold: maxUnconsumed >> 1,
					prop:      prop,
				}
				w.id = fmt.Sprintf("%p", w)
				watcherSnMap[w.id] = propSN
				req.replyCh <- w
				if slowestWatcherID == "" {
					slowestWatcherSN = propSN
					slowestWatcherID = w.id
				}
			case req := <-prop.consumedCh:
				//更新SN
				newSN := watcherSnMap[req.watcherID] + req.consumed
				watcherSnMap[req.watcherID] = newSN
				// log.Println(req.watcherID, "sn", newSN)
				if req.watcherID == slowestWatcherID {
					//优化,自己是最慢消费者时,对比所有
					slowestWatcherSN = newSN
					for id, sn := range watcherSnMap {
						if sn < slowestWatcherSN {
							slowestWatcherSN = sn
							slowestWatcherID = id
						}
					}
				} else {
					//不是最慢消费者时,只比一次
					if newSN < slowestWatcherSN {
						slowestWatcherSN = newSN
						slowestWatcherID = req.watcherID
					}
				}
			}
		}
	}()
	return prop
}

func (p *Property[T]) Get() T {
	replyCh := make(chan T, 1)
	p.readCh <- replyCh
	return <-replyCh
}

// func (p *Property[T]) Set(value T) {
// 	p.writeCh <- value
// }

func (p *Property[T]) WriteChan() chan<- T {
	return p.writeCh
}

func (p *Property[T]) SetWithContext(ctx context.Context, value T) bool {
	select {
	case <-ctx.Done():
		return false
	case p.writeCh <- value:
		return true
	}
}

func (p *Property[T]) Watch() *Watcher[T] {
	replyCh := make(chan *Watcher[T], 1)
	p.watchCh <- watchRequest[T]{
		replyCh: replyCh,
	}
	return <-replyCh
}

func (p *Property[T]) ackConsumed(ack consumedACK) {
	p.consumedCh <- ack
}
