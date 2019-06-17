package admission

import (
	"context"
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/notifyqueue"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type waitQueue struct {
	mu         syncutil.Mutex
	nextToFree Priority
	chanPool   sync.Pool
	q          [numLevels][numShards]notifyQueue
}

type notifyQueue struct {
	notifyqueue.NotifyQueue
	canceled int
}

func (nq *notifyQueue) len() int {
	return nq.Len() - nq.canceled
}

func initWaitQueue(q *waitQueue) {
	*q = waitQueue{
		chanPool: sync.Pool{
			New: func() interface{} {
				return make(chan struct{}, 1)
			},
		},
	}
	for i := 0; i < numLevels; i++ {
		for j := 0; j < numShards; j++ {
			notifyqueue.Initialize(&q.q[i][j].NotifyQueue)
		}
	}
}

var ErrRejected = errors.New("rejected")

func (q *waitQueue) wait(ctx context.Context, p Priority) error {
	q.mu.Lock()
	ch := q.chanPool.Get().(chan struct{})
	pq := q.pq(p)
	pq.Enqueue(ch)
	q.mu.Unlock()
	select {
	case <-ch:
		q.chanPool.Put(ch)
		return nil
	case <-ctx.Done():
		q.mu.Lock()
		defer q.mu.Unlock()
		select {
		case ch <- struct{}{}:
			pq.canceled++
			// NB: this is an optimization to ensure that the data structure uses
			// memory that's a constant factor overhead above max size.
			if l := pq.Len(); l > 8 && pq.canceled > l/2 {
				q.consolidate(pq)
			}
		default:
		}

		return ctx.Err()
	}
}

func (q *waitQueue) releasePriority(p Priority, maxToFree int) (freed int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.releasePriorityLocked(p, maxToFree)
}

// releasePriority releases up to maxToFree elements from the queue.
// If maxToFree is less than or equal to zero it frees all of the elements.
func (q *waitQueue) releasePriorityLocked(p Priority, maxToFree int) (freed int) {
	for pq := q.pq(p); pq.Len() > 0; {
		ch := pq.Dequeue()
		select {
		case ch <- struct{}{}:
			if freed++; maxToFree > 0 && freed > maxToFree {
				return freed
			}
		default:
			<-ch
			pq.canceled--
			q.chanPool.Put(ch)
		}
	}
	return freed
}

func (q *waitQueue) release(to Priority, maxToFree int) (freed int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for p := maxPriority; freed < maxToFree && !p.less(to); p = p.dec() {
		freed += q.releasePriorityLocked(p, maxToFree-freed)
	}
	return freed
}

func (q *waitQueue) consolidate(pq *notifyQueue) {
	l := pq.Len()
	if l == 0 {
		return
	}
	var canceled int
	pq.Enqueue(pq.Dequeue()) // Always put the head back into the queue
	for i := 1; i < l; i++ {
		ch := pq.Dequeue()
		select {
		case <-ch:
			q.chanPool.Put(ch)
			canceled++
		default:
			pq.Enqueue(ch)
		}
	}
	pq.canceled = 0
}

func (q *waitQueue) pq(p Priority) *notifyQueue {
	return &q.q[bucketFromLevel(p.Level)][bucketFromShard(p.Shard)]
}

func (q *waitQueue) emptyAtShard(s uint8) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, l := range levels {
		if q.pq(Priority{l, s}).len() > 0 {
			return false
		}
	}
	return true
}
