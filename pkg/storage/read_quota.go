package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/qos"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var disableReadQuota = settings.RegisterBoolSetting(
	"kv.read_quota.disabled",
	"set to true to disable the read quota.",
	false,
)

// TODO(ajwerner): explore other cases where read only requests should not be
// blocked. Internal requests should already receive a high priority.
func requiresReadQuota(r *Replica, ba *roachpb.BatchRequest) bool {
	if ba.Txn != nil && ba.Txn.Key != nil {
		return false
	}
	if disableReadQuota.Get(&r.store.ClusterSettings().SV) {
		return false
	}
	if _, isQueryTxnRequest := ba.GetArg(roachpb.QueryTxn); isQueryTxnRequest {
		return false
	}
	if _, isQueryIntent := ba.GetArg(roachpb.QueryIntent); isQueryIntent {
		return false
	}
	return true
}

type readQuota struct {
	qp *quotapool.IntPool
	s  *Store
	mu struct {
		syncutil.RWMutex
		minWaitNanos   int64
		maxWaitNanos   int64
		totalWaitNanos int64
		numRequests    int64
	}
	acquisitionPool sync.Pool
}

func (rq *readQuota) putAcquisition(a *acquisition) {
	rq.acquisitionPool.Put(a)
}

func (rq *readQuota) getAcquisition(l qos.Level) *acquisition {
	a := rq.acquisitionPool.Get().(*acquisition)
	*a = acquisition{rq: rq, l: l}
	return a
}

func (rq *readQuota) WaitStats() (avg, min, max time.Duration, queueLen, requests int64) {
	rq.mu.Lock()

	total := time.Duration(atomic.LoadInt64(&rq.mu.totalWaitNanos))
	min = time.Duration(atomic.LoadInt64(&rq.mu.minWaitNanos))
	max = time.Duration(atomic.LoadInt64(&rq.mu.maxWaitNanos))
	requests = atomic.LoadInt64(&rq.mu.numRequests)

	atomic.StoreInt64(&rq.mu.totalWaitNanos, 0)
	atomic.StoreInt64(&rq.mu.minWaitNanos, 0)
	atomic.StoreInt64(&rq.mu.maxWaitNanos, 0)
	atomic.StoreInt64(&rq.mu.numRequests, 0)

	rq.mu.Unlock()

	if requests > 0 {
		// Integer division is fine, 1ns is not long.
		avg = total / time.Duration(requests)
	} // else { avg = 0 }

	return avg, min, max, int64(rq.qp.Len()), requests
}

// acquisition exists to avoid having to allocate a closure around the priority
// value to use quotapool.IntPool.AcquireFunc.
type acquisition struct {
	rq    *readQuota
	l     qos.Level
	guess int64
}

func (a *acquisition) acquireFunc(ctx context.Context, quota int64) (fulfilled bool, took int64) {
	// if a.guess == 0 {
	// 	a.guess = a.rq.guessReadSize(a.l.Class)
	// }
	if log.V(1) {
		log.Infof(ctx, "attempting to acquire %v %v", a.guess, quota)
	}
	if a.guess <= quota {
		return true, 1
	}
	return false, 0
}

func (rq *readQuota) acquire(ctx context.Context, p qos.Level) (*quotapool.IntAlloc, error) {
	a := rq.getAcquisition(p)
	defer rq.putAcquisition(a)
	return rq.qp.AcquireFunc(ctx, a.acquireFunc)
}

func (rq *readQuota) onAcquisition(
	ctx context.Context, poolName string, r quotapool.Request, start time.Time,
) {
	took := timeutil.Since(start)
	if log.V(1) {
		log.Infof(ctx, "acquire took %v for %v", took, r)
	}
	rq.mu.RLock()
	setIfGt(&rq.mu.maxWaitNanos, int64(took))
	setIfLt(&rq.mu.minWaitNanos, int64(took))
	atomic.AddInt64(&rq.mu.totalWaitNanos, int64(took))
	atomic.AddInt64(&rq.mu.numRequests, 1)
	rq.mu.RUnlock()
	//rq.s.metrics.ReadQuotaAcquisitions.Inc(1)
	//rq.s.metrics.ReadQuotaTimeSpentWaitingRate10s.Add(float64(took.Nanoseconds()))
	//rq.s.metrics.ReadQuotaTimeSpentWaitingSummary10s.Add(float64(took.Nanoseconds()))
}

func setIf(p *int64, v int64, predicate func(v, pv int64) bool) {
	for {
		pv := atomic.LoadInt64(p)
		if !predicate(v, pv) {
			return
		}
		if atomic.CompareAndSwapInt64(p, pv, v) {
			return
		}
	}
}

func lt(a, b int64) bool { return a < b }
func gt(a, b int64) bool { return a > b }

func setIfGt(p *int64, v int64) { setIf(p, v, gt) }
func setIfLt(p *int64, v int64) { setIf(p, v, lt) }

// const (
// 	bias    = .2
// 	maxSize = 1 << 28
// )

func (s *Store) initializeReadQuota() {
	s.readQuota = readQuota{
		qp: quotapool.NewIntPool("read quota", 512,
			quotapool.LogSlowAcquisition,
			quotapool.OnAcquisition(s.readQuota.onAcquisition)),
		s: s,
		acquisitionPool: sync.Pool{
			New: func() interface{} {
				return new(acquisition)
			},
		},
	}
}

// func queryReadResponseSizes(s *StoreMetrics, c qos.Class, f func(tdigest.Reader)) {
// 	switch c {
// 	case qos.ClassHigh:
// 		s.ReadResponseSizeMaxLevel.ReadStale(f)
// 	case qos.ClassDefault:
// 		s.ReadResponseSizeDefLevel.ReadStale(f)
// 	case qos.ClassLow:
// 		s.ReadResponseSizeMinLevel.ReadStale(f)
// 	}
// }

// func (rq *readQuota) avgReadSize(c qos.Class) (avg int64) {
// 	queryReadResponseSizes(rq.s.metrics, c, func(td tdigest.Reader) {
// 		avg = int64(td.TotalSum() / td.TotalCount())
// 	})
// 	return avg
// }

// func (rq *readQuota) guessReadSize(c qos.Class) (guess int64) {
// 	queryReadResponseSizes(rq.s.metrics, c, func(td tdigest.Reader) {
// 		q := bias * rand.Float64()
// 		q += (1 - q) * rand.Float64()
// 		guess = int64(td.ValueAt(q)) + 1
// 	})
// 	return guess
// }
