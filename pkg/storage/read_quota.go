package storage

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ajwerner/tdigest"
	"github.com/cockroachdb/cockroach/pkg/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type readQuota struct {
	qp *quotapool.IntPool
	s  *Store
	mu struct {
		syncutil.Mutex
		totalWait time.Duration
		minWait   time.Duration
		maxWait   time.Duration
		requests  int64
	}
	acquisitionPool sync.Pool
}

func (rq *readQuota) putAcquisition(a *acquisition) {
	rq.acquisitionPool.Put(a)
}

func (rq *readQuota) getAcquisition(p admission.Priority) *acquisition {
	a := rq.acquisitionPool.Get().(*acquisition)
	*a = acquisition{
		rq: rq,
		p:  p,
	}
	return a
}

func (rq *readQuota) WaitStats() (avg, min, max time.Duration, len, requests int64) {
	rq.mu.Lock()
	avg = time.Duration(float64(rq.mu.totalWait) / float64(rq.mu.requests))
	min = rq.mu.minWait
	max = rq.mu.maxWait
	requests = rq.mu.requests
	rq.mu.totalWait = 0
	rq.mu.maxWait = 0
	rq.mu.minWait = 0
	rq.mu.requests = 0
	rq.mu.Unlock()
	return avg, min, max, int64(rq.qp.Len()), requests
}

// acquisition exists to avoid having to allocate a closure around the priority
// value to use quotapool.IntPool.AcquireFunc.
type acquisition struct {
	rq    *readQuota
	p     admission.Priority
	guess int64
}

func (a *acquisition) acquireFunc(ctx context.Context, quota int64) (fulfilled bool, took int64) {
	if a.guess == 0 {
		a.guess = a.rq.guessReadSize(a.p.Level)
	}
	if log.V(1) {
		log.Infof(ctx, "attempting to acquire %v %v", a.guess, quota)
	}
	if a.guess <= quota {
		return true, a.guess
	}
	return false, 0
}

func (rq *readQuota) acquire(
	ctx context.Context, p admission.Priority,
) (*quotapool.IntAlloc, error) {
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
	rq.mu.Lock()
	if took > rq.mu.maxWait {
		rq.mu.maxWait = took
	}
	if rq.mu.minWait == 0 || took < rq.mu.minWait {
		rq.mu.minWait = took
	}
	rq.mu.totalWait += took
	rq.mu.requests++
	rq.mu.Unlock()
	rq.s.metrics.ReadQuotaAcquisitions.Inc(1)
	rq.s.metrics.ReadQuotaTimeSpentWaitingRate10s.Add(float64(took.Nanoseconds()))
	rq.s.metrics.ReadQuotaTimeSpentWaitingSummary10s.Add(float64(took.Nanoseconds()))
}

const (
	bias    = .2
	maxSize = 1 << 28
)

func (s *Store) initializeReadQuota() {
	s.readQuota = readQuota{
		qp: quotapool.NewIntPool("read quota", 1<<28,
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

func queryReadResponseSizes(s *StoreMetrics, level uint8, f func(tdigest.Reader)) {
	switch level {
	case admission.MaxLevel:
		s.ReadResponseSizeMaxLevel.ReadStale(f)
	case admission.DefaultLevel:
		s.ReadResponseSizeDefLevel.ReadStale(f)
	case admission.MinLevel:
		s.ReadResponseSizeMinLevel.ReadStale(f)
	}
}

func (rq *readQuota) avgReadSize(level uint8) (avg int64) {
	queryReadResponseSizes(rq.s.metrics, level, func(td tdigest.Reader) {
		avg = int64(td.TotalSum() / td.TotalCount())
	})
	return avg
}

func (rq *readQuota) guessReadSize(level uint8) (guess int64) {
	queryReadResponseSizes(rq.s.metrics, level, func(td tdigest.Reader) {
		q := bias * rand.Float64()
		q += (1 - q) * rand.Float64()
		guess = int64(td.ValueAt(q)) + 1
	})
	return guess
}
