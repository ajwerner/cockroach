package storage

import (
	"context"
	"math/rand"
	"time"

	"github.com/ajwerner/tdigest"
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
		maxWait   time.Duration
		requests  int64
	}
}

func (rq *readQuota) WaitStats() (avg, max time.Duration) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	avg = time.Duration(float64(rq.mu.totalWait) / float64(rq.mu.requests))
	max = time.Duration(rq.mu.maxWait)
	rq.mu.totalWait = 0
	rq.mu.maxWait = 0
	rq.mu.requests = 0
	return avg, max
}

func (rq *readQuota) acquire(ctx context.Context) (*quotapool.IntAlloc, error) {
	return rq.qp.AcquireFunc(ctx, rq.acquireFunc)
}

func (rq *readQuota) onAcquisition(
	ctx context.Context, poolName string, _ quotapool.Request, start time.Time,
) {
	took := timeutil.Since(start)
	rq.mu.Lock()
	if took > rq.mu.maxWait {
		rq.mu.maxWait = took
	}
	rq.mu.totalWait += took
	rq.mu.requests++
	rq.mu.Unlock()
	rq.s.metrics.ReadQuotaAcquisitions.Inc(1)
	rq.s.metrics.ReadQuotaTimeSpentWaitingRate10s.Add(float64(took.Nanoseconds()))
	rq.s.metrics.ReadQuotaTimeSpentWaitingSummary10s.Add(float64(took.Nanoseconds()))
}

const bias = .2

func (s *Store) initializeReadQuota() {
	s.readQuota = readQuota{
		qp: quotapool.NewIntPool("read quota", int64(2*(1<<30)),
			quotapool.LogSlowAcquisition,
			quotapool.OnAcquisition(s.readQuota.onAcquisition)),
		s: s,
	}
}

func (rq *readQuota) acquireFunc(ctx context.Context, quota int64) (fulfilled bool, took int64) {
	if guess := rq.guessReadSize(); guess <= quota {
		return true, guess
	}
	return false, 0
}

func (rq *readQuota) guessReadSize() (guess int64) {
	rq.s.metrics.ReadResponseSizeSummary1m.ReadStale(func(r tdigest.Reader) {
		q := bias * rand.Float64()
		q += (1 - q) * rand.Float64()
		guess = int64(r.ValueAt(q)) + 1
	})
	return guess
}
