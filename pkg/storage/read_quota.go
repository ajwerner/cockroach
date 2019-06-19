package storage

import (
	"context"
	"math/rand"
	"time"

	"github.com/ajwerner/tdigest"
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

func (rq *readQuota) acquire(ctx context.Context) (*quotapool.IntAlloc, error) {
	return rq.qp.AcquireFunc(ctx, rq.acquireFunc)
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

const bias = .2

func (s *Store) initializeReadQuota() {
	s.readQuota = readQuota{
		qp: quotapool.NewIntPool("read quota", int64(1<<30),
			quotapool.LogSlowAcquisition,
			quotapool.OnAcquisition(s.readQuota.onAcquisition)),
		s: s,
	}
}

func (rq *readQuota) acquireFunc(ctx context.Context, quota int64) (fulfilled bool, took int64) {
	guess := rq.guessReadSize()
	if log.V(1) {
		log.Infof(ctx, "attempting to acquire %v %v", guess, quota)
	}
	if guess <= quota {
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
