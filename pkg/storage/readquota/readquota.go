package readquota

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// NewPool creates a new ProposalQuota with a maximum number of bytes.
func NewPool(max int64, quotaRequired func() int64) *Pool {
	p := Pool{
		max:           quota(max),
		quotaRequired: quotaRequired,
		quotaSyncPool: sync.Pool{
			New: func() interface{} { q := quota(0); return &q },
		},
		requestSyncPool: sync.Pool{
			New: func() interface{} { return &request{} },
		},
		metrics: makeMetrics(),
	}
	p.qp = quotapool.New("read", (*pool)(&p))
	return &p
}

// Pool manages dispensing quota to clients.
type Pool struct {
	qp              *quotapool.QuotaPool
	metrics         Metrics
	max             quota
	quotaRequired   func() int64
	quotaSyncPool   sync.Pool
	requestSyncPool sync.Pool

	mu struct {
		syncutil.Mutex
		totalWait time.Duration
		maxWait   time.Duration
		requests  int64
	}
}

func (p *Pool) WaitStats() (avg, max time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	avg = time.Duration(float64(p.mu.totalWait) / float64(p.mu.requests))
	max = time.Duration(p.mu.maxWait)
	p.mu.totalWait = 0
	p.mu.maxWait = 0
	p.mu.requests = 0
	return avg, max
}

// Acquire acquires the desired quantity of quota.
func (p *Pool) Acquire(ctx context.Context) (acquired int64, err error) {
	defer func() {
		if err == nil {
			p.metrics.Acquisitions.Inc(1)
		}
	}()
	r := p.newRequest()
	defer p.putRequest(r)
	if err := p.qp.Acquire(ctx, r); err != nil {
		return 0, err
	}
	return int64(*r.Acquired().(*quota)), nil
}

// Metrics returns the pools metrics struct.
func (p *Pool) Metrics() *Metrics {
	return &p.metrics
}

// Add returns quota to the pool.
func (p *Pool) Add(v int64) {
	vv := p.quotaSyncPool.Get().(*quota)
	*vv = quota(v)
	p.qp.Add(vv)
}

// ApproximateQuota will correctly report approximately the amount of quota
// available in the pool. It is accurate only if there are no ongoing
// acquisition goroutines. If there are, the return value can be up to 'v' less
// than actual available quota where 'v' is the value the acquisition goroutine
// first in line is attempting to acquire.
func (p *Pool) ApproximateQuota() int64 {
	q := p.qp.ApproximateQuota()
	if q == nil {
		return 0
	}
	return int64(*q.(*quota))
}

// Close signals to all ongoing and subsequent acquisitions that they are
// free to return to their callers without error.
//
// Safe for concurrent use.
func (p *Pool) Close() {
	p.qp.Close()
}

func (p *Pool) newRequest() *request {
	r := p.requestSyncPool.Get().(*request)
	r.want = -1
	r.got = p.quotaSyncPool.Get().(*quota)
	return r
}

func (p *Pool) putRequest(r *request) {
	*r.got = 0
	p.quotaSyncPool.Put(r.got)
	p.requestSyncPool.Put(r)
}

// pool implements quotapool.Pool.
type pool Pool

// InitialQuota initializes the quotapool with a quantity of Quota.
func (p *pool) InitialQuota() quotapool.Quota {
	q := p.quotaSyncPool.Get().(*quota)
	*q = p.max
	return q
}

// Merge combines two Quota values into one.
func (p *pool) Merge(a, b quotapool.Quota) quotapool.Quota {
	aa, bb := a.(*quota), b.(*quota)
	*aa += *bb
	*bb = 0
	if *aa > p.max {
		*aa = p.max
	}
	p.quotaSyncPool.Put(b)
	return a
}

type quota int64

type request struct {
	want quota
	got  *quota
}

func (q *quota) String() string {
	if q == nil {
		return humanizeutil.IBytes(0)
	}
	return humanizeutil.IBytes(int64(*q))
}

func (r *request) Waited(p quotapool.Pool, took time.Duration) {
	pp := p.(*pool)
	pp.mu.Lock()
	if took > pp.mu.maxWait {
		pp.mu.maxWait = took
	}
	pp.mu.totalWait += took
	pp.mu.requests++
	pp.mu.Unlock()
	pp.metrics.TimeSpentWaitingRate10s.Add(float64(took.Nanoseconds()))
	pp.metrics.TimeSpentWaitingSummary10s.Add(float64(took.Nanoseconds()))
}

func (r *request) Acquire(p quotapool.Pool, v quotapool.Quota) (extra quotapool.Quota) {
	if r.want < 0 {
		pp := p.(*pool)
		start := timeutil.Now()
		r.want = quota(pp.quotaRequired())
		pp.metrics.RequiredTook.Inc(timeutil.Since(start).Nanoseconds())
	}
	vq := v.(*quota)
	*r.got += *vq
	if *r.got > r.want {
		*vq = *r.got - r.want
		*r.got = r.want
		return vq
	}
	*vq = 0
	p.(*pool).quotaSyncPool.Put(vq)
	return nil
}

func (r *request) Acquired() quotapool.Quota { return r.got }

func (r *request) Fulfilled() bool {
	return r.want > 0 && *r.got == r.want
}
