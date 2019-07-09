package readcontrol

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/qos"
	"github.com/cockroachdb/cockroach/pkg/qos/admission"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Controller gates access for read requests based on quality of service.
type Controller struct {
	clusterSetting      *cluster.Settings
	admissionController *admission.Controller
	readQuota           *quotapool.IntPool
	quotaStats          quotaStats

	acquisitionSyncPool sync.Pool
	startOnce           sync.Once
}

// TODO(ajwerner): properly plumb admission control configuration.
// TODO(ajwerner): plumb through read quota configuration
func (c *Controller) Initialize(ctx context.Context, settings *cluster.Settings) {
	*c = Controller{
		clusterSetting: settings,
		acquisitionSyncPool: sync.Pool{
			New: func() interface{} {
				return new(Acquisition)
			},
		},
		readQuota: quotapool.NewIntPool("read quota", 1024,
			quotapool.LogSlowAcquisition,
			quotapool.OnAcquisition(c.onAcquisition)),
	}
	admissionCfg := admission.Config{
		Name:         "read",
		TickInterval: 250 * time.Millisecond,
		OverloadSignal: func(cur qos.Level) (overloaded bool, lim qos.Level) {
			requests, avg, min, max := c.quotaStats.WaitStats(true)
			qLen := int64(c.readQuota.Len())
			if log.V(1) {
				log.Infof(context.TODO(), "overload signal %v: avg %v, min %v, max %v, qLen %v, reqs %v",
					cur, avg, min, max, qLen, requests)
			}
			return (min > 5*time.Millisecond || avg > 10*time.Millisecond) && qLen > requests/10,
				qos.Level{Class: qos.ClassHigh, Shard: 0}
		},
		PruneRate:  .05,
		GrowRate:   .01,
		MaxBlocked: 10000,
	}
	c.admissionController = admission.NewController(admissionCfg)
}

func (c *Controller) AdmissionMetrics() *admission.Metrics {
	return c.admissionController.Metrics()
}

func (c *Controller) Start(ctx context.Context, stopper *stop.Stopper) {
	c.startOnce.Do(func() {
		c.admissionController.RunTicker(ctx, stopper)
	})
}

// quotaStats accumulates statistics about how long readQuota acquisitions take.
type quotaStats struct {
	syncutil.RWMutex
	minWaitNanos   int64
	maxWaitNanos   int64
	totalWaitNanos int64
	numRequests    int64
}

func (c *Controller) Admit(ctx context.Context, ba *roachpb.BatchRequest) (*Acquisition, error) {
	a := c.getAcquisition(ctx, ba)
	if !requiresReadControl(c.clusterSetting, ba) || a.l.Class == 4 {
		a.l.Class = qos.ClassHigh
		// TODO(ajwerner): return some default acquistion
		return a, nil
	}
	if err := a.admit(ctx); err != nil {
		c.putAcquisition(a)
		return nil, err
	}
	if err := a.acquire(ctx); err != nil {
		c.putAcquisition(a)
		return nil, err
	}
	return a, nil
}

var disableReadQuota = settings.RegisterBoolSetting(
	"kv.read_controller.disabled",
	"set to true to disable the read quota.",
	false,
)

// TODO(ajwerner): explore other cases where read only requests should not be
// blocked. Internal requests should already receive a high priority.
func requiresReadControl(settings *cluster.Settings, ba *roachpb.BatchRequest) bool {
	if disableReadQuota.Get(&settings.SV) {
		return false
	}
	if ba.Txn != nil && ba.Txn.Key != nil {
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

func (c *Controller) putAcquisition(a *Acquisition) {
	c.acquisitionSyncPool.Put(a)
}

func (c *Controller) getAcquisition(ctx context.Context, ba *roachpb.BatchRequest) *Acquisition {
	a := c.acquisitionSyncPool.Get().(*Acquisition)
	*a = Acquisition{controller: c}
	if qosLevel, ok := qos.LevelFromContext(ctx); ok {
		a.l = qosLevel
	} else {
		a.l.Class = 4
	}
	return a
}

func (qs *quotaStats) readLocked() (reqs int64, total, min, max time.Duration) {
	reqs = atomic.LoadInt64(&qs.numRequests)
	total = time.Duration(atomic.LoadInt64(&qs.totalWaitNanos))
	min = time.Duration(atomic.LoadInt64(&qs.minWaitNanos))
	max = time.Duration(atomic.LoadInt64(&qs.maxWaitNanos))
	return reqs, total, min, max
}

func (qs *quotaStats) clearLocked() {
	atomic.StoreInt64(&qs.totalWaitNanos, 0)
	atomic.StoreInt64(&qs.minWaitNanos, 0)
	atomic.StoreInt64(&qs.maxWaitNanos, 0)
	atomic.StoreInt64(&qs.numRequests, 0)
}

func (qs *quotaStats) WaitStats(reset bool) (reqs int64, avg, min, max time.Duration) {
	qs.Lock()
	reqs, total, min, max := qs.readLocked()
	if reset {
		qs.clearLocked()
	}
	qs.Unlock()
	if reqs > 0 {
		// Integer division is fine, 1ns is not long.
		avg = total / time.Duration(reqs)
	}
	return reqs, avg, min, max
}

// Acquisition exists to avoid having to allocate a closure around the priority
// value to use quotapool.IntPool.AcquireFunc.
type Acquisition struct {
	ctx        context.Context
	l          qos.Level
	guess      int64
	controller *Controller
	alloc      *quotapool.IntAlloc
}

func (a *Acquisition) acquireFunc(
	ctx context.Context, p quotapool.PoolInfo,
) (took uint64, err error) {
	// if a.guess == 0 {
	// 	a.guess = a.rq.guessReadSize(a.l.Class)
	// }
	if log.V(3) {
		log.Infof(ctx, "attempting to acquire %v %v", a.guess, p)
	}
	const guess = 1
	if guess <= p.Available {
		return 1, nil
	}
	return 0, quotapool.ErrNotEnoughQuota
}

func (a *Acquisition) admit(ctx context.Context) (err error) {
	return a.controller.admissionController.Admit(ctx, a.l)
}

func (a *Acquisition) acquire(ctx context.Context) (err error) {
	a.alloc, err = a.controller.readQuota.AcquireFunc(ctx, a.acquireFunc)
	return err
}

func (a *Acquisition) Release(ctx context.Context, respSize int) {
	if a.alloc != nil {
		if respSize > 0 {
			if log.V(3) {
				log.Infof(ctx, "acquired %v, used %v", respSize, a.alloc.Acquired())
			}
		}
		a.alloc.Release()
	}
	a.controller.putAcquisition(a)
}

func (c *Controller) onAcquisition(
	ctx context.Context, poolName string, r quotapool.Request, start time.Time,
) {
	took := timeutil.Since(start)
	if log.V(3) {
		log.Infof(ctx, "acquire took %v for %v", took, r)
	}
	c.quotaStats.record(took)

	//rq.s.metrics.ReadQuotaAcquisitions.Inc(1)
	//rq.s.metrics.ReadQuotaTimeSpentWaitingRate10s.Add(float64(took.Nanoseconds()))
	//rq.s.metrics.ReadQuotaTimeSpentWaitingSummary10s.Add(float64(took.Nanoseconds()))
}

func (qs *quotaStats) record(took time.Duration) {
	qs.RLock()
	setIfGt(&qs.maxWaitNanos, int64(took))
	setIfLt(&qs.minWaitNanos, int64(took))
	atomic.AddInt64(&qs.totalWaitNanos, int64(took))
	atomic.AddInt64(&qs.numRequests, 1)
	qs.RUnlock()
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

func lt(a, b int64) bool { return b == 0 || a < b }
func gt(a, b int64) bool { return a > b }

func setIfGt(p *int64, v int64) { setIf(p, v, gt) }
func setIfLt(p *int64, v int64) { setIf(p, v, lt) }

// const (
// 	bias    = .2
// 	maxSize = 1 << 28
// )

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
