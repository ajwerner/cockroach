package readcontrol

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajwerner/tdigest"
	"github.com/cockroachdb/cockroach/pkg/qos"
	"github.com/cockroachdb/cockroach/pkg/qos/admission"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Controller gates access for read requests based on quality of service.
type Controller struct {
	metrics             Metrics
	clusterSetting      *cluster.Settings
	admissionController *admission.Controller
	readQuota           *quotapool.IntPool
	quotaStats          quotaStats

	currentLimit   int64
	numBlocked     int64
	queueLimit     int64
	leastQuotaSeen int64

	queueSizeFunc func(limit int) (queueSize int)

	acquisitionSyncPool sync.Pool
	startOnce           sync.Once
}

func maxF64(a, b float64) float64 {
	if a < b {
		return b
	}
	return a
}

func minF64(a, b float64) float64 {
	if a > b {
		return b
	}
	return a
}

// TODO(ajwerner): properly plumb admission control configuration.
// TODO(ajwerner): plumb through read quota configuration
func (c *Controller) Initialize(
	ctx context.Context, settings *cluster.Settings, summary *metric.WindowedSummary,
) {
	initialLimit := 2 * runtime.NumCPU()
	*c = Controller{
		clusterSetting: settings,
		acquisitionSyncPool: sync.Pool{
			New: func() interface{} {
				return new(Acquisition)
			},
		},
		readQuota: quotapool.NewIntPool("read quota", uint64(initialLimit),
			quotapool.LogSlowAcquisition,
			quotapool.OnAcquisition(c.onAcquisition)),
		currentLimit: int64(initialLimit),
		queueLimit:   int64(math.Ceil(math.Sqrt(float64(initialLimit)))) + 1,
	}
	const longMeasurementPeriod = 5 * time.Minute
	const shortMeasurementPeriod = 2 * time.Second
	admissionCfg := admission.Config{
		Name:         "read",
		TickInterval: 250 * time.Millisecond,
		OverloadSignal: func(cur qos.Level) (overloaded bool, lim qos.Level) {
			requests, avg, min, max, handleReady := c.quotaStats.WaitStats(true)

			qLen := int64(c.readQuota.Len())

			var longLat, shortLat float64
			summary.ReadAt(longMeasurementPeriod, func(_ time.Duration, r tdigest.Reader) {
				longLat = r.TrimmedMean(.1, .6)
			})
			summary.ReadAt(shortMeasurementPeriod, func(_ time.Duration, r tdigest.Reader) {
				shortLat = r.InnerMean(.9)
			})
			// This is a hack to try to deal with the case where the system's load has
			// diminished drastically. We want to accelerate the return to steady
			// state.
			// TODO(ajwerner): make this better.
			// if shortLat < longLat/2 {
			// 	summary.ReadAt(longMeasurementPeriod/5, func(_ time.Duration, r tdigest.Reader) {
			// 		longLat = r.TrimmedMean(.9)
			// 	})
			// }
			curLimit := atomic.LoadInt64(&c.currentLimit)
			quota := atomic.SwapInt64(&c.leastQuotaSeen, curLimit)
			inFlight := curLimit - int64(quota)
			queueLimit := atomic.LoadInt64(&c.queueLimit)
			numBlocked := atomic.SwapInt64(&c.numBlocked, 0)
			if log.V(1) {
				log.Infof(context.TODO(), "overload signal %v: avg %v, min %v, max %v, qLen %v, reqs %v, inFlight %v, longDur %v, shortDur %v, curLimit %v, numBlocked %v, quota %v, queueLimit %v",
					cur, avg, min, max, qLen, requests, inFlight, time.Duration(longLat), time.Duration(shortLat), curLimit, numBlocked, quota, queueLimit)
			}
			const tolerance = 1.5
			const smoothing = .2
			const minLimit = 4

			if inFlight < int64(queueLimit/2) && numBlocked == 0 && curLimit > int64(2*initialLimit) {
				// Do nothing
			} else {

				gradient := maxF64(.5, minF64(1.0, (tolerance*longLat)/shortLat))
				calculatedLimit := int64(math.Ceil(float64(curLimit)*gradient)) + queueLimit
				newLimit := int64(maxF64(minLimit, float64(calculatedLimit)*smoothing+float64(curLimit)*(1-smoothing)))
				queueLimit = int64(math.Ceil(math.Sqrt(float64(newLimit)))) + 1
				if log.V(1) {

					log.Infof(context.TODO(), "updating read quota limit: oldLimit=%v, calcLimit=%v, newLimit=%v, shortDur=%v, longDur=%v, gradient=%v, inFlight=%v, queueLen=%v",
						curLimit,
						calculatedLimit,
						newLimit,
						time.Duration(shortLat),
						time.Duration(longLat),
						gradient,
						inFlight,
						queueLimit)
				}
				if newLimit != curLimit {
					atomic.StoreInt64(&c.currentLimit, newLimit)
					atomic.StoreInt64(&c.queueLimit, queueLimit)
					atomic.StoreInt64(&c.leastQuotaSeen, newLimit)
					c.readQuota.UpdateCapacity(uint64(newLimit))
					if delta := newLimit - curLimit; delta > 0 {
						c.metrics.Increases.Inc(delta)
					} else {
						c.metrics.Decreases.Inc(-delta)
					}
					c.metrics.CurrentLimit.Update(newLimit)
				}
			}
			return numBlocked > 0 ||
					(min > 5*time.Millisecond || avg > 20*time.Millisecond) &&
						(qLen > requests/10 || requests > 100) ||
					handleReady > 500*time.Millisecond && requests > 100,
				qos.Level{Class: qos.ClassHigh, Shard: 0}
		},
		PruneRate:          .05,
		GrowRate:           .01,
		MaxBlocked:         200,
		MaxReqsPerInterval: 2000,
	}
	c.admissionController = admission.NewController(admissionCfg)
	c.metrics = makeMetrics()
	c.metrics.AdmissionMetrics = c.admissionController.Metrics()
	c.metrics.CurrentLimit.Update(c.currentLimit)
}

type Metrics struct {
	AdmissionMetrics *admission.Metrics

	CurrentLimit *metric.Gauge
	Increases    *metric.Counter
	Decreases    *metric.Counter
}

var (
	metaCurrentLimit = metric.Metadata{
		Name:        "readcontrol.current_limit",
		Help:        "Gauge of the current limit for the read controller",
		Measurement: "Concurrent Requests",
		Unit:        metric.Unit_COUNT,
	}

	metaIncreases = metric.Metadata{
		Name:        "readcontrol.increases",
		Help:        "Counter of increases to the limit for the read controller",
		Measurement: "Concurrent Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaDecreases = metric.Metadata{
		Name:        "readcontrol.decreases",
		Help:        "Counter of decreases to the limit for the read controller",
		Measurement: "Concurrent Requests",
		Unit:        metric.Unit_COUNT,
	}
)

func makeMetrics() Metrics {
	return Metrics{
		CurrentLimit: metric.NewGauge(metaCurrentLimit),
		Increases:    metric.NewCounter(metaIncreases),
		Decreases:    metric.NewCounter(metaDecreases),
	}
}

func (c *Controller) Metrics() *Metrics {
	return &c.metrics
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

	maxRaftReadyDuration int64
}

// Admit is going to first check what should happen to this request given its
// qos.Level and the current admission and rejection level. Then, assuming it
// should be admitted, it attempts to acquire quota from the readquota. If the
// queue is full for read quota then the request is blocked for the remainder of
// this interval to repeat the process later.
func (c *Controller) Admit(
	ctx context.Context, ba *roachpb.BatchRequest,
) (_ *Acquisition, err error) {
	defer func() {
		if err == admission.ErrRejected {
			err = roachpb.NewReadRejectedError()
		}
	}()
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
	for {
		switch err := a.acquire(ctx); err {
		case errQueueFull:
			atomic.AddInt64(&c.numBlocked, 1)
			if err = c.admissionController.Block(ctx, a.l); err != nil {
				c.putAcquisition(a)
				if err == admission.ErrRejected {

				}
				return nil, err
			}
		case nil:
			return a, nil
		default:
			c.putAcquisition(a)
			return nil, err
		}
	}
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
	if ba.RangeID <= 20 {
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
	*a = Acquisition{}
	c.acquisitionSyncPool.Put(a)
}

func (c *Controller) RecordRaftReadyDuration(d time.Duration) {
	c.quotaStats.recordRaftReady(d)
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

func (qs *quotaStats) recordRaftReady(d time.Duration) {
	qs.RLock()
	defer qs.RUnlock()
	setIfGt(&qs.maxRaftReadyDuration, int64(d))
}

func (qs *quotaStats) readLocked() (reqs int64, total, min, max, maxRaftReady time.Duration) {
	reqs = atomic.LoadInt64(&qs.numRequests)
	total = time.Duration(atomic.LoadInt64(&qs.totalWaitNanos))
	min = time.Duration(atomic.LoadInt64(&qs.minWaitNanos))
	max = time.Duration(atomic.LoadInt64(&qs.maxWaitNanos))
	maxRaftReady = time.Duration(atomic.LoadInt64(&qs.maxRaftReadyDuration))
	return reqs, total, min, max, maxRaftReady
}

func (qs *quotaStats) clearLocked() {
	atomic.StoreInt64(&qs.totalWaitNanos, 0)
	atomic.StoreInt64(&qs.minWaitNanos, 0)
	atomic.StoreInt64(&qs.maxWaitNanos, 0)
	atomic.StoreInt64(&qs.numRequests, 0)
	atomic.StoreInt64(&qs.maxRaftReadyDuration, 0)
}

func (qs *quotaStats) WaitStats(
	reset bool,
) (reqs int64, avg, min, max, maxRaftReady time.Duration) {
	qs.Lock()
	reqs, total, min, max, maxRaftReady := qs.readLocked()
	if reset {
		qs.clearLocked()
	}
	qs.Unlock()
	if reqs > 0 {
		// Integer division is fine, 1ns is not long.
		avg = total / time.Duration(reqs)
	}
	return reqs, avg, min, max, maxRaftReady
}

// Acquisition exists to avoid having to allocate a closure around the priority
// value to use quotapool.IntPool.AcquireFunc.
type Acquisition struct {
	ctx        context.Context
	l          qos.Level
	guess      int64
	quota      int64
	controller *Controller
	alloc      *quotapool.IntAlloc
}

// func (a *Acquisition) Acquire(ctx context.Context) error {
// 	return a.acquire(ctx)
// }

var errQueueFull = errors.New("queue full")

func (a *Acquisition) acquireFunc(
	ctx context.Context, p quotapool.PoolInfo,
) (took uint64, err error) {
	// if a.guess == 0 {
	// 	a.guess = a.rq.guessReadSize(a.l.Class)
	// }
	if lim := atomic.LoadInt64(&a.controller.queueLimit); int(lim) < p.Len {
		return 0, errQueueFull
	}
	if log.V(3) {
		log.Infof(ctx, "attempting to acquire %v %v", a.guess, p)
	}
	const guess = 1
	if guess <= p.Available {
		a.quota = int64(p.Available)
		return 1, nil
	}
	return 0, quotapool.ErrNotEnoughQuota
}

func (a *Acquisition) admit(ctx context.Context) (err error) {
	return a.controller.admissionController.Admit(ctx, a.l)
}

func (a *Acquisition) acquire(ctx context.Context) (err error) {
	a.alloc, err = a.controller.readQuota.AcquireFunc(ctx, a.acquireFunc)
	a.controller.recordInFlight(a.quota)
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

func (c *Controller) recordInFlight(quotaSize int64) {
	setIfLt(&c.leastQuotaSeen, quotaSize)
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
