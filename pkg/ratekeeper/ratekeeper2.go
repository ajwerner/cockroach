package ratekeeper

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Errorkeeper keeps track of how many distsql flows we allow to run at the same
// time. The idea is that we are going to track the percentage of read requests
// in which observe overload retries on a per-node basis and then we'll use that
// to decrease the allowed number of flows.
//
// On each tick we'll look at the percentage of requests which are getting
// rejected and any time that percentage increases we lower the number of
// concurrent flows we allow.
type errorKeeper struct {
	mu         sync.RWMutex
	numRetries int64
	lastErr    time.Time
	numErrors  *metric.Counter
}

const updateThreshold = time.Second

func (rk *errorKeeper) ReportError() {
	rk.numErrors.Inc(1)
	now := time.Now()
	rk.mu.RLock()
	atomic.AddInt64(&rk.numRetries, 1)
	last := rk.lastErr
	rk.mu.RUnlock()
	if now.Sub(last) > updateThreshold {
		rk.updateLastErr(now)
	}
}

func (rk *errorKeeper) updateLastErr(to time.Time) {
	rk.mu.Lock()
	defer rk.mu.Unlock()
	if to.After(rk.lastErr) {
		rk.lastErr = to
	}
}

func (rk *errorKeeper) LastError() time.Time {
	rk.mu.RLock()
	defer rk.mu.RUnlock()
	return rk.lastErr
}

type RateKeeper struct {
	cfg      Config
	metrics  Metrics
	stopper  *stop.Stopper
	chanPool sync.Pool
	errorKeeper

	acquiring chan acquireRequest
	releasing chan releaseRequest
}

func (rk *RateKeeper) Metrics() *Metrics {
	return &rk.metrics
}

func NewRateKeeper(ctx context.Context, stopper *stop.Stopper, cfg Config) *RateKeeper {
	rk := &RateKeeper{
		cfg:     cfg,
		metrics: newMetrics(),
		stopper: stopper,
		chanPool: sync.Pool{
			New: func() interface{} { return make(chan Quota) },
		},
		acquiring: make(chan acquireRequest),
		releasing: make(chan releaseRequest),
	}
	rk.numErrors = rk.metrics.Errors
	rk.cfg.setDefaults()
	stopper.RunAsyncTask(ctx, "ratekeeper.loop", rk.loop)
	return rk
}

func (rk *RateKeeper) Acquire(ctx context.Context, cost float64) (Quota, error) {
	if cost < rk.cfg.MinCost {
		cost = rk.cfg.MinCost
	}
	c := rk.chanPool.Get().(chan Quota)
	select {
	case rk.acquiring <- acquireRequest{
		ctx:     ctx,
		c:       c,
		cost:    cost,
		entered: time.Now(),
	}:
	case <-ctx.Done():
		return Quota{}, ctx.Err()
	case <-rk.stopper.ShouldQuiesce():
		return Quota{}, stop.ErrUnavailable
	}
	select {
	case q := <-c:
		rk.chanPool.Put(c)
		if q.cost == 0 {
			return q, &roachpb.ScanBackpressureError{}
		}
		return q, nil
	case <-ctx.Done():
		return Quota{}, ctx.Err()
	case <-rk.stopper.ShouldQuiesce():
		return Quota{}, stop.ErrUnavailable
	}
}

func (rk *RateKeeper) Release(ctx context.Context, q Quota, took time.Duration, err error) {
	select {
	case rk.releasing <- releaseRequest{
		Quota: q,
		err:   err,
		took:  took,
	}:
	case <-rk.stopper.ShouldQuiesce():
		return
	}
}

func (rk *RateKeeper) loop(ctx context.Context) {
	// we're going to track the
	//  time / cost as an average
	//  cost / time as the rate at which we allow cost to flow through the system
	// sum the cost for the window
	// sum the time / query average for the window
	// with that we'll say we want to quota to be some factor above the smoothed
	// cost per second flowing through the system.
	// TODO(ajwerner): move this to a cluster setting
	const maxLargeInFlight = 3
	const maxLargeQueue = 10
	var (
		releases                  int
		totalCost                 float64
		trailingCostPerSecond     float64
		trailingAvgCost           float64
		timePerCost               float64
		trailingQPS               float64
		trailingTimePerCost       float64
		totalTime                 time.Duration
		trailingSecondsPerRequest float64

		inFlight    float64
		quotaSize   = 200 * rk.cfg.MinCost
		targetQuota float64

		queue []acquireRequest

		last              time.Time
		largeInFlight     int
		largeInFlightCost float64
		largeQueue        []acquireRequest

		handleRelease = func(rr releaseRequest) {
			log.Infof(ctx, "got release %v %v %v", rr.cost, rr.took, rr.isLarge)
			totalCost += rr.cost
			if !rr.isLarge {
				inFlight -= rr.cost
			} else {
				largeInFlightCost -= rr.cost
				largeInFlight--
				rk.metrics.LargeInFlight.Dec(1)
			}
			rk.metrics.TotalCost.Inc(int64(rr.cost))
			totalTime += rr.took
			timePerCost += rr.took.Seconds() / rr.cost
			releases++
		}
		recordMetrics = func() {
			rk.metrics.InFlightCost.Update(inFlight + largeInFlightCost)
			rk.metrics.NanosPerCost.Update(trailingTimePerCost)
			rk.metrics.AverageCost.Update(trailingAvgCost)
			rk.metrics.CostPerSecond.Update(trailingCostPerSecond)
			rk.metrics.Quota.Update(quotaSize)
			rk.metrics.LargeInFlight.Update(int64(largeInFlight))
			rk.metrics.QPS.Update(trailingQPS)
			rk.metrics.SecondsPerRequest.Update(trailingSecondsPerRequest)
			rk.metrics.TargetQuota.Update(targetQuota)
		}

		healthy = true
		tick    = func(now time.Time) {
			defer recordMetrics()
			// TODO(ajwerner): we should have some target interval after which
			// information exists in the average below some threshold and then
			// use that to pick a decay factor with the tick interval.
			//
			// Imagine we want to say that after 2 minutes we want a value to have
			// a weight of .001 then what's the decay factor?
			const trailingDecay = .99
			var avgCost, avgTimePerCost, timePerRequest float64
			if releases > 0 {
				avgCost = totalCost / float64(releases)
				avgTimePerCost = timePerCost / float64(releases)

				trailingTimePerCost = (trailingTimePerCost * trailingDecay) + (avgTimePerCost * (1 - trailingDecay))
				trailingAvgCost = (trailingAvgCost * trailingDecay) + (avgCost * (1 - trailingDecay))
				timePerRequest = totalTime.Seconds() / float64(releases)

			} else {
				timePerRequest = .5 * trailingSecondsPerRequest
			}
			trailingSecondsPerRequest = (trailingSecondsPerRequest * trailingDecay) + (timePerRequest * (1 - trailingDecay))
			trailingCostPerSecond = (trailingCostPerSecond * trailingDecay) + (totalCost / now.Sub(last).Seconds() * (1 - trailingDecay))
			trailingQPS = (trailingQPS * trailingDecay) + (float64(releases) * (1 - trailingDecay))
			timePerCost = 0
			totalCost = 0
			releases = 0
			lastErr := rk.LastError()
			newTargetQuota := 2 * trailingAvgCost * trailingQPS * trailingSecondsPerRequest
			targetQuota = (targetQuota * trailingDecay) + newTargetQuota*(1-trailingDecay)
			// TODO(ajwerner): make this more sophisticated.
			healthy = false
			if timeSince := now.Sub(lastErr); timeSince < time.Second {
				quotaSize *= .90
			} else if timeSince > 30*time.Second {
				healthy = true
				// step towards targetQuota linearly
				if quotaSize > targetQuota {
					quotaSize = targetQuota
				} else {
					// 1 minute from now we'd like to be at targetQuota
					deltaY := targetQuota - quotaSize
					step := deltaY * (rk.cfg.TickInterval.Seconds() / 3 * time.Minute.Seconds())
					if step > quotaSize*.05 {
						quotaSize *= 1.05
					}
				}
			}
			const minQuota = 1024
			if quotaSize < minQuota {
				quotaSize = minQuota
			}
			last = now
		}

		ticker = timeutil.NewTimer()
		send   = func(qr acquireRequest, q Quota) bool {
			select {
			case <-qr.ctx.Done():
				return false
			case qr.c <- q:
				return true
			}
		}
		dequeueLarge = func(countsAsLarge bool) {
			ar := largeQueue[0]
			if send(ar, Quota{cost: ar.cost, isLarge: countsAsLarge}) {
				if countsAsLarge {
					largeInFlight++
					largeInFlightCost += ar.cost
					rk.metrics.LargeInFlight.Inc(1)
				} else {
					inFlight += ar.cost
				}
			}
			rk.metrics.QueuedLargeRequests.Dec(1)
			largeQueue[0] = acquireRequest{}
			largeQueue = largeQueue[1:]
		}
		dequeueNormal = func() {
			ar := queue[0]
			if send(ar, Quota{cost: ar.cost}) {
				inFlight += ar.cost
			}
			rk.metrics.QueuedRequests.Dec(1)
			queue[0] = acquireRequest{}
			queue = queue[1:]
		}
		dequeue = func() {
			// first we want to check if we can now declassify any of the large requests
			for len(largeQueue) > 0 && largeQueue[0].cost < quotaSize-inFlight && largeQueue[0].cost < quotaSize/2 && largeQueue[0].cost < trailingAvgCost*10 {
				dequeueLarge(false)
			}
			for healthy && len(largeQueue) > 0 && largeInFlight < maxLargeInFlight {
				dequeueLarge(true)
			}
			for len(queue) > 0 && queue[0].cost < quotaSize-inFlight {
				dequeueNormal()
			}
		}
	)
	ticker.Reset(rk.cfg.TickInterval)
	for {
		select {
		case t := <-ticker.C:
			ticker.Read = true
			tick(t)
			dequeue()
			ticker.Reset(rk.cfg.TickInterval)
		case ar := <-rk.acquiring:
			if ar.cost < quotaSize-inFlight {
				if send(ar, Quota{cost: ar.cost}) {
					inFlight += ar.cost
				}
			} else if ar.cost > quotaSize/2 || ar.cost > trailingAvgCost*10 {
				if healthy && largeInFlight < maxLargeInFlight {
					if send(ar, Quota{cost: ar.cost, isLarge: true}) {
						largeInFlightCost += ar.cost
						largeInFlight++
					}
				} else if len(largeQueue) < maxLargeQueue {
					// TODO: check queue size
					largeQueue = append(largeQueue, ar)
					rk.metrics.QueuedLargeRequests.Inc(1)
				} else {
					if send(ar, Quota{}) {
						rk.metrics.RejectedLargeRequests.Inc(1)
					}
				}
			} else if len(queue) < rk.cfg.QueueMax {
				queue = append(queue, ar)
				rk.metrics.QueuedRequests.Inc(1)
			} else {
				if send(ar, Quota{}) {
					rk.metrics.RejectedRequests.Inc(1)
				}
			}
		case rr := <-rk.releasing:
			handleRelease(rr)
			dequeue()
		case <-rk.stopper.ShouldQuiesce():
			return
		}
	}
}

type Quota struct {
	cost    float64
	isLarge bool
}

type acquireRequest struct {
	ctx     context.Context
	c       chan<- Quota
	entered time.Time
	cost    float64
}

type releaseRequest struct {
	Quota
	err  error
	took time.Duration
}

type Config struct {
	Settings *cluster.Settings

	// QueueMax is the maximum number of requests which can be queued before
	// it is rejected.
	QueueMax int

	MinCost float64

	// TickInterval is how frequently to update the estimates.
	TickInterval time.Duration
}

const (
	defaultQueueMax     = 1024
	defaultTickInterval = 200 * time.Millisecond
	defaultMinCost      = 8
)

func (c *Config) setDefaults() {
	if c.QueueMax <= 0 {
		c.QueueMax = defaultQueueMax
	}
	if c.TickInterval <= 0 {
		c.TickInterval = defaultTickInterval
	}
	if c.MinCost <= 0 {
		c.MinCost = defaultMinCost
	}
}

var (
	metaTargetQuota = metric.Metadata{
		Name: "ratekeeper.target_quota",
		Help: "target quota",
		Unit: metric.Unit_COUNT,
	}
	metaInFlightCost = metric.Metadata{
		Name: "ratekeeper.in_flight_cost",
		Help: "cost of in flight requests",
		Unit: metric.Unit_COUNT,
	}
	metaCurQuota = metric.Metadata{
		Name: "ratekeeper.cur_quota",
		Help: "Cost quota for new requests",
		Unit: metric.Unit_COUNT,
	}
	metaLargeInFlight = metric.Metadata{
		Name: "ratekeeper.large_in_flight",
		Help: "number of large requests in flight",
		Unit: metric.Unit_COUNT,
	}
	metaNanosPerCost = metric.Metadata{
		Name: "ratekeeper.nanos_per_cost",
		Help: "time per cost estimate",
		Unit: metric.Unit_NANOSECONDS,
	}
	metaCostPerSec = metric.Metadata{
		Name: "ratekeeper.cost_per_second",
		Help: "Exponential moving average of cost per second",
		Unit: metric.Unit_COUNT,
	}
	metaAvgCost = metric.Metadata{
		Name: "ratekeeper.average_cost",
		Help: "average cost per",
		Unit: metric.Unit_COUNT,
	}
	metaErrorsReported = metric.Metadata{
		Name: "ratekeeper.seen_errors",
		Help: "errors seen",
		Unit: metric.Unit_COUNT,
	}
	metaRejectedLargeRequests = metric.Metadata{
		Name: "ratekeeper.rejected_large_requests",
		Help: "number of large requests rejected",
		Unit: metric.Unit_COUNT,
	}
	metaRejectedRequests = metric.Metadata{
		Name: "ratekeeper.rejected_requests",
		Help: "number of requests rejected",
		Unit: metric.Unit_COUNT,
	}
	metaQueuedRequests = metric.Metadata{
		Name: "ratekeeper.queued",
		Help: "number of requests currently queued",
		Unit: metric.Unit_COUNT,
	}
	metaQueuedLargeRequests = metric.Metadata{
		Name: "ratekeeper.queued_large_requests",
		Help: "number of large requests currently queued",
		Unit: metric.Unit_COUNT,
	}
	metaTotalCost = metric.Metadata{
		Name: "ratekeeper.total_cost",
		Help: "counter of total cost which has completed",
		Unit: metric.Unit_COUNT,
	}
	metaTotalTimePerCost = metric.Metadata{
		Name: "ratekeeper.total_time_per_cost",
		Help: "counter of total seconds/cost",
		Unit: metric.Unit_SECONDS,
	}
	metaTotalTime = metric.Metadata{
		Name: "ratekeeper.total_time",
		Help: "counter of total time",
		Unit: metric.Unit_SECONDS,
	}
	metaQPS = metric.Metadata{
		Name: "ratekeeper.trailing_qps",
		Help: "trailing qps",
		Unit: metric.Unit_COUNT,
	}
	metaSecondsPerRequest = metric.Metadata{
		Name: "ratekeeper.seconds_per_request",
		Help: "trailing average seconds per request",
		Unit: metric.Unit_SECONDS,
	}
)

type Metrics struct {
	InFlightCost          *metric.GaugeFloat64
	Quota                 *metric.GaugeFloat64
	LargeInFlight         *metric.Gauge
	CostPerSecond         *metric.GaugeFloat64
	AverageCost           *metric.GaugeFloat64
	NanosPerCost          *metric.GaugeFloat64
	Errors                *metric.Counter
	RejectedLargeRequests *metric.Counter
	RejectedRequests      *metric.Counter
	QueuedRequests        *metric.Gauge
	QueuedLargeRequests   *metric.Gauge
	TotalCost             *metric.Counter
	QPS                   *metric.GaugeFloat64
	SecondsPerRequest     *metric.GaugeFloat64
	TargetQuota           *metric.GaugeFloat64
}

func newMetrics() Metrics {
	return Metrics{
		InFlightCost:          metric.NewGaugeFloat64(metaInFlightCost),
		CostPerSecond:         metric.NewGaugeFloat64(metaCostPerSec),
		Quota:                 metric.NewGaugeFloat64(metaCurQuota),
		AverageCost:           metric.NewGaugeFloat64(metaAvgCost),
		LargeInFlight:         metric.NewGauge(metaLargeInFlight),
		NanosPerCost:          metric.NewGaugeFloat64(metaNanosPerCost),
		Errors:                metric.NewCounter(metaErrorsReported),
		RejectedLargeRequests: metric.NewCounter(metaRejectedLargeRequests),
		RejectedRequests:      metric.NewCounter(metaRejectedRequests),
		QueuedRequests:        metric.NewGauge(metaQueuedRequests),
		QueuedLargeRequests:   metric.NewGauge(metaQueuedLargeRequests),
		TotalCost:             metric.NewCounter(metaTotalCost),
		QPS:                   metric.NewGaugeFloat64(metaQPS),
		SecondsPerRequest:     metric.NewGaugeFloat64(metaSecondsPerRequest),
		TargetQuota:           metric.NewGaugeFloat64(metaTargetQuota),
	}
}
