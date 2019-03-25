package ratekeeper

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
		return 0, ctx.Err()
	case <-rk.stopper.ShouldQuiesce():
		return 0, stop.ErrUnavailable
	}
	select {
	case q := <-c:
		rk.chanPool.Put(c)
		if q == 0 {
			return q, &roachpb.ScanBackpressureError{}
		}
		return q, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-rk.stopper.ShouldQuiesce():
		return 0, stop.ErrUnavailable
	}
}

func (rk *RateKeeper) Release(ctx context.Context, q Quota, took time.Duration, err error) {
	select {
	case rk.releasing <- releaseRequest{
		quota: float64(q),
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

	var (
		releases              int
		totalCost             float64
		trailingCostPerSecond float64
		trailingAvgCost       float64
		timePerCost           float64
		trailingQPS           float64
		trailingTimePerCost   float64

		inFlight  float64
		quotaSize = 200 * rk.cfg.MinCost

		queue []acquireRequest

		last          time.Time
		largeInFlight int
		largeQueue    []acquireRequest

		handleRelease = func(rr releaseRequest) {
			totalCost += float64(rr.quota)
			inFlight -= float64(rr.quota)
			timePerCost += (rr.took.Seconds() * 1e9) / float64(rr.quota)
			releases++
		}
		recordMetrics = func() {
			rk.metrics.InFlightCost.Update(inFlight)
			rk.metrics.NanosPerCost.Update(trailingTimePerCost)
			rk.metrics.AverageCost.Update(trailingCostPerSecond)
			rk.metrics.Quota.Update(quotaSize)
			rk.metrics.LargeInFlight.Update(int64(largeInFlight))
		}
		logState = func() {
			//log.Infof(ctx, "trailingAvgNanosPerCost: %v; trailingAvgCost: %v", trailingAvgNanosPerCost, trailingAvgCost)
		}
		tick = func(now time.Time) {
			defer logState()
			defer recordMetrics()
			if releases == 0 {
				return
			}
			// TODO(ajwerner): we should have some target interval after which
			// information exists in the average below some threshold and then
			// use that to pick a decay factor with the tick interval.
			//
			// Imagine we want to say that after 2 minutes we want a value to have
			// a weight of .001 then what's the decay factor?
			const trailingDecay = .95
			avgCost := totalCost / float64(releases)
			avgTimePerCost := timePerCost / float64(releases)
			trailingCostPerSecond = totalCost / now.Sub(last).Seconds()
			trailingAvgCost = (trailingAvgCost * trailingDecay) + (avgCost * (1 - trailingDecay))

			trailingTimePerCost = (trailingTimePerCost * trailingDecay) + (avgTimePerCost * (1 - trailingDecay))
			trailingQPS = (trailingQPS * trailingDecay) + (float64(releases) * (1 - trailingDecay))
			timePerCost = 0
			totalCost = 0
			releases = 0
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
		healthy = true
	)
	ticker.Reset(rk.cfg.TickInterval)
	for {
		select {
		case t := <-ticker.C:
			ticker.Read = true
			tick(t)
			ticker.Reset(rk.cfg.TickInterval)
		case ar := <-rk.acquiring:
			if /* ar.cost < quotaSize-inFlight */ true {
				if send(ar, Quota(ar.cost)) {
					inFlight += ar.cost
				}
			} else if ar.cost > quotaSize/2 {
				// TODO(ajwerner): move this to a cluster setting
				const maxLargeInFlight = 3
				if healthy && largeInFlight < maxLargeInFlight {
					if send(ar, Quota(ar.cost)) {
						largeInFlight++
					}
				} else {
					// TODO: check queue size
					largeQueue = append(largeQueue, ar)
				}
			} else {
				queue = append(queue, ar)
			}
		case rr := <-rk.releasing:
			handleRelease(rr)
		case <-rk.stopper.ShouldQuiesce():
			return
		}
	}
}

type Quota float64

type acquireRequest struct {
	ctx     context.Context
	c       chan<- Quota
	entered time.Time
	cost    float64
}

type releaseRequest struct {
	quota float64
	err   error
	took  time.Duration
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
)

type Metrics struct {
	InFlightCost  *metric.GaugeFloat64
	Quota         *metric.GaugeFloat64
	LargeInFlight *metric.Gauge
	AverageCost   *metric.GaugeFloat64
	NanosPerCost  *metric.GaugeFloat64
	Errors        *metric.Counter
}

func newMetrics() Metrics {
	return Metrics{
		InFlightCost:  metric.NewGaugeFloat64(metaInFlightCost),
		Quota:         metric.NewGaugeFloat64(metaCurQuota),
		AverageCost:   metric.NewGaugeFloat64(metaAvgCost),
		LargeInFlight: metric.NewGauge(metaLargeInFlight),
		NanosPerCost:  metric.NewGaugeFloat64(metaNanosPerCost),
		Errors:        metric.NewCounter(metaErrorsReported),
	}
}
