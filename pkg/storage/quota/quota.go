package quota

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	metaResponseEstimate = metric.Metadata{
		Name:        "quota.response_estimate",
		Help:        "Estimated size of responses given backoff and recovery towards target.",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaTargetEstimate = metric.Metadata{
		Name:        "quota.target_response_estimate",
		Help:        "Exponential moving average size of responses based on observations.",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaInUse = metric.Metadata{
		Name:        "quota.in_use",
		Help:        "Bytes currently in use by in-flight requests",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaQueued = metric.Metadata{
		Name:        "quota.queued",
		Help:        "Requests currently queued",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaQPS = metric.Metadata{
		Name:        "quota.estimated_qps",
		Help:        "Estimate of QPS",
		Measurement: "Requests per second",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalQueued = metric.Metadata{
		Name:        "quota.total_queued",
		Help:        "Total number of requests which have been queued",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalTimeQueued = metric.Metadata{
		Name:        "quota.total_time_queued",
		Help:        "Total amount of time spent in the queue",
		Measurement: "Nanoseconds spend queueing",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRejected = metric.Metadata{
		Name:        "quota.rejected_requests",
		Help:        "Total number of bytes acquired",
		Measurement: "Nanoseconds spend queueing",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// QuotaSize is the follower reads closed timestamp update target duration.
var QuotaSize = settings.RegisterIntSetting(
	"kv.read_quota.byte_size",
	"bytes allocated by read quota pool",
	1<<28,
)

func newMetrics() Metrics {
	return Metrics{
		ResponseEstimate: metric.NewGauge(metaResponseEstimate),
		TargetEstimate:   metric.NewGauge(metaTargetEstimate),
		InUse:            metric.NewGauge(metaInUse),
		Queued:           metric.NewGauge(metaQueued),
		QPS:              metric.NewGaugeFloat64(metaQPS),
		TotalQueued:      metric.NewCounter(metaTotalQueued),
		TotalTimeQueued:  metric.NewCounter(metaTotalTimeQueued),
		Rejected:         metric.NewCounter(metaRejected),
	}
}

type Metrics struct {
	ResponseEstimate *metric.Gauge
	TargetEstimate   *metric.Gauge
	InUse            *metric.Gauge
	Queued           *metric.Gauge
	QPS              *metric.GaugeFloat64
	TotalQueued      *metric.Counter
	TotalTimeQueued  *metric.Counter
	Rejected         *metric.Counter
}

// TODO(ajwerner): there's a lot we could do to make this more sophisticated.
// One idea that seems interesting from the foundationdb folks is to put

type Config struct {
	Settings *cluster.Settings

	// QuotaSize is the number of bytes which can be used for read requests.
	QuotaSize uint64

	// QueueMax is the maximum number of requests which can be queued before
	// it is rejected.
	QueueMax int

	// QueueThreshold is the queue size at which the estimate will begin to
	// diminish
	QueueThreshold int

	// MaxEstimate is the maximum size a request will ever be estimated to be.
	MaxEstimate uint32

	// InitialEstimate is the initial guess for the size of a request.
	InitialEstimate uint32

	// BackoffFactor is how quickly the estimate should grow in the face of
	// under guessing.
	BackoffFactor float64

	// TickInterval is how frequently to update the estimates.
	TickInterval time.Duration

	// ResponseMin is the minimum size of a response we'll use to estimate
	// the average response size.
	ResponseMin uint32
}

const (
	defaultQuotaSize       = 1 << 30 // 1 GB
	defaultQueueMax        = 1024
	defaultQueueThreshold  = 512
	defaultMaxEstimate     = 64 * 1 << 20 // 64 MB
	defaultInitialEstimate = 64 * 1 << 10 // 64 kB
	defaultBackoffFactor   = 1.5
	defaultTickInterval    = 500 * time.Millisecond
	defaultResponseMin     = 1 << 10 // 1 kB
)

func (c *Config) setDefaults() {
	if c.QuotaSize <= 0 {
		c.QuotaSize = defaultQuotaSize
	}
	if c.QueueMax <= 0 {
		c.QueueMax = defaultQueueMax
	}
	if c.QueueThreshold <= 0 {
		c.QueueThreshold = defaultQueueThreshold
	}
	if c.MaxEstimate <= 0 {
		c.MaxEstimate = defaultMaxEstimate
	}
	if c.InitialEstimate <= 0 {
		c.InitialEstimate = defaultInitialEstimate
	}
	if c.BackoffFactor <= 0 {
		c.BackoffFactor = defaultBackoffFactor
	}
	if c.TickInterval <= 0 {
		c.TickInterval = defaultTickInterval
	}
	if c.ResponseMin <= 0 {
		c.ResponseMin = defaultResponseMin
	}
}

type Quota struct {
	acquired uint32
	used     uint32
}

func NewReadQuota(ctx context.Context, stopper *stop.Stopper, cfg Config) *QuotaPool {
	// TODO(ajwerner): consider adding a buffer to the acquiring and releasing
	// channels.
	rq := &QuotaPool{
		cfg:     cfg,
		metrics: newMetrics(),
		stopper: stopper,
		chanPool: sync.Pool{
			New: func() interface{} { return make(chan Quota) },
		},
		acquiring: make(chan quotaRequest, 1024),
		//		canceling: make(chan chan Quota),
		releasing: make(chan Quota, 1024),
	}
	rq.cfg.setDefaults()
	stopper.RunAsyncTask(ctx, "quota.loop", rq.loop)
	//	stopper.RunAsyncTask(ctx, "quota.loop", rq.cancelLoop)
	return rq
}

type QuotaPool struct {
	cfg      Config
	metrics  Metrics
	stopper  *stop.Stopper
	chanPool sync.Pool

	acquiring chan quotaRequest
	// canceling chan chan Quota
	releasing chan Quota
}

func (rq *QuotaPool) Metrics() *Metrics { return &rq.metrics }

var ErrNoQuota = errors.New("queue full")

func (rq *QuotaPool) Release(q Quota, used uint32) {
	q.used = used
	select {
	case rq.releasing <- q:
	case <-rq.stopper.ShouldQuiesce():
	}
}

func (rq *QuotaPool) Acquire(ctx context.Context, isRetry bool) (Quota, error) {
	c := rq.chanPool.Get().(chan Quota)
	select {
	case rq.acquiring <- quotaRequest{
		ctx:     ctx,
		c:       c,
		entered: time.Now(),
		isRetry: isRetry,
	}:
	case <-ctx.Done():
		return Quota{}, ctx.Err()
	case <-rq.stopper.ShouldQuiesce():
		return Quota{}, stop.ErrUnavailable
	}
	select {
	case q := <-c:
		rq.chanPool.Put(c)
		if q.acquired == 0 {
			return q, &roachpb.ScanBackpressureError{}
		}
		return q, nil
	case <-ctx.Done():
		return Quota{}, ctx.Err()
	case <-rq.stopper.ShouldQuiesce():
		return Quota{}, stop.ErrUnavailable
	}
}

type quotaRequest struct {
	ctx     context.Context
	c       chan<- Quota
	entered time.Time
	isRetry bool
}

func (rq *QuotaPool) loop(ctx context.Context) {

	var (
		// TODO(ajwerner): Make this a ring buffer.
		queue []quotaRequest

		recoveryTarget = (60 * time.Second).Seconds()

		// We want to keep an estimate of the trailing average sizes of responses
		// and then we want to update the currently used estimate.

		// okay so we have an approximation of QPS and we use that to know how much
		// to move in our linear approximation of the current request sizes

		estimate      = float64(rq.cfg.InitialEstimate)
		trailingAvg   = estimate
		step          float64
		queries       int
		totalQueries  float64
		lastQPS       float64
		inUse         uint64
		last          time.Time
		updateMetrics = func() {
			rq.metrics.QPS.Update(lastQPS)
			rq.metrics.Queued.Update(int64(len(queue)))
			rq.metrics.InUse.Update(int64(inUse))
			rq.metrics.TargetEstimate.Update(int64(trailingAvg))
			rq.metrics.ResponseEstimate.Update(int64(estimate))
		}
		send = func(qr quotaRequest, q Quota) bool {
			select {
			case <-qr.ctx.Done():
				return false
			case qr.c <- q:
				return true
			}
		}
		updateEstimate = func(q Quota) {
			if q.acquired < q.used {
				estimate *= rq.cfg.BackoffFactor
				if uint32(estimate) > rq.cfg.MaxEstimate {
					estimate = float64(rq.cfg.MaxEstimate)
				}
				// log.Infof(ctx, "increasing estimate to %v", estimate)
			} else if q.used > rq.cfg.ResponseMin {
				if totalQueries == 0 {
					trailingAvg = float64(q.used)
				} else if totalQueries < 1024 {
					trailingAvg = float64(q.used) * (1 / totalQueries)
				} else {
					trailingDecay := 2 / (lastQPS + 1)
					trailingAvg = float64(q.used)*trailingDecay + float64(q.used)*(1-trailingDecay)
				}
				if len(queue) < rq.cfg.QueueThreshold {
					if newEstimate := estimate - step; newEstimate > trailingAvg {
						estimate = newEstimate
					} else {
						estimate = trailingAvg
					}
				}
				//log.Infof(ctx, "decreasing estimate by %v to %v %v", step, estimate, len(queue))
			}
		}
		tick = func(now time.Time) {
			defer updateMetrics()
			if queries == 0 {
				return
			}
			// On each tick we compute the QPS and use the average over the last two
			// ticks.
			qps := float64(queries) / now.Sub(last).Seconds()
			last = now
			qpsEstimate := (qps / 2) + (lastQPS / 2)
			lastQPS = qpsEstimate
			// log.Infof(ctx, "queries %v qps %v", queries, qpsEstimate)
			queries = 0
			if estimate < trailingAvg {
				estimate = trailingAvg
				step = 0
				return
			}
			deltaY := estimate - trailingAvg
			if qpsEstimate < 64 {
				qpsEstimate = 64
			}
			step = deltaY / (recoveryTarget * qpsEstimate)
			rq.cfg.QuotaSize = uint64(QuotaSize.Get(&rq.cfg.Settings.SV))
		}
		ticker = timeutil.NewTimer()
	)
	ticker.Reset(rq.cfg.TickInterval)
	for {
		select {
		case t := <-ticker.C:
			//log.Infof(ctx, "tick: %v %v %v %v", inUse, estimate, trailingAvg, queries)
			ticker.Read = true
			tick(t)
			ticker.Reset(rq.cfg.TickInterval)
		case qr := <-rq.acquiring:
			if est := uint64(estimate); est+inUse > rq.cfg.QuotaSize {
				lq := len(queue)
				if (lq > rq.cfg.QueueMax) ||
					(!qr.isRetry && lq > (rq.cfg.QueueMax/2) &&
						rand.Float64() < (float64(lq)/float64(rq.cfg.QueueMax))) {
					if send(qr, Quota{}) {
						rq.metrics.Rejected.Inc(1)
					}
				} else {
					rq.metrics.TotalQueued.Inc(1)
					queue = append(queue, qr)
				}
			} else {
				if send(qr, Quota{acquired: uint32(est)}) {
					queries++
					inUse += est
				}
			}
		case q := <-rq.releasing:
			updateEstimate(q)
			inUse -= uint64(q.acquired)
			est := uint64(estimate)
			for len(queue) > 0 && inUse+est < rq.cfg.QuotaSize {
				if send(queue[0], Quota{acquired: uint32(est)}) {
					rq.metrics.TotalTimeQueued.Inc(time.Since(queue[0].entered).Nanoseconds())
					queries++
					inUse += est
				}
				queue[0] = quotaRequest{}
				queue = queue[1:]
			}
		case <-rq.stopper.ShouldQuiesce():
			return
		}
	}
}

// // cancelLoop
// func (rq *QuotaPool) cancelLoop(ctx context.Context) {
// 	var (
// 		toCancel     []chan Quota
// 		nextToCancel = func() chan Quota {
// 			if len(toCancel) > 0 {
// 				return toCancel[0]
// 			}
// 			return nil
// 		}
// 		toRelease     []Quota
// 		nextToRelease = func() Quota {
// 			if len(toRelease) > 0 {
// 				return toRelease[0]
// 			}
// 			return Quota{}
// 		}
// 		releaseChan = func() chan<- Quota {
// 			if len(toRelease) > 0 {
// 				return rq.releasing
// 			}
// 			return nil
// 		}
// 	)
// 	for {
// 		select {
// 		case c := <-rq.canceling:
// 			toCancel = append(toCancel, c)
// 		case q := <-nextToCancel():
// 			toRelease = append(toRelease, q)
// 		case releaseChan() <- nextToRelease():
// 			toRelease = toRelease[1:]
// 		case <-rq.stopper.ShouldQuiesce():
// 			return
// 		}
// 	}
// }
