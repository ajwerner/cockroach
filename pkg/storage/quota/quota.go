package quota

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	tdigest "github.com/ajwerner/tdigestc/go"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/time/rate"
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
	metaAvgAggBytesPerSec = metric.Metadata{
		Name:        "quota.avg_agg_bytes_per_sec",
		Help:        "Aggregate bytes per second served by the entire system",
		Measurement: "Bytes/s flowing out of the system",
		Unit:        metric.Unit_BYTES,
	}
	metaAvgPerReqBytesPerSec = metric.Metadata{
		Name:        "quota.avg_per_req_bytes_per_sec",
		Help:        "Per request bytes per second served by the entire system",
		Measurement: "Bytes/s flowing out of the system",
		Unit:        metric.Unit_BYTES,
	}
	metaAvgPerReqBytesPerRec = metric.Metadata{
		Name:        "quota.avg_per_req_bytes",
		Help:        "Per request bytes served by the entire system",
		Measurement: "Bytes/s flowing out of the system",
		Unit:        metric.Unit_BYTES,
	}
	metaAvgTimePerRead = metric.Metadata{
		Name:        "quota.avg_time_per_read",
		Help:        "Average time per read",
		Measurement: "Bytes/s flowing out of the system",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaQPSRateExperiment = metric.Metadata{
		Name:        "quota.qps_rate_experiment",
		Help:        "QPS estimated through a rate",
		Measurement: "QPS",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaQuotaBackoffFactor = metric.Metadata{
		Name:        "quota.backoff_factor",
		Help:        "Ratio between aggregate throughput and per-req throughput",
		Measurement: "ration",
		Unit:        metric.Unit_PERCENT,
	}
	metaInside = metric.Metadata{
		Name:        "quota.inside",
		Measurement: "count",
		Unit:        metric.Unit_COUNT,
	}
	metaTD001 = metric.Metadata{
		Name:        "latency.td.001",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
	metaTD01 = metric.Metadata{
		Name:        "latency.td.01",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
	metaTD10 = metric.Metadata{
		Name:        "latency.td.10",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
	metaTD50 = metric.Metadata{
		Name:        "latency.td.50",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
	metaTD90 = metric.Metadata{
		Name:        "latency.td.90",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
	metaTD99 = metric.Metadata{
		Name:        "latency.td.99",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
	metaTD999 = metric.Metadata{
		Name:        "latency.td.999",
		Measurement: "time",
		Unit:        metric.Unit_COUNT,
	}
)

// QuotaSize is the follower reads closed timestamp update target duration.
var QuotaSize = settings.RegisterIntSetting(
	"kv.read_quota.byte_size",
	"bytes allocated by read quota pool",
	1<<28,
)

// RateLimit is the current rate limit for reads in the system
var RateLimit = settings.RegisterFloatSetting(
	"kv.rate_limit.read_qps",
	"reads allowed per second",
	10000,
)

// RateLimitJSON is the current rate limit for reads in the system
var RateLimitJSON = settings.RegisterStringSetting(
	"kv.rate_limit.read_qps_per_node",
	"reads allowed per second",
	"{}",
)

func makeMetrics() Metrics {
	return Metrics{
		ResponseEstimate:  metric.NewGauge(metaResponseEstimate),
		TargetEstimate:    metric.NewGauge(metaTargetEstimate),
		InUse:             metric.NewGauge(metaInUse),
		Queued:            metric.NewGauge(metaQueued),
		QPS:               metric.NewGaugeFloat64(metaQPS),
		TotalQueued:       metric.NewCounter(metaTotalQueued),
		TotalTimeQueued:   metric.NewCounter(metaTotalTimeQueued),
		Rejected:          metric.NewCounter(metaRejected),
		AggBytesPerSec:    metric.NewGaugeFloat64(metaAvgAggBytesPerSec),
		PerReqBytesPerSec: metric.NewGaugeFloat64(metaAvgPerReqBytesPerSec),
		AvgTimePerRead:    metric.NewGaugeFloat64(metaAvgTimePerRead),
		QPSRate:           metric.NewRate(10 * time.Second),
		QPSRateGauge:      metric.NewGaugeFloat64(metaQPSRateExperiment),
		PerReqBytes:       metric.NewGaugeFloat64(metaAvgPerReqBytesPerRec),
		BackoffFactor:     metric.NewGaugeFloat64(metaQuotaBackoffFactor),
		Inside:            metric.NewGaugeFloat64(metaInside),
		TD001:             metric.NewGaugeFloat64(metaTD001),
		TD01:              metric.NewGaugeFloat64(metaTD01),
		TD10:              metric.NewGaugeFloat64(metaTD10),
		TD50:              metric.NewGaugeFloat64(metaTD50),
		TD90:              metric.NewGaugeFloat64(metaTD90),
		TD99:              metric.NewGaugeFloat64(metaTD99),
		TD999:             metric.NewGaugeFloat64(metaTD999),
	}
}

type Metrics struct {
	ResponseEstimate  *metric.Gauge
	TargetEstimate    *metric.Gauge
	InUse             *metric.Gauge
	Queued            *metric.Gauge
	QPS               *metric.GaugeFloat64
	TotalQueued       *metric.Counter
	TotalTimeQueued   *metric.Counter
	Rejected          *metric.Counter
	AggBytesPerSec    *metric.GaugeFloat64
	PerReqBytesPerSec *metric.GaugeFloat64
	PerReqBytes       *metric.GaugeFloat64
	AvgTimePerRead    *metric.GaugeFloat64
	QPSRate           *metric.Rate
	QPSRateGauge      *metric.GaugeFloat64
	BackoffFactor     *metric.GaugeFloat64
	Inside            *metric.GaugeFloat64
	TD001             *metric.GaugeFloat64
	TD01              *metric.GaugeFloat64
	TD10              *metric.GaugeFloat64
	TD50              *metric.GaugeFloat64
	TD90              *metric.GaugeFloat64
	TD99              *metric.GaugeFloat64
	TD999             *metric.GaugeFloat64
}

// TODO(ajwerner): there's a lot we could do to make this more sophisticated.
// One idea that seems interesting from the foundationdb folks is to put

type Config struct {
	NodeID   roachpb.NodeID
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
	defaultQuotaSize       = 1 << 40 // 1 GB
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
	took     time.Duration
	res      *rate.Reservation
}

func NewReadQuota(ctx context.Context, stopper *stop.Stopper, cfg Config) *QuotaPool {
	// TODO(ajwerner): consider adding a buffer to the acquiring and releasing
	// channels.
	rq := &QuotaPool{
		cfg:     cfg,
		metrics: makeMetrics(),
		stopper: stopper,
		chanPool: sync.Pool{
			New: func() interface{} { return make(chan Quota) },
		},
		l:         rate.NewLimiter(rate.Limit(RateLimit.Get(&cfg.Settings.SV)), 10),
		acquiring: make(chan quotaRequest, 4096),
		//		canceling: make(chan chan Quota),
		releasing: make(chan Quota, 4096),
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
	l        *rate.Limiter

	acquiring chan quotaRequest
	// canceling chan chan Quota
	releasing chan Quota
}

func (rq *QuotaPool) Metrics() *Metrics { return &rq.metrics }

var ErrNoQuota = errors.New("queue full")

const usedMin = 4096

func (rq *QuotaPool) Release(q Quota, used uint32, took time.Duration) {
	q.took = took - q.took
	q.used = used
	if q.used < usedMin {
		q.used = usedMin
	}
	select {
	case rq.releasing <- q:
	case <-rq.stopper.ShouldQuiesce():
	}
}

func (rq *QuotaPool) Acquire(ctx context.Context, isRetry bool) (Quota, error) {
	start := time.Now()
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
		delay := q.res.Delay()
		if delay != 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return Quota{}, ctx.Err()
			}
		}
		q.took = time.Since(start)
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

		estimate     = float64(rq.cfg.InitialEstimate)
		trailingAvg  = estimate
		step         float64
		queries      int
		totalQueries float64
		lastQPS      float64
		inUse        uint64
		last         time.Time
		lastRel      time.Time
		//rateHistogram     = hdrhistogram.NewWindowed(50, 0, 128000000, 2)
		totalBytes        float64
		totalTime         float64
		totalRate         float64
		totalReleased     int
		aggBytesPerSec    float64
		aggBytesPerReq    float64
		perReqBytesPerSec float64
		l                 = RateLimit.Get(&rq.cfg.Settings.SV)
		haveLJSON         = false
		lJSON             = RateLimitJSON.Get(&rq.cfg.Settings.SV)
		timePerReq        float64
		td                = tdigest.New(128)
		agg               = tdigest.New(128)
		updateMetrics     = func() {
			rq.metrics.QPS.Update(lastQPS)
			rq.metrics.Queued.Update(int64(len(queue)))
			rq.metrics.InUse.Update(int64(inUse))
			rq.metrics.TargetEstimate.Update(int64(trailingAvg))
			rq.metrics.ResponseEstimate.Update(int64(estimate))
			rq.metrics.AvgTimePerRead.Update(timePerReq)
			// If we know the average QPS and we know the average time per request then we know the average amount of stuff in the system
			// If we use that for an interval * time time of the interval that gives us
			//m := rateHistogram.Merge()
			rq.metrics.AggBytesPerSec.Update(aggBytesPerSec)
			rq.metrics.PerReqBytes.Update(aggBytesPerReq)
			rq.metrics.PerReqBytesPerSec.Update(perReqBytesPerSec)
			//rateHistogram.Rotate()
			qps := rq.metrics.QPSRate.Value()
			rq.metrics.QPSRateGauge.Update(qps)
			rq.metrics.TD001.Update(agg.ValueAt(.001))
			rq.metrics.TD01.Update(agg.ValueAt(.01))
			rq.metrics.TD10.Update(agg.ValueAt(.1))
			rq.metrics.TD50.Update(agg.ValueAt(.5))
			rq.metrics.TD90.Update(agg.ValueAt(.9))
			rq.metrics.TD99.Update(agg.ValueAt(.99))
			rq.metrics.TD999.Update(agg.ValueAt(.999))
			if perReqBytesPerSec > 0 {
				rq.metrics.BackoffFactor.Update(aggBytesPerSec / (perReqBytesPerSec / (qps * timePerReq / 1e9)))
			}
			rq.metrics.Inside.Update(qps * timePerReq / 1e9)
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
			if q.acquired < uint32(estimate)+1 {
				estimate *= rq.cfg.BackoffFactor
				if uint32(estimate) > rq.cfg.MaxEstimate {
					estimate = float64(rq.cfg.MaxEstimate)
				}

			} else if q.used > rq.cfg.ResponseMin {
				if totalQueries == 0 {
					trailingAvg = float64(q.used)
				} else if totalQueries < 1024 {
					trailingAvg = float64(q.used) * (1 / totalQueries)
				} else {
					trailingDecay := float64(2 / (lastQPS + 1))
					trailingAvg = float64(q.used)*trailingDecay + float64(q.used)*(1-trailingDecay)
				}
				if len(queue) < rq.cfg.QueueThreshold {
					if newEstimate := estimate - step; newEstimate > trailingAvg {
						estimate = newEstimate
					} else {
						estimate = trailingAvg
					}
				}

			}

			td.Record(float64(q.took))
			rq.metrics.QPSRate.Add(1)
			if q.took > 0 {
				totalBytes += float64(q.used)
				totalTime += float64(q.took.Nanoseconds())
				totalRate += (float64(q.used) * float64(q.used)) / q.took.Seconds()
				// rateHistogram.Current.RecordValue(int64(totalRate))
				totalReleased++
			}
		}
		tick = func(now time.Time) {
			if newL := RateLimit.Get(&rq.cfg.Settings.SV); newL != l && !haveLJSON {
				l = newL
				rq.l.SetLimit(rate.Limit(newL))
			}
			if jsStr := RateLimitJSON.Get(&rq.cfg.Settings.SV); jsStr != lJSON {
				lJSON = jsStr
				var m map[string]float64
				if err := json.Unmarshal([]byte(jsStr), &m); err == nil {
					if r, ok := m[strconv.Itoa(int(rq.cfg.NodeID))]; ok {
						haveLJSON = true
						rq.l.SetLimit(rate.Limit(r))
					}
				} else {
					rq.l.SetLimit(rate.Limit(l))
				}
			}
			defer updateMetrics()
			const decay = .98
			const alpha = (1 - decay)
			agg.Decay(decay)
			agg.Merge(td)
			td.Reset()
			if totalReleased != 0 {
				aggBytesPerSec = decay*aggBytesPerSec + (alpha * (totalBytes / now.Sub(lastRel).Seconds()))

				aggBytesPerReq = decay*aggBytesPerReq + (alpha * (totalBytes / float64(totalReleased)))
				timePerReq = decay*timePerReq + (alpha * (totalTime / float64(totalReleased)))

				perReqBytesPerSec = decay*perReqBytesPerSec + (alpha * (totalRate / totalBytes))
			} else {
				timePerReq = decay * timePerReq
				perReqBytesPerSec = decay * perReqBytesPerSec
				perReqBytesPerSec = decay * perReqBytesPerSec
				aggBytesPerSec = decay * aggBytesPerSec
			}
			lastRel = now
			totalBytes = 0
			totalTime = 0
			totalRate = 0
			totalReleased = 0
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
			if est := uint64(estimate); false {
				lq := len(queue)
				if (lq > rq.cfg.QueueMax) ||
					(!qr.isRetry && rand.Float64() < (float64(lq)/float64(rq.cfg.QueueMax))) {
					if send(qr, Quota{}) {
						rq.metrics.Rejected.Inc(1)
					}
				} else {
					rq.metrics.TotalQueued.Inc(1)
					queue = append(queue, qr)
				}
			} else {
				if send(qr, Quota{acquired: uint32(est), res: rq.l.Reserve()}) {
					queries++
					inUse += est
				}
			}
		case q := <-rq.releasing:
			updateEstimate(q)
			inUse -= uint64(q.acquired)
			est := uint64(estimate)
			for len(queue) > 0 && inUse+est < rq.cfg.QuotaSize {
				if send(queue[0], Quota{acquired: uint32(est), res: rq.l.Reserve()}) {
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
