package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Controller keeps track of the allowed level for requests.
// It stores a current admission level and decides whether requests can be
// admitted.
// In order to make this decision the controller needs to tick every so often
//
type Controller struct {
	maxReqsPerInterval uint64
	tickInterval       time.Duration
	pruneRate          float64
	growRate           float64
	overloadSignal     func(Priority) bool
	metrics            Metrics

	// We need some sort of queues
	// Maybe we just hide those as stack-local state in some processing goroutine.
	mu struct {
		syncutil.RWMutex
		cond sync.Cond

		curPriority Priority
		nextTick    time.Time

		numReqs uint64
		hist    histogram

		byteCounts uint64
		byteHist   histogram

		lastDec int
	}
}

type Metrics struct {
	AdmissionLevel *metric.Gauge
	NumTicks       *metric.Counter
	Inc            *metric.Counter
	Dec            *metric.Counter
}

func makeMeta(md metric.Metadata, name string) metric.Metadata {
	md.Name = name + "." + md.Name
	return md
}

func makeMetrics(name string) Metrics {
	return Metrics{
		AdmissionLevel: metric.NewGauge(makeMeta(metaAdmissionLevel, name)),
		NumTicks:       metric.NewCounter(makeMeta(metaNumTicks, name)),
		Inc:            metric.NewCounter(makeMeta(metaNumInc, name)),
		Dec:            metric.NewCounter(makeMeta(metaNumDec, name)),
	}
}

// WaitForAdmitted waits for the current context to be admitted.
func WaitForAdmitted(ctx context.Context, c *Controller) error {
	// Make a retrier
	// Get the priority
	// Wait with some backoff
	// Eventually make this more efficient
	opts := retry.Options{
		Multiplier:     1.25,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Second,
	}
	prio := PriorityFromContext(ctx)
	retry := retry.StartWithCtx(ctx, opts)
	for retry.Next() {
		now := timeutil.Now()
		if c.AdmitAt(prio, now) {
			return nil
		}
	}
	// retry.Next() will only return false if our context is canceled or the
	// stopper has been stopped.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return stop.ErrUnavailable
}

var (
	metaAdmissionLevel = metric.Metadata{
		Name:        "admission.level",
		Help:        "Current admission level",
		Unit:        metric.Unit_COUNT,
		Measurement: "admission level",
	}
	metaNumTicks = metric.Metadata{
		Name:        "admission.ticks",
		Help:        "Number of ticks",
		Unit:        metric.Unit_COUNT,
		Measurement: "ticks",
	}
	metaNumDec = metric.Metadata{
		Name:        "admission.decrease",
		Help:        "Number of times the tick has decreased the level",
		Unit:        metric.Unit_COUNT,
		Measurement: "ticks",
	}
	metaNumInc = metric.Metadata{
		Name:        "admission.increase",
		Help:        "Number of times the tick has increase the level",
		Unit:        metric.Unit_COUNT,
		Measurement: "ticks",
	}
)

func (c *Controller) stringLocked() string {
	return fmt.Sprintf("Controller{curLevel: %v, numReqs: %d, nextTick: %s (%s), hist: %v, numBytes: %d, histBytes: %v}",
		c.mu.curPriority, c.mu.numReqs, c.mu.nextTick, timeutil.Until(c.mu.nextTick), c.mu.hist, c.mu.byteCounts, c.mu.byteHist)
}

func (c *Controller) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stringLocked()
}

func (c *Controller) Level() Priority {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.curPriority
}

// TODO(ajwerner): justify these, they're taken from the DAGOR paper and cut
// in half.
const (
	defaultMaxReqsPerInterval = 2000
	defaultTickInterval       = time.Second
	defaultPruneRate          = .05
	defaultGrowRate           = .01
)

func NewController(
	name string,
	overLoadSignal func(prev Priority) bool,
	tickInterval time.Duration,
	pruneRate, growRate float64,
) *Controller {
	if tickInterval <= 0 {
		tickInterval = defaultTickInterval
	}
	if pruneRate <= 0 {
		pruneRate = defaultPruneRate
	}
	if growRate <= 0 {
		growRate = defaultGrowRate
	}
	c := &Controller{
		maxReqsPerInterval: defaultMaxReqsPerInterval,
		tickInterval:       tickInterval,
		pruneRate:          pruneRate,
		growRate:           growRate,
		overloadSignal:     overLoadSignal,
	}
	c.metrics = makeMetrics(name)
	c.mu.cond.L = c.mu.RLocker()
	c.mu.curPriority = minPriority
	return c
}

func (c *Controller) Metrics() *Metrics {
	return &c.metrics
}

func (c *Controller) maybeTickRLocked(now time.Time) {
	if now.Before(c.mu.nextTick) {
		return
	}
	c.mu.RUnlock()
	defer c.mu.RLock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Before(c.mu.nextTick) {
		return
	}
	c.tickLocked(now)
}

func (c *Controller) Report(p Priority, size uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.mu.byteHist.record(p, size)
	atomic.AddUint64(&c.mu.byteCounts, size)
}

func (c *Controller) Admit(p Priority) bool {
	return c.AdmitAt(p, timeutil.Now())
}

func (c *Controller) AdmitAt(p Priority, now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	defer c.mu.hist.record(p, 1)
	c.maybeTickRLocked(now)
	if p.Level != MaxLevel && p.less(c.mu.curPriority) {
		return false
	}
	numReqs := atomic.AddUint64(&c.mu.numReqs, 1)
	for numReqs > c.maxReqsPerInterval {
		if numReqs == (c.maxReqsPerInterval + 1) {
			c.tickRLocked(now)
		} else {
			c.mu.cond.Wait()
		}
		// c was just ticked so we add ourselves again
		numReqs = atomic.AddUint64(&c.mu.numReqs, 1)
	}
	return true
}

func (c *Controller) tickRLocked(now time.Time) {
	c.mu.RUnlock()
	defer c.mu.RLock()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tickLocked(now)
}

func (c *Controller) tickLocked(now time.Time) {
	defer c.mu.cond.Broadcast()
	c.mu.nextTick = now.Add(c.tickInterval)
	numReqs := atomic.SwapUint64(&c.mu.numReqs, 0)
	numBytes := atomic.SwapUint64(&c.mu.byteCounts, 0)
	n, hist := numReqs, &c.mu.hist
	if numBytes > 0 {
		n, hist = numBytes, &c.mu.byteHist
	}
	prev := c.mu.curPriority
	const minNumReqs = 0
	overloaded := numReqs > minNumReqs && c.overloadSignal(prev)
	if overloaded {
		c.mu.lastDec = 0
		c.metrics.Inc.Inc(1)
		c.mu.curPriority = findHigherPriority(c.mu.curPriority, n, c.pruneRate, hist)
	} else if c.mu.lastDec < 2 {
		c.mu.lastDec++
	} else {
		c.mu.lastDec = 0
		c.metrics.Dec.Inc(1)
		c.mu.curPriority = findLowerPriority(c.mu.curPriority, n, c.growRate, hist)
	}
	if log.V(1) {
		log.Infof(context.TODO(), "tick: overload %v (%v, %v count, bytes) prev %v next %v %v", overloaded, numReqs, numBytes, prev, c.mu.curPriority, c.stringLocked())
	}
	c.mu.hist = histogram{}
	c.mu.byteHist = histogram{}
	c.metrics.AdmissionLevel.Update(int64(c.mu.curPriority.Encode()))
	c.metrics.NumTicks.Inc(1)
}

var maxPriority = Priority{MaxLevel, maxShard}
var minPriority = Priority{MinLevel, minShard}

func findHigherPriority(prev Priority, total uint64, pruneRate float64, h *histogram) Priority {
	if total == 0 || h.countAboveIsEmpty(prev) {
		return prev
	}
	reqs := total
	target := uint64(float64(total) * (1 - pruneRate))
	lastWithSome := prev
	for cur := prev; cur.Level != MaxLevel; cur = cur.inc() {
		if count := h.countAt(cur); count > 0 {
			reqs -= count
			lastWithSome = cur
		}
		if reqs < target {
			return cur
		}
	}
	return lastWithSome
}

func findLowerPriority(prev Priority, total uint64, growRate float64, h *histogram) Priority {
	reqs := total
	cur := prev.dec()
	target := total
	if count := h.countForLevelAbove(cur); count != 0 {
		target += uint64(float64(count)*growRate) + 1
	} else {
		return cur
	}
	for ; cur != minPriority; prev, cur = cur, cur.dec() {
		reqs += h.countAt(cur)
		if reqs > target {
			return cur
		}
	}
	return minPriority
}

func (p Priority) dec() (r Priority) {
	if p == minPriority {
		return p
	}
	if p.Shard == minShard {
		return Priority{
			Level: levelFromBucket(bucketFromLevel(p.Level) - 1),
			Shard: maxShard,
		}
	}
	return Priority{
		Level: p.Level,
		Shard: shardFromBucket(bucketFromShard(p.Shard) - 1),
	}
}

func (p Priority) inc() Priority {
	if p == maxPriority {
		return p
	}
	if p.Shard == maxShard {
		return Priority{
			Level: levelFromBucket(bucketFromLevel(p.Level) + 1),
			Shard: minShard,
		}
	}
	return Priority{
		Level: p.Level,
		Shard: shardFromBucket(bucketFromShard(p.Shard) + 1),
	}
}

type histogram struct {
	counters [numLevels][numShards]uint64
}

func (h *histogram) countForLevelAbove(p Priority) (count uint64) {
	level := bucketFromLevel(p.Level)
	for shard := bucketFromShard(p.Level) + 1; shard < numShards; shard++ {
		count += atomic.LoadUint64(&h.counters[level][shard])
	}
	return count
}

func (h *histogram) countAboveIsEmpty(p Priority) bool {
	for l := p.inc(); l != maxPriority; l = l.inc() {
		if h.countAt(l) > 0 {
			return false
		}
	}
	return true
}

func (h *histogram) countAt(p Priority) uint64 {
	level, shard := bucketFromLevel(p.Level), bucketFromShard(p.Shard)
	return atomic.LoadUint64(&h.counters[level][shard])
}

func (h *histogram) record(p Priority, c uint64) {
	level, shard := bucketFromLevel(p.Level), bucketFromShard(p.Shard)
	atomic.AddUint64(&h.counters[level][shard], c)
}
