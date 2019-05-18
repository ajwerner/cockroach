package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Controller keeps track of the allowed level for requests.
// It stores a current admission level and decides whether requests can be
// admitted.
// In order to make this decision the controller needs to tick every so often
//
type Controller struct {
	maxReqsPerInterval uint32
	tickInterval       time.Duration
	pruneRate          float64
	overloadSignal     func() bool
	metrics            Metrics

	// We need some sort of queues
	// Maybe we just hide those as stack-local state in some processing goroutine.
	mu struct {
		syncutil.RWMutex
		cond sync.Cond

		curPriority Priority
		nextTick    time.Time

		numReqs uint32
		hist    histogram
	}
}

type Metrics struct {
	AdmissionLevel *metric.Gauge
	NumTicks       *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		AdmissionLevel: metric.NewGauge(metaAdmissionLevel),
		NumTicks:       metric.NewCounter(metaNumTicks),
	}
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
)

func (c *Controller) stringLocked() string {
	return fmt.Sprintf("Controller{curLevel: %v, numReqs: %d, nextTick: %s (%s), hist: %v}",
		c.mu.curPriority, c.mu.numReqs, c.mu.nextTick, timeutil.Until(c.mu.nextTick), c.mu.hist)
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

// TODO(ajwerner): justify these, they're taken from the DAGOR paper.
const (
	defaultMaxReqsPerInterval = 2000
	defaultTickInterval       = 1000 * time.Millisecond
	defaultPruneRate          = .05
)

func NewController(overLoadSignal func() bool) *Controller {
	c := &Controller{
		maxReqsPerInterval: defaultMaxReqsPerInterval,
		tickInterval:       defaultTickInterval,
		pruneRate:          defaultPruneRate,
		overloadSignal:     overLoadSignal,
	}
	c.metrics = makeMetrics()
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

func (c *Controller) Admit(p Priority) bool {
	return c.AdmitAt(p, timeutil.Now())
}

func (c *Controller) AdmitAt(p Priority, now time.Time) bool {
	if p.Level == MaxLevel {
		return true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.maybeTickRLocked(now)
	if p.less(c.mu.curPriority) {
		return false
	}
	numReqs := atomic.AddUint32(&c.mu.numReqs, 1)
	for numReqs > c.maxReqsPerInterval {
		if numReqs == (c.maxReqsPerInterval + 1) {
			c.tickRLocked(now)
		} else {
			c.mu.cond.Wait()
		}
		// c was just ticked so we add ourselves again
		numReqs = atomic.AddUint32(&c.mu.numReqs, 1)
	}
	c.mu.hist.record(p)
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
	numReqs := atomic.SwapUint32(&c.mu.numReqs, 0)
	const minNumReqs = 32
	overloaded := numReqs > minNumReqs && c.overloadSignal()
	prev := c.mu.curPriority
	if overloaded {
		c.mu.curPriority = findNextPriority(c.mu.curPriority, numReqs, c.pruneRate, &c.mu.hist)
	} else {
		c.mu.curPriority = c.mu.curPriority.dec()
	}
	if log.V(1) {
		log.Infof(context.TODO(), "tick: overload %v %v prev %v next %v %v", overloaded, numReqs, prev, c.mu.curPriority, c.stringLocked())
	}
	c.mu.hist = histogram{}
	c.metrics.AdmissionLevel.Update(int64(c.mu.curPriority.Encode()))
	c.metrics.NumTicks.Inc(1)
}

var maxPriority = Priority{MaxLevel, maxShard}
var minPriority = Priority{MinLevel, minShard}

func findNextPriority(prev Priority, total uint32, pruneRate float64, h *histogram) Priority {
	reqs := total
	target := uint32(float64(total) * (1 - pruneRate))
	for cur := prev; cur != maxPriority; cur = cur.inc() {
		reqs -= h.countAt(cur)
		if reqs < target {
			return cur
		}
	}
	return maxPriority
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
	counters [numLevels][numShards]uint32
}

func (h *histogram) countAt(p Priority) uint32 {
	level, shard := bucketFromLevel(p.Level), bucketFromShard(p.Shard)
	return atomic.LoadUint32(&h.counters[level][shard])
}

func (h *histogram) record(p Priority) {
	level, shard := bucketFromLevel(p.Level), bucketFromShard(p.Shard)
	atomic.AddUint32(&h.counters[level][shard], 1)
}
