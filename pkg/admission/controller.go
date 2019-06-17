package admission

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/perf/benchstat"
)

// Controller keeps track of the allowed level for requests.
// It stores a current admission level and decides whether requests can be
// admitted.
// In order to make this decision the controller needs to tick every so often
//
type Controller struct {
	maxReqsPerInterval int64
	maxBlocked         int64
	tickInterval       time.Duration
	pruneRate          float64
	growRate           float64
	overloadSignal     func(Priority) bool
	metrics            Metrics

	wq waitQueue

	// We need some sort of queues
	// Maybe we just hide those as stack-local state in some processing goroutine.
	mu struct {
		syncutil.RWMutex
		tickCond  sync.Cond
		blockCond sync.Cond

		nextTick time.Time

		admissionLevel Priority
		rejectionLevel Priority

		numReqs    int64
		numBlocked int64
		hist       histogram

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

// // WaitForAdmitted waits for the current context to be admitted.
// func WaitForAdmitted(ctx context.Context, c *Controller) error {
// 	// Make a retrier
// 	// Get the priority
// 	// Wait with some backoff
// 	// Eventually make this more efficient
// 	opts := retry.Options{
// 		Multiplier:     1.25,
// 		InitialBackoff: time.Millisecond,
// 		MaxBackoff:     time.Second,
// 	}
// 	prio := PriorityFromContext(ctx)
// 	retry := retry.StartWithCtx(ctx, opts)
// 	for retry.Next() {
// 		now := timeutil.Now()
// 		if c.AdmitAt(prio, now) {
// 			return nil
// 		}
// 	}
// 	// retry.Next() will only return false if our context is canceled or the
// 	// stopper has been stopped.
// 	if ctxErr := ctx.Err(); ctxErr != nil {
// 		return ctxErr
// 	}
// 	return stop.ErrUnavailable
// }

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
	var b strings.Builder
	b.WriteString("S  ")
	for lb := numLevels - 1; true; lb-- {
		b.WriteString("|  ")
		if levelString, ok := levelStrings[levelFromBucket(lb)]; ok {
			b.WriteString(levelString)
		} else {
			b.WriteString("   ")
		}
		b.WriteString(" ")
		if lb == 0 {
			b.WriteString("|\n")
			break
		}
	}
	//b.WriteString("-------------------------\n")
	var skipped bool
	for i, s := range shards {
		sb := numShards - i - 1
		if sb < numShards-1 && sb > 0 &&
			c.mu.admissionLevel.Shard != s &&
			c.mu.rejectionLevel.Shard != s &&
			c.mu.hist.emptyAtShard(s) &&
			c.wq.emptyAtShard(s) {
			skipped = true
			continue
		} else if skipped {
			skipped = false
			b.WriteString("...\n")
		}
		fmt.Fprintf(&b, "%-3d|", sb)
		for j, l := range levels {
			lb := numLevels - j - 1
			p := Priority{l, s}
			pad := "  "
			if c.mu.admissionLevel == p {
				pad = "\u27a1 "
			} else if p != minPriority && c.mu.rejectionLevel == p {
				pad = "\u21e8 "
			}
			var v float64
			switch {
			case !p.less(c.mu.admissionLevel):
				v = float64(atomic.LoadUint64(&c.mu.hist.counters[lb][sb]))
			case c.mu.rejectionLevel.less(p):
				pq := c.wq.pq(p)
				v = float64(pq.len())
			}
			fmt.Fprintf(&b, "%s%-4v|", pad, benchstat.NewScaler(v, "")(v))
			if lb == 0 {
				break
			}
		}
		if sb > 0 {
			b.WriteString("\n")
		} else {
			break
		}
	}
	return b.String()
}

func (c *Controller) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stringLocked()
}

func (c *Controller) AdmissionLevel() Priority {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.admissionLevel
}

// TODO(ajwerner): justify these, they're taken from the DAGOR paper and cut
// in half.
const (
	defaultMaxReqsPerInterval = 2000
	defaultTickInterval       = time.Second
	defaultPruneRate          = .05
	defaultGrowRate           = .01
	defaultMaxBlocked         = 1000
)

func NewController(
	name string,
	overLoadSignal func(prev Priority) bool,
	tickInterval time.Duration,
	maxBlocked int64,
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
	if maxBlocked <= 0 {
		maxBlocked = defaultMaxBlocked
	}
	c := &Controller{
		maxReqsPerInterval: defaultMaxReqsPerInterval,
		tickInterval:       tickInterval,
		pruneRate:          pruneRate,
		growRate:           growRate,
		overloadSignal:     overLoadSignal,
		maxBlocked:         maxBlocked,
	}
	c.metrics = makeMetrics(name)
	c.mu.tickCond.L = c.mu.RLocker()
	c.mu.blockCond.L = c.mu.RLocker()
	c.mu.admissionLevel = minPriority
	c.mu.rejectionLevel = minPriority
	initWaitQueue(&c.wq)
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
	// TODO(ajwerner): remove
}

func (c *Controller) Admit(ctx context.Context, p Priority) error {
	return c.AdmitAt(ctx, p, timeutil.Now())
}

func (c *Controller) raiseRejectionLevelRLocked(p Priority) {
	defer c.mu.blockCond.Broadcast()
	c.mu.RUnlock()
	defer c.mu.RLock()
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.mu.rejectionLevel
	if p.less(r) {
		return
	}
	c.wq.mu.Lock()
	defer c.wq.mu.Unlock()
	blocked := atomic.LoadInt64(&c.mu.numBlocked)
	for blocked >= c.maxBlocked {
		blocked -= int64(c.wq.releasePriorityLocked(r, -1))
		r = r.inc()
		c.mu.rejectionLevel = r
	}
	atomic.StoreInt64(&c.mu.numBlocked, blocked)
}

func (c *Controller) AdmitAt(ctx context.Context, p Priority, now time.Time) error {
	c.mu.RLock()
	c.maybeTickRLocked(now)
	if p.Level != MaxLevel {
		for p.less(c.mu.admissionLevel) {
			if c.mu.rejectionLevel != minPriority && p.less(c.mu.rejectionLevel) {
				// TODO(ajwerner): increment rejected histogram
				c.mu.RUnlock()
				return ErrRejected
			}
			if numBlocked := atomic.AddInt64(&c.mu.numBlocked, 1); numBlocked == c.maxBlocked {
				c.raiseRejectionLevelRLocked(p)
				continue
			} else if numBlocked > c.maxBlocked {
				c.mu.blockCond.Wait()
				continue
			}
			c.mu.RUnlock()
			if err := c.wq.wait(ctx, p); err != nil {
				return err
			}
			c.mu.RLock()
		}
	}
	defer c.mu.RUnlock()
	defer c.mu.hist.record(p, 1)
	numReqs := atomic.AddInt64(&c.mu.numReqs, 1)
	for numReqs > c.maxReqsPerInterval {
		if numReqs == (c.maxReqsPerInterval + 1) {
			c.tickRLocked(now)
		} else {
			c.mu.tickCond.Wait()
		}
		// c was just ticked so we add ourselves again
		numReqs = atomic.AddInt64(&c.mu.numReqs, 1)
	}
	return nil
}

func (c *Controller) tickRLocked(now time.Time) {
	c.mu.RUnlock()
	defer c.mu.RLock()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tickLocked(now)
}

func (c *Controller) tickLocked(now time.Time) {
	defer c.mu.tickCond.Broadcast()
	c.mu.nextTick = now.Add(c.tickInterval)
	numReqs := atomic.SwapInt64(&c.mu.numReqs, 0)
	n, hist := numReqs, &c.mu.hist
	prev := c.mu.admissionLevel
	const minNumReqs = 0
	overloaded := numReqs > minNumReqs && c.overloadSignal(prev)
	if overloaded {
		c.mu.lastDec = 0
		c.metrics.Inc.Inc(1)
		c.mu.admissionLevel = findHigherPriority(c.mu.admissionLevel, n, c.pruneRate, hist)
	} else if c.mu.lastDec < 1 {
		c.mu.lastDec++
	} else {
		c.mu.lastDec = 0
		c.metrics.Dec.Inc(1)
		c.mu.admissionLevel = findLowerPriority(c.mu.admissionLevel, n, c.growRate, hist, &c.wq)
	}
	if log.V(1) {
		log.Infof(context.TODO(), "tick: overload %v (%d) prev %v next %v %v", overloaded, numReqs, prev, c.mu.admissionLevel, c.stringLocked())
	}
	c.mu.hist = histogram{}
	c.metrics.AdmissionLevel.Update(int64(c.mu.admissionLevel.Encode()))
	c.metrics.NumTicks.Inc(1)
}

var maxPriority = Priority{MaxLevel, maxShard}
var minPriority = Priority{MinLevel, minShard}

func findHigherPriority(prev Priority, total int64, pruneRate float64, h *histogram) Priority {
	if total == 0 || h.countAboveIsEmpty(prev) {
		return prev
	}
	reqs := uint64(total)
	target := uint64(float64(total) * (1 - pruneRate))
	lastWithSome := prev
	for cur := prev; cur.Level != MaxLevel; cur = cur.inc() {
		if reqs <= target {
			return cur
		}
		if count := h.countAt(cur); count > 0 {
			reqs -= count
			lastWithSome = cur
		}
	}
	return lastWithSome
}

// TODO(ajwerner): incorporate rejected histogram.
func findLowerPriority(
	prev Priority, total int64, growRate float64, h *histogram, wq *waitQueue,
) Priority {
	reqs := uint64(total)
	cur := prev.dec()
	target := uint64(total)
	var toFree uint64
	if count := h.countForLevelAbove(cur); count != 0 {
		toFree = uint64(float64(count)*growRate) + 1
	} else {
		fmt.Println("count")
		return cur
	}
	target += toFree
	fmt.Println(target, toFree, reqs)
	for ; cur != minPriority; prev, cur = cur, cur.dec() {
		released := uint64(wq.releasePriority(cur, -1))
		reqs += released
		if reqs >= target {
			return cur
		}
	}
	return minPriority
}

type histogram struct {
	counters [numLevels][numShards]uint64
}

func (h *histogram) emptyAtShard(s uint8) bool {
	sb := bucketFromShard(s)
	for lb := 0; lb < numLevels; lb++ {
		if atomic.LoadUint64(&h.counters[lb][sb]) > 0 {
			return false
		}
	}
	return true
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
