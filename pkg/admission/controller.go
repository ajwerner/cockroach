package admission

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/perf/benchstat"
)

// Controller keeps track of the allowed level for requests.
// It stores a current admission level and decides whether requests can be
// admitted.
// In order to make this decision the controller needs to tick every so often.
type Controller struct {
	cfg     Config
	metrics Metrics

	wq waitQueue

	// We need some sort of queues
	// Maybe we just hide those as stack-local state in some processing goroutine.
	mu struct {
		syncutil.RWMutex
		tickCond  sync.Cond
		blockCond sync.Cond

		nextTick time.Time
		ticks    int

		admissionLevel Priority
		rejectionLevel Priority
		rejectionOn    bool

		numReqs         int64
		numBlocked      int64
		numBlockWaiting int64
		numCanceled     int64

		// We can keep a histogram of a trailing average of bytes read for each
		// bucket (or just level? can I just use the average of the summary if I
		// have one for each level?)
		//
		// We can also keep a histogram of ages of blocked entries
		// - actually should this just be kept in the blocking queue?
		// - we can do this by keeping track of the age when a thing is added
		//   vs the next tick time.
		// - then on the tick we add the time of new stuff added (if we tick before
		//   the scheduled tick then we can subtract the delta times the num added.
		// - Then we increment by the time since the last tick times the previous
		//   number stored to have to total age.
		// - When something is removed, we remove its lifetime from the queue
		//   relative to the last tick and add it to the removed time.
		// - We should ideally also track some sort of sizes on a per-level basis
		//   and unblock based on the sizes

		// If we know the average age and we know the number then it's easy to estimate
		// the load increase due to that

		hist histogram
	}
}

func (c *Controller) stringRLocked() string {
	var b strings.Builder
	b.WriteString("---")
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
			} else if c.mu.rejectionOn && c.mu.rejectionLevel == p {
				pad = "\u21e8 "
			}
			var v float64
			switch {
			case !p.Less(c.mu.admissionLevel):
				v = float64(atomic.LoadUint64(&c.mu.hist.counters[lb][sb]))
			case !p.Less(c.mu.rejectionLevel):
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
	fmt.Fprintf(&b, "\nint: %d; ad: %d; bl: %d",
		c.mu.ticks,
		atomic.LoadInt64(&c.mu.numReqs),
		atomic.LoadInt64(&c.mu.numBlocked)-atomic.LoadInt64(&c.mu.numCanceled))
	return b.String()
}

func (c *Controller) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stringRLocked()
}

func (c *Controller) AdmissionLevel() Priority {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.admissionLevel
}

func (c *Controller) RejectionLevel() Priority {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.rejectionLevel
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

type Config struct {

	// Name is used to generate metrics and for logging.
	Name string

	// MaxReqsPerInterval is the maximum number of requests which can occur in
	// an interval. If this number is exceeded a tick will occur.
	MaxReqsPerInterval int

	// TickInterval is the maximum amount of time between ticks.
	// Ticks may also occur if the maximum number of requests per interval
	// is exceeded.
	TickInterval time.Duration

	// RandomizationFactor controls the percent by which TickInterval should be
	// purturbed. The true time interval will be
	//
	//   TickInterval*(1 +- RandomizationFactor)
	//
	RandomizationFactor float64

	// OverloadSignal is used during ticks to determine whether the admission
	// level should rise or fall. To keep the level the same return (true, cur).
	OverloadSignal func(cur Priority) (overloaded bool, max Priority)

	// ScaleFactor is used weigh requests at a given level.
	ScaleFactor func(level uint8) int64

	// MaxBlocked determines the maximum number of requests which may be blocked.
	// If a request would lead to more than this number of requests being blocked
	// the rejection level will increase.
	MaxBlocked int64

	// The PruneRate and GrowRate work by examining the traffic which occurred
	// in the previous interval scaled by the scale factor and then moving the
	// admission level up or down to match the new target.

	// PruneRate determines the rate at which traffic will be pruned when the
	// system is overloaded.
	PruneRate float64

	// PruneRate determines the rate at which traffic will add when the system
	// is not overloaded.
	GrowRate float64
}

func defaultSizeFactor(level uint8) int64 { return 1 }

func (cfg *Config) setDefaults() {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.PruneRate <= 0 {
		cfg.PruneRate = defaultPruneRate
	}
	if cfg.GrowRate <= 0 {
		cfg.GrowRate = defaultGrowRate
	}
	if cfg.MaxBlocked <= 0 {
		cfg.MaxBlocked = defaultMaxBlocked
	}
	if cfg.MaxReqsPerInterval <= 0 {
		cfg.MaxReqsPerInterval = defaultMaxReqsPerInterval
	}
	if cfg.ScaleFactor == nil {
		cfg.ScaleFactor = defaultSizeFactor
	}
}

func NewController(ctx context.Context, stopper *stop.Stopper, cfg Config) *Controller {
	cfg.setDefaults()

	c := &Controller{cfg: cfg}
	c.metrics = makeMetrics(cfg.Name)
	c.mu.tickCond.L = c.mu.RLocker()
	c.mu.blockCond.L = c.mu.RLocker()
	c.mu.admissionLevel = minPriority
	c.mu.rejectionLevel = minPriority
	initWaitQueue(&c.wq)
	if stopper != nil {
		if err := stopper.RunAsyncTask(ctx, cfg.Name+"admission.controller.ticker", func(ctx context.Context) {
			t := timeutil.NewTimer()
			for {
				t.Reset(c.cfg.TickInterval)
				select {
				case <-stopper.ShouldQuiesce():
					return
				case <-ctx.Done():
					return
				case <-t.C:
					t.Read = true
					_ = c.Admit(ctx, maxPriority)
				}
			}
		}); err != nil {
			panic(err)
		}
	}
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
	blocked := atomic.LoadInt64(&c.mu.numBlocked)
	blockWaiting := atomic.LoadInt64(&c.mu.numBlockWaiting)
	canceled := atomic.SwapInt64(&c.mu.numCanceled, 0)
	blocked -= blockWaiting
	blocked -= canceled
	c.wq.mu.Lock()
	for blocked > c.cfg.MaxBlocked {
		if !c.mu.rejectionOn {
			c.mu.rejectionOn = true
		} else {
			r = r.inc()
		}
		blocked -= int64(c.wq.releasePriorityLocked(r))
		c.mu.rejectionLevel = r
	}
	c.wq.mu.Unlock()
	blocked-- // count this one
	atomic.StoreInt64(&c.mu.numBlocked, blocked)
	atomic.StoreInt64(&c.mu.numBlockWaiting, 0)
}

func (c *Controller) AdmitAt(ctx context.Context, p Priority, now time.Time) error {
	c.mu.RLock()
	if log.V(3) {
		log.Infof(ctx, "attempting to admit %d@%s: %v %v", p, now.Format("00.0"), c.mu.admissionLevel, c.mu.nextTick.Sub(now))
	}
	c.maybeTickRLocked(now)
	if p.Level != MaxLevel {
		for p.Less(c.mu.admissionLevel) {
			// Reject requests which are at or below the current rejection level.
			if c.mu.rejectionLevel != minPriority && !c.mu.rejectionLevel.Less(p) {
				// TODO(ajwerner): increment rejected histogram
				c.mu.RUnlock()
				return ErrRejected
			}
			if numBlocked := atomic.AddInt64(&c.mu.numBlocked, 1); numBlocked == c.cfg.MaxBlocked+1 {
				c.raiseRejectionLevelRLocked(p)
				continue
			} else if numBlocked > c.cfg.MaxBlocked {
				atomic.AddInt64(&c.mu.numBlockWaiting, 1)
				c.mu.blockCond.Wait()
				continue
			}
			c.mu.RUnlock()
			c.metrics.NumBlocked.Inc(1)
			waitErr := c.wq.wait(ctx, p)
			c.metrics.NumUnblocked.Inc(1)
			if waitErr != nil {
				atomic.AddInt64(&c.mu.numCanceled, 1)
				return waitErr
			}
			c.mu.RLock()
		}
	}
	defer c.mu.RUnlock()
	defer c.mu.hist.record(p, 1)
	numReqs := atomic.AddInt64(&c.mu.numReqs, 1)
	for numReqs > int64(c.cfg.MaxReqsPerInterval) {
		if numReqs == (int64(c.cfg.MaxReqsPerInterval) + 1) {
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

func randomizeInterval(d time.Duration, factor float64) time.Duration {
	r := (2*rand.Float64() - 1) * factor
	dd := time.Duration(float64(d) * (1 + r))
	return dd
}

func (c *Controller) tickLocked(now time.Time) {
	defer c.mu.tickCond.Broadcast()
	c.mu.ticks++
	interval := randomizeInterval(c.cfg.TickInterval, c.cfg.RandomizationFactor)
	c.mu.nextTick = now.Add(interval)
	numReqs := atomic.SwapInt64(&c.mu.numReqs, 0)
	prev := c.mu.admissionLevel
	overloaded, max := c.cfg.OverloadSignal(prev)
	if overloaded {
		c.metrics.Inc.Inc(1)
		c.raiseAdmissionLevelLocked(max)
	} else if c.mu.admissionLevel != minPriority {
		c.metrics.Dec.Inc(1)
		freed := c.lowerAdmissionLevelLocked()
		numCanceled := atomic.SwapInt64(&c.mu.numCanceled, 0)
		unblocked := int64(freed) + numCanceled
		numBlocked := atomic.AddInt64(&c.mu.numBlocked, -1*unblocked)
		if numBlocked < c.cfg.MaxBlocked/2 {
			c.mu.rejectionLevel = minPriority
			c.mu.rejectionOn = false
		}
		c.metrics.CurNumBlocked.Update(numBlocked)
	}
	if log.V(1) {
		log.Infof(context.TODO(), "tick %v: overload %v (%d) prev %v next %v %v",
			now.Format("00.0"), overloaded, numReqs, prev, c.mu.admissionLevel, c.stringRLocked())
	}
	c.mu.hist = histogram{}
	c.metrics.AdmissionLevel.Update(int64(c.mu.admissionLevel.Encode()))
	if c.mu.rejectionOn {
		c.metrics.RejectionLevel.Update(int64(c.mu.rejectionLevel.Encode()))
	} else {
		c.metrics.RejectionLevel.Update(0)
	}
	c.metrics.NumTicks.Inc(1)
}

var maxPriority = Priority{MaxLevel, maxShard}
var minPriority = Priority{MinLevel, minShard}

func (c *Controller) raiseAdmissionLevelLocked(max Priority) {
	if c.mu.admissionLevel == max {
		return
	}
	cur := c.mu.admissionLevel
	sizeFactor, total := admittedAbove(cur, &c.mu.hist, c.cfg.ScaleFactor)
	target := int64(float64(total) * (1 - c.cfg.PruneRate))
	prev, cur := cur, cur.inc()
	for ; cur.Less(max) && cur.Level != MaxLevel; prev, cur = cur, cur.inc() {
		if cur.Level != prev.Level {
			sizeFactor = c.cfg.ScaleFactor(cur.Level)
		}
		total -= int64(c.mu.hist.countAt(cur)) * sizeFactor
		if total <= target {
			break
		}
	}
	c.mu.admissionLevel = cur
}

// findLowerPriorityLocked computes the new priority for the Controller
// given the activity of the last interval. It does not modify c other
// than by calling into the waitQueue.
func (c *Controller) lowerAdmissionLevelLocked() (freed int) {
	prev := c.mu.admissionLevel
	cur := prev.dec()
	sizeFactor, total := admittedAbove(cur, &c.mu.hist, c.cfg.ScaleFactor)
	target := int64(float64(total)*(1+c.cfg.GrowRate)) + 1
	for ; cur != minPriority; prev, cur = cur, cur.dec() {
		if cur.Level != prev.Level {
			sizeFactor = c.cfg.ScaleFactor(cur.Level)
		}
		released := c.wq.releasePriority(cur)
		freed += released
		if released == 0 {
			released++
		}
		if total += int64(released) * sizeFactor; total >= target {
			break
		}
	}
	c.mu.admissionLevel = cur
	return freed
}

func admittedAbove(
	l Priority, h *histogram, sizeFactor func(level uint8) int64,
) (factor, count int64) {
	prev, cur, factor := maxPriority, maxPriority, sizeFactor(MaxLevel)
	for ; l.Less(cur); cur, prev = cur.dec(), cur {
		if cur.Level != prev.Level {
			factor = sizeFactor(cur.Level)
		}
		lb, sb := cur.buckets()
		count += factor * int64(atomic.LoadUint64(&h.counters[lb][sb]))
	}
	return factor, count
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
