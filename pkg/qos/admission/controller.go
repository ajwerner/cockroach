// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package admission

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/qos"
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

	// runTickerOnce ensures that at most one ticker goroutine is ever run.
	// See RunTicker().
	runTickerOnce sync.Once

	mu struct {
		syncutil.RWMutex
		tickCond  sync.Cond
		blockCond sync.Cond

		nextTick time.Time
		ticks    int

		admissionLevel qos.Level
		rejectionLevel qos.Level
		rejectionOn    bool

		numAdmitted     int64
		numBlocked      int64
		numBlockWaiting int64
		numCanceled     int64
		numRejected     int64

		admitHists admitHists

		wq         waitQueue
		rejectHist histogram
	}
}

const numAdmitHists = 4

// TODO(ajwerner): justify these, they're taken from the DAGOR paper and cut
// in half.
const (
	defaultMaxReqsPerInterval = 2000
	defaultTickInterval       = time.Second
	defaultPruneRate          = .05
	defaultGrowRate           = .01
	defaultMaxBlocked         = 1000
)

// Config configures a controller.
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
	//   TickInterval*(1 +/- RandomizationFactor)
	//
	RandomizationFactor float64

	// OverloadSignal is used during ticks to determine whether the admission
	// level should rise or fall. To keep the level the same return (true, cur).
	OverloadSignal func(cur qos.Level) (overloaded bool, max qos.Level)

	// ScaleFactor is used weigh requests at a given level.
	// This is useful when requests of different classes may not be of equal cost.
	ScaleFactor func(qos.Class) int64

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

func defaultSizeFactor(_ qos.Class) int64 { return 1 }

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

// NewController constructs a new Controller with the specified Config.
// If the stopper is non-nil it will be used to Admit a request at the maximum
// qos.Level every cfg.TickInterval in order to ensure that the Controller's
// state continue to update in the absence of traffic.
//
// TODO(ajwerner): remove this hacky goroutine.
func NewController(cfg Config) *Controller {
	cfg.setDefaults()

	c := &Controller{cfg: cfg}
	c.metrics = makeMetrics(cfg.Name)
	c.mu.tickCond.L = c.mu.RLocker()
	c.mu.blockCond.L = c.mu.RLocker()
	c.mu.admissionLevel = minLevel
	c.mu.rejectionLevel = minLevel
	initWaitQueue(&c.mu.wq)
	return c
}

func (c *Controller) RunTicker(ctx context.Context, stopper *stop.Stopper) {
	c.runTickerOnce.Do(func() {
		tickerName := c.cfg.Name + "admission.Controller.ticker"
		f := func(ctx context.Context) {
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
					_ = c.Admit(ctx, maxLevel)
				}
			}
		}
		if err := stopper.RunAsyncTask(ctx, tickerName, f); err != nil {
			// This could happen if we're already shutting down.
			// Either way, nothing we can do about it and it's not worth tearing down
			// the process.

			log.Warningf(ctx, "failed to run admission.Controller(%s).ticker: %v",
				c.cfg.Name, err)
		}
	})
}

func (c *Controller) runTicker(ctx context.Context, stopper *stop.Stopper) {

}

// Metrics returns a metrics struct for this Controller.
func (c *Controller) Metrics() *Metrics {
	return &c.metrics
}

// AdmissionLevel returns the current admission level.
func (c *Controller) AdmissionLevel() qos.Level {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.admissionLevel
}

// RejectionLevel returns the current rejection level.
func (c *Controller) RejectionLevel() qos.Level {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.rejectionLevel
}

// ErrRejected is returned from AdmitAt if the rejection level reaches or
// surpasses the level of the request.
var ErrRejected = errors.New("rejected")

// String returns a detailed string representation of the Controller.
//
// The representation shows for each Class, for each non-empty Shard the number
// of requests incident on that Level. The AdmissionLevel is represented as ➡
// and the rejection level (if it exists) is represented as ⇨. The numbers at
// or above the rejection level correspond to requests which have been admitted
// in the current interval. The requests between the admission and rejection
// levels represent the number of requests currently blocked at that level and
// the numbers at or below the rejection level represent the requests which have
// been rejected in this interval.
//
// Below is an example of the output:
//
//   	---|  h   |  d   |  l   |
//   	127|  0.00|  1.00|  0.00|
//   	...
//   	10 |  0.00|  0.00|  100 |
//   	9  |  0.00|  0.00|  100 |
//   	8  |  0.00|  0.00|  100 |
//   	7  |  0.00|  0.00|  100 |
//   	6  |  0.00|  0.00|  100 |
//   	5  |  0.00|  0.00|  100 |
//   	4  |  0.00|  0.00|  100 |
//   	3  |  0.00|  0.00|  100 |
//   	2  |  0.00|  0.00|  100 |
//   	1  |  0.00|➡ 100 |⇨ 100 |
//   	0  |  0.00|  99.0|  0.00|
//   	int: 3; ad: 101; bl: 999; rej: 100
//
func (c *Controller) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stringRLocked()
}

func (c *Controller) stringRLocked() string {
	var b strings.Builder
	b.WriteString("---")
	for _, cl := range qos.Classes() {
		b.WriteString("|  ")
		b.WriteString(cl.String())
		b.WriteString("   ")
	}
	b.WriteString("|\n")
	var skipped bool
	for _, s := range qos.Shards() {
		if s < qos.NumShards-1 &&
			s > 0 &&
			c.mu.admissionLevel.Shard != s &&
			c.mu.rejectionLevel.Shard != s &&
			c.mu.admitHists.hists[c.mu.admitHists.cur].emptyAtShard(s) &&
			c.mu.wq.emptyAtShard(s) {
			skipped = true
			continue
		} else if skipped {
			skipped = false
			b.WriteString("...\n")
		}
		_, _ = fmt.Fprintf(&b, "%-3d|", s)
		for _, cl := range qos.Classes() {
			p := qos.Level{cl, s}
			pad := "  "
			if c.mu.admissionLevel == p {
				pad = "\u27a1 "
			} else if c.mu.rejectionOn && c.mu.rejectionLevel == p {
				pad = "\u21e8 "
			}
			var v float64
			switch {
			case !p.Less(c.mu.admissionLevel):
				v = float64(atomic.LoadUint64(&c.mu.admitHists.hists[c.mu.admitHists.cur].counters[cl][s]))
			case !c.mu.rejectionOn || c.mu.rejectionLevel.Less(p):
				lq := c.mu.wq.lq(p)
				v = float64(lq.len())
			default:
				v = float64(atomic.LoadUint64(&c.mu.rejectHist.counters[cl][s]))
			}
			_, _ = fmt.Fprintf(&b, "%s%-4v|", pad, benchstat.NewScaler(v, "")(v))
		}
		b.WriteString("\n")
	}
	_, _ = fmt.Fprintf(&b, "int: %d; ad: %d; bl: %d; rej: %d",
		c.mu.ticks,
		atomic.LoadInt64(&c.mu.numAdmitted),
		atomic.LoadInt64(&c.mu.numBlocked)-atomic.LoadInt64(&c.mu.numCanceled),
		atomic.LoadInt64(&c.mu.numRejected))
	return b.String()
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

// Admit is a shorthand for c.AdmitAt(ctx, p, timeutil.Now()).
func (c *Controller) Admit(ctx context.Context, p qos.Level) error {
	return c.AdmitAt(ctx, p, timeutil.Now())
}

// Block will clock this request.
func (c *Controller) Block(ctx context.Context, l qos.Level) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.blockRLocked(ctx, l); err != nil {
		return err
	}
	for l.Less(c.mu.admissionLevel) {
		// Reject requests which are at or below the current rejection level.
		if c.mu.rejectionOn && !c.mu.rejectionLevel.Less(l) {
			atomic.AddInt64(&c.mu.numRejected, 1)
			c.mu.rejectHist.record(l, 1)
			return ErrRejected
		}
		if err := c.blockRLocked(ctx, l); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) blockRLocked(ctx context.Context, l qos.Level) error {
	if numBlocked := atomic.AddInt64(&c.mu.numBlocked, 1); numBlocked == c.cfg.MaxBlocked+1 {
		c.raiseRejectionLevelRLocked(l)
		return nil
	} else if numBlocked > c.cfg.MaxBlocked {
		atomic.AddInt64(&c.mu.numBlockWaiting, 1)
		c.mu.blockCond.Wait()
		return nil
	}
	c.metrics.NumBlocked.Inc(1)
	waitErr := c.waitRLocked(ctx, l)
	if waitErr == nil {
		c.metrics.NumUnblocked.Inc(1)
		return nil
	}
	atomic.AddInt64(&c.mu.numCanceled, 1)
	return waitErr
}

// AdmitAt attempts to admit a request at the specified level and time.
// Admit at may return ErrRejected if the rejection level at any point rises to
// or above l. Context cancellations will also propagate an error.
func (c *Controller) AdmitAt(ctx context.Context, l qos.Level, now time.Time) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if log.V(3) {
		log.Infof(ctx, "attempting to admit %d@%d: %v %v", l, now.Unix(), c.mu.admissionLevel, c.mu.nextTick.Sub(now))
	}
	c.maybeTickRLocked(now)
	if l.Class != qos.ClassHigh {
		for l.Less(c.mu.admissionLevel) {
			// Reject requests which are at or below the current rejection level.
			if c.mu.rejectionLevel != minLevel && !c.mu.rejectionLevel.Less(l) {
				atomic.AddInt64(&c.mu.numRejected, 1)
				c.mu.rejectHist.record(l, 1)
				return ErrRejected
			}
			if err := c.blockRLocked(ctx, l); err != nil {
				return err
			}
		}
	}
	defer c.mu.admitHists.record(l, 1)
	numAdmitted := atomic.AddInt64(&c.mu.numAdmitted, 1)
	for numAdmitted > int64(c.cfg.MaxReqsPerInterval) {
		if numAdmitted == (int64(c.cfg.MaxReqsPerInterval) + 1) {
			c.tickRLocked(now)
		} else {
			c.mu.tickCond.Wait()
		}
		// c was just ticked so we add ourselves again
		numAdmitted = atomic.AddInt64(&c.mu.numAdmitted, 1)
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
	before := c.stringRLocked()
	c.mu.nextTick = now.Add(interval)
	numAdmitted := atomic.SwapInt64(&c.mu.numAdmitted, 0)
	atomic.SwapInt64(&c.mu.numRejected, 0)
	prev := c.mu.admissionLevel
	overloaded, max := c.cfg.OverloadSignal(prev)
	if overloaded {
		c.metrics.Inc.Inc(1)
		c.raiseAdmissionLevelLocked(max)
	} else {
		freed := c.lowerAdmissionLevelLocked()
		numCanceled := atomic.SwapInt64(&c.mu.numCanceled, 0)
		unblocked := int64(freed) + numCanceled
		numBlocked := atomic.AddInt64(&c.mu.numBlocked, -1*unblocked)
		c.metrics.CurNumBlocked.Update(numBlocked)
		if numBlocked < c.cfg.MaxBlocked/2 {
			c.mu.rejectionLevel = minLevel
			c.mu.rejectionOn = false
		}
		if freed > 0 || prev != minLevel {
			c.metrics.Dec.Inc(1)
		}
	}
	if log.V(1) {
		log.Infof(context.TODO(), "%v tick %v: overload %v (%d) prev %v next %v\n%v",
			c.cfg.Name, now.Unix(), overloaded, numAdmitted, prev, c.mu.admissionLevel, before)
	}
	c.mu.admitHists.rotate()
	c.mu.rejectHist = histogram{}
	c.metrics.AdmissionLevel.Update(levelMetric(c.mu.admissionLevel))
	if c.mu.rejectionOn {
		c.metrics.RejectionLevel.Update(levelMetric(c.mu.rejectionLevel))
	} else {
		c.metrics.RejectionLevel.Update(0)
	}
	c.metrics.NumTicks.Inc(1)
}

var maxLevel = qos.Level{qos.ClassHigh, qos.NumShards - 1}
var minLevel = qos.Level{qos.ClassLow, 0}

func (c *Controller) raiseAdmissionLevelLocked(max qos.Level) {
	if c.mu.admissionLevel == max {
		return
	}
	cur := c.mu.admissionLevel
	sizeFactor, total := admittedAbove(cur, &c.mu.admitHists, c.cfg.ScaleFactor)
	target := int64(float64(total) * (1 - c.cfg.PruneRate))
	prev, cur := cur, cur.Inc()
	for ; cur.Less(max) && cur.Class != qos.ClassHigh; prev, cur = cur, cur.Inc() {
		if log.V(2) {
			log.Infof(context.Background(), "raise %v %v %v", cur, total, target)
		}
		if total <= target {
			break
		}
		if cur.Class != prev.Class {
			sizeFactor = c.cfg.ScaleFactor(cur.Class)
		}
		total -= int64((c.mu.admitHists.countAt(cur)/numAdmitHists)+1) * sizeFactor
	}
	c.mu.admissionLevel = cur
}

type admitHists struct {
	cur   int
	hists [numAdmitHists]histogram
}

func (h *admitHists) record(l qos.Level, c uint64) {
	h.hists[h.cur].record(l, c)
}

func (h *admitHists) rotate() {
	h.cur++
	if h.cur == numAdmitHists {
		h.cur = 0
	}
	h.hists[h.cur] = histogram{}
}

func (h *admitHists) countAt(l qos.Level) (c uint64) {
	for i := 0; i < numAdmitHists; i++ {
		c += atomic.LoadUint64(&h.hists[i].counters[l.Class][l.Shard])
	}
	return c
}

// findLowerLevelLocked computes the new level for the Controller
// given the activity of the last interval. It does not modify c other
// than by calling into the waitQueue.
//
// TODO(ajwerner): meter releasing requests.
func (c *Controller) lowerAdmissionLevelLocked() (freed int) {
	prev := c.mu.admissionLevel
	cur := prev
	sizeFactor, total := admittedAbove(cur, &c.mu.admitHists, c.cfg.ScaleFactor)
	target := int64(float64(total)*(1+c.cfg.GrowRate)) + 1
	for l := maxLevel; l != prev && prev.Less(l); l = l.Dec() {
		freed += c.releaseLevelLocked(l)
		if l == minLevel {
			break
		}
	}
	for cur = cur.Dec(); cur != minLevel; prev, cur = cur, cur.Dec() {
		if cur.Class != prev.Class {
			sizeFactor = c.cfg.ScaleFactor(cur.Class)
		}
		released := c.releaseLevelLocked(cur)
		freed += released
		if released == 0 {
			released++
		}
		if total += int64(released) * sizeFactor; total >= target {
			break
		}
	}
	if cur.Less(c.mu.admissionLevel) {
		c.mu.admissionLevel = cur
	}
	return freed
}

func (c *Controller) raiseRejectionLevelRLocked(l qos.Level) {
	defer c.mu.blockCond.Broadcast()
	c.mu.RUnlock()
	defer c.mu.RLock()
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.mu.rejectionLevel
	numBlocked := atomic.LoadInt64(&c.mu.numBlocked)
	blockWaiting := atomic.LoadInt64(&c.mu.numBlockWaiting)
	canceled := atomic.SwapInt64(&c.mu.numCanceled, 0)
	blocked := numBlocked - blockWaiting - canceled
	for blocked > c.cfg.MaxBlocked && c.mu.rejectionLevel.Less(maxLevel) {
		if log.V(3) {
			log.Infof(context.TODO(),
				"raising rejection level. blocked=%v, blockedWaiting=%v, canceled=%v, MaxBlocked=%v, admissionlevel=%v, rejectionLevel=%v, rejectionOn=%v",
				blocked, blockWaiting, canceled, c.cfg.MaxBlocked, c.mu.admissionLevel, r, c.mu.rejectionOn)

		}
		if !c.mu.rejectionOn {
			c.mu.rejectionOn = true
		} else {
			r = r.Inc()
		}
		blocked -= int64(c.releaseLevelLocked(r))
		c.mu.rejectionLevel = r
		if !c.mu.rejectionLevel.Less(c.mu.admissionLevel) {
			c.mu.admissionLevel = r.Inc()
		}
	}
	blocked-- // count this one
	atomic.StoreInt64(&c.mu.numBlocked, blocked)
	atomic.StoreInt64(&c.mu.numBlockWaiting, 0)
}

func (c *Controller) waitRLocked(ctx context.Context, l qos.Level) (err error) {
	lq := c.mu.wq.lq(l)
	atomic.AddInt64(&lq.numWaiting, 1)
	seq := lq.seq
	ch := lq.freed
	c.mu.RUnlock()
	defer func() {
		c.mu.RLock()
		if err != nil && lq.seq == seq {
			atomic.AddInt64(&lq.numCanceled, 1)
		}
	}()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Controller) releaseLevelLocked(l qos.Level) (freed int) {
	lq := c.mu.wq.lq(l)
	if log.V(1) {
		log.Infof(context.TODO(), "releasing %v", l, lq.len())
	}
	if lLen := lq.len(); lLen > 0 {
		freed = int(lLen)
		close(lq.freed)
		lq.freed = make(chan struct{})
		atomic.StoreInt64(&lq.numWaiting, 0)
		atomic.StoreInt64(&lq.numCanceled, 0)
		lq.seq++
	}
	return freed
}

func admittedAbove(
	l qos.Level, h *admitHists, sizeFactor func(qos.Class) int64,
) (factor, count int64) {
	prev, cur, factor := maxLevel, maxLevel, sizeFactor(qos.ClassHigh)
	for ; l.Less(cur); cur, prev = cur.Dec(), cur {
		if cur.Class != prev.Class {
			factor = sizeFactor(cur.Class)
		}
		count += factor * int64((h.countAt(cur)/numAdmitHists)+1)
		//		count += factor * int64(atomic.LoadUint64(&h.counters[cur.Class][cur.Shard]))
	}
	return factor, count
}

type waitQueue [qos.NumClasses][qos.NumShards]waitLevel

type waitLevel struct {
	numWaiting  int64
	numCanceled int64
	freed       chan struct{}
	seq         int
}

func initWaitQueue(q *waitQueue) {
	for c := range q {
		for s := range q[c] {
			q[c][s] = waitLevel{
				freed: make(chan struct{}),
			}
		}
	}
}

// len is safe to call concurrently but generally is not.
func (wl *waitLevel) len() int64 {
	w, c := atomic.LoadInt64(&wl.numWaiting), atomic.LoadInt64(&wl.numCanceled)
	return w - c
}

func (q *waitQueue) lq(l qos.Level) *waitLevel {
	return &q[l.Class][l.Shard]
}

func (q *waitQueue) emptyAtShard(s qos.Shard) bool {
	for c := qos.Class(0); c < qos.NumClasses; c++ {
		if q.lq(qos.Level{c, s}).len() > 0 {
			return false
		}
	}
	return true
}

type histogram struct {
	counters [qos.NumClasses][qos.NumShards]uint64
}

func (h *histogram) emptyAtShard(s qos.Shard) bool {
	for cl := 0; cl < qos.NumClasses; cl++ {
		if atomic.LoadUint64(&h.counters[cl][s]) > 0 {
			return false
		}
	}
	return true
}

func (h *histogram) countAt(p qos.Level) uint64 {
	return atomic.LoadUint64(&h.counters[p.Class][p.Shard])
}

func (h *histogram) record(p qos.Level, c uint64) {
	atomic.AddUint64(&h.counters[p.Class][p.Shard], c)
}
