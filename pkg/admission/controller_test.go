package admission

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

var testConfig = Config{
	Name:         "test",
	TickInterval: time.Second,
	MaxBlocked:   1000,
	GrowRate:     .05,
	PruneRate:    .01,
}

var (
	t0 = time.Unix(0, 0)
	t1 = time.Unix(1, 0)
	t2 = time.Unix(2, 0)
	t3 = time.Unix(3, 0)
	t4 = time.Unix(4, 0)
)

func TestAdmissionController(t *testing.T) {
	var overloaded atomic.Value
	overloaded.Store(false)
	ctx := context.Background()
	cfg := testConfig
	cfg.OverloadSignal = func(Priority) (bool, Priority) {
		return overloaded.Load().(bool), maxPriority
	}
	c := NewController(ctx, nil, cfg)
	assert.Equal(t, minPriority, c.AdmissionLevel())
	p := Priority{DefaultLevel, 0}
	assert.Nil(t, c.AdmitAt(ctx, p, t1))
	assert.Nil(t, c.AdmitAt(ctx, Priority{DefaultLevel, NumShards - 1}, t1))
	log.Infof(ctx, "aasdf")
	overloaded.Store(true)
	assertBlocks := func(ctx context.Context, p Priority, ts time.Time) {
		const waitForError = 5 * time.Millisecond
		errCh := make(chan error)
		ctx, cancel := context.WithCancel(ctx)
		go func() { errCh <- c.AdmitAt(ctx, p, ts) }()
		time.AfterFunc(waitForError, cancel)
		assert.Equal(t, context.Canceled, <-errCh)
	}
	pBlocks := Priority{MinLevel, 127}
	assertBlocks(ctx, pBlocks, t2)
	p.Level = MaxLevel
	assert.Nil(t, c.AdmitAt(ctx, p, t2))

	assert.Equal(t, Priority{DefaultLevel, 0}, c.AdmissionLevel())
	overloaded.Store(false)
	// Ensure that the maximum admission level never gets blocked.
	assert.Nil(t, c.AdmitAt(ctx, maxPriority, t2))
	t3 := time.Unix(3, 0)
	assert.Nil(t, c.AdmitAt(ctx, maxPriority.Dec(), t3))
}

func ExampleController_cancel_blocked() {
	var overloaded atomic.Value
	overloaded.Store(false)
	cfg := testConfig
	cfg.OverloadSignal = func(Priority) (bool, Priority) {
		return overloaded.Load().(bool), maxPriority
	}
	ctx := context.Background()
	c := NewController(ctx, nil, cfg)
	fmt.Println("The controller begins at the lowest level.")
	fmt.Println(c)
	t0 := time.Unix(0, 0)
	p0 := Priority{DefaultLevel, 0}
	fmt.Println()
	fmt.Println("Two admission requests come in and succeed in the current interval.")
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", p0, t0.Format("0.0"), c.AdmitAt(ctx, p0, t0))
	p127 := Priority{DefaultLevel, 127}
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", p127, t0.Format("0.0"), c.AdmitAt(ctx, p127, t0))
	fmt.Println(c)
	fmt.Println()
	fmt.Println("The overload signal becomes true and time advances to 0.1.")
	overloaded.Store(true)
	t1 := time.Unix(1, 0)
	fmt.Printf("AdmitAt(%v, %s) will block.\n", p0, t1.Format("0.0"))
	curLevel := c.AdmissionLevel()
	ctxToCancel, cancel := context.WithCancel(ctx)
	errCh := make(chan error)
	go func() { errCh <- c.AdmitAt(ctxToCancel, p0, t1) }()
	waitForNotAdmissionLevel(c, curLevel)
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Cancel the request at", p0, "so it is no longer blocked.")
	cancel()
	<-errCh
	fmt.Println("Observe that the request is no longer blocked.")
	fmt.Println(c)
	// Output:
	// foo
}

func ExampleController_waiting() {
	var overloaded atomic.Value
	overloaded.Store(false)
	cfg := testConfig
	cfg.MaxReqsPerInterval = 1000000
	cfg.OverloadSignal = func(Priority) (bool, Priority) {
		return overloaded.Load().(bool), maxPriority
	}
	ctx := context.Background()
	c := NewController(ctx, nil, cfg)
	pMin := Priority{DefaultLevel, 1}
	pMax := Priority{DefaultLevel, 10}
	const reqsToAdd = 1000
	const reqsPerPriority = reqsToAdd / 10
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", pMin, t0.Format("0.0"), c.AdmitAt(ctx, pMin, t0))
	p127 := Priority{DefaultLevel, 127}
	fmt.Println("Add", reqsToAdd, "requests uniformly between", pMin, "and", pMax, "to test adjustment.")
	for p := pMin; !pMax.Less(p); p = p.Inc() {
		for i := 0; i < reqsPerPriority; i++ {
			if err := c.AdmitAt(ctx, p, t1); err != nil {
				fmt.Println("Got an error", err)
			}
		}
	}
	fmt.Println("After adding all of the requests the controller state looks like this:")
	fmt.Println(c)
	overloaded.Store(true)
	fmt.Println("Now add another request above the admission level in the next interval leading to a tick.")
	if err := c.AdmitAt(ctx, p127, t2); err != nil {
		fmt.Println("Got an error", err)
	}
	fmt.Println("Notice that the admission level has risen to def:3.")
	fmt.Println(c)
	fmt.Println("Now, once again add the same load as before and wait until all requests are either admitted or blocked.")
	fmt.Println("9951 should be admitted and 50 should be blocked.")
	overloaded.Store(false)
	for p := pMin; !pMax.Less(p); p = p.Inc() {
		for i := 0; i < reqsPerPriority; i++ {
			if !p.Less(c.AdmissionLevel()) {
				c.AdmitAt(ctx, p, t2)
			} else {
				go c.AdmitAt(ctx, p, t2)
			}
		}
	}
	waitForState(c, 801, 200)
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Now a new request comes in at 0.4 leading to a tick moves the admission level to def:2.")
	c.AdmitAt(ctx, p127, t3)
	waitForState(c, 101, 100)
	fmt.Println(c)
	fmt.Println("Now another request at 0.5 ticks the admission level to def:1.")
	c.AdmitAt(ctx, p127, t4)
	waitForState(c, 101, 0)
	fmt.Println("Simultaneously more requests than can fit in the waitQueue arrive at 0.5.")
	fmt.Println("The requests are added in shard order waiting until we know all requests")
	fmt.Println("at the previous are blocked by the time we add the next level")
	fmt.Println("in order to ensure test determinism")
	fmt.Println(c)
	expectedNumBlocked := int(c.numBlocked())
	errCh := make(chan error)
	addAtPriority := func(p Priority) {
		for i := 0; i < reqsPerPriority; i++ {
			go func() { errCh <- c.AdmitAt(ctx, p, t4) }()
			expectedNumBlocked++
		}
		for c.numBlocked() < expectedNumBlocked {
			time.Sleep(time.Millisecond)
		}
		if expectedNumBlocked >= int(cfg.MaxBlocked) {
			expectedNumBlocked -= reqsPerPriority
		}
	}
	addAtPriority(Priority{DefaultLevel, 0})
	pMin.Level, pMax.Level = MinLevel, MinLevel
	for p := pMin; !pMax.Less(p); p = p.Inc() {
		addAtPriority(p)
	}
	for i := 0; i < 100; i++ {
		if err := <-errCh; err != ErrRejected {
			fmt.Println("Got an unexpected", err)
		}
	}
	time.Sleep(time.Second)
	fmt.Println(c)
	// Output:
	// The controller begins at the lowest level.
	// S  |  max |  def |  min |
	// -------------------------
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 0  |  0.00|  0.00|➡ 0.00|
	//
	// AdmitAt(def:0, 0.1) = <nil>.
	// AdmitAt(def:30, 0.1) = <nil>.
	// The controller records the two successful admissions.
	// S  |  max |  def |  min |
	// -------------------------
	// 127|  0.00|  0.00|  0.00|
	// 126|  0.00|  1.00|  0.00|
	// ...
	// 0  |  0.00|  1.00|➡ 0.00|
	//
	// Set overloaded true and attempt to admit at (def,0)@0.6 in the next interval.
	// AdmitAt(def:0, 0.6) will block.
	// S  |  max |  def |  min |
	// -------------------------
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 1  |  0.00|➡ 0.00|  0.00|
	// 0  |  0.00|  1.00|⇨ 0.00|
	//
	// Cancel the request at def:0 so it is no longer blocked.
	// S  |  max |  def |  min |
	// -------------------------
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 1  |  0.00|➡ 0.00|  0.00|
	// 0  |  0.00|  0.00|⇨ 0.00|
}

func waitForNotAdmissionLevel(c *Controller, l Priority) {
	for c.AdmissionLevel() == l {
		time.Sleep(time.Millisecond)
	}
}

func waitForState(c *Controller, numReqs, numBlocked int) {
	for (numReqs > 0 && c.numReqs() < numReqs) ||
		(numBlocked > 0 && c.numBlocked() < numBlocked) {
		time.Sleep(time.Millisecond)
	}
}

func (c *Controller) numReqs() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int(atomic.LoadInt64(&c.mu.numReqs))
}

func (c *Controller) numBlocked() (blocked int) {
	c.wq.mu.Lock()
	defer c.wq.mu.Unlock()
	for lb := 0; lb < NumLevels; lb++ {
		for sb := 0; sb < NumShards; sb++ {
			blocked += c.wq.q[lb][sb].len()
		}
	}
	return blocked
}
