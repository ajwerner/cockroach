package admission

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdmissionController(t *testing.T) {
	var overloaded atomic.Value
	overloaded.Store(false)
	c := NewController("test",
		func(Priority) bool { return overloaded.Load().(bool) },
		100*time.Millisecond, 10, .05, .01)
	assert.Equal(t, minPriority, c.AdmissionLevel())
	t100 := time.Unix(0, 100e6)
	ctx := context.Background()
	p := Priority{DefaultLevel, 0}
	assert.Nil(t, c.AdmitAt(ctx, p, t100))
	assert.Nil(t, c.AdmitAt(ctx, Priority{DefaultLevel, maxShard}, t100))
	overloaded.Store(true)
	t200 := time.Unix(0, 200e6)

	assertBlocks := func(ctx context.Context, p Priority, ts time.Time) {
		const waitForError = 5 * time.Millisecond
		errCh := make(chan error)
		ctx, cancel := context.WithCancel(ctx)
		go func() { errCh <- c.AdmitAt(ctx, p, ts) }()
		time.AfterFunc(waitForError, cancel)
		assert.Equal(t, context.Canceled, <-errCh)
	}
	assertBlocks(ctx, p, t200)
	p.Level = MaxLevel
	assert.Nil(t, c.AdmitAt(ctx, p, t200))

	// Ensure that the maximum admission level never gets blocked.
	assert.Equal(t, Priority{DefaultLevel, maxShard}, c.AdmissionLevel())
	overloaded.Store(false)
	assert.Nil(t, c.AdmitAt(ctx, maxPriority, t200))
	t1101 := time.Unix(0, 1101e6)
	assert.Nil(t, c.AdmitAt(ctx, maxPriority.dec(), t1101))
}

func ExampleController() {
	var overloaded atomic.Value
	overloaded.Store(false)
	c := NewController("test",
		func(Priority) bool { return overloaded.Load().(bool) },
		100*time.Millisecond, 1000, .05, .01)
	fmt.Println("The controller begins at the lowest level.")
	fmt.Println(c)
	t100 := time.Unix(0, 100e6)
	ctx := context.Background()
	p0 := Priority{DefaultLevel, 0}
	fmt.Println()
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", p0, t100.Format("0.0"), c.AdmitAt(ctx, p0, t100))
	p127 := Priority{DefaultLevel, shardFromBucket(127)}
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", p127, t100.Format("0.0"), c.AdmitAt(ctx, p127, t100))
	fmt.Println("The controller records the two successful admissions.")
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Set overloaded true and attempt to admit at (def,0)@0.6 in the next interval.")
	overloaded.Store(true)
	t200 := time.Unix(0, 200e6)
	fmt.Printf("AdmitAt(%v, %s) will block.\n", p0, t200.Format("0.0"))
	ctxToCancel, cancel := context.WithCancel(ctx)
	errCh := make(chan error)
	go func() {
		errCh <- c.AdmitAt(ctxToCancel, p0, t200)
	}()
	for c.AdmissionLevel() == minPriority {
		time.Sleep(time.Millisecond)
	}
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Cancel the request at", p0, "so it is no longer blocked.")
	cancel()
	<-errCh
	fmt.Println(c)
	pMin := Priority{DefaultLevel, shardFromBucket(1)}
	pMax := Priority{DefaultLevel, shardFromBucket(100)}
	const reqsToAdd = 1000
	const reqsPerPriority = reqsToAdd / 100
	fmt.Println("Add", reqsToAdd, "requests uniformly between", pMin, "and", pMax, "to test adjustment")
	for p := pMin; !pMax.less(p); p = p.inc() {
		for i := 0; i < reqsPerPriority; i++ {
			if err := c.AdmitAt(ctx, p, t200); err != nil {
				fmt.Println("Got an error", err)
			}
		}
	}
	fmt.Println("After adding all of the requests the controller state looks like this:")
	fmt.Println(c)
	fmt.Println("Now add another request above the admission level in the next interval leading to a tick.")
	t300 := time.Unix(0, 300e6)
	if err := c.AdmitAt(ctx, p127, t300); err != nil {
		fmt.Println("Got an error", err)
	}
	fmt.Println("Notice that the admission level has risen to def:6.")
	fmt.Println(c)
	fmt.Println("Now, once again add the same load as before and wait until all requests are either admitted or blocked.")
	fmt.Println("9951 should be admitted and 50 should be blocked.")
	overloaded.Store(false)
	for p := pMin; !pMax.less(p); p = p.inc() {
		for i := 0; i < reqsPerPriority; i++ {
			if !p.less(c.AdmissionLevel()) {
				c.AdmitAt(ctx, p, t300)
			} else {
				go c.AdmitAt(ctx, p, t300)
			}
		}
	}
	for c.numBlocked() < 50 {
		time.Sleep(time.Millisecond)
	}
	fmt.Println()
	fmt.Println("Now a new request comes in at 0.4 leading to a tick which does not move the admission level")
	t400 := time.Unix(0, 400e6)
	c.AdmitAt(ctx, p127, t400)
	for c.numBlocked() < 50 || c.numReqs() < 1 {
		time.Sleep(time.Millisecond)
	}
	fmt.Println("Now another request at  comes in at 0.5 leading to a tick moving the admission level to def:5.")
	t500 := time.Unix(0, 500e6)
	c.AdmitAt(ctx, p127, t500)
	for c.numReqs() < 11 {
		time.Sleep(time.Millisecond)
	}
	fmt.Println(c)
	fmt.Println(c.numBlocked())
	fmt.Println("Add more requests than can fit in the waitQueue")
	pMin.Level, pMax.Level = MinLevel, MinLevel

	for p := pMax; !p.less(pMin); p = p.dec() {
		for i := 0; i < reqsPerPriority; i++ {
			go func(p Priority) { errCh <- c.AdmitAt(ctx, p, t500) }(p)
		}
	}
	for i := 0; i < 40; i++ {
		if err := <-errCh; err != ErrRejected {
			fmt.Println("Got an unexpected", err)
		}
	}
	time.Sleep(time.Second)
	fmt.Println(c.numBlocked(), c)
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

func (c *Controller) numReqs() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int(atomic.LoadInt64(&c.mu.numReqs))
}

func (c *Controller) numBlocked() (blocked int) {
	c.wq.mu.Lock()
	defer c.wq.mu.Unlock()
	for lb := 0; lb < numLevels; lb++ {
		for sb := 0; sb < numShards; sb++ {
			blocked += c.wq.q[lb][sb].len()
		}
	}
	return blocked
}
