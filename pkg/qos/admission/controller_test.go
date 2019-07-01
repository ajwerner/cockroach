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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/qos"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

var testConfig = Config{
	Name:         "test",
	TickInterval: time.Second,
	MaxBlocked:   1000,
	GrowRate:     .01,
	PruneRate:    .05,
}

var (
	t0 = time.Unix(0, 0)
	t1 = time.Unix(1, 0)
	t2 = time.Unix(2, 0)
	t3 = time.Unix(3, 0)
	t4 = time.Unix(4, 0)
)

var (
	lh0   = qos.Level{qos.ClassHigh, 0}
	lh127 = qos.Level{qos.ClassHigh, 127}

	ld0   = qos.Level{qos.ClassDefault, 0}
	ld1   = qos.Level{qos.ClassDefault, 1}
	ld10  = qos.Level{qos.ClassDefault, 10}
	ld127 = qos.Level{qos.ClassDefault, 127}

	ll0   = qos.Level{qos.ClassLow, 0}
	ll1   = qos.Level{qos.ClassLow, 1}
	ll10  = qos.Level{qos.ClassLow, 10}
	ll127 = qos.Level{qos.ClassLow, 127}
)

func TestAdmissionController(t *testing.T) {
	var overloaded atomic.Value
	overloaded.Store(false)
	ctx := context.Background()
	cfg := testConfig
	cfg.OverloadSignal = func(qos.Level) (bool, qos.Level) {
		return overloaded.Load().(bool), lh127
	}
	c := NewController(ctx, nil, cfg)
	assert.Equal(t, minLevel, c.AdmissionLevel())
	assert.Nil(t, c.AdmitAt(ctx, ld0, t1))
	assert.Nil(t, c.AdmitAt(ctx, ld127, t1))
	overloaded.Store(true)
	errCh := make(chan error)
	assertBlocks := func(ctx context.Context, l qos.Level, ts time.Time) {
		go func() { errCh <- c.AdmitAt(ctx, l, ts) }()
		const waitForError = 5 * time.Millisecond
		select {
		case err := <-errCh:
			t.Fatalf("expected the request to block and not to recieve an error, got %v", err)
		case <-time.After(waitForError):
		}
	}
	assertBlocks(ctx, ld0, t2)
	assert.Nil(t, c.AdmitAt(ctx, lh0, t2))
	assert.Equal(t, ld1, c.AdmissionLevel())
	overloaded.Store(false)
	// Ensure that the maximum admission level never gets blocked.
	assert.Nil(t, c.AdmitAt(ctx, lh127, t2))
	// Admit again and ensure that the admission level decreases, unblocking the
	// previously blocked goroutine.
	assert.Nil(t, c.AdmitAt(ctx, ld127, t3))
	assert.Nil(t, <-errCh)
	assert.Equal(t, ld0, c.AdmissionLevel())
}

func ExampleController_cancel() {
	var overloaded atomic.Value
	overloaded.Store(false)
	cfg := testConfig
	cfg.OverloadSignal = func(qos.Level) (bool, qos.Level) {
		return overloaded.Load().(bool), maxLevel
	}
	ctx := context.Background()
	c := NewController(ctx, nil, cfg)
	fmt.Println("The controller begins at the lowest level.")
	fmt.Println(c)
	t0 := time.Unix(0, 0)
	fmt.Println()
	fmt.Println("Three requests come in and succeed in the current interval.")
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", ld127, t0.Format("0.0"), c.AdmitAt(ctx, ld127, t0))
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", ld0, t0.Format("0.0"), c.AdmitAt(ctx, ld0, t0))
	fmt.Printf("AdmitAt(%v, %s) = %v.\n", ld1, t0.Format("0.0"), c.AdmitAt(ctx, ld1, t0))
	fmt.Println(c)
	fmt.Println()
	fmt.Println("The overload signal becomes true and time advances to 0.1.")
	overloaded.Store(true)
	t1 := time.Unix(1, 0)
	fmt.Printf("AdmitAt(%v, %s) will block.\n", ld0, t1.Format("0.0"))
	ctxToCancel, cancel := context.WithCancel(ctx)
	errCh := make(chan error)
	go func() { errCh <- c.AdmitAt(ctxToCancel, ld0, t1) }()
	waitForState(c, 0, 1)
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Cancel the request at", ld0, "so it is no longer blocked.")
	cancel()
	<-errCh
	fmt.Println("Observe that the request is no longer blocked.")
	fmt.Println(c)
	// Output:
	// The controller begins at the lowest level.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 0  |  0.00|  0.00|➡ 0.00|
	// int: 0; ad: 0; bl: 0; rej: 0
	//
	// Three requests come in and succeed in the current interval.
	// AdmitAt(d:127, 0.0) = <nil>.
	// AdmitAt(d:0, 0.0) = <nil>.
	// AdmitAt(d:1, 0.0) = <nil>.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  1.00|  0.00|
	// ...
	// 1  |  0.00|  1.00|  0.00|
	// 0  |  0.00|  1.00|➡ 0.00|
	// int: 1; ad: 3; bl: 0; rej: 0
	//
	// The overload signal becomes true and time advances to 0.1.
	// AdmitAt(d:0, 0.0) will block.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 1  |  0.00|➡ 0.00|  0.00|
	// 0  |  0.00|  1.00|  0.00|
	// int: 2; ad: 0; bl: 1; rej: 0
	//
	// Cancel the request at d:0 so it is no longer blocked.
	// Observe that the request is no longer blocked.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 1  |  0.00|➡ 0.00|  0.00|
	// 0  |  0.00|  0.00|  0.00|
	// int: 2; ad: 0; bl: 0; rej: 0
}

func ExampleController_waiting() {
	var overloaded atomic.Value
	overloaded.Store(false)
	cfg := testConfig
	cfg.MaxReqsPerInterval = 1000000
	cfg.OverloadSignal = func(qos.Level) (bool, qos.Level) {
		return overloaded.Load().(bool), maxLevel
	}
	ctx := context.Background()
	c := NewController(ctx, nil, cfg)
	const reqsToAdd = 1000
	const reqsPerLevel = reqsToAdd / 10
	fmt.Println("Admit", reqsToAdd, "requests uniformly between", ld1, "and", ld10, " at t1.")
	for l := ld1; !ld10.Less(l); l = l.Inc() {
		for i := 0; i < reqsPerLevel; i++ {
			if err := c.AdmitAt(ctx, l, t1); err != nil {
				fmt.Println("Got an error", err)
			}
		}
	}
	fmt.Println("After adding all of the requests the controller state looks like this:")
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Set the overload signal to true and Admit a request at t2, leading to a tick.")
	overloaded.Store(true)
	fmt.Printf("AdmitAt(%v, t%d) = %v.\n", ld127, t0.Unix(), c.AdmitAt(ctx, ld127, t0))
	if err := c.AdmitAt(ctx, ld127, t2); err != nil {
		fmt.Println("Got an error", err)
	}
	fmt.Println("Notice that the admission level has risen to", c.AdmissionLevel())
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Attempt to admit", reqsToAdd, "requests uniformly between", ld1, "and", ld10, "at t2.")
	fmt.Println("901 should be admitted and 100 should be blocked.")
	for l := ld1; !ld10.Less(l); l = l.Inc() {
		for i := 0; i < reqsPerLevel; i++ {
			if !l.Less(c.AdmissionLevel()) {
				c.AdmitAt(ctx, l, t2)
			} else {
				go c.AdmitAt(ctx, l, t2)
			}
		}
	}
	waitForState(c, 901, 100)
	fmt.Println(c)
	fmt.Println()
	fmt.Println("Set the overload signal to false.")
	overloaded.Store(false)
	fmt.Println("Admit a request at t3 leading to a tick moves the admission level down to def:1.")
	c.AdmitAt(ctx, ld127, t3)
	waitForState(c, 101, 0)
	fmt.Println(c)
	fmt.Println("Also in t3 add more requests than can fit in the waitQueue (1000).")
	fmt.Println("This is done by adding", reqsPerLevel-1, "requests to", ld0,
		"and", reqsPerLevel, "uniformly from", ll10, "to", ll1, ".")
	fmt.Println("This will lead to requests at", ll1, "being rejected.")
	// NB: The requests are added in decreasing order to ensure that the rejection
	// level rises deterministically.
	expectedNumBlocked := int(c.numBlocked())
	errCh := make(chan error)
	addAtLevel := func(n int, p qos.Level) {
		for i := 0; i < n; i++ {
			go func() { errCh <- c.AdmitAt(ctx, p, t3) }()
			expectedNumBlocked++
		}
		if expectedNumBlocked >= int(cfg.MaxBlocked) {
			expectedNumBlocked -= n
		}
		waitForState(c, 0, expectedNumBlocked)
	}
	addAtLevel(reqsPerLevel-1, ld0)
	for l := ll10; ll0.Less(l); l = l.Dec() {
		addAtLevel(reqsPerLevel, l)
	}
	for i := 0; i < 100; i++ {
		if err := <-errCh; err != ErrRejected {
			fmt.Println("Got an unexpected", err)
		}
	}
	fmt.Println(c)
	// Output:
	// Admit 1000 requests uniformly between d:1 and d:10  at t1.
	// After adding all of the requests the controller state looks like this:
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  0.00|  0.00|
	// ...
	// 10 |  0.00|  100 |  0.00|
	// 9  |  0.00|  100 |  0.00|
	// 8  |  0.00|  100 |  0.00|
	// 7  |  0.00|  100 |  0.00|
	// 6  |  0.00|  100 |  0.00|
	// 5  |  0.00|  100 |  0.00|
	// 4  |  0.00|  100 |  0.00|
	// 3  |  0.00|  100 |  0.00|
	// 2  |  0.00|  100 |  0.00|
	// 1  |  0.00|  100 |  0.00|
	// 0  |  0.00|  0.00|➡ 0.00|
	// int: 1; ad: 1000; bl: 0; rej: 0
	//
	// Set the overload signal to true and Admit a request at t2, leading to a tick.
	// AdmitAt(d:127, t0) = <nil>.
	// Notice that the admission level has risen to d:2
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  1.00|  0.00|
	// ...
	// 2  |  0.00|➡ 0.00|  0.00|
	// ...
	// 0  |  0.00|  0.00|  0.00|
	// int: 2; ad: 1; bl: 0; rej: 0
	//
	// Attempt to admit 1000 requests uniformly between d:1 and d:10 at t2.
	// 901 should be admitted and 100 should be blocked.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  1.00|  0.00|
	// ...
	// 10 |  0.00|  100 |  0.00|
	// 9  |  0.00|  100 |  0.00|
	// 8  |  0.00|  100 |  0.00|
	// 7  |  0.00|  100 |  0.00|
	// 6  |  0.00|  100 |  0.00|
	// 5  |  0.00|  100 |  0.00|
	// 4  |  0.00|  100 |  0.00|
	// 3  |  0.00|  100 |  0.00|
	// 2  |  0.00|➡ 100 |  0.00|
	// 1  |  0.00|  100 |  0.00|
	// 0  |  0.00|  0.00|  0.00|
	// int: 2; ad: 901; bl: 100; rej: 0
	//
	// Set the overload signal to false.
	// Admit a request at t3 leading to a tick moves the admission level down to def:1.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  1.00|  0.00|
	// ...
	// 1  |  0.00|➡ 100 |  0.00|
	// 0  |  0.00|  0.00|  0.00|
	// int: 3; ad: 101; bl: 0; rej: 0
	// Also in t3 add more requests than can fit in the waitQueue (1000).
	// This is done by adding 99 requests to d:0 and 100 uniformly from l:10 to l:1 .
	// This will lead to requests at l:1 being rejected.
	// ---|  h   |  d   |  l   |
	// 127|  0.00|  1.00|  0.00|
	// ...
	// 10 |  0.00|  0.00|  100 |
	// 9  |  0.00|  0.00|  100 |
	// 8  |  0.00|  0.00|  100 |
	// 7  |  0.00|  0.00|  100 |
	// 6  |  0.00|  0.00|  100 |
	// 5  |  0.00|  0.00|  100 |
	// 4  |  0.00|  0.00|  100 |
	// 3  |  0.00|  0.00|  100 |
	// 2  |  0.00|  0.00|  100 |
	// 1  |  0.00|➡ 100 |⇨ 100 |
	// 0  |  0.00|  99.0|  0.00|
	// int: 3; ad: 101; bl: 999; rej: 100
}

func waitForNotAdmissionLevel(c *Controller, l qos.Level) {
	for c.AdmissionLevel() == l {
		time.Sleep(time.Millisecond)
	}
}

func waitForState(c *Controller, expNumAdmitted, expNumBlocked int) {
	for {
		curNumReqs := c.numAdmitted()
		curNumBlocked := c.numBlocked()
		if (expNumAdmitted <= 0 || curNumReqs >= expNumAdmitted) &&
			(expNumBlocked <= 0 || curNumBlocked >= expNumBlocked) {
			return
		}
		if log.V(1) {
			log.InfofDepth(context.TODO(), 1, "waiting for numAdmitted=%d (cur %d) and numBlocked=%d (cur %d)",
				expNumAdmitted, curNumReqs, expNumBlocked, curNumBlocked)
		}
		time.Sleep(time.Millisecond)
	}
}

func (c *Controller) numAdmitted() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int(atomic.LoadInt64(&c.mu.numAdmitted))
}

func (c *Controller) numBlocked() (blocked int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for cl := 0; cl < qos.NumClasses; cl++ {
		for s := 0; s < qos.NumShards; s++ {
			blocked += int(c.mu.wq[cl][s].len())
		}
	}
	return blocked
}
