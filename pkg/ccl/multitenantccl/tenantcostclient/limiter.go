// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// limiter is used to rate-limit KV requests according to a local token bucket.
//
// The Wait() method is called when a KV request requires RUs. The other methods
// are used to adjust/reconfigure/replenish the local token bucket.
type limiter struct {
	timeSource timeutil.TimeSource
	tb         tokenBucket
	qp         *quotapool.AbstractPool
}

// Initial settings for the local token bucket. They are used only until the
// first TokenBucket request returns. We allow immediate use of the initial RUs
// (we essentially borrow them and pay them back in the first TokenBucket
// request). The intention is to avoid any throttling during start-up in normal
// circumstances.
const initialRUs = 10000
const initialRate = 100

func (l *limiter) Init(timeSource timeutil.TimeSource, notifyChan chan struct{}) {
	*l = limiter{
		timeSource: timeSource,
	}

	l.tb.Init(timeSource.Now(), notifyChan, initialRate, initialRUs)

	// We use OnWaitStartLocked because otherwise we have a race between the token
	// bucket noticing that it can't fulfill a request, and AvailableTokens()
	// accounting for the RUs that are waiting.
	//
	// We have a similar problem on finish, but the consequences of overcounting
	// waiting RUs are not very problematic.
	l.qp = quotapool.New(
		"tenant-side-limiter", l,
		quotapool.WithTimeSource(timeSource),
	)
}

func (l *limiter) Close() {
	l.qp.Close("shutting down")
}

// Wait removes the needed RUs from the bucket, waiting as necessary until it is
// possible.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU) error {
	r := newWaitRequest(needed)
	defer putWaitRequest(r)

	return l.qp.Acquire(ctx, r)
}

// AdjustTokens adds or removes tokens from the bucket. Tokens are added when we
// receive more tokens from the host cluster. Tokens are removed when
// consumption has occurred without Wait(): accounting for CPU usage and the
// number of read bytes.
func (l *limiter) AdjustTokens(now time.Time, delta tenantcostmodel.RU) {
	if delta == 0 {
		return
	}
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		l.tb.AdjustTokens(now, delta)
		// We notify the head of the queue if we added RUs, in which case that
		// request might be allowed to go through earlier.
		return delta > 0
	})
}

// Reconfigure is used to call tokenBucket.Reconfigure under the pool's lock.
func (l *limiter) Reconfigure(now time.Time, args tokenBucketReconfigureArgs) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.tb.Reconfigure(now, args)
		// Notify the head of the queue; the new configuration might allow that
		// request to go through earlier.
		return true
	})
}

// AvailableTokens returns the current number of available RUs. This can be
// negative if we accumulated debt or we have waiting requests.
func (l *limiter) AvailableTokens(now time.Time) tenantcostmodel.RU {
	var result tenantcostmodel.RU
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = l.tb.AvailableTokens(now)
		return false
	})
	return result
}

// SetupNotification is used to call tokenBucket.SetupNotification under the
// pool's lock.
func (l *limiter) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.tb.SetupNotification(now, threshold)
		// We return true so that if there is a request waiting, TryToFulfill gets
		// called again which may produce a notification.
		return true
	})
}

// waitRequest is used to wait for adequate resources in the tokenBucket.
type waitRequest struct {
	needed tenantcostmodel.RU
}

var _ quotapool.OnCancelRequest = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(needed tenantcostmodel.RU) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{needed: needed}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

// Acquire is part of quotapool.Request.
func (req *waitRequest) Acquire(
	ctx context.Context, r quotapool.Resource, waited bool,
) (fulfilled bool, tryAgainAfter time.Duration) {
	l := r.(*limiter)
	now := l.timeSource.Now()
	return l.tb.TryToFulfill(now, req.needed, !waited)
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}

// OnCancel is part of quotapool.OnCancelRequest.
func (req *waitRequest) OnCancel(ctx context.Context, r quotapool.Resource) {
	r.(*limiter).tb.waiting -= req.needed
}
