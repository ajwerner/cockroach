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

package quotapool

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Option is used to configure a QuotaPool.
type Option interface {
	apply(*config)
}

// OnSlowAcquisition creates an Option to configure a callback upon slow
// acquisitions. Only one OnSlowAcquisition may be used. If multiple are
// specified only the last will be used.
func OnSlowAcquisition(threshold time.Duration, f SlowAcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.slowAcquisitionThreshold = threshold
		cfg.onSlowAcquisition = f
	})
}

// LogSlowAcquisition is an Option to log slow acquisitions.
var LogSlowAcquisition = OnSlowAcquisition(base.SlowRequestThreshold, logSlowAcquire)

func logSlowAcquire(ctx context.Context, poolName string, r Request, start time.Time) func() {
	log.Warningf(ctx, "have been waiting %s attempting to acquire %s quota",
		timeutil.Since(start), poolName)
	return func() {
		log.Infof(ctx, "acquired %s quota after %s",
			poolName, timeutil.Since(start))
	}
}

// SlowAcquisitionFunc is used to configure a quotapool to call a function when
// quota acquisition is slow. The returned callback is called when the
// acquisition occurs.
type SlowAcquisitionFunc func(
	ctx context.Context, poolName string, r Request, start time.Time,
) (onAcquire func())

// AcquisitionFunc is used to configure a quotapool to call a funciton after
// an acquisition has occurred.
type AcquisitionFunc func(
	ctx context.Context, poolName string, r Request, start time.Time,
)

// OnAcquisition creates an Option to configure a callback upon acquisition.
// It is often useful for recording metrics.
func OnAcquisition(f AcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.onAcquisition = f
	})
}

type optionFunc func(cfg *config)

func (f optionFunc) apply(cfg *config) { f(cfg) }

type config struct {
	name                     string
	onAcquisition            AcquisitionFunc
	onSlowAcquisition        SlowAcquisitionFunc
	slowAcquisitionThreshold time.Duration
}
