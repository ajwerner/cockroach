// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tskeys

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// Resolution is used to enumerate the different resolution values supported by
// Cockroach.
type Resolution int64

func (r Resolution) String() string {
	switch r {
	case Resolution10s:
		return "10s"
	case Resolution30m:
		return "30m"
	case TestingResolution1ns:
		return "1ns"
	case TestingResolution50ns:
		return "50ns"
	case TestingResolutionInvalid:
		return "BAD"
	}
	return fmt.Sprintf("%d", r)
}

// Resolution enumeration values are directly serialized and persisted into
// system keys; these values must never be altered or reordered. If new rollup
// resolutions are added, the IsRollup() method must be modified as well.
const (
	// Resolution10s stores data with a sample resolution of 10 seconds.
	Resolution10s Resolution = 1
	// Resolution30m stores roll-up data from a higher resolution at a sample
	// resolution of 30 minutes.
	Resolution30m Resolution = 2
	// TestingResolution1ns stores data with a sample resolution of 1 nanosecond.
	// Used only for testing.
	TestingResolution1ns Resolution = 998
	// TestingResolution50ns stores roll-up data from the 1ns resolution at a
	// sample resolution of 50 nanoseconds. Used for testing.
	TestingResolution50ns Resolution = 999
	// TestingResolutionInvalid is an invalid resolution used only for testing. It
	// causes an error to be thrown in certain methods. It is invalid because its
	// sample period is not a divisor of its slab period.
	TestingResolutionInvalid Resolution = 1000
)

// sampleDurationByResolution is a map used to retrieve the sample duration
// corresponding to a Resolution value. Sample durations are expressed in
// nanoseconds.
var sampleDurationByResolution = map[Resolution]int64{
	Resolution10s:            int64(time.Second * 10),
	Resolution30m:            int64(time.Minute * 30),
	TestingResolution1ns:     1,  // 1ns resolution only for tests.
	TestingResolution50ns:    50, // 50ns rollup only for tests.
	TestingResolutionInvalid: 10, // Invalid resolution.
}

// slabDurationByResolution is a map used to retrieve the slab duration
// corresponding to a Resolution value; the slab duration determines how many
// samples are stored at a single Cockroach key/value. Slab durations are
// expressed in nanoseconds.
var slabDurationByResolution = map[Resolution]int64{
	Resolution10s:            int64(time.Hour),
	Resolution30m:            int64(time.Hour * 24),
	TestingResolution1ns:     10,   // 1ns resolution only for tests.
	TestingResolution50ns:    1000, // 50ns rollup only for tests.
	TestingResolutionInvalid: 11,
}

// SampleDuration returns the sample duration corresponding to this resolution
// value, expressed in nanoseconds.
func (r Resolution) SampleDuration() int64 {
	duration, ok := sampleDurationByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no sample duration found for resolution value %v", r))
	}
	return duration
}

// SlabDuration returns the slab duration corresponding to this resolution
// value, expressed in nanoseconds. The slab duration determines how many
// consecutive samples are stored in a single Cockroach key/value.
func (r Resolution) SlabDuration() int64 {
	duration, ok := slabDurationByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no slab duration found for resolution value %v", r))
	}
	return duration
}

// IsRollup returns true if this resolution contains rollup data: statistical
// values about a large number of samples taken over a long period, such as
// the min, max and sum.
func (r Resolution) IsRollup() bool {
	return r == Resolution30m || r == TestingResolution50ns
}

// TargetRollupResolution returns a target resolution that data from this
// resolution should be rolled up into in lieu of deletion. For example,
// Resolution10s has a target rollup resolution of Resolution30m.
func (r Resolution) TargetRollupResolution() (Resolution, bool) {
	switch r {
	case Resolution10s:
		return Resolution30m, true
	case TestingResolution1ns:
		return TestingResolution50ns, true
	}
	return r, false
}

func NormalizeToPeriod(timestampNanos int64, period int64) int64 {
	return timestampNanos - timestampNanos%period
}

func (r Resolution) NormalizeToSlab(timestampNanos int64) int64 {
	return NormalizeToPeriod(timestampNanos, r.SlabDuration())
}

// ResolutionFromProto translates the resolution enum value from time series
// proto to ts.Resolution.
func ResolutionFromProto(r tspb.TimeSeriesResolution) Resolution {
	switch r {
	case tspb.TimeSeriesResolution_RESOLUTION_10S:
		return Resolution10s
	case tspb.TimeSeriesResolution_RESOLUTION_30M:
		return Resolution30m
	default:
	}
	return TestingResolutionInvalid
}
