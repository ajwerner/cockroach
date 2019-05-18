// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package readquota

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaAcquisitions = metric.Metadata{
		Name:        "readquota.acquisitions",
		Help:        "Counter of read quota acquisitions",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaTimeSpentWaitingRate10s = metric.Metadata{
		Name:        "readquota.waiting.rate_10s",
		Help:        "The rate at which requests wait in the quota pool to acquire quota",
		Measurement: "Nanoseconds/second",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaTimeSpentWaitingSummary10s = metric.Metadata{
		Name:        "readquota.waiting.summary_10s",
		Help:        "The distribution of time requests wait in the quota pool to acquire quota",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaRequiredTook = metric.Metadata{
		Name:        "readquota.required_took",
		Help:        "Counter of nanoseconds spent determining how much to read",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// Metrics is a metrics struct for the read quota.
type Metrics struct {
	TimeSpentWaitingRate10s    *metric.Rate
	TimeSpentWaitingSummary10s *metric.Summary
	Acquisitions               *metric.Counter
	RequiredTook               *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		TimeSpentWaitingRate10s:    metric.NewRate(metaTimeSpentWaitingRate10s, 10*time.Second),
		TimeSpentWaitingSummary10s: metric.NewSummary(metaTimeSpentWaitingSummary10s, 10*time.Second),
		Acquisitions:               metric.NewCounter(metaAcquisitions),
		RequiredTook:               metric.NewCounter(metaRequiredTook),
	}
}
