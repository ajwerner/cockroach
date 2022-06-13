// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bootstrap

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

var staticSplits = []roachpb.RKey{
	roachpb.RKey(keys.NodeLivenessPrefix),           // end of meta records / start of node liveness span
	roachpb.RKey(keys.NodeLivenessKeyMax),           // end of node liveness span
	roachpb.RKey(keys.TimeseriesPrefix),             // start of timeseries span
	roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), // end of timeseries span
	roachpb.RKey(keys.TableDataMin),                 // end of system ranges / start of system config tables
}

// StaticSplits are predefined split points in the system keyspace.
// Corresponding ranges are created at cluster bootstrap time.
//
// There are two reasons for a static split. First, spans that are critical to
// cluster stability, like the node liveness span, are split into their own
// ranges to ease debugging (see #17297). Second, spans in the system keyspace
// that can be targeted by zone configs, like the meta span and the timeseries
// span, are split off into their own ranges because zone configs cannot apply
// to fractions of a range.
//
// Note that these are not the only splits created at cluster bootstrap; splits
// between various system tables are also created.
func StaticSplits() []roachpb.RKey {
	return staticSplits
}
